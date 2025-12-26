"""
Main execution logic for Diaz Diff Checker.

This module contains the primary run functions that orchestrate:
- Local file comparison
- Folder batch processing
- URL-based fetch and compare (full implementation)
"""

import asyncio
import csv
import gc
import gzip
import hashlib
import json
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qsl

import aiohttp

from .csv_reader import StreamingCSVReader
from .differ import EfficientDiffer, calculate_in_stock_percentage
from .progress import ProgressDisplay
from .utils import (
    parse_url_params_to_json,
    generate_run_folder_name,
    save_run_metadata,
    create_summary_structure,
    extract_dedup_key,
)


async def run_local_diff(
    prod_file: str,
    dev_file: str,
    differ: EfficientDiffer,
    summary_dir: str,
    diff_rows: Optional[int] = None,
) -> None:
    """
    Compare two local CSV files.
    
    Args:
        prod_file: Path to production file
        dev_file: Path to development file
        differ: Configured EfficientDiffer instance
        summary_dir: Directory to save summary
        diff_rows: Optional row limit
    """
    logging.info(f"Comparing local files:\n  Prod: {prod_file}\n  Dev: {dev_file}")
    
    os.makedirs(summary_dir, exist_ok=True)
    
    try:
        diff_start_time = datetime.now()
        diff_stats = differ.compute_diff(prod_file, dev_file)
        
        summary_obj: OrderedDict[str, Any] = OrderedDict()
        summary_obj["mode"] = "local"
        summary_obj["prod_file"] = os.path.basename(prod_file)
        summary_obj["dev_file"] = os.path.basename(dev_file)
        summary_obj.update(diff_stats)
        
        # Calculate in-stock percentages
        prod_reader = StreamingCSVReader(prod_file, max_rows=diff_rows)
        if 'availability' in prod_reader.read_headers():
            summary_obj["prod_in_stock_percentage"] = calculate_in_stock_percentage(
                prod_file, diff_rows
            )
        
        dev_reader = StreamingCSVReader(dev_file, max_rows=diff_rows)
        if 'availability' in dev_reader.read_headers():
            summary_obj["dev_in_stock_percentage"] = calculate_in_stock_percentage(
                dev_file, diff_rows
            )
        
        if "prod_in_stock_percentage" in summary_obj and "dev_in_stock_percentage" in summary_obj:
            summary_obj["in_stock_percentage_difference"] = round(
                abs(summary_obj["prod_in_stock_percentage"] - summary_obj["dev_in_stock_percentage"]), 
                2
            )
        
        diff_duration = (datetime.now() - diff_start_time).total_seconds()
        summary_obj["runtime_seconds"] = round(diff_duration, 2)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_filename = os.path.join(summary_dir, f"diffs_summary_local_{timestamp}.json")
        with open(summary_filename, "w") as f:
            json.dump(summary_obj, f, indent=2)
        
        logging.info(f"Local diff summary written to {summary_filename}")
        logging.info(f"Runtime: {diff_duration:.2f}s")
        
        # Print summary to console
        logging.info(f"\n{'='*60}")
        logging.info(f"Results:")
        logging.info(f"  Rows added:   {diff_stats.get('rows_added', 0)}")
        logging.info(f"  Rows removed: {diff_stats.get('rows_removed', 0)}")
        logging.info(f"  Rows updated: {diff_stats.get('rows_updated', 0)}")
        logging.info(f"{'='*60}")
        
    except Exception as e:
        logging.error(f"Error comparing local files: {e}")
        raise


async def run_folder_diff(
    folder_path: str,
    differ: EfficientDiffer,
    summary_dir: str,
    max_concurrent_diffs: int = 10,
    diff_rows: Optional[int] = None,
) -> None:
    """
    Batch process all prod/dev file pairs in a folder.
    
    Args:
        folder_path: Directory containing response files
        differ: Configured EfficientDiffer instance
        summary_dir: Directory to save summary
        max_concurrent_diffs: Maximum parallel diffs
        diff_rows: Optional row limit
    """
    logging.info(f"Running folder diff mode on: {folder_path}")
    
    os.makedirs(summary_dir, exist_ok=True)
    run_start_time = datetime.now()
    
    # Find file pairs
    pattern = re.compile(r"^(prod|dev)_response_(\d+)_(\w+)\.txt$")
    files = os.listdir(folder_path)
    
    groups: Dict[str, Dict[str, str]] = {}
    for filename in files:
        match = pattern.match(filename)
        if match:
            env = match.group(1)
            test_case = match.group(2)
            file_hash = match.group(3)
            key = f"{test_case}_{file_hash}"
            
            if key not in groups:
                groups[key] = {}
            groups[key][env] = os.path.join(folder_path, filename)
    
    if not groups:
        logging.error("No matching file pairs found in folder")
        return
    
    logging.info(f"Found {len(groups)} file pairs to process")
    
    async def process_folder_diff(key: str, env_files: Dict[str, str]) -> Dict[str, Any]:
        """Process a single file pair."""
        test_case = key.split("_")[0]
        diff_start_time = datetime.now()
        
        test_summary: Dict[str, Any] = {"test_case": test_case}
        
        if "prod" not in env_files or "dev" not in env_files:
            test_summary["error"] = {"msg": "Missing prod or dev file"}
            test_summary["non_200"] = True
            return test_summary
        
        try:
            diff_stats = await asyncio.to_thread(
                differ.compute_diff, env_files["prod"], env_files["dev"]
            )
            test_summary.update(diff_stats)
            
            # Calculate in-stock percentages
            async def calc_in_stock(file_path):
                reader = StreamingCSVReader(file_path, max_rows=diff_rows)
                if 'availability' in reader.read_headers():
                    return await asyncio.to_thread(
                        calculate_in_stock_percentage, file_path, diff_rows
                    )
                return None
            
            prod_in_stock, dev_in_stock = await asyncio.gather(
                calc_in_stock(env_files["prod"]),
                calc_in_stock(env_files["dev"])
            )
            
            if prod_in_stock is not None:
                test_summary["prod_in_stock_percentage"] = prod_in_stock
            if dev_in_stock is not None:
                test_summary["dev_in_stock_percentage"] = dev_in_stock
            
            if prod_in_stock is not None and dev_in_stock is not None:
                test_summary["in_stock_percentage_difference"] = round(
                    abs(prod_in_stock - dev_in_stock), 2
                )
            
            diff_duration = (datetime.now() - diff_start_time).total_seconds()
            test_summary["runtime_seconds"] = round(diff_duration, 2)
            
            logging.info(f"  [Test {test_case}] ✓ Diff completed in {diff_duration:.2f}s")
            
        except Exception as e:
            logging.error(f"  [Test {test_case}] ✗ Error: {e}")
            test_summary["error"] = {"msg": str(e)}
            test_summary["non_200"] = True
            diff_duration = (datetime.now() - diff_start_time).total_seconds()
            test_summary["runtime_seconds"] = round(diff_duration, 2)
        
        return test_summary
    
    # Process with semaphore
    semaphore = asyncio.Semaphore(max_concurrent_diffs)
    
    async def bounded_diff(key, env_files):
        async with semaphore:
            return await process_folder_diff(key, env_files)
    
    tasks = [bounded_diff(key, env_files) for key, env_files in groups.items()]
    results = await asyncio.gather(*tasks)
    
    # Sort by test case
    results.sort(key=lambda x: int(x.get("test_case", 0)))
    
    total_runtime = (datetime.now() - run_start_time).total_seconds()
    
    overall_summary = create_summary_structure(
        count=len(results),
        runtime_seconds=total_runtime,
        test_cases=results,
    )
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_filename = os.path.join(summary_dir, f"folder_diffs_summary_{timestamp}.json")
    with open(summary_filename, "w") as f:
        json.dump(overall_summary, f, indent=2)
    
    logging.info(f"Summary written to {summary_filename}")
    logging.info(f"Total runtime: {total_runtime:.2f}s")


async def fetch_and_save(
    session,
    url: str,
    verify_ssl: bool,
    test_case: int,
    environment: str,
    output_dir: str,
    verbose: bool = False,
) -> Tuple[int, str, str, Any, str, Optional[str], Dict[str, Any]]:
    """
    Fetch URL and stream response to file.
    
    Returns:
        Tuple of (test_case, environment, file_path, status_code, response_text, shop_name, request_params)
    """
    if verbose:
        logging.info(f"[Test Case {test_case} - {environment.upper()}] Requesting URL: {url}")
    
    status_code = None
    shop_name = None
    
    # Parse URL
    parsed_url = urlparse(url)
    query_params = parse_qsl(parsed_url.query)
    request_params = parse_url_params_to_json(parsed_url.query)
    
    # Extract shop name
    for key, value in query_params:
        if key == "connection_info[shop_name]":
            shop_name = value
            break
    
    # Generate file name
    query_params.sort(key=lambda x: x[0])
    param_string = '&'.join(f"{k}={v}" for k, v in query_params)
    hash_value = hashlib.md5(param_string.encode('utf-8')).hexdigest()
    file_name = f"{environment}_response_{test_case}_{hash_value}.txt"
    file_path = os.path.join(output_dir, file_name)
    
    try:
        async with session.get(url, ssl=verify_ssl) as response:
            status_code = response.status
            
            # Stream to file
            with open(file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
            
            # Decompress if gzipped
            with open(file_path, 'rb') as f:
                header = f.read(2)
                f.seek(0)
                
                if header == b'\x1f\x8b':
                    content = f.read()
                    try:
                        decompressed = gzip.decompress(content)
                        with open(file_path, 'wb') as out_f:
                            out_f.write(decompressed)
                        if verbose:
                            logging.info(
                                f"[Test Case {test_case} - {environment.upper()}] "
                                f"Decompressed gzip file"
                            )
                    except Exception as e:
                        logging.error(
                            f"[Test Case {test_case} - {environment.upper()}] "
                            f"Error decompressing: {e}"
                        )
            
            if status_code != 200:
                logging.warning(
                    f"[Test Case {test_case} - {environment.upper()}] "
                    f"Non-200 response: {status_code}"
                )
            else:
                logging.debug(
                    f"[Test Case {test_case} - {environment.upper()}] "
                    f"Received response with status {status_code}"
                )
    
    except asyncio.TimeoutError:
        logging.error(f"[Test Case {test_case} - {environment.upper()}] Timeout")
        status_code = "timeout"
        with open(file_path, 'w') as f:
            f.write(f"TimeoutError: No response received from URL: {url}")
    
    except Exception as e:
        logging.error(f"[Test Case {test_case} - {environment.upper()}] Error: {str(e)}")
        status_code = "error"
        with open(file_path, 'w') as f:
            f.write(f"Error: {str(e)}")
    
    # Read error response text
    response_text = ""
    if status_code not in [200, "timeout", "error"]:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            response_text = f.read(1000)
    
    return (test_case, environment, file_path, status_code, response_text, shop_name, request_params)


async def run_url_mode(
    args,
    differ: EfficientDiffer,
) -> None:
    """
    Full URL mode: fetch from prod/dev URLs and compare.
    
    Args:
        args: Parsed command line arguments
        differ: Configured EfficientDiffer instance
    """
    run_start_time = datetime.now()
    
    # Read parameters file
    logging.info(f"Reading parameters from: {args.params_file}")
    
    try:
        with open(args.params_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            param_list = [row["params"] for row in reader if "params" in row]
    except Exception as e:
        logging.error(f"Error reading parameters file: {e}")
        return
    
    if not param_list:
        logging.error("No valid parameters found in params file")
        return
    
    # Deduplicate by unique identifier parameters
    dedup_keys = getattr(args, 'dedup_keys', None) or ["connection_info[store_hash]"]
    
    original_count = len(param_list)
    seen_identifiers: Set[str] = set()
    deduplicated_params = []
    duplicates_removed = 0
    
    for params in param_list:
        dedup_id = extract_dedup_key(params, dedup_keys)
        if dedup_id:
            if dedup_id in seen_identifiers:
                duplicates_removed += 1
                continue
            seen_identifiers.add(dedup_id)
        deduplicated_params.append(params)
    
    if duplicates_removed > 0:
        logging.info(
            f"Deduplicated: {original_count} → {len(deduplicated_params)} "
            f"test cases ({duplicates_removed} duplicates removed)"
        )
        param_list = deduplicated_params
    
    # Apply source limit if specified
    if args.source_limit and args.source_limit > 0:
        original_count = len(param_list)
        param_list = param_list[:args.source_limit]
        logging.info(
            f"Limiting to {len(param_list)} of {original_count} test cases "
            f"(--source-limit {args.source_limit})"
        )
    
    total_cases = len(param_list)
    total_tasks = total_cases * 2
    logging.info(f"Found {total_cases} test cases. Total URL calls: {total_tasks}")
    
    # Create output directories
    run_folder_name = generate_run_folder_name(
        args.params_file,
        primary_key=args.primary_key,
        timeout=args.timeout,
        max_examples=args.max_examples,
        diff_rows=args.diff_rows,
        source_limit=args.source_limit,
        verbose=args.verbose,
    )
    run_output_dir = os.path.join(args.output_dir, run_folder_name)
    
    logging.info(f"Creating run folder: {run_output_dir}")
    os.makedirs(run_output_dir, exist_ok=True)
    os.makedirs(args.summary_dir, exist_ok=True)
    
    # Save run metadata
    metadata_path = save_run_metadata(
        run_output_dir,
        params_file=args.params_file,
        primary_key=args.primary_key,
        timeout=args.timeout,
        max_examples=args.max_examples,
        max_concurrent_diffs=args.max_concurrent_diffs,
        max_concurrent_fetches=args.max_concurrent_fetches,
        diff_rows=args.diff_rows,
        source_limit=args.source_limit,
        verbose=args.verbose,
        output_dir=args.output_dir,
        summary_dir=args.summary_dir,
    )
    logging.info(f"Run metadata saved to {metadata_path}")
    
    # Log concurrency settings
    logging.info(f"Max concurrent fetches: {args.max_concurrent_fetches}")
    logging.info(f"Max concurrent diffs: {args.max_concurrent_diffs}")
    
    # Initialize progress display
    progress = ProgressDisplay(total_fetches=total_tasks, total_diffs=total_cases)
    progress.initial_draw()
    
    # Parallel processing state
    results: Dict[int, Dict] = {}  # test_case -> {env -> info}
    diff_results: Dict[int, Dict] = {}  # test_case -> summary
    pending_diffs: Set[int] = set()  # Test cases with diffs in progress
    
    fetch_semaphore = asyncio.Semaphore(args.max_concurrent_fetches)
    diff_semaphore = asyncio.Semaphore(args.max_concurrent_diffs)
    diff_tasks: List[asyncio.Task] = []
    
    # Get URLs from args
    prod_base_url = args.prod_url
    dev_base_url = args.dev_url
    
    async def process_diff(test_case: int, prod_info: Dict[str, Any], dev_info: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single diff - runs in thread pool for CPU-bound work."""
        async with diff_semaphore:
            progress.log(f"[Test {test_case}] Starting diff...")
            
            test_summary: Dict[str, Any] = {"test_case": test_case}
            test_summary["prod_status"] = prod_info.get("status")
            test_summary["dev_status"] = dev_info.get("status")
            
            shop_name = prod_info.get("shop_name") or dev_info.get("shop_name")
            if shop_name:
                test_summary["shop_name"] = shop_name
            
            # Include request parameters
            request_params = prod_info.get("request_params") or dev_info.get("request_params")
            if request_params:
                test_summary["request_params"] = request_params
            
            # Check for non-200 responses
            if prod_info.get("status") != 200 or dev_info.get("status") != 200:
                error_obj: Dict[str, Any] = {"msg": "Non-200 responses detected", "response": {}}
                if prod_info.get("status") != 200:
                    error_obj["response"]["prod"] = {
                        "status": prod_info.get("status"),
                        "output": prod_info.get("response_text", "")[:1000]
                    }
                if dev_info.get("status") != 200:
                    error_obj["response"]["dev"] = {
                        "status": dev_info.get("status"),
                        "output": dev_info.get("response_text", "")[:1000]
                    }
                test_summary["error"] = error_obj
                test_summary["non_200"] = True
                progress.increment_errors()
                return test_summary
            
            # Perform diff in thread pool
            try:
                start_time = datetime.now()
                
                diff_stats = await asyncio.to_thread(
                    differ.compute_diff, prod_info["file"], dev_info["file"]
                )
                
                diff_duration = (datetime.now() - start_time).total_seconds()
                test_summary.update(diff_stats)
                
                # Log diff results
                rows_changed = diff_stats.get("rows_updated", 0)
                rows_added = diff_stats.get("rows_added", 0)
                rows_removed = diff_stats.get("rows_removed", 0)
                
                if rows_changed > 0 or rows_added > 0 or rows_removed > 0:
                    progress.log(
                        f"[Test {test_case}] +{rows_added} added, "
                        f"-{rows_removed} removed, ~{rows_changed} changed"
                    )
                else:
                    progress.log(f"[Test {test_case}] No differences")
                
                # Calculate in-stock percentages
                async def calc_in_stock(file_path):
                    reader = StreamingCSVReader(file_path, max_rows=args.diff_rows)
                    if 'availability' in reader.read_headers():
                        return await asyncio.to_thread(
                            calculate_in_stock_percentage, file_path, args.diff_rows
                        )
                    return None
                
                prod_in_stock, dev_in_stock = await asyncio.gather(
                    calc_in_stock(prod_info["file"]),
                    calc_in_stock(dev_info["file"])
                )
                
                if prod_in_stock is not None:
                    test_summary["prod_in_stock_percentage"] = prod_in_stock
                if dev_in_stock is not None:
                    test_summary["dev_in_stock_percentage"] = dev_in_stock
                
                if prod_in_stock is not None and dev_in_stock is not None:
                    test_summary["in_stock_percentage_difference"] = round(
                        abs(prod_in_stock - dev_in_stock), 2
                    )
                
                # Add runtime
                total_test_duration = (datetime.now() - start_time).total_seconds()
                test_summary["runtime_seconds"] = round(total_test_duration, 2)
                
            except Exception as e:
                progress.log(f"[Test {test_case}] ✗ Error: {str(e)}")
                progress.increment_errors()
                test_summary["error"] = {"msg": str(e)}
                test_summary["non_200"] = True
                error_duration = (datetime.now() - start_time).total_seconds()
                test_summary["runtime_seconds"] = round(error_duration, 2)
            
            return test_summary
    
    async def on_diff_complete(task: asyncio.Task, test_case: int):
        """Callback when a diff task completes."""
        try:
            result = task.result()
            diff_results[test_case] = result
            progress.increment_diffs()
        except Exception as e:
            progress.log(f"[Test {test_case}] Diff task failed: {e}")
            progress.increment_errors()
            diff_results[test_case] = {
                "test_case": test_case,
                "error": {"msg": str(e)},
                "non_200": True
            }
            progress.increment_diffs()
        finally:
            pending_diffs.discard(test_case)
            gc.collect()
    
    def maybe_start_diff(test_case: int) -> Optional[asyncio.Task]:
        """Start a diff if both prod and dev are ready."""
        if test_case in pending_diffs or test_case in diff_results:
            return None
        
        if test_case in results and "prod" in results[test_case] and "dev" in results[test_case]:
            pending_diffs.add(test_case)
            prod_info = results[test_case]["prod"]
            dev_info = results[test_case]["dev"]
            
            # Clear results to free memory
            del results[test_case]
            
            # Create and schedule diff task
            task = asyncio.create_task(process_diff(test_case, prod_info, dev_info))
            task.add_done_callback(
                lambda t: asyncio.create_task(on_diff_complete(t, test_case))
            )
            return task
        return None
    
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    
    async def process_test_case(session, idx: int, params: str):
        """
        Process a single test case with staggered prod/dev fetches.
        
        Concurrency is controlled by fetch_semaphore.
        Within each test case, prod and dev are fetched sequentially
        to avoid bulk operation conflicts.
        Half start with prod, half start with dev to balance load.
        """
        prod_url = f"{prod_base_url}?{params.lstrip('?')}"
        dev_url = f"{dev_base_url}?{params.lstrip('?')}"
        
        # Alternate which environment goes first to balance load
        if idx % 2 == 0:
            first_env, first_url, first_ssl = "prod", prod_url, True
            second_env, second_url, second_ssl = "dev", dev_url, False
        else:
            first_env, first_url, first_ssl = "dev", dev_url, False
            second_env, second_url, second_ssl = "prod", prod_url, True
        
        async with fetch_semaphore:
            progress.log(f"[Test {idx}] Starting ({first_env} first)...")
            
            # Fetch first environment
            (test_case1, env1, file_path1, status1, 
             response_text1, shop_name1, request_params1) = await fetch_and_save(
                session, first_url, verify_ssl=first_ssl, test_case=idx,
                environment=first_env, output_dir=run_output_dir, verbose=args.verbose
            )
            progress.increment_fetches()
            progress.log(f"[Test {idx}] {first_env.upper()} done (status={status1})")
            
            # Fetch second environment
            (test_case2, env2, file_path2, status2,
             response_text2, shop_name2, request_params2) = await fetch_and_save(
                session, second_url, verify_ssl=second_ssl, test_case=idx,
                environment=second_env, output_dir=run_output_dir, verbose=args.verbose
            )
            progress.increment_fetches()
            progress.log(f"[Test {idx}] {second_env.upper()} done (status={status2})")
        
        # Build results dict
        results[idx] = {
            first_env: {
                "file": file_path1,
                "status": status1,
                "response_text": response_text1,
                "shop_name": shop_name1,
                "request_params": request_params1
            },
            second_env: {
                "file": file_path2,
                "status": status2,
                "response_text": response_text2,
                "shop_name": shop_name2,
                "request_params": request_params2
            }
        }
        
        # Start diff immediately since both are ready
        diff_task = maybe_start_diff(idx)
        if diff_task:
            diff_tasks.append(diff_task)
    
    # Run all test cases
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            process_test_case(session, idx, params) 
            for idx, params in enumerate(param_list)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Wait for remaining diffs
    if diff_tasks:
        progress.log(f"Waiting for {len(pending_diffs)} remaining diffs...")
        await asyncio.gather(*diff_tasks, return_exceptions=True)
    
    # Clear progress display
    progress.finish()
    
    logging.info("All fetches and diffs completed!")
    
    # Calculate total runtime
    total_runtime = (datetime.now() - run_start_time).total_seconds()
    
    # Build final summary
    overall_summary: OrderedDict[str, Any] = OrderedDict()
    overall_summary["count"] = 0
    overall_summary["run_folder"] = run_folder_name
    overall_summary["total_runtime_seconds"] = round(total_runtime, 2)
    overall_summary["test_cases"] = []
    
    for test_case in sorted(diff_results.keys(), key=lambda x: int(x)):
        overall_summary["test_cases"].append(diff_results[test_case])
    
    overall_summary["count"] = len(overall_summary["test_cases"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Write overall summary
    overall_filename = os.path.join(args.summary_dir, f"diffs_summary_{timestamp}.json")
    with open(overall_filename, "w") as f:
        json.dump(overall_summary, f, indent=2)
    logging.info(f"Overall summary written to {overall_filename}")
    
    # Save summary to run folder
    run_summary_path = os.path.join(run_output_dir, "summary.json")
    with open(run_summary_path, "w") as f:
        json.dump(overall_summary, f, indent=2)
    logging.info(f"Run summary also saved to {run_summary_path}")
    
    # Write updates summary (only rows with changes)
    updates_summary: OrderedDict[str, Any] = OrderedDict()
    updates_summary["count"] = 0
    updates_summary["run_folder"] = run_folder_name
    updates_summary["total_runtime_seconds"] = round(total_runtime, 2)
    updates_summary["test_cases"] = []
    
    # Write errors summary
    errors_summary: OrderedDict[str, Any] = OrderedDict()
    errors_summary["count"] = 0
    errors_summary["run_folder"] = run_folder_name
    errors_summary["total_runtime_seconds"] = round(total_runtime, 2)
    errors_summary["test_cases"] = []
    
    for test in overall_summary["test_cases"]:
        if test.get("non_200") or test.get("error"):
            errors_summary["test_cases"].append(test)
        elif (test.get("rows_added", 0) > 0 or 
              test.get("rows_removed", 0) > 0 or 
              test.get("rows_updated", 0) > 0):
            updates_summary["test_cases"].append(test)
    
    updates_summary["count"] = len(updates_summary["test_cases"])
    updates_filename = os.path.join(args.summary_dir, f"diffs_summary_updates_{timestamp}.json")
    with open(updates_filename, "w") as f:
        json.dump(updates_summary, f, indent=2)
    logging.info(f"Updates summary written to {updates_filename}")
    
    errors_summary["count"] = len(errors_summary["test_cases"])
    errors_filename = os.path.join(args.summary_dir, f"diffs_summary_errors_{timestamp}.json")
    with open(errors_filename, "w") as f:
        json.dump(errors_summary, f, indent=2)
    logging.info(f"Errors summary written to {errors_filename}")
    
    logging.info(f"\n{'='*60}")
    logging.info(f"Run complete! Response files saved to: {run_output_dir}")
    logging.info(f"Total runtime: {total_runtime:.2f}s")
    logging.info(f"{'='*60}")


def run_main(args) -> None:
    """
    Main entry point that dispatches to the appropriate mode.
    
    Args:
        args: Parsed command line arguments
    """
    asyncio.run(_async_main(args))


async def _async_main(args) -> None:
    """Async main function."""
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level, 
        format='%(asctime)s %(levelname)s: %(message)s'
    )
    
    primary_keys = [k.strip() for k in args.primary_key.split(",")]
    logging.info(f"Using primary key(s): {primary_keys}")
    
    differ = EfficientDiffer(
        primary_keys, 
        max_examples=args.max_examples, 
        max_rows=args.diff_rows
    )
    
    if args.diff_rows:
        logging.info(f"Row limit: {args.diff_rows} rows per file")
    
    # Local folder mode
    if args.local_folder:
        await run_folder_diff(
            args.local_folder,
            differ,
            args.summary_dir,
            args.max_concurrent_diffs,
            args.diff_rows,
        )
        return
    
    # Local file mode
    if args.local_prod and args.local_dev:
        await run_local_diff(
            args.local_prod,
            args.local_dev,
            differ,
            args.summary_dir,
            args.diff_rows,
        )
        return
    
    # URL mode
    if not args.prod_url or not args.dev_url:
        logging.error(
            "URL mode requires --prod-url and --dev-url.\n"
            "Example:\n"
            "  diaz-diff --params-file tests.csv \\\n"
            "    --prod-url 'https://api.prod.example.com/endpoint' \\\n"
            "    --dev-url 'https://api.dev.example.com/endpoint'"
        )
        return
    
    await run_url_mode(args, differ)