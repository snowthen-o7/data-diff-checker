"""
Utility functions for Data Diff Checker.

Includes URL parameter parsing, file naming, and other helper functions.
"""

import hashlib
import json
import os
import re
from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import parse_qsl


def parse_url_params_to_json(params_string: str) -> Dict:
    """
    Parse URL query parameters into a nested JSON-friendly dictionary.
    
    Handles PHP-style nested params like:
        connection_info[shop_name]=test&connection_info[api_key]=abc
        connection_info[product_filters][0][filter]=published_status
    
    Converts to:
        {
            "connection_info": {
                "shop_name": "test", 
                "api_key": "abc",
                "product_filters": [
                    {"filter": "published_status"}
                ]
            }
        }
    
    Numeric indices like [0], [1] create arrays; string keys create objects.
    All values are URL-decoded.
    
    Args:
        params_string: URL query string (with or without leading '?')
        
    Returns:
        Nested dictionary structure
    """
    if not params_string:
        return {}
    
    # Remove leading ? if present
    if params_string.startswith('?'):
        params_string = params_string[1:]
    
    result = {}
    
    # Parse the query string (automatically URL-decodes)
    parsed = parse_qsl(params_string, keep_blank_values=True)
    
    for key, value in parsed:
        # Extract all bracket keys: "a[b][c][d]" -> ["a", "b", "c", "d"]
        parts = []
        
        # Get the base key (before any brackets)
        base_match = re.match(r'^([^\[]+)', key)
        if base_match:
            parts.append(base_match.group(1))
        
        # Extract all bracketed keys
        bracket_keys = re.findall(r'\[([^\]]*)\]', key)
        parts.extend(bracket_keys)
        
        if not parts:
            continue
        
        # Navigate/create the nested structure
        current = result
        for i, part in enumerate(parts[:-1]):
            next_part = parts[i + 1]
            is_next_numeric = next_part.isdigit()
            
            # Determine if current part is a numeric index
            if part.isdigit():
                idx = int(part)
                # Ensure list is long enough
                while len(current) <= idx:
                    current.append({} if not parts[i + 1].isdigit() else [])
                if (
                    current[idx] is None 
                    or (isinstance(current[idx], dict) and not current[idx]) 
                    or (isinstance(current[idx], list) and not current[idx])
                ):
                    current[idx] = [] if is_next_numeric else {}
                current = current[idx]
            else:
                # String key - ensure dict exists
                if part not in current:
                    current[part] = [] if is_next_numeric else {}
                elif is_next_numeric and not isinstance(current[part], list):
                    # Convert to list if we discover it needs to be one
                    current[part] = []
                current = current[part]
        
        # Set the final value
        final_key = parts[-1]
        if final_key.isdigit():
            idx = int(final_key)
            while len(current) <= idx:
                current.append(None)
            current[idx] = value
        else:
            current[final_key] = value
    
    return result


def generate_run_folder_name(
    params_file: str,
    primary_key: str = "id",
    timeout: int = 900,
    max_examples: int = 10,
    diff_rows: Optional[int] = None,
    source_limit: Optional[int] = None,
    verbose: bool = False,
) -> str:
    """
    Generate a unique folder name for a run based on timestamp and parameters.
    
    Format: YYYYMMDD_HHMMSS_<params_base>_<flags>
    
    Args:
        params_file: Path to the params file
        primary_key: Primary key configuration
        timeout: Timeout value
        max_examples: Max examples value
        diff_rows: Row limit if set
        source_limit: Source limit if set
        verbose: Verbose flag
        
    Returns:
        Folder name string
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get base name of params file (without extension)
    params_base = os.path.splitext(os.path.basename(params_file))[0]
    
    # Build flags string
    flags = []
    if primary_key != 'id':
        pk_safe = primary_key.replace(',', '-').replace(' ', '')
        flags.append(f"pk_{pk_safe}")
    if timeout != 900:
        flags.append(f"t{timeout}")
    if max_examples != 10:
        flags.append(f"ex{max_examples}")
    if diff_rows is not None:
        flags.append(f"diffrows{diff_rows}")
    if source_limit is not None:
        flags.append(f"srclimit{source_limit}")
    if verbose:
        flags.append("verbose")
    
    # Construct folder name
    parts = [timestamp, params_base]
    if flags:
        parts.append("_".join(flags))
    
    return "_".join(parts)


def save_run_metadata(
    run_dir: str,
    params_file: Optional[str] = None,
    primary_key: str = "id",
    timeout: int = 900,
    max_examples: int = 10,
    max_concurrent_diffs: int = 10,
    max_concurrent_fetches: int = 250,
    diff_rows: Optional[int] = None,
    source_limit: Optional[int] = None,
    verbose: bool = False,
    local_prod: Optional[str] = None,
    local_dev: Optional[str] = None,
    local_folder: Optional[str] = None,
    output_dir: str = "responses",
    summary_dir: str = "summaries",
) -> str:
    """
    Save metadata about a run to the run directory.
    
    Args:
        run_dir: Directory to save metadata in
        Other args: Configuration values to record
        
    Returns:
        Path to the saved metadata file
    """
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "params_file": os.path.abspath(params_file) if params_file else None,
        "primary_key": primary_key,
        "timeout": timeout,
        "max_examples": max_examples,
        "max_concurrent_diffs": max_concurrent_diffs,
        "max_concurrent_fetches": max_concurrent_fetches,
        "diff_rows": diff_rows,
        "source_limit": source_limit,
        "verbose": verbose,
        "local_prod": local_prod,
        "local_dev": local_dev,
        "local_folder": local_folder,
        "output_dir": output_dir,
        "summary_dir": summary_dir,
    }
    
    metadata_path = os.path.join(run_dir, "run_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    return metadata_path


def generate_file_hash(params: str) -> str:
    """
    Generate an MD5 hash from URL parameters for file naming.
    
    Args:
        params: URL parameter string
        
    Returns:
        Short MD5 hash string
    """
    return hashlib.md5(params.encode('utf-8')).hexdigest()


def create_summary_structure(
    count: int = 0,
    run_folder: Optional[str] = None,
    runtime_seconds: float = 0.0,
    test_cases: Optional[List] = None,
) -> OrderedDict:
    """
    Create a standard summary structure.
    
    Args:
        count: Number of test cases
        run_folder: Name of the run folder
        runtime_seconds: Total runtime
        test_cases: List of test case results
        
    Returns:
        OrderedDict with standard summary structure
    """
    summary = OrderedDict()
    summary["count"] = count
    if run_folder:
        summary["run_folder"] = run_folder
    summary["total_runtime_seconds"] = round(runtime_seconds, 2)
    summary["test_cases"] = test_cases or []
    return summary


def extract_dedup_key(
    params: str, 
    dedup_keys: List[str]
) -> Optional[str]:
    """
    Extract a unique identifier from URL params for deduplication.
    
    Args:
        params: URL parameter string
        dedup_keys: List of parameter names to check
        
    Returns:
        First matching key=value string, or None if no match
    """
    parsed = parse_qsl(params.lstrip('?'), keep_blank_values=True)
    param_dict = {k: v for k, v in parsed}
    
    for dedup_key in dedup_keys:
        if dedup_key in param_dict and param_dict[dedup_key]:
            return f"{dedup_key}={param_dict[dedup_key]}"
    
    return None
