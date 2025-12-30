#!/usr/bin/env python3

import sys
import asyncio
import aiohttp
import csv
from urllib.parse import urlparse, parse_qsl, unquote
import hashlib
from datetime import datetime
import argparse
import logging
import os
import json
from collections import OrderedDict, defaultdict
import gzip
import re
from typing import Iterator, Dict, Set, Tuple, List, Optional
import gc
import shutil
import threading
import time

# Safely set CSV field size limit
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int //= 10

# Base URLs for production and development
PRODUCTION_BASE_URL = "https://preprocess.proxy.feedonomics.com/preprocess/run_preprocess.php"
DEVELOPMENT_BASE_URL = "https://3.15.124.182:8012/preprocess/run_preprocess.php"

# Maximum number of example IDs to include in output
MAX_EXAMPLE_IDS = 10


def enable_windows_ansi_support() -> bool:
    """
    Enable ANSI escape code support on Windows 10+.
    Returns True if successful or not on Windows, False if it failed.
    """
    if sys.platform != 'win32':
        return True
    
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        
        # Get handle to stderr (STD_ERROR_HANDLE = -12)
        STD_ERROR_HANDLE = -12
        handle = kernel32.GetStdHandle(STD_ERROR_HANDLE)
        
        # Get current console mode
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        
        # Enable ENABLE_VIRTUAL_TERMINAL_PROCESSING (0x0004)
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        new_mode = mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
        
        if not kernel32.SetConsoleMode(handle, new_mode):
            return False
        
        return True
    except Exception:
        return False


# Try to enable ANSI support on Windows at module load time
_WINDOWS_ANSI_ENABLED = enable_windows_ansi_support()


class ProgressDisplay:
    """
    Terminal progress display with progress bar and recent activity log.
    Updates in place instead of scrolling output.
    Falls back to standard logging if not a TTY or ANSI isn't supported.
    """
    
    def __init__(self, total_fetches: int, total_diffs: int, max_log_lines: int = 8):
        self.total_fetches = total_fetches
        self.total_diffs = total_diffs
        self.max_log_lines = max_log_lines
        
        self.completed_fetches = 0
        self.completed_diffs = 0
        self.errors = 0
        self.start_time = time.time()
        
        self.log_lines: List[str] = []
        self.lock = threading.Lock()
        
        # Check if we can use ANSI escape codes for the progress display
        # Works on: Unix TTYs, Windows 10+ with ANSI enabled
        self.is_windows = sys.platform == 'win32'
        if self.is_windows:
            # On Windows, only use TTY mode if ANSI support was successfully enabled
            self.is_tty = sys.stderr.isatty() and _WINDOWS_ANSI_ENABLED
        else:
            # On Unix-like systems, just check if it's a TTY
            self.is_tty = sys.stderr.isatty()
        
        self.display_height = 0  # Track how many lines we've drawn
        
        # Track last progress log time for fallback mode (to avoid spam)
        self._last_progress_log = 0
        self._progress_log_interval = 5  # Log progress every 5 seconds in fallback mode
        
        # Get terminal width
        try:
            self.term_width = shutil.get_terminal_size().columns
        except:
            self.term_width = 80
    
    def _make_progress_bar(self, current: int, total: int, width: int = 30, label: str = "") -> str:
        """Create a progress bar string"""
        if total == 0:
            pct = 100
            filled = width
        else:
            pct = (current / total) * 100
            filled = int(width * current / total)
        
        bar = "█" * filled + "░" * (width - filled)
        return f"{label}[{bar}] {current}/{total} ({pct:.1f}%)"
    
    def _format_elapsed(self) -> str:
        """Format elapsed time"""
        elapsed = time.time() - self.start_time
        mins, secs = divmod(int(elapsed), 60)
        return f"{mins:02d}:{secs:02d}"
    
    def _clear_display(self):
        """Clear the previous display area"""
        if self.display_height > 0:
            # Move cursor up and clear each line
            sys.stderr.write(f"\033[{self.display_height}A")  # Move up
            for _ in range(self.display_height):
                sys.stderr.write("\033[2K\n")  # Clear line
            sys.stderr.write(f"\033[{self.display_height}A")  # Move back up
    
    def _draw(self):
        """Draw the progress display"""
        if not self.is_tty:
            return
        
        lines = []
        
        # Header
        elapsed = self._format_elapsed()
        lines.append(f"┌─ Diaz Diff Checker ─ Elapsed: {elapsed} ─{'─' * (self.term_width - 40)}┐")
        
        # Progress bars
        fetch_bar = self._make_progress_bar(self.completed_fetches, self.total_fetches, 25, "Fetches: ")
        diff_bar = self._make_progress_bar(self.completed_diffs, self.total_diffs, 25, "Diffs:   ")
        
        lines.append(f"│ {fetch_bar:<{self.term_width - 4}} │")
        lines.append(f"│ {diff_bar:<{self.term_width - 4}} │")
        
        # Error count if any
        if self.errors > 0:
            error_line = f"│ ⚠ Errors: {self.errors:<{self.term_width - 14}} │"
            lines.append(error_line)
        
        # Separator
        lines.append(f"├─ Recent Activity ─{'─' * (self.term_width - 21)}┤")
        
        # Recent log lines
        recent_logs = self.log_lines[-self.max_log_lines:]
        for log in recent_logs:
            # Truncate long lines
            truncated = log[:self.term_width - 4]
            lines.append(f"│ {truncated:<{self.term_width - 4}} │")
        
        # Pad with empty lines if fewer logs
        for _ in range(self.max_log_lines - len(recent_logs)):
            lines.append(f"│ {'':<{self.term_width - 4}} │")
        
        # Footer
        lines.append(f"└{'─' * (self.term_width - 2)}┘")
        
        # Clear previous and draw new
        self._clear_display()
        
        for line in lines:
            sys.stderr.write(line[:self.term_width] + "\n")
        
        sys.stderr.flush()
        self.display_height = len(lines)
    
    def log(self, message: str):
        """Add a log message and update display"""
        with self.lock:
            # Add timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted = f"{timestamp} {message}"
            self.log_lines.append(formatted)
            
            # Keep only recent lines
            if len(self.log_lines) > 100:
                self.log_lines = self.log_lines[-100:]
            
            if self.is_tty:
                self._draw()
            else:
                # Fall back to standard logging for non-TTY (including Windows)
                logging.info(message)
    
    def _maybe_log_progress(self):
        """On Windows/non-TTY, periodically log progress to avoid spam"""
        if self.is_tty:
            return
        
        now = time.time()
        if now - self._last_progress_log >= self._progress_log_interval:
            self._last_progress_log = now
            elapsed = self._format_elapsed()
            fetch_pct = (self.completed_fetches / self.total_fetches * 100) if self.total_fetches > 0 else 100
            diff_pct = (self.completed_diffs / self.total_diffs * 100) if self.total_diffs > 0 else 100
            logging.info(f"Progress [{elapsed}]: Fetches {self.completed_fetches}/{self.total_fetches} ({fetch_pct:.0f}%), Diffs {self.completed_diffs}/{self.total_diffs} ({diff_pct:.0f}%)")
    
    def update_fetches(self, completed: int):
        """Update fetch progress"""
        with self.lock:
            self.completed_fetches = completed
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def update_diffs(self, completed: int):
        """Update diff progress"""
        with self.lock:
            self.completed_diffs = completed
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_fetches(self):
        """Increment fetch count"""
        with self.lock:
            self.completed_fetches += 1
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_diffs(self):
        """Increment diff count"""
        with self.lock:
            self.completed_diffs += 1
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_errors(self):
        """Increment error count"""
        with self.lock:
            self.errors += 1
            if self.is_tty:
                self._draw()
    
    def finish(self):
        """Clear display and print final summary"""
        if self.is_tty:
            self._clear_display()
            sys.stderr.flush()
    
    def initial_draw(self):
        """Draw initial display"""
        if self.is_tty:
            self._draw()


def parse_url_params_to_json(params_string: str) -> dict:
    """
    Parse URL query parameters into a nested JSON-friendly dictionary.
    
    Handles PHP-style nested params like:
        connection_info[shop_name]=test&connection_info[api_key]=abc
        connection_info[product_filters][0][filter]=published_status
        connection_info[product_filters][0][value]=published
    
    Converts to:
        {
            "connection_info": {
                "shop_name": "test", 
                "api_key": "abc",
                "product_filters": [
                    {"filter": "published_status", "value": "published"}
                ]
            }
        }
    
    Numeric indices like [0], [1] create arrays; string keys create objects.
    All values are URL-decoded.
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
                if current[idx] is None or (isinstance(current[idx], dict) and not current[idx]) or (isinstance(current[idx], list) and not current[idx]):
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


class StreamingCSVReader:
    """Memory-efficient CSV reader that handles mixed delimiter files and escape styles"""
    
    def __init__(self, file_path: str, delimiter: Optional[str] = None, max_rows: Optional[int] = None):
        self.file_path = file_path
        self.delimiter = delimiter  # Data delimiter
        self.max_rows = max_rows
        self._headers: Optional[List[str]] = None
        self._row_count: Optional[int] = None
        self._header_delimiter: Optional[str] = None  # May differ from data delimiter
        self._uses_backslash_escape: bool = False  # Whether file uses \" instead of ""
        self._detect_delimiters()
    
    def _detect_delimiters(self):
        """Auto-detect delimiters and escape style"""
        if self.delimiter:
            self._header_delimiter = self.delimiter
        
        with open(self.file_path, 'r', encoding='utf-8-sig') as f:
            # Read from multiple parts of the file to detect patterns
            # This helps catch files where escape patterns only appear in later rows
            # (e.g., Shopify exports where gift cards with HTML appear after products)
            
            # Read first 32KB for header/delimiter detection
            first_sample = f.read(32768)
            lines = first_sample.split('\n')[:5]
            
            # Also sample from middle and end if file is larger
            f.seek(0, 2)  # Seek to end
            file_size = f.tell()
            
            samples = [first_sample]
            if file_size > 100000:  # Only sample more if file > 100KB
                # Sample from middle
                f.seek(file_size // 2)
                f.readline()  # Skip partial line
                samples.append(f.read(16384))
                
                # Sample from near end (last 16KB)
                if file_size > 50000:
                    f.seek(max(0, file_size - 16384))
                    f.readline()  # Skip partial line
                    samples.append(f.read(16384))
            
            sample = ''.join(samples)
            
            if not lines:
                self.delimiter = self.delimiter or ","
                self._header_delimiter = self._header_delimiter or ","
                return
            
            # Detect escaping style for the CSV itself
            # Standard CSV uses "" to escape quotes (e.g., "81 x 36""")
            # Some exports use backslash escaping (e.g., "value with \"quotes\"")
            # 
            # Important: CSVs with HTML/JSON content often contain \" sequences.
            # In standard CSV mode, these would break parsing because the quote
            # after backslash would be seen as a field delimiter.
            # 
            # Heuristic: If we see \" but NOT "", assume backslash escaping.
            # This correctly handles:
            # - Shopify exports with HTML like data-mce-fragment=\"1\"
            # - Files with embedded JSON containing \"
            # 
            # If both "" and \" exist, prefer standard "" mode as it's more common.
            has_double_quote_escape = '""' in sample
            has_backslash_quote = '\\"' in sample
            
            if has_backslash_quote and not has_double_quote_escape:
                self._uses_backslash_escape = True
                logging.debug(f"Detected backslash escape mode in {os.path.basename(self.file_path)}")
                self._uses_backslash_escape = True
            
            if not self.delimiter:
                # Analyze header
                header = lines[0]
                header_tabs = header.count("\t")
                header_commas = header.count(",")
                self._header_delimiter = "\t" if header_tabs > header_commas else ","
                
                # Analyze data (if available)
                if len(lines) > 1:
                    data_line = lines[1]
                    data_tabs = data_line.count("\t")
                    data_commas = data_line.count(",")
                    self.delimiter = "\t" if data_tabs > data_commas else ","
                    
                    # Log warning if mismatch detected
                    if self.delimiter != self._header_delimiter:
                        logging.warning(
                            f"Delimiter mismatch in {os.path.basename(self.file_path)}: "
                            f"header uses {repr(self._header_delimiter)}, data uses {repr(self.delimiter)}"
                        )
                else:
                    self.delimiter = self._header_delimiter
            else:
                self._header_delimiter = self._header_delimiter or self.delimiter
    
    def _get_csv_params(self) -> dict:
        """Get CSV reader parameters based on detected escape style"""
        params = {'delimiter': self.delimiter}
        if self._uses_backslash_escape:
            params['doublequote'] = False
            params['escapechar'] = '\\'
        return params
    
    def _open_file(self):
        """Open file with BOM handling"""
        return open(self.file_path, 'r', encoding='utf-8-sig')
    
    def read_headers(self) -> List[str]:
        """Read headers (cached after first read) - uses header's delimiter"""
        if self._headers is not None:
            return self._headers
        
        with self._open_file() as f:
            header_line = f.readline().rstrip('\r\n')
            # Parse header with its own delimiter and escape style
            params = self._get_csv_params()
            params['delimiter'] = self._header_delimiter
            reader = csv.reader([header_line], **params)
            raw_headers = next(reader)
            self._headers = [self._normalize_key(k) for k in raw_headers if k is not None]
        
        return self._headers
    
    def iterate_rows(self) -> Iterator[Dict[str, str]]:
        """Iterate through rows one at a time (true streaming)"""
        rows_yielded = 0
        
        with self._open_file() as f:
            reader = csv.DictReader(f, **self._get_csv_params())
            
            for row in reader:
                if self.max_rows is not None and rows_yielded >= self.max_rows:
                    break
                
                yield {
                    self._normalize_key(k): v 
                    for k, v in row.items() 
                    if k is not None
                }
                rows_yielded += 1
    
    def iterate_rows_with_line_numbers(self) -> Iterator[Tuple[int, Dict[str, str]]]:
        """
        Iterate through rows with line numbers.
        
        Line numbers reflect the starting line of each row (1-indexed),
        correctly handling multi-line quoted fields.
        """
        rows_yielded = 0
        
        with self._open_file() as f:
            reader = csv.DictReader(f, **self._get_csv_params())
            
            # Track the line number where each row starts
            # reader.line_num gives where the row ENDS, so we track the previous end
            prev_line_end = 1  # Header ends at line 1
            
            for row in reader:
                if self.max_rows is not None and rows_yielded >= self.max_rows:
                    break
                
                # Row starts right after the previous row ended
                row_start_line = prev_line_end + 1
                prev_line_end = reader.line_num
                
                yield (row_start_line, {
                    self._normalize_key(k): v 
                    for k, v in row.items() 
                    if k is not None
                })
                rows_yielded += 1
    
    def count_rows(self) -> int:
        """Count rows (cached after first count, respects max_rows)"""
        if self._row_count is not None:
            return self._row_count
            
        count = 0
        with self._open_file() as f:
            # Use csv.reader to correctly count logical rows
            # (handles multi-line quoted fields)
            reader = csv.reader(f, **self._get_csv_params())
            next(reader)  # Skip header
            for _ in reader:
                count += 1
                if self.max_rows is not None and count >= self.max_rows:
                    break
        self._row_count = count
        return count
    
    @staticmethod
    def _normalize_key(key: str) -> str:
        """Normalize a key by stripping whitespace and quotes"""
        return key.strip().strip('"')


class EfficientDiffer:
    """Memory-efficient diff calculator using hash-based comparison"""
    
    def __init__(self, primary_keys: List[str], max_examples: int = MAX_EXAMPLE_IDS, max_rows: Optional[int] = None):
        self.primary_keys = primary_keys
        self.max_examples = max_examples
        self.max_rows = max_rows
    
    def compute_diff(self, prod_file: str, dev_file: str) -> Dict:
        """Compute differences between two CSV files efficiently"""
        prod_reader = StreamingCSVReader(prod_file, max_rows=self.max_rows)
        dev_reader = StreamingCSVReader(dev_file, max_rows=self.max_rows)
        
        # Get headers (cached)
        prod_headers = set(prod_reader.read_headers())
        dev_headers = set(dev_reader.read_headers())
        
        # Log headers for debugging
        logging.debug(f"    Prod headers: {sorted(prod_headers)}")
        logging.debug(f"    Dev headers: {sorted(dev_headers)}")
        logging.debug(f"    Primary key(s): {self.primary_keys}")
        
        # Check if primary keys exist
        missing_prod = [k for k in self.primary_keys if k not in prod_headers]
        if missing_prod:
            # Provide helpful error with actual headers
            raise ValueError(
                f"Primary keys {missing_prod} not found in production file. "
                f"Available columns: {sorted(prod_headers)}"
            )
        missing_dev = [k for k in self.primary_keys if k not in dev_headers]
        if missing_dev:
            raise ValueError(
                f"Primary keys {missing_dev} not found in development file. "
                f"Available columns: {sorted(dev_headers)}"
            )
        
        common_keys = prod_headers & dev_headers
        prod_only_keys = prod_headers - dev_headers
        dev_only_keys = dev_headers - prod_headers
        
        # Identify columns to exclude from "meaningful change" detection
        excluded_columns = {
            k for k in common_keys 
            if 'inventory' in k.lower() or 'availability' in k.lower() or '_fdx' in k.lower()
        }
        comparison_keys = common_keys - excluded_columns
        
        # Process differences using single-pass approach where possible
        diff_stats = self._process_differences_optimized(
            prod_reader, dev_reader, common_keys, comparison_keys
        )
        
        # Add metadata
        diff_stats.update({
            'common_keys': sorted(list(common_keys)),
            'prod_only_keys': sorted(list(prod_only_keys)),
            'dev_only_keys': sorted(list(dev_only_keys)),
            'prod_row_count': prod_reader.count_rows(),
            'dev_row_count': dev_reader.count_rows()
        })
        
        return diff_stats
    
    def _make_composite_key(self, row: Dict[str, str]) -> str:
        """Create a composite key from primary key values"""
        return "||".join(str(row.get(k, "")) for k in self.primary_keys)
    
    def _get_primary_key_display(self, row: Dict[str, str]) -> str:
        """Get a display-friendly primary key (single value or composite)"""
        if len(self.primary_keys) == 1:
            value = row.get(self.primary_keys[0])
            # Handle None values (shouldn't happen but defensive)
            if value is None:
                return "<missing>"
            return str(value)
        
        parts = []
        for k in self.primary_keys:
            value = row.get(k)
            if value is None:
                parts.append("<missing>")
            else:
                parts.append(str(value))
        return "_".join(parts)
    
    def _hash_row(self, row: Dict[str, str], keys: Set[str]) -> str:
        """Create a hash of row values for the given keys (memory efficient comparison)"""
        # Sort keys for consistent hashing
        values = "|".join(str(row.get(k, "")) for k in sorted(keys))
        return hashlib.md5(values.encode('utf-8')).hexdigest()
    
    def _process_differences_optimized(self, prod_reader: StreamingCSVReader,
                                       dev_reader: StreamingCSVReader,
                                       common_keys: Set[str],
                                       comparison_keys: Set[str]) -> Dict:
        """
        Optimized difference processing using hash-based comparison.
        
        Memory optimization strategy:
        1. First pass on prod: Build index with row hashes (not full row data)
        2. First pass on dev: Build index with row hashes
        3. Compare hashes to find changed rows
        4. Second pass only on changed rows to get detailed changes
        """
        
        rows_added = 0
        rows_removed = 0
        rows_changed_meaningful = 0  # Rows with non-inventory/availability/_fdx changes
        rows_changed_excluded_only = 0  # Rows where ONLY inventory/availability/_fdx changed
        detailed_changes = defaultdict(int)
        example_ids = OrderedDict()
        example_ids_added = OrderedDict()
        example_ids_removed = OrderedDict()
        
        total_prod_rows = prod_reader.count_rows()
        total_dev_rows = dev_reader.count_rows()
        logging.debug(f"    Processing {total_prod_rows} prod rows, {total_dev_rows} dev rows...")
        
        # Phase 1: Build prod index with hashes (memory efficient)
        # Store: composite_key -> (line_num, full_hash, comparison_hash, display_key)
        prod_index: Dict[str, Tuple[int, str, str, str]] = {}
        
        rows_processed = 0
        first_row_validated = False
        duplicate_keys_count = 0
        for line_num, row in prod_reader.iterate_rows_with_line_numbers():
            # Validate first row to catch column name issues early
            if not first_row_validated:
                for pk in self.primary_keys:
                    if pk not in row:
                        available_keys = sorted(row.keys())
                        raise ValueError(
                            f"Primary key '{pk}' not found in row data. "
                            f"This may indicate a BOM or encoding issue. "
                            f"Available keys in row: {available_keys}"
                        )
                # Log the first row's primary key value for debugging
                first_pk_value = self._get_primary_key_display(row)
                logging.debug(f"    First prod row (line {line_num}) primary key value: '{first_pk_value}'")
                first_row_validated = True
            
            composite_key = self._make_composite_key(row)
            display_key = self._get_primary_key_display(row)
            
            # Warn about duplicate keys (can cause line number confusion)
            if composite_key in prod_index:
                duplicate_keys_count += 1
                if duplicate_keys_count <= 3:
                    logging.warning(f"    Duplicate primary key in prod file: '{display_key}' at line {line_num} (previously seen at line {prod_index[composite_key][0]})")
            
            full_hash = self._hash_row(row, common_keys)
            comp_hash = self._hash_row(row, comparison_keys) if comparison_keys else full_hash
            prod_index[composite_key] = (line_num, full_hash, comp_hash, display_key)
            
            rows_processed += 1
            if rows_processed % 50000 == 0:
                logging.debug(f"    Indexed {rows_processed}/{total_prod_rows} prod rows...")
        
        if duplicate_keys_count > 3:
            logging.warning(f"    ... and {duplicate_keys_count - 3} more duplicate keys in prod file")
        if duplicate_keys_count > 0:
            logging.warning(f"    Total duplicate keys in prod: {duplicate_keys_count} (line numbers may reference last occurrence)")
        
        logging.debug(f"    Prod index built: {len(prod_index)} unique keys")
        
        # Phase 2: Build dev index first, then identify changes
        # IMPORTANT: We build the full index first (keeping last occurrence for duplicates),
        # then compare using final stored hashes. This ensures counting matches detailed comparison.
        dev_index: Dict[str, Tuple[int, str, str]] = {}
        all_changed_keys: Set[str] = set()  # All keys with any changes (for detailed counts)
        meaningful_change_keys: Set[str] = set()  # Keys with non-inventory/availability/_fdx changes
        excluded_only_keys: Set[str] = set()  # Keys where ONLY inventory/availability/_fdx changed
        
        added_examples_collected = 0
        added_keys = set()  # Track unique added keys
        rows_processed = 0
        
        # First pass: Build the dev index (last occurrence wins for duplicates)
        for line_num, row in dev_reader.iterate_rows_with_line_numbers():
            composite_key = self._make_composite_key(row)
            full_hash = self._hash_row(row, common_keys)
            comp_hash = self._hash_row(row, comparison_keys) if comparison_keys else full_hash
            dev_index[composite_key] = (line_num, full_hash, comp_hash)
            
            # Track added rows (keys not in prod)
            if composite_key not in prod_index:
                if composite_key not in added_keys:
                    rows_added += 1
                    added_keys.add(composite_key)
                    # Collect example for added row (first occurrence)
                    if added_examples_collected < self.max_examples:
                        display_key = self._get_primary_key_display(row)
                        example_ids_added[display_key] = {"dev_line_num": line_num}
                        added_examples_collected += 1
            
            rows_processed += 1
            if rows_processed % 50000 == 0:
                logging.debug(f"    Processed {rows_processed}/{total_dev_rows} dev rows...")
        
        # Now compare final stored hashes to identify changes
        # This ensures counting uses the same data as Phase 3 detailed comparison
        for composite_key, (dev_line, dev_full_hash, dev_comp_hash) in dev_index.items():
            if composite_key in prod_index:
                prod_line, prod_full_hash, prod_comp_hash, _ = prod_index[composite_key]
                if dev_full_hash != prod_full_hash:
                    all_changed_keys.add(composite_key)
                    # Categorize: meaningful change vs excluded-only change
                    if dev_comp_hash != prod_comp_hash:
                        rows_changed_meaningful += 1
                        meaningful_change_keys.add(composite_key)
                    else:
                        rows_changed_excluded_only += 1
                        excluded_only_keys.add(composite_key)
        
        # Count removed rows and collect examples
        removed_examples_collected = 0
        for composite_key, (prod_line, _, _, display_key) in prod_index.items():
            if composite_key not in dev_index:
                rows_removed += 1
                # Collect example for removed row
                if removed_examples_collected < self.max_examples:
                    example_ids_removed[display_key] = {"prod_line_num": prod_line}
                    removed_examples_collected += 1
        
        logging.debug(f"    Found {len(meaningful_change_keys)} meaningful changes, {len(excluded_only_keys)} excluded-only changes")
        
        # Phase 3: Get detailed changes for ALL changed rows (second pass)
        if all_changed_keys:
            # Build a lookup of prod rows we need
            # IMPORTANT: Do NOT break early - we need the LAST occurrence of each key
            # to match prod_index behavior (which overwrites on duplicates)
            needed_prod_rows: Dict[str, Dict[str, str]] = {}
            for line_num, row in prod_reader.iterate_rows_with_line_numbers():
                composite_key = self._make_composite_key(row)
                if composite_key in all_changed_keys:
                    # Overwrite to get the last occurrence (matches prod_index behavior)
                    needed_prod_rows[composite_key] = {k: row.get(k, "") for k in common_keys}
            
            # Second pass on dev to build needed_dev_rows (LAST occurrence of each changed key)
            # This matches dev_index behavior for consistency
            needed_dev_rows: Dict[str, Tuple[int, Dict[str, str]]] = {}
            for line_num, row in dev_reader.iterate_rows_with_line_numbers():
                composite_key = self._make_composite_key(row)
                if composite_key in all_changed_keys:
                    # Overwrite to get the last occurrence (matches dev_index behavior)
                    needed_dev_rows[composite_key] = (line_num, {k: row.get(k, "") for k in common_keys})
            
            # Now compare each changed key's last occurrences
            examples_collected = 0
            for composite_key in all_changed_keys:
                if composite_key not in needed_prod_rows or composite_key not in needed_dev_rows:
                    continue  # Skip if we don't have both sides
                
                prod_row = needed_prod_rows[composite_key]
                dev_line_num, dev_row = needed_dev_rows[composite_key]
                is_meaningful_candidate = composite_key in meaningful_change_keys
                has_meaningful_change = False
                
                for key in common_keys:
                    prod_val = prod_row.get(key, "")
                    dev_val = dev_row.get(key, "")
                    if prod_val != dev_val:
                        key_lower = key.lower()
                        is_excluded = 'inventory' in key_lower or 'availability' in key_lower or '_fdx' in key_lower
                        
                        # Only count meaningful columns in detailed_changes
                        if not is_excluded:
                            detailed_changes[key] += 1
                            has_meaningful_change = True
                
                # Add to examples if meaningful and under limit
                if is_meaningful_candidate and has_meaningful_change and examples_collected < self.max_examples:
                    # Get display key from dev_row
                    if len(self.primary_keys) == 1:
                        pk_val = dev_row.get(self.primary_keys[0])
                        display_key = "<missing>" if pk_val is None else str(pk_val)
                    else:
                        parts = []
                        for k in self.primary_keys:
                            v = dev_row.get(k)
                            parts.append("<missing>" if v is None else str(v))
                        display_key = "_".join(parts)
                    
                    prod_line_num = prod_index[composite_key][0]
                    
                    # Warn if display key looks suspicious
                    if display_key in ("None", "<missing>", ""):
                        logging.warning(f"    Suspicious primary key value '{display_key}' at dev line {dev_line_num}. Row keys: {list(dev_row.keys())[:5]}...")
                    
                    example_ids[display_key] = {
                        "prod_line_num": prod_line_num,
                        "dev_line_num": dev_line_num
                    }
                    if examples_collected == 0:
                        logging.debug(f"    First example: ID='{display_key}' (composite_key='{composite_key}') prod_line={prod_line_num}, dev_line={dev_line_num}")
                    examples_collected += 1
            
            # Clean up
            del needed_prod_rows
            del needed_dev_rows
            gc.collect()
        
        logging.debug(f"    Diff complete: +{rows_added} added, -{rows_removed} removed, ~{rows_changed_meaningful} meaningful changes, ~{rows_changed_excluded_only} excluded-only changes")
        
        result = {
            'rows_added': rows_added,
            'rows_removed': rows_removed,
            'rows_updated': rows_changed_meaningful,  # Only meaningful changes
            'rows_updated_excluded_only': rows_changed_excluded_only,  # inventory/availability/_fdx only
            'detailed_key_update_counts': dict(detailed_changes),
            'example_ids': dict(example_ids)
        }
        
        # Only include added/removed examples if there are any
        if example_ids_added:
            result['example_ids_added'] = dict(example_ids_added)
        if example_ids_removed:
            result['example_ids_removed'] = dict(example_ids_removed)
        
        return result


def calculate_in_stock_percentage_streaming(file_path: str, max_rows: Optional[int] = None) -> float:
    """Calculate in-stock percentage without loading entire file"""
    reader = StreamingCSVReader(file_path, max_rows=max_rows)
    headers = reader.read_headers()
    
    if 'availability' not in headers:
        return 0.0
    
    total = 0
    in_stock = 0
    
    for row in reader.iterate_rows():
        total += 1
        availability = (row.get('availability') or '').strip().lower()
        if availability == 'in stock':
            in_stock += 1
    
    if total == 0:
        return 0.0
    
    return round((in_stock / total) * 100, 2)


def generate_run_folder_name(params_file: str, args: argparse.Namespace) -> str:
    """Generate a unique folder name for this run based on timestamp and parameters"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get base name of params file (without extension)
    params_base = os.path.splitext(os.path.basename(params_file))[0]
    
    # Build flags string
    flags = []
    if args.primary_key != 'id':
        # Sanitize primary key for folder name
        pk_safe = args.primary_key.replace(',', '-').replace(' ', '')
        flags.append(f"pk_{pk_safe}")
    if args.timeout != 900:
        flags.append(f"t{args.timeout}")
    if args.max_examples != MAX_EXAMPLE_IDS:
        flags.append(f"ex{args.max_examples}")
    if args.diff_rows is not None:
        flags.append(f"diffrows{args.diff_rows}")
    if args.source_limit is not None:
        flags.append(f"srclimit{args.source_limit}")
    if args.verbose:
        flags.append("verbose")
    
    # Construct folder name
    parts = [timestamp, params_base]
    if flags:
        parts.append("_".join(flags))
    
    return "_".join(parts)


def save_run_metadata(run_dir: str, args: argparse.Namespace):
    """Save metadata about this run to the run directory"""
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "params_file": os.path.abspath(args.params_file) if args.params_file else None,
        "primary_key": args.primary_key,
        "timeout": args.timeout,
        "max_examples": args.max_examples,
        "max_concurrent_diffs": args.max_concurrent_diffs,
        "max_concurrent_fetches": args.max_concurrent_fetches,
        "diff_rows": args.diff_rows,
        "source_limit": args.source_limit,
        "verbose": args.verbose,
        "local_prod": args.local_prod if args.local_prod else None,
        "local_dev": args.local_dev if args.local_dev else None,
        "local_folder": args.local_folder if args.local_folder else None,
        "output_dir": args.output_dir,
        "summary_dir": args.summary_dir
    }
    
    metadata_path = os.path.join(run_dir, "run_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    return metadata_path


async def fetch_and_save_streaming(session, url, verify_ssl, test_case, environment, output_dir, verbose):
    """Fetch URL and stream response directly to file"""
    if verbose:
        logging.info(f"[Test Case {test_case} - {environment.upper()}] Requesting URL: {url}")
    
    status_code = None
    shop_name = None
    
    # Parse URL and extract parameters
    parsed_url = urlparse(url)
    query_params = parse_qsl(parsed_url.query)
    
    # Parse params into JSON-friendly structure
    request_params = parse_url_params_to_json(parsed_url.query)
    
    # Extract shop name for backward compatibility
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
            
            # Stream response to file
            with open(file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
            
            # Check if file is gzipped and decompress if needed
            with open(file_path, 'rb') as f:
                header = f.read(2)
                f.seek(0)
                
                if header == b'\x1f\x8b':
                    # File is gzipped, decompress it
                    content = f.read()
                    try:
                        decompressed = gzip.decompress(content)
                        with open(file_path, 'wb') as out_f:
                            out_f.write(decompressed)
                        if verbose:
                            logging.info(f"[Test Case {test_case} - {environment.upper()}] Decompressed gzip file")
                    except Exception as e:
                        logging.error(f"[Test Case {test_case} - {environment.upper()}] Error decompressing: {e}")
            
            if status_code != 200:
                logging.warning(f"[Test Case {test_case} - {environment.upper()}] Non-200 response: {status_code}")
            else:
                logging.info(f"[Test Case {test_case} - {environment.upper()}] Received response with status {status_code}")
    
    except asyncio.TimeoutError:
        logging.error(f"[Test Case {test_case} - {environment.upper()}] Timeout for URL: {url}")
        status_code = "timeout"
        with open(file_path, 'w') as f:
            f.write(f"TimeoutError: No response received from URL: {url}")
    
    except Exception as e:
        logging.error(f"[Test Case {test_case} - {environment.upper()}] Error: {str(e)}")
        status_code = "error"
        with open(file_path, 'w') as f:
            f.write(f"Error: {str(e)}")
    
    if verbose:
        logging.info(f"[Test Case {test_case} - {environment.upper()}] Wrote response to {file_path}")
    
    # For error responses, read just enough to return error message
    response_text = ""
    if status_code not in [200, "timeout", "error"]:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            response_text = f.read(1000)
    
    return (test_case, environment, file_path, status_code, response_text, shop_name, request_params)


async def main(args: argparse.Namespace):
    """Main function with memory-optimized processing"""
    run_start_time = datetime.now()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')
    
    primary_keys = [k.strip() for k in args.primary_key.split(",")]
    logging.info(f"Using primary key(s): {primary_keys}")
    
    differ = EfficientDiffer(primary_keys, max_examples=args.max_examples, max_rows=args.diff_rows)
    
    if args.diff_rows:
        logging.info(f"Row limit: {args.diff_rows} rows per file")
    
    # Local folder mode
    if args.local_folder:
        logging.info(f"Running folder diff mode on: {args.local_folder}")
        
        # Create summary dir
        os.makedirs(args.summary_dir, exist_ok=True)
        
        pattern = re.compile(r"^(prod|dev)_response_(\d+)_(\w+)\.txt$")
        files = os.listdir(args.local_folder)
        
        groups = {}
        for filename in files:
            match = pattern.match(filename)
            if match:
                env = match.group(1)
                test_case = match.group(2)
                hash_value = match.group(3)
                key = (test_case, hash_value)
                if key not in groups:
                    groups[key] = {}
                groups[key][env] = os.path.join(args.local_folder, filename)
        
        # Determine concurrency
        logging.info(f"Max concurrent diffs: {args.max_concurrent_diffs}")
        
        async def process_folder_diff(key, env_files):
            """Process a single diff from folder mode"""
            test_case, hash_value = key
            test_summary = {"test_case": test_case, "hash": hash_value}
            
            if "prod" not in env_files or "dev" not in env_files:
                missing = "prod" if "prod" not in env_files else "dev"
                test_summary["error"] = {"msg": f"Missing {missing} file"}
                test_summary["non_200"] = True
                return test_summary
            
            diff_start_time = datetime.now()
            
            try:
                # Run diff in thread pool
                diff_stats = await asyncio.to_thread(
                    differ.compute_diff, env_files["prod"], env_files["dev"]
                )
                test_summary.update(diff_stats)
                
                # Calculate in-stock percentages in parallel
                async def calc_in_stock(file_path):
                    reader = StreamingCSVReader(file_path, max_rows=args.diff_rows)
                    if 'availability' in reader.read_headers():
                        return await asyncio.to_thread(
                            calculate_in_stock_percentage_streaming, file_path, args.diff_rows
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
        
        # Process diffs with semaphore for concurrency control
        semaphore = asyncio.Semaphore(args.max_concurrent_diffs)
        
        async def bounded_diff(key, env_files):
            async with semaphore:
                return await process_folder_diff(key, env_files)
        
        # Run all diffs in parallel
        tasks = [bounded_diff(key, env_files) for key, env_files in groups.items()]
        results = await asyncio.gather(*tasks)
        
        # Sort results by test case
        results.sort(key=lambda x: int(x.get("test_case", 0)))
        
        # Calculate total runtime
        total_runtime = (datetime.now() - run_start_time).total_seconds()
        
        overall_summary = OrderedDict()
        overall_summary["count"] = len(results)
        overall_summary["total_runtime_seconds"] = round(total_runtime, 2)
        overall_summary["test_cases"] = results
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        summary_filename = os.path.join(args.summary_dir, f"folder_diffs_summary_{timestamp}.json")
        with open(summary_filename, "w") as f:
            json.dump(overall_summary, f, indent=2)
        logging.info(f"Summary written to {summary_filename}")
        logging.info(f"Total runtime: {total_runtime:.2f}s")
        
        return
    
    # Local file comparison mode
    if args.local_prod and args.local_dev:
        logging.info(f"Comparing local files:\n  Prod: {args.local_prod}\n  Dev: {args.local_dev}")
        
        # Create summary dir
        os.makedirs(args.summary_dir, exist_ok=True)
        
        try:
            diff_start_time = datetime.now()
            diff_stats = differ.compute_diff(args.local_prod, args.local_dev)
            
            summary_obj = OrderedDict()
            summary_obj["mode"] = "local"
            summary_obj["prod_file"] = os.path.basename(args.local_prod)
            summary_obj["dev_file"] = os.path.basename(args.local_dev)
            summary_obj.update(diff_stats)
            
            # Calculate in-stock percentages
            prod_reader = StreamingCSVReader(args.local_prod, max_rows=args.diff_rows)
            if 'availability' in prod_reader.read_headers():
                summary_obj["prod_in_stock_percentage"] = calculate_in_stock_percentage_streaming(args.local_prod, args.diff_rows)
            
            dev_reader = StreamingCSVReader(args.local_dev, max_rows=args.diff_rows)
            if 'availability' in dev_reader.read_headers():
                summary_obj["dev_in_stock_percentage"] = calculate_in_stock_percentage_streaming(args.local_dev, args.diff_rows)
            
            if "prod_in_stock_percentage" in summary_obj and "dev_in_stock_percentage" in summary_obj:
                summary_obj["in_stock_percentage_difference"] = round(
                    abs(summary_obj["prod_in_stock_percentage"] - summary_obj["dev_in_stock_percentage"]), 2
                )
            
            # Add runtime
            diff_duration = (datetime.now() - diff_start_time).total_seconds()
            summary_obj["runtime_seconds"] = round(diff_duration, 2)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_filename = os.path.join(args.summary_dir, f"diffs_summary_local_{timestamp}.json")
            with open(summary_filename, "w") as f:
                json.dump(summary_obj, f, indent=2)
            logging.info(f"Local diff summary written to {summary_filename}")
            logging.info(f"Runtime: {diff_duration:.2f}s")
            
        except Exception as e:
            logging.error(f"Error comparing local files: {e}")
        
        return
    
    # URL mode - validate params file first before creating directories
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
    
    # Deduplicate by unique identifier parameters (shop_name, store_hash, etc.)
    # Add new deduplication keys here as needed
    DEDUP_KEYS = [
        #"connection_info[shop_name]",
        "connection_info[store_hash]",
        # Add more keys here as needed, e.g.:
        # "connection_info[account_id]",
    ]
    
    def extract_dedup_key(params: str) -> Optional[str]:
        """Extract a unique identifier from URL params for deduplication.
        Returns the first matching dedup key value found."""
        parsed = parse_qsl(params.lstrip('?'), keep_blank_values=True)
        param_dict = {k: v for k, v in parsed}
        
        for dedup_key in DEDUP_KEYS:
            if dedup_key in param_dict and param_dict[dedup_key]:
                return f"{dedup_key}={param_dict[dedup_key]}"
        return None
    
    original_count = len(param_list)
    seen_identifiers = set()
    deduplicated_params = []
    duplicates_removed = 0
    
    for params in param_list:
        dedup_id = extract_dedup_key(params)
        if dedup_id:
            if dedup_id in seen_identifiers:
                duplicates_removed += 1
                continue
            seen_identifiers.add(dedup_id)
        deduplicated_params.append(params)
    
    if duplicates_removed > 0:
        logging.info(f"Deduplicated: {original_count} → {len(deduplicated_params)} test cases ({duplicates_removed} duplicates removed)")
        param_list = deduplicated_params
    
    # Apply limit if specified
    if args.source_limit and args.source_limit > 0:
        original_count = len(param_list)
        param_list = param_list[:args.source_limit]
        logging.info(f"Limiting to {len(param_list)} of {original_count} test cases (--source-limit {args.source_limit})")
    
    total_cases = len(param_list)
    total_tasks = total_cases * 2
    logging.info(f"Found {total_cases} test cases. Total URL calls: {total_tasks}")
    
    # Now create directories since params are valid
    run_folder_name = generate_run_folder_name(args.params_file, args)
    run_output_dir = os.path.join(args.output_dir, run_folder_name)
    
    logging.info(f"Creating run folder: {run_output_dir}")
    os.makedirs(run_output_dir, exist_ok=True)
    os.makedirs(args.summary_dir, exist_ok=True)
    
    # Save run metadata
    metadata_path = save_run_metadata(run_output_dir, args)
    logging.info(f"Run metadata saved to {metadata_path}")
    
    # Semaphores for concurrency control
    logging.info(f"Max concurrent fetches: {args.max_concurrent_fetches}")
    logging.info(f"Max concurrent diffs: {args.max_concurrent_diffs}")
    
    # Initialize progress display (replaces scrolling logs with in-place updates)
    progress = ProgressDisplay(total_fetches=total_tasks, total_diffs=total_cases)
    progress.initial_draw()
    
    # Parallel processing state
    results = {}  # Stores fetch results: test_case -> {env -> info}
    diff_results = {}  # Stores diff results: test_case -> summary
    pending_diffs = set()  # Test cases with diffs in progress
    
    fetch_semaphore = asyncio.Semaphore(args.max_concurrent_fetches)
    diff_semaphore = asyncio.Semaphore(args.max_concurrent_diffs)
    
    async def process_diff(test_case: int, prod_info: dict, dev_info: dict) -> dict:
        """Process a single diff - runs in thread pool for CPU-bound work"""
        async with diff_semaphore:
            progress.log(f"[Test {test_case}] Starting diff...")
            
            test_summary = {"test_case": test_case}
            test_summary["prod_status"] = prod_info.get("status")
            test_summary["dev_status"] = dev_info.get("status")
            
            shop_name = prod_info.get("shop_name") or dev_info.get("shop_name")
            if shop_name:
                test_summary["shop_name"] = shop_name
            
            # Include request parameters (URL-decoded as JSON)
            request_params = prod_info.get("request_params") or dev_info.get("request_params")
            if request_params:
                test_summary["request_params"] = request_params
            
            # Check for non-200 responses
            if prod_info.get("status") != 200 or dev_info.get("status") != 200:
                error_obj = {"msg": "Non-200 responses detected", "response": {}}
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
            
            # Perform diff in thread pool (CPU-bound operation)
            try:
                start_time = datetime.now()
                
                # Run CPU-bound diff in thread pool
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
                    progress.log(f"[Test {test_case}] +{rows_added} added, -{rows_removed} removed, ~{rows_changed} changed")
                else:
                    progress.log(f"[Test {test_case}] No differences")
                
                # Calculate in-stock percentages in thread pool
                async def calc_in_stock(file_path):
                    reader = StreamingCSVReader(file_path, max_rows=args.diff_rows)
                    if 'availability' in reader.read_headers():
                        return await asyncio.to_thread(
                            calculate_in_stock_percentage_streaming, file_path, args.diff_rows
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
                
                # Add runtime to summary
                total_test_duration = (datetime.now() - start_time).total_seconds()
                test_summary["runtime_seconds"] = round(total_test_duration, 2)
                
            except Exception as e:
                progress.log(f"[Test {test_case}] ✗ Error: {str(e)}")
                progress.increment_errors()
                test_summary["error"] = {"msg": str(e)}
                test_summary["non_200"] = True
                # Still track runtime even for errors
                error_duration = (datetime.now() - start_time).total_seconds()
                test_summary["runtime_seconds"] = round(error_duration, 2)
            
            return test_summary
    
    async def on_diff_complete(task: asyncio.Task, test_case: int):
        """Callback when a diff task completes"""
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
    
    def maybe_start_diff(test_case: int):
        """Start a diff if both prod and dev are ready"""
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
            task.add_done_callback(lambda t: asyncio.create_task(on_diff_complete(t, test_case)))
            return task
        return None
    
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    diff_tasks = []
    
    async def process_test_case(session, idx: int, params: str):
        """Process a single test case with staggered prod/dev fetches.
        
        Concurrency is controlled by fetch_semaphore.
        Within each test case, prod and dev are fetched sequentially 
        to avoid Shopify bulk operation conflicts.
        Half start with prod, half start with dev to balance load.
        """
        prod_url = f"{PRODUCTION_BASE_URL}{params}"
        dev_url = f"{DEVELOPMENT_BASE_URL}{params}"
        
        # Alternate which environment goes first to balance load
        # Even test cases: prod first, odd test cases: dev first
        if idx % 2 == 0:
            first_env, first_url = "prod", prod_url
            second_env, second_url = "dev", dev_url
        else:
            first_env, first_url = "dev", dev_url
            second_env, second_url = "prod", prod_url
        
        # Use semaphore to limit concurrent fetch operations
        async with fetch_semaphore:
            progress.log(f"[Test {idx}] Starting ({first_env} first)...")
            
            # Fetch first environment
            test_case1, env1, file_path1, status1, response_text1, shop_name1, request_params1 = await fetch_and_save_streaming(
                session, first_url, verify_ssl=False, test_case=idx,
                environment=first_env, output_dir=run_output_dir, verbose=args.verbose
            )
            progress.increment_fetches()
            progress.log(f"[Test {idx}] {first_env.upper()} done (status={status1})")
            
            # Fetch second environment (only after first completes)
            test_case2, env2, file_path2, status2, response_text2, shop_name2, request_params2 = await fetch_and_save_streaming(
                session, second_url, verify_ssl=False, test_case=idx,
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
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # All test cases are scheduled, but fetch_semaphore controls how many actively fetch
        # Within each test case, prod/dev requests are sequential to avoid Shopify bulk op conflicts
        tasks = [process_test_case(session, idx, params) for idx, params in enumerate(param_list)]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Wait for any remaining diffs to complete
    if diff_tasks:
        progress.log(f"Waiting for {len(pending_diffs)} remaining diffs...")
        await asyncio.gather(*diff_tasks, return_exceptions=True)
    
    # Clear progress display before final output
    progress.finish()
    
    logging.info("All fetches and diffs completed!")
    
    # Calculate total runtime
    total_runtime = (datetime.now() - run_start_time).total_seconds()
    
    # Build final summary in order
    overall_summary = OrderedDict()
    overall_summary["count"] = 0
    overall_summary["run_folder"] = run_folder_name
    overall_summary["total_runtime_seconds"] = round(total_runtime, 2)
    overall_summary["test_cases"] = []
    
    for test_case in sorted(diff_results.keys(), key=lambda x: int(x)):
        overall_summary["test_cases"].append(diff_results[test_case])
    
    # Write summaries
    overall_summary["count"] = len(overall_summary["test_cases"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Overall summary
    overall_filename = os.path.join(args.summary_dir, f"diffs_summary_{timestamp}.json")
    with open(overall_filename, "w") as f:
        json.dump(overall_summary, f, indent=2)
    logging.info(f"Overall summary written to {overall_filename}")
    
    # Also save summary to run folder for easy reference
    run_summary_path = os.path.join(run_output_dir, "summary.json")
    with open(run_summary_path, "w") as f:
        json.dump(overall_summary, f, indent=2)
    logging.info(f"Run summary also saved to {run_summary_path}")
    
    # Updates summary
    updates_summary = OrderedDict()
    updates_summary["count"] = 0
    updates_summary["run_folder"] = run_folder_name
    updates_summary["total_runtime_seconds"] = round(total_runtime, 2)
    updates_summary["test_cases"] = []
    
    # Errors summary
    errors_summary = OrderedDict()
    errors_summary["count"] = 0
    errors_summary["run_folder"] = run_folder_name
    errors_summary["total_runtime_seconds"] = round(total_runtime, 2)
    errors_summary["test_cases"] = []
    
    for test in overall_summary["test_cases"]:
        if test.get("non_200") or test.get("error"):
            errors_summary["test_cases"].append(test)
        elif test.get("rows_added", 0) > 0 or test.get("rows_removed", 0) > 0 or test.get("rows_updated", 0) > 0:
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
    logging.info(f"{'='*60}")


def create_parser():
    """Create argument parser with enhanced help visuals"""
    
    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        def __init__(self, prog, indent_increment=2, max_help_position=40, width=100):
            super().__init__(prog, indent_increment, max_help_position, width)
        
        def _format_action_invocation(self, action):
            if not action.option_strings:
                return super()._format_action_invocation(action)
            parts = []
            if action.option_strings:
                parts.append(', '.join(action.option_strings))
            return '  '.join(parts)
    
    banner = r"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ██████╗ ██╗ █████╗ ███████╗    ██████╗ ██╗███████╗███████╗                 ║
║   ██╔══██╗██║██╔══██╗╚══███╔╝    ██╔══██╗██║██╔════╝██╔════╝                 ║
║   ██║  ██║██║███████║  ███╔╝     ██║  ██║██║█████╗  █████╗                   ║
║   ██║  ██║██║██╔══██║ ███╔╝      ██║  ██║██║██╔══╝  ██╔══╝                   ║
║   ██████╔╝██║██║  ██║███████╗    ██████╔╝██║██║     ██║                      ║
║   ╚═════╝ ╚═╝╚═╝  ╚═╝╚══════╝    ╚═════╝ ╚═╝╚═╝     ╚═╝                      ║
║                                                                              ║
║            ██████╗██╗  ██╗███████╗ ██████╗██╗  ██╗███████╗██████╗            ║
║           ██╔════╝██║  ██║██╔════╝██╔════╝██║ ██╔╝██╔════╝██╔══██╗           ║
║           ██║     ███████║█████╗  ██║     █████╔╝ █████╗  ██████╔╝           ║
║           ██║     ██╔══██║██╔══╝  ██║     ██╔═██╗ ██╔══╝  ██╔══██╗           ║
║           ╚██████╗██║  ██║███████╗╚██████╗██║  ██╗███████╗██║  ██║           ║
║            ╚═════╝╚═╝  ╚═╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝           ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    
    description = f"""{banner}
  Memory-optimized CSV diff tool with streaming processing.
  
  Compare CSV responses between production and development environments,
  with support for local file comparison, folder-based batch processing,
  and URL-based automated testing.

┌─────────────────────────────────────────────────────────────────────────────┐
│  MODES OF OPERATION                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. URL Mode (default)                                                      │
│     Fetches responses from prod/dev URLs and compares them.                 │
│     Requires: --params-file with URL parameters                             │
│     Output: Creates timestamped run folder with all responses               │
│                                                                             │
│  2. Local File Mode                                                         │
│     Compares two local CSV files directly.                                  │
│     Requires: --local-prod and --local-dev                                  │
│                                                                             │
│  3. Folder Mode                                                             │
│     Batch processes all prod/dev file pairs in a folder.                    │
│     Requires: --local-folder                                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
"""
    
    epilog = """
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXAMPLES                                                                   │
└─────────────────────────────────────────────────────────────────────────────┘

  Compare local files:
  ─────────────────────
    %(prog)s --local-prod production.csv --local-dev development.csv

  Compare with composite primary key:
  ────────────────────────────────────
    %(prog)s --local-prod prod.csv --local-dev dev.csv --primary-key "sku,locale"

  Run URL-based tests with custom timeout:
  ─────────────────────────────────────────
    %(prog)s --params-file test_params.csv --timeout 1200 --verbose

  Batch process a folder of response files:
  ──────────────────────────────────────────
    %(prog)s --local-folder ./responses --primary-key id

  Limit example IDs in output:
  ─────────────────────────────
    %(prog)s --local-prod prod.csv --local-dev dev.csv --max-examples 5

  Test with only first 10 URLs from params file:
  ───────────────────────────────────────────────
    %(prog)s --params-file all_tests.csv --source-limit 10

┌─────────────────────────────────────────────────────────────────────────────┐
│  OUTPUT STRUCTURE                                                           │
└─────────────────────────────────────────────────────────────────────────────┘

  URL Mode creates a unique folder per run:
  
    responses/
    └── 20241126_143052_params_pk_sku-locale/
        ├── run_metadata.json          # Run configuration
        ├── summary.json               # Copy of diff summary
        ├── prod_response_0_abc123.txt # Response files
        ├── dev_response_0_abc123.txt
        └── ...

  Summary files are written to the summary directory:
  
    summaries/
    ├── diffs_summary_<timestamp>.json         # All results
    ├── diffs_summary_updates_<timestamp>.json # Only differences
    └── diffs_summary_errors_<timestamp>.json  # Only errors

┌─────────────────────────────────────────────────────────────────────────────┐
│  MEMORY OPTIMIZATIONS                                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  • True streaming CSV processing (no full file loading)
  • Hash-based row comparison (stores hashes, not full row data)
  • Cached headers and row counts (avoids redundant file reads)
  • Incremental garbage collection between test cases
  • Two-pass algorithm: quick hash comparison, then detailed diff

┌─────────────────────────────────────────────────────────────────────────────┐
│  PARALLEL PROCESSING                                                        │
└─────────────────────────────────────────────────────────────────────────────┘

  • URL fetches run concurrently (controlled by --max-concurrent-fetches, default 200)
  • Within each test case, prod/dev are fetched sequentially to avoid bulk op conflicts
  • Diffs start immediately when both prod & dev files are ready
  • Multiple diffs run in parallel (controlled by --max-concurrent-diffs, default 10)
  • In-stock percentage calculations run in parallel per test case

┌─────────────────────────────────────────────────────────────────────────────┐
│  NOTES                                                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  • Auto-detects CSV delimiters (comma or tab), even if header and data differ
  • Gzipped responses are automatically decompressed
  • rows_updated counts only meaningful changes (excludes inventory/availability/_fdx)
  • rows_updated_excluded_only counts rows where ONLY those columns changed
  • Example IDs only include rows with meaningful changes
  • Run folders include timestamp + params file + flags for easy identification
  • Use --diff-rows for quick testing on large files (processes first N rows only)
  • Use --source-limit to test a subset of URLs from the params file
  • Requests are deduplicated by shop_name/store_hash to avoid redundant API calls
"""
    
    parser = argparse.ArgumentParser(
        prog='diaz_diff_checker.py',
        description=description,
        epilog=epilog,
        formatter_class=CustomHelpFormatter,
        add_help=False
    )
    
    # Help group
    help_group = parser.add_argument_group(
        '📖 Help',
        'Display help information'
    )
    help_group.add_argument(
        '-h', '--help',
        action='help',
        default=argparse.SUPPRESS,
        help='Show this help message and exit'
    )
    
    # Core options
    core_group = parser.add_argument_group(
        '⚙️  Core Options',
        'Primary configuration for diff operations'
    )
    core_group.add_argument(
        '--primary-key', '-k',
        type=str,
        default='id',
        metavar='KEY',
        help='Primary key column(s) for row matching.\n'
             'Use comma-separated values for composite keys.\n'
             'Example: "id" or "sku,locale"\n'
             '(default: id)'
    )
    core_group.add_argument(
        '--timeout', '-t',
        type=int,
        default=900,
        metavar='SECS',
        help='HTTP request timeout in seconds.\n'
             '(default: 900 = 15 minutes)'
    )
    core_group.add_argument(
        '--max-examples', '-m',
        type=int,
        default=MAX_EXAMPLE_IDS,
        metavar='NUM',
        help='Maximum number of example IDs to include\n'
             'in output for tracking row differences.\n'
             f'(default: {MAX_EXAMPLE_IDS})'
    )
    core_group.add_argument(
        '--max-concurrent-diffs', '-c',
        type=int,
        default=10,
        metavar='NUM',
        help='Maximum number of diffs to run in parallel.\n'
             '(default: 10)'
    )
    core_group.add_argument(
        '--max-concurrent-fetches', '-F',
        type=int,
        default=250,
        metavar='NUM',
        help='Maximum number of concurrent URL fetch operations.\n'
             'Each test case requires 2 fetches (prod + dev).\n'
             '(default: 200)'
    )
    core_group.add_argument(
        '--diff-rows', '-r',
        type=int,
        default=None,
        metavar='NUM',
        dest='diff_rows',
        help='Maximum rows to process per CSV file.\n'
             'Useful for quick testing on large files.\n'
             '(default: no limit)'
    )
    core_group.add_argument(
        '--source-limit', '-l',
        type=int,
        default=None,
        metavar='NUM',
        dest='source_limit',
        help='Limit number of test cases from params file.\n'
             'Useful for quick testing with subset of URLs.\n'
             '(default: no limit, process all)'
    )
    
    # Input sources
    input_group = parser.add_argument_group(
        '📥 Input Sources',
        'Specify input files or folders (choose one mode)'
    )
    input_group.add_argument(
        '--params-file', '-p',
        type=str,
        default='params.csv',
        metavar='FILE',
        help='CSV file containing URL parameters.\n'
             'Must have a "params" column.\n'
             '(default: params.csv)'
    )
    input_group.add_argument(
        '--local-prod',
        type=str,
        default='',
        metavar='FILE',
        help='Local production CSV file to compare.\n'
             'Use with --local-dev for local mode.'
    )
    input_group.add_argument(
        '--local-dev',
        type=str,
        default='',
        metavar='FILE',
        help='Local development CSV file to compare.\n'
             'Use with --local-prod for local mode.'
    )
    input_group.add_argument(
        '--local-folder', '-f',
        type=str,
        default='',
        metavar='DIR',
        help='Folder containing response file pairs.\n'
             'Files must match pattern:\n'
             '  prod_response_<N>_<hash>.txt\n'
             '  dev_response_<N>_<hash>.txt'
    )
    
    # Output configuration
    output_group = parser.add_argument_group(
        '📤 Output Configuration',
        'Control where results are saved'
    )
    output_group.add_argument(
        '--output-dir', '-o',
        type=str,
        default='responses',
        metavar='DIR',
        help='Base directory for response files.\n'
             'A timestamped subfolder is created per run.\n'
             '(default: responses)'
    )
    output_group.add_argument(
        '--summary-dir', '-s',
        type=str,
        default='summaries',
        metavar='DIR',
        help='Directory for JSON summary reports.\n'
             '(default: summaries)'
    )
    
    # Debugging
    debug_group = parser.add_argument_group(
        '🔍 Debugging',
        'Options for troubleshooting and verbose output'
    )
    debug_group.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose/debug output.\n'
             'Shows detailed progress and timing info.'
    )
    
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    asyncio.run(main(args))