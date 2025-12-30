"""
Memory-efficient diff calculator using hash-based comparison.

This module provides efficient CSV comparison that:
- Uses MD5 hashes for fast row comparison (stores hashes, not full rows)
- Performs two-pass algorithm: quick hash comparison, then detailed diff
- Separates "meaningful" changes from inventory/availability changes
- Tracks line numbers for debugging
- Supports composite primary keys
"""

import gc
import hashlib
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from .csv_reader import StreamingCSVReader
from .config import DEFAULT_MAX_EXAMPLES, EXCLUDED_COLUMN_PATTERNS


class EfficientDiffer:
    """
    Memory-efficient diff calculator for CSV files.
    
    Uses a two-pass algorithm:
    1. First pass: Build hash indexes of both files for quick comparison
    2. Second pass: Generate detailed diffs only for changed rows
    
    Features:
        - Hash-based comparison (stores hashes, not full row data)
        - Composite primary key support
        - Separates meaningful changes from excluded column changes
        - Incremental garbage collection
        - Example ID collection with line numbers
    
    Example:
        >>> differ = EfficientDiffer(primary_keys=["id"])
        >>> result = differ.compute_diff("prod.csv", "dev.csv")
        >>> print(f"Rows updated: {result['rows_updated']}")
    
    Args:
        primary_keys: List of column names that uniquely identify rows
        max_examples: Maximum example IDs to collect for each change type
        max_rows: Optional limit on rows to process per file
        excluded_patterns: Column name patterns to exclude from "meaningful" changes
    """
    
    def __init__(
        self,
        primary_keys: List[str],
        max_examples: int = DEFAULT_MAX_EXAMPLES,
        max_rows: Optional[int] = None,
        excluded_patterns: Optional[List[str]] = None,
    ):
        self.primary_keys = primary_keys
        self.max_examples = max_examples
        self.max_rows = max_rows
        self.excluded_patterns = excluded_patterns or EXCLUDED_COLUMN_PATTERNS
    
    def _is_excluded_column(self, column_name: str) -> bool:
        """Check if a column should be excluded from meaningful change detection."""
        col_lower = column_name.lower()
        return any(pattern.lower() in col_lower for pattern in self.excluded_patterns)
    
    def _make_composite_key(self, row: Dict[str, str]) -> str:
        """Create a composite key from primary key values."""
        return "||".join(str(row.get(k, "")) for k in self.primary_keys)
    
    def _get_primary_key_display(self, row: Dict[str, str]) -> str:
        """Get a display-friendly primary key (single value or composite)."""
        if len(self.primary_keys) == 1:
            value = row.get(self.primary_keys[0])
            return "<missing>" if value is None else str(value)
        
        parts = []
        for k in self.primary_keys:
            value = row.get(k)
            parts.append("<missing>" if value is None else str(value))
        return "_".join(parts)
    
    def _hash_row(self, row: Dict[str, str], keys: Set[str]) -> str:
        """Create an MD5 hash of row values for the given keys."""
        # Sort keys for consistent hashing
        values = "|".join(str(row.get(k, "")) for k in sorted(keys))
        return hashlib.md5(values.encode('utf-8')).hexdigest()
    
    def compute_diff(self, prod_file: str, dev_file: str) -> Dict:
        """
        Compute differences between two CSV files.
        
        Args:
            prod_file: Path to the production/baseline CSV file
            dev_file: Path to the development/comparison CSV file
            
        Returns:
            Dictionary containing:
                - rows_added: Count of rows in dev but not prod
                - rows_removed: Count of rows in prod but not dev
                - rows_updated: Count of rows with meaningful changes
                - rows_updated_excluded_only: Count of rows with only excluded changes
                - detailed_key_update_counts: Per-column change counts
                - example_ids: Sample IDs of changed rows with line numbers
                - example_ids_added: Sample IDs of added rows
                - example_ids_removed: Sample IDs of removed rows
                - common_keys: Columns present in both files
                - prod_only_keys: Columns only in production
                - dev_only_keys: Columns only in development
                - prod_row_count: Total rows in production file
                - dev_row_count: Total rows in development file
                
        Raises:
            ValueError: If primary key columns are missing from either file
        """
        prod_reader = StreamingCSVReader(prod_file, max_rows=self.max_rows)
        dev_reader = StreamingCSVReader(dev_file, max_rows=self.max_rows)
        
        # Get headers (cached)
        prod_headers = set(prod_reader.read_headers())
        dev_headers = set(dev_reader.read_headers())
        
        logging.debug(f"    Prod headers: {sorted(prod_headers)}")
        logging.debug(f"    Dev headers: {sorted(dev_headers)}")
        logging.debug(f"    Primary key(s): {self.primary_keys}")
        
        # Validate primary keys exist
        missing_prod = [k for k in self.primary_keys if k not in prod_headers]
        if missing_prod:
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
        
        # Compute column sets
        common_keys = prod_headers & dev_headers
        prod_only_keys = prod_headers - dev_headers
        dev_only_keys = dev_headers - prod_headers
        
        # Identify excluded columns
        excluded_columns = {k for k in common_keys if self._is_excluded_column(k)}
        comparison_keys = common_keys - excluded_columns
        
        # Process differences
        diff_stats = self._process_differences(
            prod_reader, dev_reader, common_keys, comparison_keys
        )
        
        # Add metadata
        diff_stats.update({
            'common_keys': sorted(list(common_keys)),
            'prod_only_keys': sorted(list(prod_only_keys)),
            'dev_only_keys': sorted(list(dev_only_keys)),
            'prod_row_count': prod_reader.count_rows(),
            'dev_row_count': dev_reader.count_rows(),
        })
        
        return diff_stats
    
    def _process_differences(
        self,
        prod_reader: StreamingCSVReader,
        dev_reader: StreamingCSVReader,
        common_keys: Set[str],
        comparison_keys: Set[str],
    ) -> Dict:
        """
        Process differences between two files using hash-based comparison.
        
        Three-phase algorithm:
        1. Build prod index with hashes
        2. Build dev index, detect added rows, find changed rows via hash comparison
        3. Second pass on changed rows to collect detailed changes
        """
        # Phase 1: Build production index
        # Format: composite_key -> (line_num, full_hash, comparison_hash, display_key)
        prod_index: Dict[str, Tuple[int, str, str, str]] = {}
        total_prod_rows = prod_reader.count_rows()
        
        logging.debug(f"    Building prod index ({total_prod_rows} rows)...")
        
        rows_processed = 0
        for line_num, row in prod_reader.iterate_rows_with_line_numbers():
            composite_key = self._make_composite_key(row)
            full_hash = self._hash_row(row, common_keys)
            comp_hash = (
                self._hash_row(row, comparison_keys) 
                if comparison_keys else full_hash
            )
            display_key = self._get_primary_key_display(row)
            
            # Last occurrence wins for duplicates
            prod_index[composite_key] = (line_num, full_hash, comp_hash, display_key)
            
            rows_processed += 1
            if rows_processed % 50000 == 0:
                logging.debug(f"    Processed {rows_processed}/{total_prod_rows} prod rows...")
        
        # Phase 2: Build dev index and detect changes
        logging.debug(f"    Building dev index and comparing...")
        
        total_dev_rows = dev_reader.count_rows()
        
        # Initialize counters and collections
        rows_added = 0
        rows_removed = 0
        rows_changed_meaningful = 0
        rows_changed_excluded_only = 0
        
        detailed_changes: Dict[str, int] = defaultdict(int)
        example_ids: Dict[str, Dict] = {}
        example_ids_added: Dict[str, Dict] = {}
        example_ids_removed: Dict[str, Dict] = {}
        
        # Dev index: composite_key -> (line_num, full_hash, comparison_hash)
        dev_index: Dict[str, Tuple[int, str, str]] = {}
        all_changed_keys: Set[str] = set()
        meaningful_change_keys: Set[str] = set()
        excluded_only_keys: Set[str] = set()
        
        added_examples_collected = 0
        added_keys: Set[str] = set()
        rows_processed = 0
        
        # First pass: Build dev index (last occurrence wins)
        for line_num, row in dev_reader.iterate_rows_with_line_numbers():
            composite_key = self._make_composite_key(row)
            full_hash = self._hash_row(row, common_keys)
            comp_hash = (
                self._hash_row(row, comparison_keys) 
                if comparison_keys else full_hash
            )
            dev_index[composite_key] = (line_num, full_hash, comp_hash)
            
            # Track added rows (keys not in prod)
            if composite_key not in prod_index:
                if composite_key not in added_keys:
                    rows_added += 1
                    added_keys.add(composite_key)
                    # Collect example for added row
                    if added_examples_collected < self.max_examples:
                        display_key = self._get_primary_key_display(row)
                        example_ids_added[display_key] = {"dev_line_num": line_num}
                        added_examples_collected += 1
            
            rows_processed += 1
            if rows_processed % 50000 == 0:
                logging.debug(f"    Processed {rows_processed}/{total_dev_rows} dev rows...")
        
        # Compare hashes to identify changes
        for composite_key, (dev_line, dev_full_hash, dev_comp_hash) in dev_index.items():
            if composite_key in prod_index:
                prod_line, prod_full_hash, prod_comp_hash, _ = prod_index[composite_key]
                if dev_full_hash != prod_full_hash:
                    all_changed_keys.add(composite_key)
                    # Categorize: meaningful vs excluded-only
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
                if removed_examples_collected < self.max_examples:
                    example_ids_removed[display_key] = {"prod_line_num": prod_line}
                    removed_examples_collected += 1
        
        logging.debug(
            f"    Found {len(meaningful_change_keys)} meaningful changes, "
            f"{len(excluded_only_keys)} excluded-only changes"
        )
        
        # Phase 3: Get detailed changes for changed rows (second pass)
        if all_changed_keys:
            # Build lookup of needed prod rows (last occurrence to match index)
            needed_prod_rows: Dict[str, Dict[str, str]] = {}
            for line_num, row in prod_reader.iterate_rows_with_line_numbers():
                composite_key = self._make_composite_key(row)
                if composite_key in all_changed_keys:
                    needed_prod_rows[composite_key] = {
                        k: row.get(k, "") for k in common_keys
                    }
            
            # Second pass on dev (last occurrence)
            needed_dev_rows: Dict[str, Tuple[int, Dict[str, str]]] = {}
            for line_num, row in dev_reader.iterate_rows_with_line_numbers():
                composite_key = self._make_composite_key(row)
                if composite_key in all_changed_keys:
                    needed_dev_rows[composite_key] = (
                        line_num, 
                        {k: row.get(k, "") for k in common_keys}
                    )
            
            # Compare each changed row
            examples_collected = 0
            for composite_key in all_changed_keys:
                if composite_key not in needed_prod_rows:
                    continue
                if composite_key not in needed_dev_rows:
                    continue
                
                prod_row = needed_prod_rows[composite_key]
                dev_line_num, dev_row = needed_dev_rows[composite_key]
                is_meaningful = composite_key in meaningful_change_keys
                has_meaningful_change = False
                
                for key in common_keys:
                    prod_val = prod_row.get(key, "")
                    dev_val = dev_row.get(key, "")
                    if prod_val != dev_val:
                        is_excluded = self._is_excluded_column(key)
                        
                        # Only count meaningful columns in detailed_changes
                        if not is_excluded:
                            detailed_changes[key] += 1
                            has_meaningful_change = True
                
                # Collect example if meaningful
                if is_meaningful and has_meaningful_change:
                    if examples_collected < self.max_examples:
                        display_key = self._get_primary_key_display(dev_row)
                        prod_line_num = prod_index[composite_key][0]
                        
                        if display_key in ("None", "<missing>", ""):
                            logging.warning(
                                f"    Suspicious primary key '{display_key}' "
                                f"at dev line {dev_line_num}"
                            )
                        
                        example_ids[display_key] = {
                            "prod_line_num": prod_line_num,
                            "dev_line_num": dev_line_num,
                        }
                        
                        if examples_collected == 0:
                            logging.debug(
                                f"    First example: ID='{display_key}' "
                                f"prod_line={prod_line_num}, dev_line={dev_line_num}"
                            )
                        examples_collected += 1
            
            # Clean up
            del needed_prod_rows
            del needed_dev_rows
            gc.collect()
        
        logging.debug(
            f"    Diff complete: +{rows_added} added, -{rows_removed} removed, "
            f"~{rows_changed_meaningful} meaningful, "
            f"~{rows_changed_excluded_only} excluded-only"
        )
        
        result = {
            'rows_added': rows_added,
            'rows_removed': rows_removed,
            'rows_updated': rows_changed_meaningful,
            'rows_updated_excluded_only': rows_changed_excluded_only,
            'detailed_key_update_counts': dict(detailed_changes),
            'example_ids': dict(example_ids),
        }
        
        if example_ids_added:
            result['example_ids_added'] = dict(example_ids_added)
        if example_ids_removed:
            result['example_ids_removed'] = dict(example_ids_removed)
        
        return result


def calculate_in_stock_percentage(
    file_path: str, 
    max_rows: Optional[int] = None
) -> float:
    """
    Calculate the percentage of rows with 'in stock' availability.
    
    Args:
        file_path: Path to CSV file
        max_rows: Optional limit on rows to process
        
    Returns:
        Percentage of rows where availability == 'in stock' (0.0-100.0)
        Returns 0.0 if no availability column or no rows
    """
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
