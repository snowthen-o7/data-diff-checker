"""
Memory-efficient streaming CSV reader with automatic delimiter and escape detection.

This module provides a CSV reader that:
- Streams rows one at a time (true streaming, no full file loading)
- Auto-detects delimiters (comma vs tab) for both header and data
- Handles mixed delimiter files (header with different delimiter than data)
- Detects and handles backslash vs double-quote escaping
- Caches headers and row counts for efficiency
- Handles UTF-8 BOM markers
"""

import csv
import logging
import os
import re
import sys
from typing import Dict, Iterator, List, Optional, Tuple


# Safely set CSV field size limit to handle large fields (e.g., HTML content)
_max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(_max_int)
        break
    except OverflowError:
        _max_int //= 10


class StreamingCSVReader:
    """
    Memory-efficient CSV reader with automatic format detection.
    
    Features:
        - True streaming iteration (rows yielded one at a time)
        - Auto-detection of delimiters (comma or tab)
        - Handles files where header and data use different delimiters
        - Detects backslash escaping vs standard double-quote escaping
        - Caches headers and row counts after first read
        - Handles UTF-8 BOM markers transparently
    
    Example:
        >>> reader = StreamingCSVReader("data.csv")
        >>> print(reader.read_headers())
        ['id', 'name', 'price']
        >>> for row in reader.iterate_rows():
        ...     print(row['id'], row['name'])
    
    Args:
        file_path: Path to the CSV file
        delimiter: Optional explicit delimiter (auto-detected if not provided)
        max_rows: Optional limit on rows to process
    """
    
    def __init__(
        self, 
        file_path: str, 
        delimiter: Optional[str] = None, 
        max_rows: Optional[int] = None
    ):
        self.file_path = file_path
        self.delimiter = delimiter  # Data delimiter
        self.max_rows = max_rows
        
        # Cached values (populated on first access)
        self._headers: Optional[List[str]] = None
        self._row_count: Optional[int] = None
        self._header_delimiter: Optional[str] = None  # May differ from data delimiter
        self._uses_backslash_escape: bool = False
        
        # Run detection
        self._detect_delimiters()
    
    def _detect_delimiters(self) -> None:
        """
        Auto-detect delimiters and escape style by sampling the file.
        
        Samples from multiple positions in the file to catch escape patterns
        that may only appear in certain rows (e.g., HTML in product descriptions).
        """
        if self.delimiter:
            self._header_delimiter = self.delimiter
        
        with open(self.file_path, 'r', encoding='utf-8-sig') as f:
            # Read first chunk for header/delimiter detection
            first_sample = f.read(32768)
            lines = first_sample.split('\n')[:5]
            
            # Sample from middle and end for escape pattern detection
            f.seek(0, 2)  # Seek to end
            file_size = f.tell()
            
            samples = [first_sample]
            if file_size > 100000:  # Sample more if file > 100KB
                # Middle sample
                f.seek(file_size // 2)
                f.readline()  # Skip partial line
                samples.append(f.read(16384))
                
                # End sample
                if file_size > 50000:
                    f.seek(max(0, file_size - 16384))
                    f.readline()  # Skip partial line
                    samples.append(f.read(16384))
            
            sample = ''.join(samples)
            
            if not lines:
                self.delimiter = self.delimiter or ","
                self._header_delimiter = self._header_delimiter or ","
                return
            
            # Detect escaping style
            # Standard CSV: uses "" to escape quotes (e.g., "81 x 36""")
            # Some exports: use backslash (e.g., "value with \"quotes\"")
            #
            # Files with HTML/JSON often contain \" sequences that would break
            # standard parsing. If we see \" but NOT "", use backslash mode.
            has_double_quote_escape = '""' in sample
            has_backslash_quote = '\\"' in sample
            
            if has_backslash_quote and not has_double_quote_escape:
                self._uses_backslash_escape = True
                logging.debug(
                    f"Detected backslash escape mode in {os.path.basename(self.file_path)}"
                )
            
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
                            f"header uses {repr(self._header_delimiter)}, "
                            f"data uses {repr(self.delimiter)}"
                        )
                else:
                    self.delimiter = self._header_delimiter
            else:
                self._header_delimiter = self._header_delimiter or self.delimiter
    
    def _get_csv_params(self) -> dict:
        """Get CSV reader parameters based on detected escape style."""
        params = {'delimiter': self.delimiter}
        if self._uses_backslash_escape:
            params['doublequote'] = False
            params['escapechar'] = '\\'
        return params
    
    def _open_file(self):
        """Open file with BOM handling."""
        return open(self.file_path, 'r', encoding='utf-8-sig')
    
    @staticmethod
    def _normalize_key(key: str) -> str:
        """Normalize a column name by stripping whitespace and quotes."""
        return key.strip().strip('"')
    
    def read_headers(self) -> List[str]:
        """
        Read and return column headers.
        
        Headers are cached after the first read for efficiency.
        Uses the header-specific delimiter (which may differ from data delimiter).
        
        Returns:
            List of column names (normalized)
        """
        if self._headers is not None:
            return self._headers
        
        with self._open_file() as f:
            header_line = f.readline().rstrip('\r\n')
            # Parse header with its own delimiter and escape style
            params = self._get_csv_params()
            params['delimiter'] = self._header_delimiter
            reader = csv.reader([header_line], **params)
            raw_headers = next(reader)
            self._headers = [
                self._normalize_key(k) for k in raw_headers if k is not None
            ]
        
        return self._headers
    
    def iterate_rows(self) -> Iterator[Dict[str, str]]:
        """
        Iterate through rows one at a time (true streaming).
        
        Yields:
            Dictionary mapping column names to values for each row
            
        Note:
            Respects max_rows limit if set.
        """
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
        Iterate through rows with their source line numbers.
        
        Line numbers reflect the starting line of each row (1-indexed),
        correctly accounting for multi-line quoted fields.
        
        Yields:
            Tuple of (line_number, row_dict) for each row
        """
        rows_yielded = 0
        
        with self._open_file() as f:
            reader = csv.DictReader(f, **self._get_csv_params())
            
            # Track line number where each row starts
            # reader.line_num gives where the row ENDS, so track previous end
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
        """
        Count the number of data rows in the file.
        
        Result is cached after first count. Respects max_rows limit.
        Uses CSV reader to correctly count logical rows (handles multi-line fields).
        
        Returns:
            Number of data rows (excluding header)
        """
        if self._row_count is not None:
            return self._row_count
            
        count = 0
        with self._open_file() as f:
            reader = csv.reader(f, **self._get_csv_params())
            next(reader)  # Skip header
            for _ in reader:
                count += 1
                if self.max_rows is not None and count >= self.max_rows:
                    break
        
        self._row_count = count
        return count
    
    @property
    def detected_delimiter(self) -> str:
        """Return the detected or configured data delimiter."""
        return self.delimiter
    
    @property
    def detected_header_delimiter(self) -> str:
        """Return the detected header delimiter."""
        return self._header_delimiter
    
    @property
    def uses_backslash_escaping(self) -> bool:
        """Return whether file uses backslash escaping."""
        return self._uses_backslash_escape
