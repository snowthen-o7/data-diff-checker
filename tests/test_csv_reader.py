"""Tests for StreamingCSVReader."""

import os
import tempfile
import pytest
from data_diff_checker.csv_reader import StreamingCSVReader


class TestStreamingCSVReader:
    """Tests for the StreamingCSVReader class."""

    def test_read_simple_csv(self):
        """Test reading a simple CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,price\n")
            f.write("1,Widget,9.99\n")
            f.write("2,Gadget,19.99\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            headers = reader.read_headers()
            
            assert headers == ['id', 'name', 'price']
            
            rows = list(reader.iterate_rows())
            assert len(rows) == 2
            assert rows[0] == {'id': '1', 'name': 'Widget', 'price': '9.99'}
            assert rows[1] == {'id': '2', 'name': 'Gadget', 'price': '19.99'}
        finally:
            os.unlink(f.name)
    
    def test_detect_tab_delimiter(self):
        """Test auto-detection of tab delimiter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
            f.write("id\tname\tprice\n")
            f.write("1\tWidget\t9.99\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            assert reader.detected_delimiter == '\t'
            
            rows = list(reader.iterate_rows())
            assert rows[0]['name'] == 'Widget'
        finally:
            os.unlink(f.name)
    
    def test_max_rows_limit(self):
        """Test row limiting."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,value\n")
            for i in range(100):
                f.write(f"{i},{i*10}\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name, max_rows=5)
            rows = list(reader.iterate_rows())
            
            assert len(rows) == 5
            assert reader.count_rows() == 5
        finally:
            os.unlink(f.name)
    
    def test_count_rows_cached(self):
        """Test that row count is cached."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id\n1\n2\n3\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            count1 = reader.count_rows()
            count2 = reader.count_rows()
            
            assert count1 == count2 == 3
        finally:
            os.unlink(f.name)
    
    def test_iterate_with_line_numbers(self):
        """Test iteration with line numbers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,value\n")
            f.write("a,1\n")
            f.write("b,2\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            rows_with_lines = list(reader.iterate_rows_with_line_numbers())
            
            assert len(rows_with_lines) == 2
            assert rows_with_lines[0][0] == 2  # First data row is line 2
            assert rows_with_lines[1][0] == 3
        finally:
            os.unlink(f.name)
    
    def test_handle_quoted_fields(self):
        """Test handling of quoted fields with commas."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,description\n')
            f.write('1,"Hello, World"\n')
            f.write('2,"Line 1\nLine 2"\n')
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            rows = list(reader.iterate_rows())
            
            assert rows[0]['description'] == 'Hello, World'
            assert rows[1]['description'] == 'Line 1\nLine 2'
        finally:
            os.unlink(f.name)
    
    def test_handle_utf8_bom(self):
        """Test handling of UTF-8 BOM marker."""
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as f:
            # Write UTF-8 BOM followed by CSV content
            f.write(b'\xef\xbb\xbfid,name\n1,Test\n')
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            headers = reader.read_headers()
            
            # BOM should be stripped from first header
            assert headers[0] == 'id'
        finally:
            os.unlink(f.name)
    
    def test_normalize_headers(self):
        """Test header normalization (whitespace and quotes)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('"id" , "name" , price \n')
            f.write('1,Test,9.99\n')
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            headers = reader.read_headers()
            
            # Should strip whitespace and quotes
            assert 'id' in headers
            assert 'name' in headers
            assert 'price' in headers
        finally:
            os.unlink(f.name)
    
    def test_empty_file(self):
        """Test handling of empty file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            rows = list(reader.iterate_rows())
            assert len(rows) == 0
        finally:
            os.unlink(f.name)
    
    def test_header_only_file(self):
        """Test file with only headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,price\n")
            f.name
        
        try:
            reader = StreamingCSVReader(f.name)
            headers = reader.read_headers()
            rows = list(reader.iterate_rows())
            
            assert headers == ['id', 'name', 'price']
            assert len(rows) == 0
            assert reader.count_rows() == 0
        finally:
            os.unlink(f.name)
