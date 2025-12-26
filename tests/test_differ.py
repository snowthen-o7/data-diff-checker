"""Tests for EfficientDiffer."""

import os
import tempfile
import pytest
from diaz_diff_checker.differ import EfficientDiffer, calculate_in_stock_percentage


class TestEfficientDiffer:
    """Tests for the EfficientDiffer class."""

    def _create_csv(self, content: str) -> str:
        """Helper to create a temporary CSV file."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        f.write(content)
        f.close()
        return f.name
    
    def test_identical_files(self):
        """Test comparing identical files."""
        content = "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
        prod_file = self._create_csv(content)
        dev_file = self._create_csv(content)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_added'] == 0
            assert result['rows_removed'] == 0
            assert result['rows_updated'] == 0
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_detect_added_rows(self):
        """Test detection of added rows."""
        prod = "id,name\n1,A\n2,B\n"
        dev = "id,name\n1,A\n2,B\n3,C\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_added'] == 1
            assert result['rows_removed'] == 0
            assert '3' in result.get('example_ids_added', {})
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_detect_removed_rows(self):
        """Test detection of removed rows."""
        prod = "id,name\n1,A\n2,B\n3,C\n"
        dev = "id,name\n1,A\n2,B\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_added'] == 0
            assert result['rows_removed'] == 1
            assert '3' in result.get('example_ids_removed', {})
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_detect_updated_rows(self):
        """Test detection of updated rows."""
        prod = "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
        dev = "id,name,price\n1,Widget,10.99\n2,Gadget,19.99\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_updated'] == 1
            assert result['detailed_key_update_counts']['price'] == 1
            assert '1' in result.get('example_ids', {})
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_composite_primary_key(self):
        """Test using composite primary key."""
        prod = "sku,locale,name\nA,en,Apple\nA,de,Apfel\n"
        dev = "sku,locale,name\nA,en,Apple\nA,de,Apfel Updated\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['sku', 'locale'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_updated'] == 1
            # Composite key should be joined with underscore
            assert 'A_de' in result.get('example_ids', {})
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_excluded_columns(self):
        """Test that inventory/availability changes are excluded from rows_updated."""
        prod = "id,name,inventory,availability\n1,Widget,100,in stock\n"
        dev = "id,name,inventory,availability\n1,Widget,50,out of stock\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            # Changes should be excluded-only, not meaningful
            assert result['rows_updated'] == 0
            assert result['rows_updated_excluded_only'] == 1
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_mixed_changes(self):
        """Test rows with both meaningful and excluded changes."""
        prod = "id,name,inventory\n1,Widget,100\n"
        dev = "id,name,inventory\n1,Widget Updated,50\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            # Has meaningful change (name), so counted in rows_updated
            assert result['rows_updated'] == 1
            assert result['rows_updated_excluded_only'] == 0
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_missing_primary_key_error(self):
        """Test error when primary key is missing."""
        content = "name,price\nWidget,9.99\n"
        prod_file = self._create_csv(content)
        dev_file = self._create_csv(content)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            
            with pytest.raises(ValueError, match="Primary keys.*not found"):
                differ.compute_diff(prod_file, dev_file)
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_column_differences(self):
        """Test detection of column differences between files."""
        prod = "id,name,old_field\n1,Widget,value\n"
        dev = "id,name,new_field\n1,Widget,value\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert 'old_field' in result['prod_only_keys']
            assert 'new_field' in result['dev_only_keys']
            assert 'id' in result['common_keys']
            assert 'name' in result['common_keys']
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_max_examples_limit(self):
        """Test that max_examples limits example collection."""
        # Create files with 20 changed rows
        prod_rows = ["id,value"] + [f"{i},old" for i in range(20)]
        dev_rows = ["id,value"] + [f"{i},new" for i in range(20)]
        
        prod_file = self._create_csv("\n".join(prod_rows))
        dev_file = self._create_csv("\n".join(dev_rows))
        
        try:
            differ = EfficientDiffer(primary_keys=['id'], max_examples=5)
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['rows_updated'] == 20
            assert len(result['example_ids']) <= 5
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)
    
    def test_row_counts(self):
        """Test row count reporting."""
        prod = "id\n1\n2\n3\n"
        dev = "id\n1\n2\n3\n4\n5\n"
        
        prod_file = self._create_csv(prod)
        dev_file = self._create_csv(dev)
        
        try:
            differ = EfficientDiffer(primary_keys=['id'])
            result = differ.compute_diff(prod_file, dev_file)
            
            assert result['prod_row_count'] == 3
            assert result['dev_row_count'] == 5
        finally:
            os.unlink(prod_file)
            os.unlink(dev_file)


class TestCalculateInStockPercentage:
    """Tests for the calculate_in_stock_percentage function."""
    
    def _create_csv(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        f.write(content)
        f.close()
        return f.name
    
    def test_all_in_stock(self):
        """Test with all items in stock."""
        content = "id,availability\n1,in stock\n2,in stock\n3,in stock\n"
        file_path = self._create_csv(content)
        
        try:
            result = calculate_in_stock_percentage(file_path)
            assert result == 100.0
        finally:
            os.unlink(file_path)
    
    def test_all_out_of_stock(self):
        """Test with all items out of stock."""
        content = "id,availability\n1,out of stock\n2,out of stock\n"
        file_path = self._create_csv(content)
        
        try:
            result = calculate_in_stock_percentage(file_path)
            assert result == 0.0
        finally:
            os.unlink(file_path)
    
    def test_mixed_availability(self):
        """Test with mixed availability."""
        content = "id,availability\n1,in stock\n2,out of stock\n3,in stock\n4,out of stock\n"
        file_path = self._create_csv(content)
        
        try:
            result = calculate_in_stock_percentage(file_path)
            assert result == 50.0
        finally:
            os.unlink(file_path)
    
    def test_no_availability_column(self):
        """Test file without availability column."""
        content = "id,name\n1,Widget\n"
        file_path = self._create_csv(content)
        
        try:
            result = calculate_in_stock_percentage(file_path)
            assert result == 0.0
        finally:
            os.unlink(file_path)
    
    def test_case_insensitive(self):
        """Test case insensitive matching of 'in stock'."""
        content = "id,availability\n1,In Stock\n2,IN STOCK\n3,in stock\n"
        file_path = self._create_csv(content)
        
        try:
            result = calculate_in_stock_percentage(file_path)
            assert result == 100.0
        finally:
            os.unlink(file_path)
