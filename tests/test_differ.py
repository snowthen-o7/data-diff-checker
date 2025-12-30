"""
Comprehensive tests for Data Diff Checker.

Run with: pytest tests/ -v
"""

import pytest
import os
from pathlib import Path

from data_diff_checker import EfficientDiffer, StreamingCSVReader


# Get the fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestStreamingCSVReader:
    """Tests for CSV reading functionality."""

    def test_read_basic_csv(self):
        """Test reading a basic CSV file."""
        reader = StreamingCSVReader(FIXTURES_DIR / "basic_prod.csv")
        headers = reader.read_headers()
        
        assert "id" in headers
        assert "sku" in headers
        assert "title" in headers
        assert len(headers) == 8

    def test_read_row_count(self):
        """Test counting rows in CSV."""
        reader = StreamingCSVReader(FIXTURES_DIR / "basic_prod.csv")
        reader.read_headers()
        
        rows = list(reader.iterate_rows())
        assert len(rows) == 10

    def test_read_edge_cases_csv(self):
        """Test reading CSV with special characters."""
        reader = StreamingCSVReader(FIXTURES_DIR / "edge_cases_prod.csv")
        headers = reader.read_headers()
        
        assert "id" in headers
        assert "name" in headers
        
        rows = list(reader.iterate_rows())
        assert len(rows) == 10
        
        # Check that quoted fields are handled correctly
        assert "Name, With Comma" in rows[1]["name"]

    def test_detect_delimiter(self):
        """Test auto-detection of delimiter."""
        reader = StreamingCSVReader(FIXTURES_DIR / "basic_prod.csv")
        headers = reader.read_headers()
        
        # Should detect comma delimiter
        assert reader.delimiter == ","


class TestEfficientDiffer:
    """Tests for the diff algorithm."""

    def test_missing_primary_key_error(self):
        """Test that missing primary key raises ValueError."""
        differ = EfficientDiffer(primary_keys=["nonexistent_column"])

        with pytest.raises(ValueError) as exc_info:
            differ.compute_diff(
                FIXTURES_DIR / "basic_prod.csv",
                FIXTURES_DIR / "basic_dev.csv"
            )

        assert "not found" in str(exc_info.value).lower()
        assert "nonexistent_column" in str(exc_info.value)

    def test_empty_csv_handling(self):
        """Test handling of empty CSV files (header only)."""
        import tempfile

        # Create temporary empty CSV files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as prod_f:
            prod_f.write("id,name,price\n")
            prod_path = prod_f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as dev_f:
            dev_f.write("id,name,price\n")
            dev_path = dev_f.name

        try:
            differ = EfficientDiffer(primary_keys=["id"])
            result = differ.compute_diff(prod_path, dev_path)

            assert result["rows_added"] == 0
            assert result["rows_removed"] == 0
            assert result["rows_updated"] == 0
            assert result["prod_row_count"] == 0
            assert result["dev_row_count"] == 0
        finally:
            os.unlink(prod_path)
            os.unlink(dev_path)

    def test_basic_diff(self):
        """Test basic diff with simple primary key."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        # Check structure
        assert "rows_added" in result
        assert "rows_removed" in result
        assert "rows_updated" in result
        assert "rows_updated_excluded_only" in result
        assert "detailed_key_update_counts" in result
        
        # Check counts
        # Row 8 (id=8, Classic Item) was removed
        # Rows 11, 12 were added
        assert result["rows_removed"] == 1
        assert result["rows_added"] == 2
        
        # Check row counts
        assert result["prod_row_count"] == 10
        assert result["dev_row_count"] == 11

    def test_composite_key_diff(self):
        """Test diff with composite primary key (sku, locale)."""
        differ = EfficientDiffer(primary_keys=["sku", "locale"])
        result = differ.compute_diff(
            FIXTURES_DIR / "composite_key_prod.csv",
            FIXTURES_DIR / "composite_key_dev.csv"
        )
        
        # PROD-001,it_IT and PROD-005,en_US were added
        assert result["rows_added"] == 2
        
        # PROD-004,en_US was removed
        assert result["rows_removed"] == 1
        
        # Check that composite keys work
        assert "example_ids" in result or "example_ids_added" in result

    def test_excluded_columns_only_changes(self):
        """Test that inventory/availability-only changes are tracked separately."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "excluded_cols_prod.csv",
            FIXTURES_DIR / "excluded_cols_dev.csv"
        )
        
        # All changes should be inventory/availability only
        # No meaningful (non-excluded) changes
        assert result["rows_updated"] == 0
        
        # But excluded-only changes should be tracked
        assert result["rows_updated_excluded_only"] == 5
        
        # No rows added or removed
        assert result["rows_added"] == 0
        assert result["rows_removed"] == 0

    def test_mixed_changes(self):
        """Test file with both meaningful and excluded-only changes."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        # Should have both types of changes
        assert result["rows_updated"] > 0
        # Some rows only have inventory changes
        
        # Check detailed column counts
        assert "detailed_key_update_counts" in result
        counts = result["detailed_key_update_counts"]
        
        # Price, title, and description should show changes
        assert any(k in counts for k in ["price", "title", "description"])

    def test_edge_cases_handling(self):
        """Test handling of special characters, whitespace, etc."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "edge_cases_prod.csv",
            FIXTURES_DIR / "edge_cases_dev.csv"
        )
        
        # Should handle all edge cases without crashing
        assert "rows_updated" in result
        assert "rows_added" in result
        
        # Row 11 was added
        assert result["rows_added"] == 1
        
        # No rows removed
        assert result["rows_removed"] == 0

    def test_example_ids_collection(self):
        """Test that example IDs are collected with line numbers."""
        differ = EfficientDiffer(primary_keys=["id"], max_examples=5)
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        # Check example_ids structure
        if result["rows_updated"] > 0:
            assert "example_ids" in result
            for key, info in result["example_ids"].items():
                assert "prod_line_num" in info
                assert "dev_line_num" in info
                # Line numbers should be 1-indexed (header is line 1)
                assert info["prod_line_num"] >= 2
                assert info["dev_line_num"] >= 2

    def test_max_examples_limit(self):
        """Test that max_examples limits the number of example IDs."""
        differ = EfficientDiffer(primary_keys=["id"], max_examples=2)
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        # Should have at most 2 examples in each category
        if "example_ids" in result:
            assert len(result["example_ids"]) <= 2
        if "example_ids_added" in result:
            assert len(result["example_ids_added"]) <= 2
        if "example_ids_removed" in result:
            assert len(result["example_ids_removed"]) <= 2

    def test_schema_detection(self):
        """Test detection of schema differences (prod-only vs dev-only columns)."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        # Both files have the same schema, so no differences expected
        assert "common_keys" in result
        assert "prod_only_keys" in result
        assert "dev_only_keys" in result
        
        # All columns should be common (same schema)
        assert len(result["prod_only_keys"]) == 0
        assert len(result["dev_only_keys"]) == 0

    def test_case_sensitivity(self):
        """Test case-sensitive vs case-insensitive comparison."""
        import tempfile

        # Create test files with case-only differences
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as prod_f:
            prod_f.write("id,name\n")
            prod_f.write("1,Alice\n")
            prod_path = prod_f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as dev_f:
            dev_f.write("id,name\n")
            dev_f.write("1,ALICE\n")
            dev_path = dev_f.name

        try:
            # Case-sensitive (default) should detect change
            differ_sensitive = EfficientDiffer(primary_keys=["id"], case_sensitive=True)
            result_sensitive = differ_sensitive.compute_diff(prod_path, dev_path)
            assert result_sensitive["rows_updated"] == 1

            # Case-insensitive should NOT detect change
            differ_insensitive = EfficientDiffer(primary_keys=["id"], case_sensitive=False)
            result_insensitive = differ_insensitive.compute_diff(prod_path, dev_path)
            assert result_insensitive["rows_updated"] == 0
        finally:
            os.unlink(prod_path)
            os.unlink(dev_path)

    def test_whitespace_trimming(self):
        """Test whitespace trimming option."""
        import tempfile

        # Create test files with whitespace-only differences
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as prod_f:
            prod_f.write("id,name\n")
            prod_f.write("1,Alice\n")
            prod_path = prod_f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as dev_f:
            dev_f.write("id,name\n")
            dev_f.write("1,  Alice  \n")
            dev_path = dev_f.name

        try:
            # With trimming (default) should NOT detect change
            differ_trim = EfficientDiffer(primary_keys=["id"], trim_whitespace=True)
            result_trim = differ_trim.compute_diff(prod_path, dev_path)
            assert result_trim["rows_updated"] == 0

            # Without trimming should detect change
            differ_no_trim = EfficientDiffer(primary_keys=["id"], trim_whitespace=False)
            result_no_trim = differ_no_trim.compute_diff(prod_path, dev_path)
            assert result_no_trim["rows_updated"] == 1
        finally:
            os.unlink(prod_path)
            os.unlink(dev_path)

    def test_empty_result_structure(self):
        """Test diff result structure when comparing identical files."""
        differ = EfficientDiffer(primary_keys=["id"])
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_prod.csv"  # Same file
        )
        
        # No changes when comparing file to itself
        assert result["rows_added"] == 0
        assert result["rows_removed"] == 0
        assert result["rows_updated"] == 0
        assert result["rows_updated_excluded_only"] == 0


class TestIntegration:
    """Integration tests for full workflow."""

    def test_full_diff_workflow(self):
        """Test complete diff workflow from file to result."""
        differ = EfficientDiffer(
            primary_keys=["sku", "locale"],
            max_examples=10
        )
        
        result = differ.compute_diff(
            FIXTURES_DIR / "composite_key_prod.csv",
            FIXTURES_DIR / "composite_key_dev.csv"
        )
        
        # Verify complete result structure
        required_keys = [
            "rows_added",
            "rows_removed", 
            "rows_updated",
            "rows_updated_excluded_only",
            "detailed_key_update_counts",
            "common_keys",
            "prod_only_keys",
            "dev_only_keys",
            "prod_row_count",
            "dev_row_count"
        ]
        
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

    def test_large_file_handling(self):
        """Test that the algorithm handles larger files efficiently."""
        # This is more of a smoke test - actual large file tests would
        # need to be done with generated fixtures
        differ = EfficientDiffer(primary_keys=["id"])
        
        # Should complete without memory issues
        result = differ.compute_diff(
            FIXTURES_DIR / "basic_prod.csv",
            FIXTURES_DIR / "basic_dev.csv"
        )
        
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
