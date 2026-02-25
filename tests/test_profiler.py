"""Tests for the profiler module."""

import csv

import pytest

from databridge_core.profiler import profile_data, detect_schema_drift


class TestProfileData:
    def test_basic_profile(self, customers_a):
        result = profile_data(customers_a)
        assert result["rows"] == 10
        assert result["columns"] == 5
        assert result["structure_type"] == "Dimension/Reference"
        assert "column_types" in result
        assert "data_quality" in result
        assert "statistics" in result

    def test_potential_keys(self, customers_a):
        result = profile_data(customers_a)
        # id should be a potential key (unique for all rows)
        assert "id" in result["potential_key_columns"]

    def test_data_quality(self, customers_a):
        result = profile_data(customers_a)
        dq = result["data_quality"]
        assert dq["duplicate_rows"] == 0
        assert dq["duplicate_percentage"] == 0.0


class TestDetectSchemaDrift:
    def test_no_drift(self, customers_a, customers_b):
        result = detect_schema_drift(customers_a, customers_b)
        # Same columns, same types
        assert result["columns_added"] == []
        assert result["columns_removed"] == []
        assert result["has_drift"] == False

    def test_drift_detected(self, customers_a, tmp_path):
        # Create a CSV with different columns
        drift_path = tmp_path / "drifted.csv"
        rows = [
            {"id": "1", "name": "Alice", "department": "Finance", "salary": "75000"},
        ]
        with open(drift_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        result = detect_schema_drift(customers_a, str(drift_path))
        assert result["has_drift"] == True
        assert len(result["columns_added"]) > 0 or len(result["columns_removed"]) > 0
