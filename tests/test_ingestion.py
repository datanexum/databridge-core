"""Tests for the ingestion module."""

import json

import pytest

from databridge_core.ingestion import load_csv, load_json, parse_table_from_text


class TestLoadCsv:
    def test_basic_load(self, customers_a):
        result = load_csv(customers_a)
        assert result["rows"] == 10
        assert "id" in result["columns"]
        assert len(result["preview"]) == 5  # default preview_rows

    def test_custom_preview(self, customers_a):
        result = load_csv(customers_a, preview_rows=3)
        assert len(result["preview"]) == 3

    def test_dtypes(self, customers_a):
        result = load_csv(customers_a)
        assert "dtypes" in result
        assert len(result["dtypes"]) == 5

    def test_null_counts(self, customers_a):
        result = load_csv(customers_a)
        assert "null_counts" in result

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_csv("/nonexistent/path/to/file.csv")


class TestLoadJson:
    def test_array_json(self, tmp_path):
        path = tmp_path / "data.json"
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        with open(path, "w") as f:
            json.dump(data, f)

        result = load_json(str(path))
        assert result["rows"] == 2
        assert "id" in result["columns"]

    def test_object_json(self, tmp_path):
        path = tmp_path / "data.json"
        data = {"id": [1, 2], "name": ["Alice", "Bob"]}
        with open(path, "w") as f:
            json.dump(data, f)

        result = load_json(str(path))
        assert result["rows"] == 2

    def test_single_object_json(self, tmp_path):
        path = tmp_path / "data.json"
        data = {"id": 1, "name": "Alice"}
        with open(path, "w") as f:
            json.dump(data, f)

        result = load_json(str(path))
        assert result["rows"] == 1


class TestParseTableFromText:
    def test_tab_delimited(self):
        text = "Name\tAge\tCity\nAlice\t30\tNY\nBob\t25\tLA"
        result = parse_table_from_text(text)
        assert result["columns"] == ["Name", "Age", "City"]
        assert result["row_count"] == 2

    def test_pipe_delimited(self):
        text = "Name|Age|City\nAlice|30|NY\nBob|25|LA"
        result = parse_table_from_text(text, delimiter="pipe")
        assert result["row_count"] == 2

    def test_auto_detect(self):
        text = "Name\tAge\nAlice\t30"
        result = parse_table_from_text(text)
        assert result["row_count"] == 1

    def test_single_row(self):
        text = "just one row"
        result = parse_table_from_text(text)
        assert "raw_row" in result

    def test_empty_text(self):
        with pytest.raises(ValueError, match="No text content"):
            parse_table_from_text("")
