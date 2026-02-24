"""Tests for the reconciler module (hasher, merger, transform)."""

import pytest

from databridge_core.reconciler import (
    compare_hashes,
    get_orphan_details,
    get_conflict_details,
    merge_sources,
    transform_column,
)


class TestCompareHashes:
    def test_basic_comparison(self, customers_a, customers_b):
        result = compare_hashes(customers_a, customers_b, "id")
        stats = result["statistics"]

        assert result["source_a"]["total_rows"] == 10
        assert result["source_b"]["total_rows"] == 9
        assert stats["exact_matches"] >= 4  # at least 4 identical rows
        assert stats["conflicts"] >= 2  # at least balance and name conflicts
        assert stats["orphans_only_in_source_a"] >= 1  # id 9, 10
        assert stats["orphans_only_in_source_b"] >= 1  # id 11
        assert 0 <= stats["match_rate_percent"] <= 100

    def test_specific_compare_columns(self, customers_a, customers_b):
        result = compare_hashes(customers_a, customers_b, "id", "name,email")
        assert "name" in result["compare_columns"]
        assert "email" in result["compare_columns"]

    def test_invalid_column(self, customers_a, customers_b):
        with pytest.raises(ValueError, match="not found"):
            compare_hashes(customers_a, customers_b, "nonexistent_col")


class TestOrphanDetails:
    def test_both_orphans(self, customers_a, customers_b):
        result = get_orphan_details(customers_a, customers_b, "id")
        assert "orphans_in_a" in result
        assert "orphans_in_b" in result
        assert result["orphans_in_a"]["total"] >= 1
        assert result["orphans_in_b"]["total"] >= 1

    def test_orphans_source_a_only(self, customers_a, customers_b):
        result = get_orphan_details(customers_a, customers_b, "id", orphan_source="a")
        assert "orphans_in_a" in result
        assert "orphans_in_b" not in result

    def test_orphans_source_b_only(self, customers_a, customers_b):
        result = get_orphan_details(customers_a, customers_b, "id", orphan_source="b")
        assert "orphans_in_b" in result
        assert "orphans_in_a" not in result


class TestConflictDetails:
    def test_basic_conflicts(self, customers_a, customers_b):
        result = get_conflict_details(customers_a, customers_b, "id")
        assert result["total_conflicts"] >= 2
        assert len(result["conflicts"]) <= result["total_conflicts"]

        for conflict in result["conflicts"]:
            assert "key" in conflict
            assert "differences" in conflict
            for diff in conflict["differences"]:
                assert "column" in diff
                assert "value_a" in diff
                assert "value_b" in diff
                assert "similarity" in diff

    def test_conflict_limit(self, customers_a, customers_b):
        result = get_conflict_details(customers_a, customers_b, "id", limit=1)
        assert len(result["conflicts"]) <= 1


class TestMergeSources:
    def test_inner_merge(self, customers_a, customers_b):
        result = merge_sources(customers_a, customers_b, "id", "inner")
        assert result["merge_type"] == "inner"
        assert result["merged_rows"] <= min(result["source_a_rows"], result["source_b_rows"])

    def test_outer_merge(self, customers_a, customers_b):
        result = merge_sources(customers_a, customers_b, "id", "outer")
        assert result["merged_rows"] >= max(result["source_a_rows"], result["source_b_rows"])

    def test_merge_with_output(self, customers_a, customers_b, tmp_dir):
        output = str(tmp_dir / "merged.csv")
        result = merge_sources(customers_a, customers_b, "id", "inner", output)
        assert result["saved_to"] == output

        import pandas as pd
        df = pd.read_csv(output)
        assert len(df) == result["merged_rows"]


class TestTransformColumn:
    def test_upper(self, customers_a):
        result = transform_column(customers_a, "name", "upper")
        assert all(v == v.upper() for v in result["preview"]["after"])

    def test_lower(self, customers_a):
        result = transform_column(customers_a, "name", "lower")
        assert all(v == v.lower() for v in result["preview"]["after"])

    def test_invalid_column(self, customers_a):
        with pytest.raises(ValueError, match="not found"):
            transform_column(customers_a, "nonexistent", "upper")

    def test_invalid_operation(self, customers_a):
        with pytest.raises(ValueError, match="Unknown operation"):
            transform_column(customers_a, "name", "invalid_op")

    def test_output_file(self, customers_a, tmp_dir):
        output = str(tmp_dir / "transformed.csv")
        result = transform_column(customers_a, "name", "upper", output)
        assert result["saved_to"] == output
