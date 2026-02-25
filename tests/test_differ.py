"""Tests for the differ module."""

from databridge_core.reconciler.differ import (
    compute_similarity,
    get_matching_blocks,
    get_opcodes,
    unified_diff,
    context_diff,
    ndiff_text,
    diff_lists,
    diff_dicts,
    diff_values_paired,
    explain_diff,
    find_close_matches,
    quick_ratio,
    real_quick_ratio,
)


class TestComputeSimilarity:
    def test_identical_strings(self):
        assert compute_similarity("hello", "hello") == 1.0

    def test_empty_strings(self):
        assert compute_similarity("", "") == 1.0

    def test_one_empty(self):
        assert compute_similarity("hello", "") == 0.0
        assert compute_similarity("", "hello") == 0.0

    def test_similar_strings(self):
        sim = compute_similarity("hello", "hallo")
        assert 0.5 < sim < 1.0

    def test_completely_different(self):
        sim = compute_similarity("abc", "xyz")
        assert sim < 0.5


class TestGetMatchingBlocks:
    def test_basic(self):
        blocks = get_matching_blocks("abcdef", "abcxyz")
        assert len(blocks) > 0
        assert blocks[0].content == "abc"

    def test_no_content(self):
        blocks = get_matching_blocks("abc", "abc", include_content=False)
        assert all(b.content is None for b in blocks)


class TestGetOpcodes:
    def test_basic(self):
        opcodes = get_opcodes("abc", "axc")
        ops = [o.operation for o in opcodes]
        assert "replace" in ops or "delete" in ops or "insert" in ops

    def test_identical(self):
        opcodes = get_opcodes("abc", "abc")
        assert all(o.operation == "equal" for o in opcodes)


class TestUnifiedDiff:
    def test_diff_output(self):
        result = unified_diff("line1\nline2\n", "line1\nline3\n")
        assert "---" in result
        assert "+++" in result

    def test_identical(self):
        result = unified_diff("same\n", "same\n")
        assert result == ""


class TestContextDiff:
    def test_diff_output(self):
        result = context_diff("line1\nline2\n", "line1\nline3\n")
        assert "***" in result


class TestNdiffText:
    def test_basic(self):
        result = ndiff_text("hello", "hallo")
        assert len(result) > 0


class TestDiffLists:
    def test_basic(self):
        result = diff_lists([1, 2, 3], [2, 3, 4])
        assert result.added_count == 1
        assert result.removed_count == 1
        assert result.common_count == 2
        assert 0 < result.jaccard_similarity < 1

    def test_identical(self):
        result = diff_lists([1, 2], [1, 2])
        assert result.jaccard_similarity == 1.0

    def test_empty(self):
        result = diff_lists([], [])
        assert result.jaccard_similarity == 1.0


class TestDiffDicts:
    def test_basic(self):
        result = diff_dicts({"a": "1", "b": "2"}, {"b": "3", "c": "4"})
        assert "a" in result.removed_keys
        assert "c" in result.added_keys
        assert "b" in result.changed_keys

    def test_identical(self):
        result = diff_dicts({"a": "1"}, {"a": "1"})
        assert result.overall_similarity == 1.0

    def test_empty(self):
        result = diff_dicts({}, {})
        assert result.overall_similarity == 1.0


class TestDiffValuesPaired:
    def test_basic(self):
        result = diff_values_paired(["hello", "world"], ["hallo", "world"])
        assert len(result) == 2
        assert result[0].similarity < 1.0
        assert result[1].similarity == 1.0


class TestExplainDiff:
    def test_identical(self):
        assert "Identical" in explain_diff("hello", "hello")

    def test_added(self):
        assert "Added" in explain_diff("", "hello")

    def test_removed(self):
        assert "Removed" in explain_diff("hello", "")

    def test_changed(self):
        result = explain_diff("hello", "hallo")
        assert "Similarity" in result


class TestFindCloseMatches:
    def test_basic(self):
        matches = find_close_matches("hello", ["hallo", "world", "help", "jello"])
        assert len(matches) > 0
        assert matches[0].rank == 1

    def test_no_matches(self):
        matches = find_close_matches("xyz", ["abc", "def"], cutoff=0.9)
        assert len(matches) == 0


class TestQuickRatio:
    def test_basic(self):
        ratio = quick_ratio("hello", "hallo")
        assert 0 < ratio <= 1.0


class TestRealQuickRatio:
    def test_basic(self):
        ratio = real_quick_ratio("hello", "hallo")
        assert 0 < ratio <= 1.0
