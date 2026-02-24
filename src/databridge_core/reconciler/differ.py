"""Text and data comparison utilities.

Pure Python difflib wrappers with structured output.
Zero external dependencies beyond stdlib + pydantic.
"""

import difflib
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# -- Types --

class DiffOpcode(BaseModel):
    """Represents a single diff operation (insert, delete, replace, equal)."""
    operation: str = Field(description="The type of diff operation")
    a_start: int = Field(description="Start index in sequence A")
    a_end: int = Field(description="End index in sequence A")
    b_start: int = Field(description="Start index in sequence B")
    b_end: int = Field(description="End index in sequence B")
    a_content: Optional[str] = Field(default=None, description="Content from sequence A")
    b_content: Optional[str] = Field(default=None, description="Content from sequence B")


class MatchingBlock(BaseModel):
    """Represents a matching block between two sequences."""
    a_start: int = Field(description="Start index in sequence A")
    b_start: int = Field(description="Start index in sequence B")
    size: int = Field(description="Length of the matching block")
    content: Optional[str] = Field(default=None, description="The matching content")


class ListDiffResult(BaseModel):
    """Result of comparing two lists."""
    list_a_count: int
    list_b_count: int
    added: List[Any] = Field(default_factory=list)
    removed: List[Any] = Field(default_factory=list)
    common: List[Any] = Field(default_factory=list)
    added_count: int = 0
    removed_count: int = 0
    common_count: int = 0
    jaccard_similarity: float = 0.0
    jaccard_percent: str = "0.0%"
    sequence_similarity: float = 0.0


class DictValueDiff(BaseModel):
    """Diff result for a single dictionary value."""
    key: str
    value_a: Any = None
    value_b: Any = None
    status: str = "unchanged"
    similarity: Optional[float] = None
    opcodes: Optional[List[DiffOpcode]] = None


class DictDiffResult(BaseModel):
    """Result of comparing two dictionaries."""
    dict_a_keys: int
    dict_b_keys: int
    added_keys: List[str] = Field(default_factory=list)
    removed_keys: List[str] = Field(default_factory=list)
    common_keys: List[str] = Field(default_factory=list)
    changed_keys: List[str] = Field(default_factory=list)
    unchanged_keys: List[str] = Field(default_factory=list)
    differences: List[DictValueDiff] = Field(default_factory=list)
    overall_similarity: float = 0.0


class SimilarStringMatch(BaseModel):
    """A similar string match result."""
    candidate: str
    similarity: float
    similarity_percent: str
    rank: int


class TransformDiff(BaseModel):
    """Diff result for a single value transformation."""
    index: int
    before: str
    after: str
    similarity: float
    opcodes: List[DiffOpcode] = Field(default_factory=list)
    explanation: str = ""


# -- Core functions --

def compute_similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two strings (0.0-1.0)."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def get_matching_blocks(a: str, b: str, include_content: bool = True) -> List[MatchingBlock]:
    """Find all matching blocks between two strings."""
    matcher = difflib.SequenceMatcher(None, a, b)
    blocks = []
    for block in matcher.get_matching_blocks():
        if block.size > 0:
            content = a[block.a:block.a + block.size] if include_content else None
            blocks.append(MatchingBlock(
                a_start=block.a, b_start=block.b,
                size=block.size, content=content,
            ))
    return blocks


def get_opcodes(a: str, b: str, include_content: bool = True) -> List[DiffOpcode]:
    """Get the sequence of operations to transform string a into string b."""
    matcher = difflib.SequenceMatcher(None, a, b)
    opcodes = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        a_content = a[i1:i2] if include_content else None
        b_content = b[j1:j2] if include_content else None
        opcodes.append(DiffOpcode(
            operation=tag,
            a_start=i1, a_end=i2,
            b_start=j1, b_end=j2,
            a_content=a_content, b_content=b_content,
        ))
    return opcodes


def unified_diff(
    a: str, b: str,
    from_label: str = "a", to_label: str = "b",
    context_lines: int = 3,
) -> str:
    """Generate unified diff format between two texts."""
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)
    if a_lines and not a_lines[-1].endswith('\n'):
        a_lines[-1] += '\n'
    if b_lines and not b_lines[-1].endswith('\n'):
        b_lines[-1] += '\n'
    diff = difflib.unified_diff(
        a_lines, b_lines,
        fromfile=from_label, tofile=to_label, n=context_lines,
    )
    return ''.join(diff)


def context_diff(
    a: str, b: str,
    from_label: str = "a", to_label: str = "b",
    context_lines: int = 3,
) -> str:
    """Generate context diff format between two texts."""
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)
    if a_lines and not a_lines[-1].endswith('\n'):
        a_lines[-1] += '\n'
    if b_lines and not b_lines[-1].endswith('\n'):
        b_lines[-1] += '\n'
    diff = difflib.context_diff(
        a_lines, b_lines,
        fromfile=from_label, tofile=to_label, n=context_lines,
    )
    return ''.join(diff)


def ndiff_text(a: str, b: str) -> str:
    """Generate character-level diff with +/-/? markers."""
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)
    if len(a_lines) <= 1 and len(b_lines) <= 1:
        result = list(difflib.ndiff(list(a), list(b)))
        return ''.join(result)
    result = list(difflib.ndiff(a_lines, b_lines))
    return ''.join(result)


def diff_lists(a: List[Any], b: List[Any]) -> ListDiffResult:
    """Compare two lists and compute various similarity metrics."""
    set_a = set(str(item) for item in a)
    set_b = set(str(item) for item in b)

    added = [item for item in b if str(item) not in set_a]
    removed = [item for item in a if str(item) not in set_b]
    common = [item for item in a if str(item) in set_b]

    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    jaccard = intersection / union if union > 0 else 1.0

    str_a = [str(item) for item in a]
    str_b = [str(item) for item in b]
    sequence_similarity = difflib.SequenceMatcher(None, str_a, str_b).ratio()

    return ListDiffResult(
        list_a_count=len(a), list_b_count=len(b),
        added=added, removed=removed, common=common,
        added_count=len(added), removed_count=len(removed),
        common_count=len(common),
        jaccard_similarity=jaccard,
        jaccard_percent=f"{jaccard * 100:.1f}%",
        sequence_similarity=sequence_similarity,
    )


def diff_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> DictDiffResult:
    """Compare two dictionaries with value-level character diffs."""
    keys_a = set(a.keys())
    keys_b = set(b.keys())

    added_keys = list(keys_b - keys_a)
    removed_keys = list(keys_a - keys_b)
    common_keys = list(keys_a & keys_b)

    changed_keys = []
    unchanged_keys = []
    differences = []

    for key in removed_keys:
        differences.append(DictValueDiff(
            key=key, value_a=a[key], value_b=None, status="removed",
        ))

    for key in added_keys:
        differences.append(DictValueDiff(
            key=key, value_a=None, value_b=b[key], status="added",
        ))

    for key in common_keys:
        val_a, val_b = a[key], b[key]
        if val_a == val_b:
            unchanged_keys.append(key)
            differences.append(DictValueDiff(
                key=key, value_a=val_a, value_b=val_b, status="unchanged",
            ))
        else:
            changed_keys.append(key)
            similarity = None
            opcodes = None
            if isinstance(val_a, str) and isinstance(val_b, str):
                similarity = compute_similarity(val_a, val_b)
                opcodes = get_opcodes(val_a, val_b)
            differences.append(DictValueDiff(
                key=key, value_a=val_a, value_b=val_b, status="changed",
                similarity=similarity, opcodes=opcodes,
            ))

    total_keys = len(keys_a | keys_b)
    if total_keys == 0:
        overall_similarity = 1.0
    else:
        score = len(unchanged_keys)
        for diff in differences:
            if diff.status == "changed" and diff.similarity is not None:
                score += diff.similarity
        overall_similarity = score / total_keys

    return DictDiffResult(
        dict_a_keys=len(keys_a), dict_b_keys=len(keys_b),
        added_keys=added_keys, removed_keys=removed_keys,
        common_keys=common_keys, changed_keys=changed_keys,
        unchanged_keys=unchanged_keys, differences=differences,
        overall_similarity=overall_similarity,
    )


def diff_values_paired(
    before_values: List[Any], after_values: List[Any],
) -> List[TransformDiff]:
    """Compare paired before/after values for transform analysis."""
    results = []
    for i, (before, after) in enumerate(zip(before_values, after_values)):
        before_str = str(before) if before is not None else ""
        after_str = str(after) if after is not None else ""
        similarity = compute_similarity(before_str, after_str)
        opcodes = get_opcodes(before_str, after_str)
        explanation = explain_diff(before_str, after_str)
        results.append(TransformDiff(
            index=i, before=before_str, after=after_str,
            similarity=similarity, opcodes=opcodes,
            explanation=explanation,
        ))
    return results


def explain_diff(a: str, b: str) -> str:
    """Generate a human-readable explanation of differences."""
    if a == b:
        return "Identical - no changes"
    if not a:
        return f"Added: '{b}'"
    if not b:
        return f"Removed: '{a}'"

    similarity = compute_similarity(a, b)
    opcodes = get_opcodes(a, b)
    lines = [f"Similarity: {similarity * 100:.1f}%"]

    changes = []
    for op in opcodes:
        if op.operation == "replace":
            changes.append(f"  Changed: '{op.a_content}' -> '{op.b_content}'")
        elif op.operation == "delete":
            changes.append(f"  Removed: '{op.a_content}'")
        elif op.operation == "insert":
            changes.append(f"  Added: '{op.b_content}'")

    if changes:
        lines.extend(changes)
    else:
        lines.append("  (whitespace or formatting changes only)")
    return "\n".join(lines)


def find_close_matches(
    word: str, candidates: List[str],
    n: int = 5, cutoff: float = 0.6,
) -> List[SimilarStringMatch]:
    """Find similar strings from a list of candidates."""
    matches = difflib.get_close_matches(word, candidates, n=n, cutoff=cutoff)
    results = []
    for i, match in enumerate(matches):
        similarity = compute_similarity(word, match)
        results.append(SimilarStringMatch(
            candidate=match, similarity=similarity,
            similarity_percent=f"{similarity * 100:.1f}%",
            rank=i + 1,
        ))
    return results


def quick_ratio(a: str, b: str) -> float:
    """Compute a quick (upper bound) similarity estimate."""
    return difflib.SequenceMatcher(None, a, b).quick_ratio()


def real_quick_ratio(a: str, b: str) -> float:
    """Compute the fastest possible similarity estimate."""
    return difflib.SequenceMatcher(None, a, b).real_quick_ratio()
