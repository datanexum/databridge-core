"""Fuzzy matching utilities.

Requires rapidfuzz as an optional dependency.
"""

from typing import Any, Dict, List

import pandas as pd

from .._io import read_csv


def fuzzy_match_columns(
    source_a_path: str,
    source_b_path: str,
    column_a: str,
    column_b: str,
    threshold: int = 80,
    limit: int = 10,
) -> Dict[str, Any]:
    """Find fuzzy matches between two columns using RapidFuzz.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        column_a: Column name in source A.
        column_b: Column name in source B.
        threshold: Minimum similarity score (0-100).
        limit: Maximum matches to return.

    Returns:
        Dict with match results and similarity scores.

    Raises:
        ImportError: If rapidfuzz is not installed.
    """
    try:
        from rapidfuzz import fuzz, process
    except ImportError:
        raise ImportError(
            "rapidfuzz not installed. Run: pip install 'databridge-core[fuzzy]'"
        )

    df_a = read_csv(source_a_path)
    df_b = read_csv(source_b_path)

    values_a = df_a[column_a].astype(str).unique().tolist()
    values_b = df_b[column_b].astype(str).unique().tolist()

    # Import diff utilities for enhanced comparison
    try:
        from .differ import get_matching_blocks, get_opcodes
        diff_available = True
    except ImportError:
        diff_available = False

    matches = []
    for val_a in values_a[:50]:  # Limit source values to prevent timeout
        result = process.extractOne(val_a, values_b, scorer=fuzz.ratio)
        if result and result[1] >= threshold:
            match_entry: Dict[str, Any] = {
                "value_a": val_a,
                "value_b": result[0],
                "similarity": result[1],
            }

            if diff_available and result[1] < 100:
                matching_blocks = get_matching_blocks(val_a, result[0])
                opcodes = get_opcodes(val_a, result[0])
                match_entry["matching_blocks"] = [
                    {"content": b.content, "size": b.size}
                    for b in matching_blocks if b.size > 1
                ]
                match_entry["alignment"] = [
                    {"op": op.operation, "a": op.a_content, "b": op.b_content}
                    for op in opcodes if op.operation != "equal"
                ]

            matches.append(match_entry)

    matches.sort(key=lambda x: x["similarity"], reverse=True)

    return {
        "column_a": column_a,
        "column_b": column_b,
        "threshold": threshold,
        "total_matches": len(matches),
        "top_matches": matches[:limit],
    }


def fuzzy_deduplicate(
    source_path: str,
    column: str,
    threshold: int = 90,
    limit: int = 10,
) -> Dict[str, Any]:
    """Find potential duplicate values within a single column.

    Args:
        source_path: Path to the CSV file.
        column: Column name to check for duplicates.
        threshold: Minimum similarity score (0-100).
        limit: Maximum duplicate groups to return.

    Returns:
        Dict with duplicate groups.

    Raises:
        ImportError: If rapidfuzz is not installed.
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        raise ImportError(
            "rapidfuzz not installed. Run: pip install 'databridge-core[fuzzy]'"
        )

    df = read_csv(source_path)
    values = df[column].astype(str).unique().tolist()
    processed: set = set()
    duplicate_groups = []

    for i, val in enumerate(values):
        if val in processed:
            continue

        similar = []
        for other_val in values[i + 1:]:
            if other_val in processed:
                continue
            score = fuzz.ratio(val, other_val)
            if score >= threshold:
                similar.append({"value": other_val, "similarity": score})
                processed.add(other_val)

        if similar:
            duplicate_groups.append({
                "primary": val,
                "similar_values": similar,
            })
            processed.add(val)

    return {
        "column": column,
        "threshold": threshold,
        "total_groups": len(duplicate_groups),
        "duplicate_groups": duplicate_groups[:limit],
    }
