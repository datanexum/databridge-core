"""Row-level hashing and comparison engine.

Compare two CSV sources by hashing rows to identify orphans and conflicts.
"""

import hashlib
from typing import Any, Dict, List, Optional

import pandas as pd

from .._io import read_csv


def _compute_row_hash(row: pd.Series, columns: list) -> str:
    """Compute a deterministic SHA-256 hash for a row (truncated to 16 chars)."""
    values = "|".join(str(row[col]) for col in columns)
    return hashlib.sha256(values.encode()).hexdigest()[:16]


def _compute_hashes(df: pd.DataFrame, columns: list) -> pd.Series:
    """Compute row hashes using apply."""
    return df.apply(lambda row: _compute_row_hash(row, columns), axis=1)


def compare_hashes(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    compare_columns: str = "",
) -> Dict[str, Any]:
    """Compare two CSV sources by hashing rows to identify orphans and conflicts.

    Args:
        source_a_path: Path to the first CSV file (source of truth).
        source_b_path: Path to the second CSV file (target).
        key_columns: Comma-separated column names that uniquely identify a row.
        compare_columns: Optional comma-separated columns to compare. Defaults to all non-key.

    Returns:
        Dict with source info, key/compare columns, and statistics.
    """
    df_a = read_csv(source_a_path)
    df_b = read_csv(source_b_path)

    keys = [k.strip() for k in key_columns.split(",")]
    if compare_columns:
        compare_cols = [c.strip() for c in compare_columns.split(",")]
    else:
        compare_cols = [c for c in df_a.columns if c not in keys]

    # Validate columns
    for col in keys + compare_cols:
        if col not in df_a.columns:
            raise ValueError(f"Column '{col}' not found in source A")
        if col not in df_b.columns:
            raise ValueError(f"Column '{col}' not found in source B")

    # Composite keys
    df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
    df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

    # Value hashes
    df_a["_value_hash"] = _compute_hashes(df_a, compare_cols)
    df_b["_value_hash"] = _compute_hashes(df_b, compare_cols)

    keys_a = set(df_a["_composite_key"])
    keys_b = set(df_b["_composite_key"])

    orphans_in_a = keys_a - keys_b
    orphans_in_b = keys_b - keys_a
    common_keys = keys_a & keys_b

    hash_map_a = df_a.set_index("_composite_key")["_value_hash"].to_dict()
    hash_map_b = df_b.set_index("_composite_key")["_value_hash"].to_dict()

    conflicts = [k for k in common_keys if hash_map_a[k] != hash_map_b[k]]
    matches = [k for k in common_keys if hash_map_a[k] == hash_map_b[k]]

    return {
        "source_a": {"path": source_a_path, "total_rows": len(df_a)},
        "source_b": {"path": source_b_path, "total_rows": len(df_b)},
        "key_columns": keys,
        "compare_columns": compare_cols,
        "statistics": {
            "orphans_only_in_source_a": len(orphans_in_a),
            "orphans_only_in_source_b": len(orphans_in_b),
            "total_orphans": len(orphans_in_a) + len(orphans_in_b),
            "conflicts": len(conflicts),
            "exact_matches": len(matches),
            "match_rate_percent": round(len(matches) / max(len(common_keys), 1) * 100, 2),
        },
    }


def get_orphan_details(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    orphan_source: str = "both",
    limit: int = 10,
) -> Dict[str, Any]:
    """Retrieve details of orphan records.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated key column names.
        orphan_source: Which orphans to return: 'a', 'b', or 'both'.
        limit: Maximum orphans per source.

    Returns:
        Dict with orphan records and counts.
    """
    df_a = read_csv(source_a_path)
    df_b = read_csv(source_b_path)
    keys = [k.strip() for k in key_columns.split(",")]

    df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
    df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

    keys_a = set(df_a["_composite_key"])
    keys_b = set(df_b["_composite_key"])

    result: Dict[str, Any] = {"orphan_source": orphan_source}

    if orphan_source in ["a", "both"]:
        orphans_a = df_a[df_a["_composite_key"].isin(keys_a - keys_b)]
        orphans_a = orphans_a.drop(columns=["_composite_key"])
        result["orphans_in_a"] = {
            "total": len(orphans_a),
            "sample": orphans_a.head(limit).to_dict(orient="records"),
        }

    if orphan_source in ["b", "both"]:
        orphans_b = df_b[df_b["_composite_key"].isin(keys_b - keys_a)]
        orphans_b = orphans_b.drop(columns=["_composite_key"])
        result["orphans_in_b"] = {
            "total": len(orphans_b),
            "sample": orphans_b.head(limit).to_dict(orient="records"),
        }

    return result


def get_conflict_details(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    compare_columns: str = "",
    limit: int = 10,
) -> Dict[str, Any]:
    """Retrieve details of conflicting records (same key, different values).

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated key column names.
        compare_columns: Optional comma-separated columns to compare.
        limit: Maximum conflicts to return.

    Returns:
        Dict with conflict details including per-column diffs.
    """
    from .differ import compute_similarity, get_opcodes, explain_diff

    df_a = read_csv(source_a_path)
    df_b = read_csv(source_b_path)
    keys = [k.strip() for k in key_columns.split(",")]

    if compare_columns:
        compare_cols = [c.strip() for c in compare_columns.split(",")]
    else:
        compare_cols = [c for c in df_a.columns if c not in keys]

    df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
    df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

    df_a["_value_hash"] = _compute_hashes(df_a, compare_cols)
    df_b["_value_hash"] = _compute_hashes(df_b, compare_cols)

    hash_map_a = df_a.set_index("_composite_key")["_value_hash"].to_dict()
    hash_map_b = df_b.set_index("_composite_key")["_value_hash"].to_dict()

    common_keys = set(df_a["_composite_key"]) & set(df_b["_composite_key"])
    conflict_keys = [k for k in common_keys if hash_map_a.get(k) != hash_map_b.get(k)]

    conflicts = []
    for key in list(conflict_keys)[:limit]:
        row_a = df_a[df_a["_composite_key"] == key].iloc[0]
        row_b = df_b[df_b["_composite_key"] == key].iloc[0]

        diff_cols = []
        for col in compare_cols:
            if str(row_a[col]) != str(row_b[col]):
                val_a_str = str(row_a[col])
                val_b_str = str(row_b[col])

                diff_entry: Dict[str, Any] = {
                    "column": col,
                    "value_a": row_a[col],
                    "value_b": row_b[col],
                }

                similarity = compute_similarity(val_a_str, val_b_str)
                diff_entry["similarity"] = round(similarity, 4)

                if 0 < similarity < 1:
                    opcodes = get_opcodes(val_a_str, val_b_str)
                    diff_entry["opcodes"] = [
                        {"operation": op.operation, "a_content": op.a_content, "b_content": op.b_content}
                        for op in opcodes if op.operation != "equal"
                    ]
                    diff_entry["explanation"] = explain_diff(val_a_str, val_b_str)

                diff_cols.append(diff_entry)

        conflicts.append({
            "key": {k: row_a[k] for k in keys},
            "differences": diff_cols,
        })

    return {
        "total_conflicts": len(conflict_keys),
        "showing": len(conflicts),
        "conflicts": conflicts,
    }
