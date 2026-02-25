"""Data profiling utilities."""

import json
from typing import Any, Dict, List, Optional

import pandas as pd


def profile_data(source_path: str) -> Dict[str, Any]:
    """Analyze data structure and quality.

    Args:
        source_path: Path to CSV file to profile.

    Returns:
        Dict with profiling statistics including structure type, cardinality,
        and data quality metrics.
    """
    df = pd.read_csv(source_path)

    cardinality = df.nunique() / len(df)

    has_date = any(
        col.lower() in ["date", "datetime", "timestamp", "created_at", "updated_at"]
        for col in df.columns
    )
    is_fact = "Transactional/Fact" if len(df) > 1000 and has_date else "Dimension/Reference"

    potential_keys = list(cardinality[cardinality > 0.99].index)

    null_pct = (df.isnull().sum() / len(df) * 100).round(2).to_dict()
    duplicate_rows = df.duplicated().sum()

    return {
        "file": source_path,
        "rows": len(df),
        "columns": len(df.columns),
        "structure_type": is_fact,
        "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "potential_key_columns": potential_keys,
        "high_cardinality_cols": list(cardinality[cardinality > 0.9].index),
        "low_cardinality_cols": list(cardinality[cardinality < 0.1].index),
        "data_quality": {
            "null_percentage": null_pct,
            "duplicate_rows": duplicate_rows,
            "duplicate_percentage": round(duplicate_rows / len(df) * 100, 2),
        },
        "statistics": json.loads(df.describe(include="all").to_json()),
    }


def detect_schema_drift(
    source_a_path: str,
    source_b_path: str,
) -> Dict[str, Any]:
    """Compare schemas between two CSV files to detect drift.

    Args:
        source_a_path: Path to first CSV (baseline).
        source_b_path: Path to second CSV (target).

    Returns:
        Dict with schema differences including added, removed, and type-changed columns.
    """
    df_a = pd.read_csv(source_a_path)
    df_b = pd.read_csv(source_b_path)

    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)

    types_a = {col: str(dtype) for col, dtype in df_a.dtypes.items()}
    types_b = {col: str(dtype) for col, dtype in df_b.dtypes.items()}

    common_cols = cols_a & cols_b

    SAFE_CONVERSIONS = {
        ("int64", "float64"): True,
        ("int32", "float64"): True,
        ("int64", "object"): False,
        ("float64", "object"): False,
        ("object", "int64"): False,
        ("object", "float64"): False,
    }

    # Use diff utilities if available
    try:
        from databridge_core.reconciler import compute_similarity
        diff_available = True
    except ImportError:
        diff_available = False

    type_changes: Dict[str, Dict[str, Any]] = {}
    for col in common_cols:
        if types_a[col] != types_b[col]:
            change_info: Dict[str, Any] = {"from": types_a[col], "to": types_b[col]}

            if diff_available:
                change_info["type_similarity"] = round(
                    compute_similarity(types_a[col], types_b[col]), 4
                )

            conversion_key = (types_a[col], types_b[col])
            if conversion_key in SAFE_CONVERSIONS:
                change_info["safe_conversion"] = SAFE_CONVERSIONS[conversion_key]
                if not SAFE_CONVERSIONS[conversion_key]:
                    change_info["warning"] = (
                        f"Conversion from {types_a[col]} to {types_b[col]} may lose data"
                    )

            type_changes[col] = change_info

    return {
        "source_a": source_a_path,
        "source_b": source_b_path,
        "columns_added": list(cols_b - cols_a),
        "columns_removed": list(cols_a - cols_b),
        "columns_common": list(common_cols),
        "type_changes": type_changes,
        "has_drift": bool((cols_b - cols_a) or (cols_a - cols_b) or type_changes),
    }
