"""Source merging utilities."""

from typing import Any, Dict, List

import pandas as pd


def merge_sources(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    merge_type: str = "inner",
    output_path: str = "",
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Merge two CSV sources on key columns.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names to join on.
        merge_type: Type of merge ('inner', 'left', 'right', 'outer').
        output_path: Optional path to save merged file.
        max_preview_rows: Maximum rows in preview.

    Returns:
        Dict with merge statistics and preview.
    """
    df_a = pd.read_csv(source_a_path)
    df_b = pd.read_csv(source_b_path)
    keys = [k.strip() for k in key_columns.split(",")]

    merged = pd.merge(df_a, df_b, on=keys, how=merge_type, suffixes=("_a", "_b"))

    result: Dict[str, Any] = {
        "source_a_rows": len(df_a),
        "source_b_rows": len(df_b),
        "merged_rows": len(merged),
        "merge_type": merge_type,
        "columns": list(merged.columns),
        "preview": merged.head(max_preview_rows).to_dict(orient="records"),
    }

    if output_path:
        merged.to_csv(output_path, index=False)
        result["saved_to"] = output_path

    return result
