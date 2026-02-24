"""Column transformation -- upper, lower, strip, trim, remove_special."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


_OPERATIONS = {"upper", "lower", "strip", "trim_spaces", "remove_special"}


def transform_column(
    source_path: str | Path,
    column: str,
    operation: str,
    output_path: str | Path = "",
) -> dict[str, Any]:
    """Apply a string transformation to a CSV column.

    Args:
        source_path: Path to the input CSV.
        column: Column name to transform.
        operation: One of 'upper', 'lower', 'strip', 'trim_spaces', 'remove_special'.
        output_path: If provided, save the transformed CSV here.

    Returns:
        Dict with column, operation, preview (before/after samples),
        and optionally saved_to.

    Raises:
        ValueError: If column is missing or operation is unknown.
    """
    df = pd.read_csv(Path(source_path))

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found")

    original_sample = df[column].head(5).tolist()

    if operation == "upper":
        df[column] = df[column].astype(str).str.upper()
    elif operation == "lower":
        df[column] = df[column].astype(str).str.lower()
    elif operation == "strip":
        df[column] = df[column].astype(str).str.strip()
    elif operation == "trim_spaces":
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    elif operation == "remove_special":
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        )
    else:
        raise ValueError(f"Unknown operation: {operation}")

    transformed_sample = df[column].head(5).tolist()

    result: dict[str, Any] = {
        "column": column,
        "operation": operation,
        "preview": {
            "before": original_sample,
            "after": transformed_sample,
        },
    }

    if output_path:
        out = Path(output_path)
        df.to_csv(out, index=False)
        result["saved_to"] = str(out)
    else:
        result["note"] = "Preview only. Provide output_path to save."

    return result
