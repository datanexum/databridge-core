"""Expectation suite generation from profiled data.

Standalone implementation for the open-source core — no external dependencies.
Generates data quality expectations that can be validated with ``validate()``.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .profile import _read_file


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def generate_expectation_suite(
    source_path: str,
    name: Optional[str] = None,
    output_dir: str = "data/expectations",
    null_threshold: float = 5.0,
    uniqueness_threshold: float = 0.99,
) -> Dict[str, Any]:
    """Generate an expectation suite by profiling a data file.

    Automatically creates expectations for:
    - Column presence (all columns expected)
    - Not-null constraints (columns with <null_threshold% nulls)
    - Uniqueness (columns with >uniqueness_threshold unique ratio)
    - Type expectations (inferred from pandas dtypes)
    - Row count range (±50% of current count)

    Args:
        source_path: Path to data file (CSV, Excel, JSON, Parquet).
        name: Suite name (defaults to filename stem).
        output_dir: Directory to persist the suite JSON.
        null_threshold: Max null % to generate not_null expectation.
        uniqueness_threshold: Min unique ratio to generate unique expectation.

    Returns:
        Dict with suite metadata and expectation count.
    """
    df = _read_file(source_path)
    suite_name = name or Path(source_path).stem
    row_count = len(df)

    expectations: List[Dict[str, Any]] = []

    # 1. Column presence
    expectations.append({
        "type": "expect_columns_to_exist",
        "columns": list(df.columns),
    })

    # 2. Row count range (±50%)
    expectations.append({
        "type": "expect_row_count_between",
        "min": max(1, int(row_count * 0.5)),
        "max": int(row_count * 1.5),
    })

    for col in df.columns:
        null_pct = df[col].isnull().sum() / row_count * 100 if row_count > 0 else 0
        unique_ratio = df[col].nunique() / row_count if row_count > 0 else 0

        # 3. Not-null
        if null_pct <= null_threshold:
            expectations.append({
                "type": "expect_column_not_null",
                "column": col,
                "max_null_pct": round(null_threshold, 2),
            })

        # 4. Uniqueness
        if unique_ratio >= uniqueness_threshold:
            expectations.append({
                "type": "expect_column_unique",
                "column": col,
            })

        # 5. Type
        dtype_str = str(df[col].dtype)
        expectations.append({
            "type": "expect_column_type",
            "column": col,
            "expected_type": dtype_str,
        })

    suite = {
        "suite_id": _new_id(),
        "name": suite_name,
        "source_file": source_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "row_count_at_creation": row_count,
        "expectations": expectations,
    }

    # Persist
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    suite_file = out / f"{suite_name}.json"
    with open(suite_file, "w", encoding="utf-8") as f:
        json.dump(suite, f, indent=2, default=str)

    return {
        "suite_name": suite_name,
        "suite_id": suite["suite_id"],
        "expectations_count": len(expectations),
        "output_file": str(suite_file),
    }


def list_expectation_suites(
    output_dir: str = "data/expectations",
) -> List[Dict[str, Any]]:
    """List all persisted expectation suites.

    Args:
        output_dir: Directory containing suite JSON files.

    Returns:
        List of suite summaries.
    """
    suites_dir = Path(output_dir)
    if not suites_dir.exists():
        return []

    results = []
    for fp in sorted(suites_dir.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            results.append({
                "name": data.get("name", fp.stem),
                "suite_id": data.get("suite_id", ""),
                "expectations_count": len(data.get("expectations", [])),
                "created_at": data.get("created_at", ""),
                "source_file": data.get("source_file", ""),
            })
        except Exception:
            continue

    return results
