"""Data validation runner — validate data against expectation suites.

Standalone implementation for the open-source core.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .profile import _read_file


def validate(
    source_path: str,
    suite_path: Optional[str] = None,
    suite_name: Optional[str] = None,
    suite_dir: str = "data/expectations",
    output_dir: str = "data/validations",
) -> Dict[str, Any]:
    """Validate a data file against an expectation suite.

    Args:
        source_path: Path to the data file to validate.
        suite_path: Direct path to suite JSON file.
        suite_name: Suite name (looked up in suite_dir).
        suite_dir: Directory containing suite JSON files.
        output_dir: Directory to persist validation results.

    Returns:
        Dict with validation status, pass/fail counts, and failure details.
    """
    t0 = time.time()

    # Load suite
    if suite_path:
        sp = Path(suite_path)
    elif suite_name:
        sp = Path(suite_dir) / f"{suite_name}.json"
    else:
        raise ValueError("Either suite_path or suite_name is required")

    if not sp.exists():
        raise FileNotFoundError(f"Suite not found: {sp}")

    with open(sp, "r", encoding="utf-8") as f:
        suite = json.load(f)

    expectations = suite.get("expectations", [])

    # Load data
    df = _read_file(source_path)
    row_count = len(df)

    passed = 0
    failed = 0
    failures: List[Dict[str, Any]] = []

    for exp in expectations:
        etype = exp.get("type", "")
        success = False

        if etype == "expect_columns_to_exist":
            expected_cols = set(exp.get("columns", []))
            actual_cols = set(df.columns)
            missing = expected_cols - actual_cols
            success = len(missing) == 0
            if not success:
                failures.append({
                    "expectation": etype,
                    "expected": list(expected_cols),
                    "observed": list(actual_cols),
                    "detail": f"Missing columns: {sorted(missing)}",
                })

        elif etype == "expect_row_count_between":
            min_rows = exp.get("min", 0)
            max_rows = exp.get("max", float("inf"))
            success = min_rows <= row_count <= max_rows
            if not success:
                failures.append({
                    "expectation": etype,
                    "expected": f"{min_rows}-{max_rows}",
                    "observed": row_count,
                    "detail": f"Row count {row_count} outside range [{min_rows}, {max_rows}]",
                })

        elif etype == "expect_column_not_null":
            col = exp.get("column", "")
            max_null_pct = exp.get("max_null_pct", 5.0)
            if col in df.columns:
                null_pct = df[col].isnull().sum() / row_count * 100 if row_count > 0 else 0
                success = null_pct <= max_null_pct
                if not success:
                    failures.append({
                        "expectation": etype,
                        "column": col,
                        "expected": f"<={max_null_pct}% null",
                        "observed": f"{null_pct:.2f}% null",
                    })
            else:
                failures.append({
                    "expectation": etype,
                    "column": col,
                    "detail": f"Column '{col}' not found",
                })

        elif etype == "expect_column_unique":
            col = exp.get("column", "")
            if col in df.columns:
                dup_count = df[col].duplicated().sum()
                success = dup_count == 0
                if not success:
                    failures.append({
                        "expectation": etype,
                        "column": col,
                        "expected": "0 duplicates",
                        "observed": f"{dup_count} duplicates",
                    })
            else:
                failures.append({
                    "expectation": etype,
                    "column": col,
                    "detail": f"Column '{col}' not found",
                })

        elif etype == "expect_column_type":
            col = exp.get("column", "")
            expected_type = exp.get("expected_type", "")
            if col in df.columns:
                actual_type = str(df[col].dtype)
                success = actual_type == expected_type
                if not success:
                    failures.append({
                        "expectation": etype,
                        "column": col,
                        "expected": expected_type,
                        "observed": actual_type,
                    })
            else:
                failures.append({
                    "expectation": etype,
                    "column": col,
                    "detail": f"Column '{col}' not found",
                })
        else:
            # Unknown expectation type — skip
            continue

        if success:
            passed += 1
        else:
            failed += 1

    duration = round(time.time() - t0, 3)
    total = passed + failed
    status = "passed" if failed == 0 else "failed"
    success_pct = round(passed / total * 100, 1) if total > 0 else 0.0

    result = {
        "validation_id": uuid.uuid4().hex[:12],
        "suite_name": suite.get("name", ""),
        "source_file": source_path,
        "status": status,
        "total_expectations": total,
        "passed": passed,
        "failed": failed,
        "success_percent": success_pct,
        "row_count": row_count,
        "duration_seconds": duration,
        "run_at": datetime.now(timezone.utc).isoformat(),
        "failures": failures,
    }

    # Persist
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    result_file = out / f"{suite.get('name', 'validation')}_{result['validation_id']}.json"
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    return result


def get_validation_results(
    suite_name: str,
    output_dir: str = "data/validations",
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Get historical validation results for a suite.

    Args:
        suite_name: Suite name prefix to filter by.
        output_dir: Directory containing validation result files.
        limit: Maximum results to return.

    Returns:
        List of validation result summaries (most recent first).
    """
    results_dir = Path(output_dir)
    if not results_dir.exists():
        return []

    results = []
    for fp in sorted(results_dir.glob(f"{suite_name}_*.json"), reverse=True):
        if len(results) >= limit:
            break
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            results.append({
                "validation_id": data.get("validation_id", ""),
                "status": data.get("status", ""),
                "run_at": data.get("run_at", ""),
                "total": data.get("total_expectations", 0),
                "passed": data.get("passed", 0),
                "failed": data.get("failed", 0),
                "success_percent": data.get("success_percent", 0),
                "duration_seconds": data.get("duration_seconds", 0),
            })
        except Exception:
            continue

    return results
