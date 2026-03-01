"""Counterexample capture for regression testing.

When monitor warnings or unexpected detection failures occur, this
module captures the failing input as a regression fixture that can
be replayed in future test runs.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_COUNTEREXAMPLE_DIR = "data/detection/counterexamples"


def capture_counterexample(
    file_path: str,
    findings: List[Dict[str, Any]],
    monitor_warnings: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
    output_dir: str = DEFAULT_COUNTEREXAMPLE_DIR,
) -> Optional[str]:
    """Capture a detection failure as a regression fixture.

    Saves the file path, findings, warnings, and context so the
    failure can be reproduced and tested against in the future.

    Args:
        file_path: Path to the file that triggered the failure.
        findings: List of finding dicts from the detection run.
        monitor_warnings: List of monitor warning dicts.
        context: Optional additional context (rules used, params, etc.).
        output_dir: Directory to save counterexamples.

    Returns:
        Path to the saved counterexample file, or None if no warnings.
    """
    if not monitor_warnings:
        return None

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    stem = Path(file_path).stem if file_path else "unknown"
    out_file = out_dir / f"cx_{stem}_{timestamp}.json"

    counterexample = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "source_file": file_path,
        "finding_count": len(findings),
        "warning_count": len(monitor_warnings),
        "warnings": monitor_warnings,
        "findings_sample": findings[:20],
        "context": context or {},
    }

    try:
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(counterexample, f, indent=2, default=str)
        logger.info("Captured counterexample: %s", out_file)
        return str(out_file)
    except Exception as e:
        logger.warning("Failed to save counterexample: %s", e)
        return None


def load_counterexamples(
    counterexample_dir: str = DEFAULT_COUNTEREXAMPLE_DIR,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Load saved counterexamples for regression testing.

    Args:
        counterexample_dir: Directory containing counterexample JSON files.
        limit: Maximum number to load.

    Returns:
        List of counterexample dicts, newest first.
    """
    cx_dir = Path(counterexample_dir)
    if not cx_dir.exists():
        return []

    files = sorted(cx_dir.glob("cx_*.json"), reverse=True)[:limit]
    results: List[Dict[str, Any]] = []

    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            data["_file"] = str(f)
            results.append(data)
        except Exception as e:
            logger.warning("Failed to load counterexample %s: %s", f, e)

    return results
