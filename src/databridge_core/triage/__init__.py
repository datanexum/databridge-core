"""DataBridge Triage — Batch Excel scanning and archetype classification.

Public API:
    scan_and_classify — Scan a directory of Excel files and classify each by archetype
    BatchExcelScanner — Low-level scanner for custom workflows
    ArchetypeClassifier — Heuristic classifier
    ReportGenerator — JSONL + JSON summary writer
"""
from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from ._classifier import ArchetypeClassifier
from ._report import ReportGenerator
from ._scanner import BatchExcelScanner
from ._types import (
    Archetype,
    BatchTriageReport,
    BatchTriageSummary,
    FileTriageResult,
    ScanStatus,
    SheetMetadata,
)

__all__ = [
    "scan_and_classify",
    "BatchExcelScanner",
    "ArchetypeClassifier",
    "ReportGenerator",
    "Archetype",
    "BatchTriageReport",
    "BatchTriageSummary",
    "FileTriageResult",
    "ScanStatus",
    "SheetMetadata",
]


def scan_and_classify(
    directory: str = "data/MVP",
    output_dir: str = "data/triage",
    max_workers: int = 4,
    deep_scan: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    """Scan all Excel files in *directory*, classify by archetype, write reports.

    Args:
        directory: Path to directory containing Excel files.
        output_dir: Where to write triage_report.jsonl and triage_summary.json.
        max_workers: Thread pool size for concurrent scanning.
        deep_scan: If True, also run full BLCE ExcelLogicExtractor per file.
        progress_callback: Optional ``callback(completed, total, filename)`` for progress.

    Returns:
        Dict with ``summary`` (aggregate stats) and ``sample_results`` (first 10 files).
    """
    t0 = time.time()

    # 1. Scan
    scanner = BatchExcelScanner(max_workers=max_workers, deep_scan=deep_scan)
    results = scanner.scan_directory(directory, progress_callback=progress_callback)

    # 2. Classify
    classifier = ArchetypeClassifier()
    classifier.classify_batch(results)

    # 3. Report
    duration = time.time() - t0
    reporter = ReportGenerator()
    report = reporter.generate(results, directory, duration, output_dir=output_dir)

    # 4. Return summary + sample (context limit: <=10 rows)
    sample = [r.model_dump(mode="json") for r in results[:10]]

    return {
        "summary": report.summary.model_dump(mode="json"),
        "sample_results": sample,
        "report_jsonl": f"{output_dir}/triage_report.jsonl",
        "summary_json": f"{output_dir}/triage_summary.json",
    }
