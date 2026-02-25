"""Report generation for batch triage results.

Writes per-file results as JSONL (one JSON object per line) and
a summary JSON file with aggregate statistics.
"""
from __future__ import annotations

import json
import os
import statistics
from pathlib import Path
from typing import Any, Dict, List

from ._types import (
    Archetype,
    BatchTriageReport,
    BatchTriageSummary,
    FileTriageResult,
    ScanStatus,
)


class ReportGenerator:
    """Generate JSONL and JSON summary reports from triage results."""

    def generate(
        self,
        results: List[FileTriageResult],
        directory: str,
        duration_seconds: float,
        output_dir: str = "data/triage",
    ) -> BatchTriageReport:
        """Build a report, write files, and return the report object.

        Writes:
        - ``{output_dir}/triage_report.jsonl`` — one JSON line per file
        - ``{output_dir}/triage_summary.json`` — aggregate statistics
        """
        summary = self._compute_summary(results, duration_seconds)
        report = BatchTriageReport(
            directory=directory,
            summary=summary,
            results=results,
        )

        # Ensure output directory exists
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        self._write_jsonl(results, out / "triage_report.jsonl")
        self._write_summary(report, out / "triage_summary.json")

        return report

    # ------------------------------------------------------------------
    # Summary computation
    # ------------------------------------------------------------------

    def _compute_summary(
        self,
        results: List[FileTriageResult],
        duration_seconds: float,
    ) -> BatchTriageSummary:
        total = len(results)
        scanned = sum(1 for r in results if r.scan_status == ScanStatus.OK)
        errors = sum(1 for r in results if r.scan_status == ScanStatus.ERROR)
        skipped = sum(1 for r in results if r.scan_status == ScanStatus.SKIPPED)

        ok_results = [r for r in results if r.scan_status == ScanStatus.OK]

        # Archetype distribution
        archetype_counts: Dict[str, int] = {}
        for a in Archetype:
            count = sum(1 for r in results if r.archetype == a)
            if count > 0:
                archetype_counts[a.value] = count

        # Totals
        total_sheets = sum(r.sheet_count for r in ok_results)
        total_formulas = sum(r.formula_count for r in ok_results)
        total_named = sum(r.named_range_count for r in ok_results)
        total_rows = sum(r.total_row_count for r in ok_results)

        # Averages
        avg_sheets = total_sheets / scanned if scanned else 0.0
        avg_formulas = total_formulas / scanned if scanned else 0.0

        # Macro/pivot counts
        macros = sum(1 for r in ok_results if r.has_macros)
        pivots = sum(1 for r in ok_results if r.has_pivot_tables)

        # File size stats
        sizes = [r.file_size_bytes for r in results if r.file_size_bytes > 0]
        min_size = min(sizes) if sizes else 0
        max_size = max(sizes) if sizes else 0
        avg_size = sum(sizes) / len(sizes) if sizes else 0.0
        median_size = float(statistics.median(sizes)) if sizes else 0.0

        # Throughput
        fps = total / duration_seconds if duration_seconds > 0 else 0.0

        return BatchTriageSummary(
            total_files=total,
            scanned=scanned,
            errors=errors,
            skipped=skipped,
            archetype_counts=archetype_counts,
            total_sheets=total_sheets,
            total_formulas=total_formulas,
            total_named_ranges=total_named,
            total_rows=total_rows,
            avg_sheets_per_file=round(avg_sheets, 2),
            avg_formulas_per_file=round(avg_formulas, 2),
            files_with_macros=macros,
            files_with_pivots=pivots,
            min_file_size=min_size,
            max_file_size=max_size,
            avg_file_size=round(avg_size, 2),
            median_file_size=round(median_size, 2),
            duration_seconds=round(duration_seconds, 2),
            files_per_second=round(fps, 2),
        )

    # ------------------------------------------------------------------
    # File writers
    # ------------------------------------------------------------------

    def _write_jsonl(self, results: List[FileTriageResult], path: Path) -> None:
        """Write one JSON object per line."""
        with open(path, "w", encoding="utf-8") as f:
            for r in results:
                line = r.model_dump(mode="json")
                f.write(json.dumps(line, ensure_ascii=False) + "\n")

    def _write_summary(self, report: BatchTriageReport, path: Path) -> None:
        """Write the full report (without per-file results) as pretty JSON."""
        data = {
            "report_id": report.report_id,
            "directory": report.directory,
            "summary": report.summary.model_dump(mode="json"),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
