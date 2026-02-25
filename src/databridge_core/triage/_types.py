"""Pydantic models for batch Excel triage.

Defines the data structures for file scanning, archetype classification,
and batch summary reporting.
"""
from __future__ import annotations

import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def _new_id(prefix: str = "triage") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class ScanStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    SKIPPED = "skipped"


class Archetype(str, Enum):
    FINANCIAL_REPORT = "Financial Report"
    DATA_EXTRACT = "Data Extract"
    MODEL_TEMPLATE = "Model/Template"
    REFERENCE_DATA = "Reference Data"
    ACADEMIC_EXERCISE = "Academic/Exercise"
    CONSOLIDATION = "Consolidation"
    UNKNOWN = "Unknown"


class SheetMetadata(BaseModel):
    """Metadata for a single worksheet."""
    name: str = ""
    row_count: int = 0
    col_count: int = 0
    formula_count: int = 0
    anchor_row: Optional[int] = None
    anchor_col: Optional[int] = None
    has_pivot_table: bool = False
    is_empty: bool = False


class FileTriageResult(BaseModel):
    """Triage result for a single Excel file."""
    file_path: str = ""
    file_name: str = ""
    file_size_bytes: int = 0
    file_extension: str = ""
    scan_status: ScanStatus = ScanStatus.OK
    error_message: Optional[str] = None

    # Workbook-level metadata
    sheet_count: int = 0
    sheet_names: List[str] = Field(default_factory=list)
    sheets: List[SheetMetadata] = Field(default_factory=list)
    total_row_count: int = 0

    # Formula and structure
    formula_count: int = 0
    named_range_count: int = 0
    has_macros: bool = False
    has_pivot_tables: bool = False

    # Deep scan fields (populated only when deep_scan=True)
    measure_count: Optional[int] = None
    dependency_count: Optional[int] = None
    confidence: Optional[float] = None

    # Classification
    archetype: Archetype = Archetype.UNKNOWN
    archetype_confidence: float = 0.0
    archetype_reasons: List[str] = Field(default_factory=list)

    # Formula pattern flags
    has_sumif_pattern: bool = False
    has_vlookup_pattern: bool = False
    has_if_chain: bool = False
    dominant_formula_functions: List[str] = Field(default_factory=list)


class BatchTriageSummary(BaseModel):
    """Aggregate statistics for the entire batch scan."""
    total_files: int = 0
    scanned: int = 0
    errors: int = 0
    skipped: int = 0
    archetype_counts: Dict[str, int] = Field(default_factory=dict)

    total_sheets: int = 0
    total_formulas: int = 0
    total_named_ranges: int = 0
    total_rows: int = 0

    avg_sheets_per_file: float = 0.0
    avg_formulas_per_file: float = 0.0

    files_with_macros: int = 0
    files_with_pivots: int = 0

    min_file_size: int = 0
    max_file_size: int = 0
    avg_file_size: float = 0.0
    median_file_size: float = 0.0

    duration_seconds: float = 0.0
    files_per_second: float = 0.0


class BatchTriageReport(BaseModel):
    """Complete batch triage report with summary and per-file results."""
    report_id: str = Field(default_factory=lambda: _new_id("report"))
    directory: str = ""
    summary: BatchTriageSummary = Field(default_factory=BatchTriageSummary)
    results: List[FileTriageResult] = Field(default_factory=list)
