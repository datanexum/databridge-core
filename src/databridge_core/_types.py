"""Shared result types for the databridge-core library.

All library functions return Python objects (dicts, dataclasses, Pydantic models).
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# -- Profiler types --

class ProfileResult(BaseModel):
    """Result of profiling a data source."""
    file: str
    rows: int
    columns: int
    structure_type: str
    column_types: Dict[str, str]
    potential_key_columns: List[str]
    high_cardinality_cols: List[str]
    low_cardinality_cols: List[str]
    data_quality: Dict[str, Any]
    statistics: Dict[str, Any]


class DriftResult(BaseModel):
    """Result of schema drift detection."""
    source_a: str
    source_b: str
    columns_added: List[str]
    columns_removed: List[str]
    columns_common: List[str]
    type_changes: Dict[str, Dict[str, Any]]
    has_drift: bool


# -- Reconciler types --

class CompareHashesResult(BaseModel):
    """Result of hash-based row comparison."""
    source_a: Dict[str, Any]
    source_b: Dict[str, Any]
    key_columns: List[str]
    compare_columns: List[str]
    statistics: Dict[str, Any]


class OrphanResult(BaseModel):
    """Result of orphan record retrieval."""
    orphan_source: str
    orphans_in_a: Optional[Dict[str, Any]] = None
    orphans_in_b: Optional[Dict[str, Any]] = None


class ConflictResult(BaseModel):
    """Result of conflict detail retrieval."""
    total_conflicts: int
    showing: int
    conflicts: List[Dict[str, Any]]


class FuzzyMatchResult(BaseModel):
    """Result of fuzzy column matching."""
    column_a: str
    column_b: str
    threshold: int
    total_matches: int
    top_matches: List[Dict[str, Any]]


class MergeResult(BaseModel):
    """Result of merging two sources."""
    source_a_rows: int
    source_b_rows: int
    merged_rows: int
    merge_type: str
    columns: List[str]
    preview: List[Dict[str, Any]]


# -- Ingestion types --

class LoadResult(BaseModel):
    """Result of loading a file."""
    file: str
    rows: int
    columns: List[str]
    preview: List[Dict[str, Any]]
    dtypes: Optional[Dict[str, str]] = None
    null_counts: Optional[Dict[str, int]] = None


class PdfExtractResult(BaseModel):
    """Result of PDF text extraction."""
    file: str
    total_pages: int
    pages_extracted: int
    content: List[Dict[str, Any]]


class OcrResult(BaseModel):
    """Result of OCR text extraction."""
    file: str
    language: str
    text: str
    character_count: int


class TableParseResult(BaseModel):
    """Result of parsing tabular data from text."""
    columns: Optional[List[str]] = None
    row_count: Optional[int] = None
    preview: Optional[List[Dict[str, Any]]] = None
    raw_row: Optional[List[str]] = None


class QueryResult(BaseModel):
    """Result of a database query."""
    rows_returned: int
    columns: List[str]
    dtypes: Optional[Dict[str, str]] = None
    preview: List[Dict[str, Any]]
    truncated: bool = False
    sql: Optional[str] = None
