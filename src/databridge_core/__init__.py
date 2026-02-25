"""DataBridge Core -- Data reconciliation, profiling, and ingestion toolkit.

Upload your Chart of Accounts. Get a production-ready financial hierarchy
and dbt models. Zero config.

Quick start::

    from databridge_core import compare_hashes, profile_data, load_csv

    result = profile_data("sales.csv")
    print(result["rows"], "rows,", result["columns"], "columns")

    comparison = compare_hashes("source.csv", "target.csv", key_columns="id")
    print(comparison["statistics"]["match_rate_percent"], "% match rate")
"""

__version__ = "1.2.0"

# Reconciler
from .reconciler import (
    compare_hashes,
    get_orphan_details,
    get_conflict_details,
    fuzzy_match_columns,
    fuzzy_deduplicate,
    merge_sources,
    compute_similarity,
    diff_lists,
    diff_dicts,
    explain_diff,
    find_close_matches,
    find_similar_strings,
    transform_column,
)

# Profiler
from .profiler import profile_data, detect_schema_drift

# Ingestion
from .ingestion import load_csv, load_json, extract_pdf_text, parse_table_from_text

# Files
from .files import find_files, stage_file

# Templates (always available)
from .templates import TemplateService, FinancialTemplate

# Integrations (always available — stdlib only)
from .integrations import BaseClient, SlackClient

# Triage (lazy — requires openpyxl)
def scan_and_classify(*args, **kwargs):
    """Scan Excel files and classify by archetype. Requires: pip install 'databridge-core[triage]'."""
    from .triage import scan_and_classify as _scan
    return _scan(*args, **kwargs)

__all__ = [
    "__version__",
    # Reconciler
    "compare_hashes",
    "get_orphan_details",
    "get_conflict_details",
    "fuzzy_match_columns",
    "fuzzy_deduplicate",
    "merge_sources",
    "compute_similarity",
    "diff_lists",
    "diff_dicts",
    "explain_diff",
    "find_close_matches",
    "find_similar_strings",
    "transform_column",
    # Profiler
    "profile_data",
    "detect_schema_drift",
    # Ingestion
    "load_csv",
    "load_json",
    "extract_pdf_text",
    "parse_table_from_text",
    # Files
    "find_files",
    "stage_file",
    # Templates
    "TemplateService",
    "FinancialTemplate",
    # Integrations
    "BaseClient",
    "SlackClient",
    # Triage
    "scan_and_classify",
]
