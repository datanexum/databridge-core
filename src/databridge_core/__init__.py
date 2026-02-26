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

__version__ = "1.5.0"

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
from .profiler import (
    profile_data,
    detect_schema_drift,
    generate_expectation_suite,
    list_expectation_suites,
    validate,
    get_validation_results,
)

# Ingestion
from .ingestion import load_csv, load_json, extract_pdf_text, parse_table_from_text

# Files
from .files import find_files, stage_file

# Templates (always available)
from .templates import TemplateService, FinancialTemplate

# Integrations (always available — stdlib only)
from .integrations import BaseClient, SlackClient

# Detection modules (always available — stdlib only)
from .erp_detect import detect_erp, detect_erp_batch
from .fraud_detect import detect_fraud, detect_fraud_batch
from .fx_validate import validate_fx, validate_fx_batch
from .standards_check import check_standards, check_standards_batch

# Grounded Detection (lazy — optional langgraph for AI pipeline)
def detect_grounded(*args, **kwargs):
    """Run KB-grounded anomaly detection on a CSV file. Requires: Knowledge Base rules in data/knowledge/."""
    from .detection import detect_grounded as _detect
    return _detect(*args, **kwargs)

def detect_grounded_batch(*args, **kwargs):
    """Run KB-grounded detection on a batch of CSV files."""
    from .detection import detect_grounded_batch as _batch
    return _batch(*args, **kwargs)

def record_feedback(*args, **kwargs):
    """Record user feedback on a detection finding for the learning loop."""
    from .detection import record_feedback as _fb
    return _fb(*args, **kwargs)

def get_detection_stats(*args, **kwargs):
    """Get detection performance and learning statistics."""
    from .detection import get_detection_stats as _stats
    return _stats(*args, **kwargs)

# Triage (lazy — requires openpyxl)
def scan_and_classify(*args, **kwargs):
    """Scan Excel files and classify by archetype. Requires: pip install 'databridge-core[triage]'."""
    from .triage import scan_and_classify as _scan
    return _scan(*args, **kwargs)

# Linker (lazy — entity linking for Logic DNA files)
def link_entities(*args, **kwargs):
    """Resolve entities across Logic DNA files. Cross-file entity linking with financial synonym awareness."""
    from .linker import link_entities as _link
    return _link(*args, **kwargs)

def find_entity(*args, **kwargs):
    """Fuzzy search for an entity across linked clusters."""
    from .linker import find_entity as _find
    return _find(*args, **kwargs)

# Connectors (lazy — requires duckdb)
def query_local(*args, **kwargs):
    """Execute SQL against local files using DuckDB. Requires: pip install 'databridge-core[duckdb]'."""
    from .connectors import query_local as _query
    return _query(*args, **kwargs)

def export_to_parquet(*args, **kwargs):
    """Export query results or tables to Parquet format. Requires: pip install 'databridge-core[duckdb]'."""
    from .connectors import export_to_parquet as _export
    return _export(*args, **kwargs)

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
    "generate_expectation_suite",
    "list_expectation_suites",
    "validate",
    "get_validation_results",
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
    # Detection
    "detect_erp",
    "detect_erp_batch",
    "detect_fraud",
    "detect_fraud_batch",
    "validate_fx",
    "validate_fx_batch",
    "check_standards",
    "check_standards_batch",
    # Grounded Detection
    "detect_grounded",
    "detect_grounded_batch",
    "record_feedback",
    "get_detection_stats",
    # Triage
    "scan_and_classify",
    # Linker
    "link_entities",
    "find_entity",
    # Connectors
    "query_local",
    "export_to_parquet",
]
