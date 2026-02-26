"""DataBridge Profiler -- Data profiling, schema drift, expectations, and validation.

Public API:
    profile_data               — Analyze data structure and quality
    detect_schema_drift        — Compare schemas between two data files
    generate_expectation_suite — Auto-generate quality expectations from data
    list_expectation_suites    — List persisted expectation suites
    validate                   — Run expectations against data
    get_validation_results     — Get historical validation results
"""

from .profile import profile_data, detect_schema_drift
from .expectations import generate_expectation_suite, list_expectation_suites
from .validation import validate, get_validation_results

__all__ = [
    "profile_data",
    "detect_schema_drift",
    "generate_expectation_suite",
    "list_expectation_suites",
    "validate",
    "get_validation_results",
]
