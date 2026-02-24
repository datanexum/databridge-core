"""DataBridge Profiler -- Data profiling and schema drift detection."""

from .profile import profile_data, detect_schema_drift

__all__ = ["profile_data", "detect_schema_drift"]
