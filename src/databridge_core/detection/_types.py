"""Pydantic models for the grounded detection module."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def _new_id(prefix: str = "det") -> str:
    """Generate a short unique ID with the given prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


# -- Enums -------------------------------------------------------------------


class Severity(str, Enum):
    """Detection rule / finding severity levels."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class FindingType(str, Enum):
    """Categories of grounded detection findings."""

    SIGN_REVERSAL = "sign_reversal"
    ROUNDING_DISCREPANCY = "rounding_discrepancy"
    MISSING_ACCOUNT = "missing_account"
    DUPLICATE_ACCOUNT = "duplicate_account"
    HIERARCHY_BREAK = "hierarchy_break"
    NAMING_VIOLATION = "naming_violation"
    BALANCE_MISMATCH = "balance_mismatch"
    FORMULA_ANOMALY = "formula_anomaly"
    CLASSIFICATION_ERROR = "classification_error"
    CUSTOM = "custom"


class FeedbackAction(str, Enum):
    """User feedback action on a detection finding."""

    CONFIRMED = "confirmed"
    DISMISSED = "dismissed"


# -- Core models --------------------------------------------------------------


class DetectionRule(BaseModel):
    """A detection rule loaded from the Knowledge Base.

    Each rule converts a KB node (of type 'rule', 'standard', 'pattern',
    'gap', 'fact', 'guardrail', etc.) into an executable check with a
    regex pattern, target field(s), and severity from the KB properties.
    """

    rule_id: str = Field(default_factory=lambda: _new_id("rule"))
    name: str = ""
    standard: str = ""  # e.g. "GAAP", "IFRS", "internal"
    finding_type: FindingType = FindingType.CUSTOM
    pattern: str = ""  # regex pattern to match in target fields
    field_targets: List[str] = Field(default_factory=list)  # CSV column names
    severity: Severity = Severity.MEDIUM
    description: str = ""
    evidence_nodes: List[str] = Field(default_factory=list)  # KB node IDs
    kb_source_file: str = ""  # KB JSON file this rule came from
    confidence: float = 0.9
    tags: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)  # raw KB props


class GroundedFinding(BaseModel):
    """A detection finding grounded in Knowledge Base citations.

    Every finding links back to the KB node(s) that justify the detection,
    providing traceable, auditable results rather than opaque flags.
    """

    finding_id: str = Field(default_factory=lambda: _new_id("finding"))
    finding_type: FindingType = FindingType.CUSTOM
    severity: Severity = Severity.MEDIUM
    account: str = ""  # account code or name that triggered the finding
    row_index: int = -1  # 0-based row in the CSV
    field: str = ""  # column name where the issue was detected
    matched_value: str = ""  # the actual value that matched the rule
    evidence: str = ""  # human-readable explanation
    kb_node_ids: List[str] = Field(default_factory=list)  # KB citations
    confidence: float = 0.0  # 0.0-1.0
    grounding_context: str = ""  # additional context from GraphRAG search
    rule_id: str = ""  # which DetectionRule triggered this
    rule_name: str = ""
    graphrag_results: List[Dict[str, Any]] = Field(default_factory=list)
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class DetectionContext(BaseModel):
    """Context assembled from the enriched index for grounded detection.

    Aggregates rules, entity maps, Logic DNA outputs, and rate tables
    so the detection engine has full situational awareness.
    """

    rules: List[DetectionRule] = Field(default_factory=list)
    entity_map: Dict[str, Any] = Field(default_factory=dict)
    logic_dnas: List[Dict[str, Any]] = Field(default_factory=list)
    rate_tables: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FeedbackRecord(BaseModel):
    """User feedback on a detection finding for the learning loop."""

    feedback_id: str = Field(default_factory=lambda: _new_id("fb"))
    finding_id: str = ""
    action: FeedbackAction = FeedbackAction.CONFIRMED
    notes: str = ""
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    # Denormalized fields for fast filtering (set by record_feedback)
    rule_id: str = ""
    finding_type: str = ""
    account: str = ""


# -- Summary models -----------------------------------------------------------


class DetectionSummary(BaseModel):
    """Summary of a single-file detection run."""

    file_path: str = ""
    total_rows: int = 0
    total_findings: int = 0
    rules_applied: int = 0
    severity_counts: Dict[str, int] = Field(default_factory=dict)
    type_counts: Dict[str, int] = Field(default_factory=dict)
    avg_confidence: float = 0.0
    graphrag_enriched: int = 0
    feedback_suppressed: int = 0
    duration_seconds: float = 0.0


class DetectionBatchSummary(BaseModel):
    """Aggregate stats across a batch of detection runs."""

    total_files: int = 0
    completed: int = 0
    failed: int = 0
    total_findings: int = 0
    total_rows_scanned: int = 0
    severity_counts: Dict[str, int] = Field(default_factory=dict)
    type_counts: Dict[str, int] = Field(default_factory=dict)
    avg_confidence: float = 0.0
    rules_loaded: int = 0
    duration_seconds: float = 0.0
