"""Grounded detection module: KB-cited anomaly detection for financial data.

Every finding is grounded in Knowledge Base nodes, providing traceable,
auditable detections rather than opaque flags. The module includes:

- **KB-grounded regex detection** — fast first pass from Knowledge Base rules
- **AI verification pipeline** — LangGraph 3-node graph (Triage → Verify →
  Reconcile) that filters false positives and discovers novel anomalies
- **Feedback learning loop** — boosts or suppresses rules based on user
  corrections over time

Public API
----------
- ``detect_grounded(file_path, ...)`` -- regex-only detection (fast)
- ``detect_grounded_batch(directory, ...)`` -- batch regex detection
- ``detect_and_verify(file_path, ...)`` -- full AI pipeline (regex + graph)
- ``run_verification_graph(...)`` -- AI verification on pre-computed candidates
- ``load_detection_rules(knowledge_dir, ...)`` -- load rules from KB
- ``get_detection_stats(feedback_path)`` -- learning performance stats
- ``record_feedback(finding_id, confirmed, ...)`` -- user feedback
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._feedback import (
    apply_feedback_filter,
    get_detection_stats,
    record_feedback,
)
from ._graph import detect_and_verify, run_verification_graph
from ._grounded import (
    detect_grounded,
    detect_grounded_batch,
)
from ._rules import load_detection_rules
from ._types import (
    DetectionBatchSummary,
    DetectionContext,
    DetectionRule,
    DetectionSummary,
    FeedbackAction,
    FeedbackRecord,
    FindingType,
    GroundedFinding,
    Severity,
    SPRTCertificate,
    ThompsonState,
)

logger = logging.getLogger(__name__)

__all__ = [
    # Public functions — regex detection
    "detect_grounded",
    "detect_grounded_batch",
    "load_detection_rules",
    # Public functions — AI verification pipeline
    "detect_and_verify",
    "run_verification_graph",
    # Public functions — feedback loop
    "get_detection_stats",
    "record_feedback",
    "apply_feedback_filter",
    # Types (re-exported for convenience)
    "DetectionRule",
    "GroundedFinding",
    "DetectionContext",
    "FeedbackRecord",
    "DetectionSummary",
    "DetectionBatchSummary",
    "FindingType",
    "Severity",
    "FeedbackAction",
    "ThompsonState",
    "SPRTCertificate",
]
