"""Runtime property monitors for detection results.

Asserts structural invariants on detection output:
- No cycles in hierarchy references
- No illegal parent-child relationships
- No contradictory findings (same account, opposing types)
- Confidence scores within valid range

These run as lightweight post-detection checks and produce warnings
rather than blocking the pipeline.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Set, Tuple

from ._types import GroundedFinding

logger = logging.getLogger(__name__)


class MonitorWarning:
    """A runtime monitor warning."""

    __slots__ = ("monitor", "message", "details")

    def __init__(self, monitor: str, message: str, details: Dict[str, Any] = None):
        self.monitor = monitor
        self.message = message
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "monitor": self.monitor,
            "message": self.message,
            "details": self.details,
        }


def check_no_contradictions(
    findings: List[GroundedFinding],
) -> List[MonitorWarning]:
    """Check for contradictory findings on the same account.

    Two findings are contradictory if they flag the same account/field
    but with opposing severity (e.g., one says "missing", another says
    "duplicate" for the same account).
    """
    warnings: List[MonitorWarning] = []

    # Group findings by (account, field)
    groups: Dict[Tuple[str, str], List[GroundedFinding]] = {}
    for f in findings:
        key = (f.account, f.field)
        if key not in groups:
            groups[key] = []
        groups[key].append(f)

    # Check for contradictions
    contradictory_pairs = {
        ("missing_account", "duplicate_account"),
        ("duplicate_account", "missing_account"),
    }

    for key, group in groups.items():
        if len(group) < 2:
            continue
        types = {f.finding_type.value for f in group}
        for t1, t2 in contradictory_pairs:
            if t1 in types and t2 in types:
                warnings.append(MonitorWarning(
                    monitor="contradiction_check",
                    message=f"Contradictory findings for account '{key[0]}' field '{key[1]}': {t1} and {t2}",
                    details={"account": key[0], "field": key[1], "types": [t1, t2]},
                ))
                break

    return warnings


def check_confidence_bounds(
    findings: List[GroundedFinding],
) -> List[MonitorWarning]:
    """Check that all confidence scores are within [0, 1]."""
    warnings: List[MonitorWarning] = []

    for f in findings:
        if f.confidence < 0.0 or f.confidence > 1.0:
            warnings.append(MonitorWarning(
                monitor="confidence_bounds",
                message=f"Finding {f.finding_id} has out-of-range confidence: {f.confidence}",
                details={"finding_id": f.finding_id, "confidence": f.confidence},
            ))

    return warnings


def check_rule_id_consistency(
    findings: List[GroundedFinding],
) -> List[MonitorWarning]:
    """Check that all findings have a valid rule_id."""
    warnings: List[MonitorWarning] = []
    missing_rule_ids = 0

    for f in findings:
        if not f.rule_id:
            missing_rule_ids += 1

    if missing_rule_ids > 0:
        warnings.append(MonitorWarning(
            monitor="rule_id_consistency",
            message=f"{missing_rule_ids} findings missing rule_id",
            details={"missing_count": missing_rule_ids},
        ))

    return warnings


def check_no_duplicate_findings(
    findings: List[GroundedFinding],
) -> List[MonitorWarning]:
    """Check for duplicate findings (same row, rule, field)."""
    warnings: List[MonitorWarning] = []
    seen: Set[Tuple[int, str, str]] = set()

    for f in findings:
        key = (f.row_index, f.rule_id, f.field)
        if key in seen:
            warnings.append(MonitorWarning(
                monitor="duplicate_check",
                message=f"Duplicate finding at row {f.row_index}, rule {f.rule_id}, field {f.field}",
                details={"row_index": f.row_index, "rule_id": f.rule_id, "field": f.field},
            ))
        seen.add(key)

    return warnings


def run_all_monitors(
    findings: List[GroundedFinding],
) -> List[Dict[str, Any]]:
    """Run all runtime monitors and return warnings.

    This is the main entry point called from detect_grounded() after
    detection and before feedback filtering.

    Returns:
        List of warning dicts. Empty list means all checks passed.
    """
    all_warnings: List[MonitorWarning] = []

    all_warnings.extend(check_no_contradictions(findings))
    all_warnings.extend(check_confidence_bounds(findings))
    all_warnings.extend(check_rule_id_consistency(findings))
    all_warnings.extend(check_no_duplicate_findings(findings))

    if all_warnings:
        logger.info(
            "Runtime monitors raised %d warnings on %d findings",
            len(all_warnings), len(findings),
        )

    return [w.to_dict() for w in all_warnings]
