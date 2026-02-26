"""Detection feedback loop: record, filter, and learn from user corrections.

User feedback on detection findings feeds back into subsequent runs:
- **Confirmed** findings boost the triggering rule's confidence.
- **Dismissed** findings reduce confidence, and patterns with >3
  consecutive dismissals (0 confirmations) are automatically suppressed.

All feedback is persisted to a JSONL file for full auditability.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._types import (
    FeedbackAction,
    FeedbackRecord,
    GroundedFinding,
)

logger = logging.getLogger(__name__)

DEFAULT_FEEDBACK_PATH = "data/detection/feedback.jsonl"

# Suppression threshold: a pattern is suppressed when it has been
# dismissed more than this many times with zero confirmations.
_SUPPRESS_THRESHOLD = 3

# Confidence adjustment factors
_CONFIRM_BOOST = 0.05  # +5% per confirmation
_DISMISS_PENALTY = 0.08  # -8% per dismissal
_MIN_CONFIDENCE = 0.05
_MAX_CONFIDENCE = 1.0


def record_feedback(
    finding_id: str,
    confirmed: bool,
    notes: str = "",
    feedback_path: str = DEFAULT_FEEDBACK_PATH,
    finding_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Record user feedback on a detection finding.

    Appends a :class:`FeedbackRecord` to the feedback JSONL file.
    On subsequent detection runs, confirmed findings boost rule
    confidence while dismissed findings reduce it.

    Args:
        finding_id: The ``finding_id`` of the finding being reviewed.
        confirmed: ``True`` if the user confirms the finding is valid,
            ``False`` if the user dismisses it as a false positive.
        notes: Optional free-text explanation from the user.
        feedback_path: Path to the feedback JSONL file.
        finding_metadata: Optional dict with ``rule_id``, ``finding_type``,
            and ``account`` for faster downstream filtering. If not
            provided, these fields are left empty and must be resolved
            from the findings file on the next load.

    Returns:
        Dict confirming the recorded feedback.
    """
    metadata = finding_metadata or {}

    record = FeedbackRecord(
        finding_id=finding_id,
        action=FeedbackAction.CONFIRMED if confirmed else FeedbackAction.DISMISSED,
        notes=notes,
        rule_id=metadata.get("rule_id", ""),
        finding_type=metadata.get("finding_type", ""),
        account=metadata.get("account", ""),
    )

    # Ensure directory exists
    fb_path = Path(feedback_path)
    fb_path.parent.mkdir(parents=True, exist_ok=True)

    # Append to JSONL
    with open(fb_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record.model_dump(mode="json"), default=str) + "\n")

    logger.info(
        "Recorded %s feedback for finding %s (rule=%s)",
        record.action.value,
        finding_id,
        record.rule_id,
    )

    return {
        "status": "recorded",
        "feedback_id": record.feedback_id,
        "finding_id": finding_id,
        "action": record.action.value,
        "notes": notes,
        "feedback_path": str(fb_path),
    }


def apply_feedback_filter(
    findings: List[GroundedFinding],
    feedback_path: str = DEFAULT_FEEDBACK_PATH,
) -> List[GroundedFinding]:
    """Filter and adjust findings based on historical feedback.

    Applies two transformations:

    1. **Suppression** -- Findings whose ``(rule_id, finding_type)``
       combination has been dismissed more than :data:`_SUPPRESS_THRESHOLD`
       times with zero confirmations are removed.
    2. **Confidence adjustment** -- For non-suppressed findings, the
       confidence score is boosted or reduced based on the net feedback
       tally for the matching rule.

    Args:
        findings: List of :class:`GroundedFinding` objects from the
            current detection run.
        feedback_path: Path to the feedback JSONL file.

    Returns:
        Filtered and confidence-adjusted list of findings.
    """
    feedback_records = _load_feedback(feedback_path)
    if not feedback_records:
        return findings

    # Build per-rule feedback tallies: (rule_id, finding_type) -> counts
    tallies = _build_tallies(feedback_records)

    filtered: List[GroundedFinding] = []
    suppressed_count = 0

    for finding in findings:
        key = (finding.rule_id, finding.finding_type.value)
        tally = tallies.get(key)

        if tally is None:
            # No feedback for this rule pattern -- keep as-is
            filtered.append(finding)
            continue

        confirmed = tally["confirmed"]
        dismissed = tally["dismissed"]

        # Suppression check
        if dismissed > _SUPPRESS_THRESHOLD and confirmed == 0:
            suppressed_count += 1
            logger.debug(
                "Suppressed finding %s (rule=%s, type=%s): %d dismissals, 0 confirmations",
                finding.finding_id,
                finding.rule_id,
                finding.finding_type.value,
                dismissed,
            )
            continue

        # Confidence adjustment
        net = confirmed - dismissed
        adjustment = net * _CONFIRM_BOOST if net >= 0 else net * _DISMISS_PENALTY
        new_confidence = max(
            _MIN_CONFIDENCE,
            min(_MAX_CONFIDENCE, finding.confidence + adjustment),
        )
        finding.confidence = round(new_confidence, 4)

        filtered.append(finding)

    if suppressed_count > 0:
        logger.info(
            "Feedback filter suppressed %d of %d findings",
            suppressed_count,
            len(findings),
        )

    return filtered


def get_detection_stats(
    feedback_path: str = DEFAULT_FEEDBACK_PATH,
) -> Dict[str, Any]:
    """Compute statistics on detection performance and learning.

    Summarises feedback tallies, suppression rules, and overall
    confirmation/dismissal rates.

    Args:
        feedback_path: Path to the feedback JSONL file.

    Returns:
        Dict with performance and learning statistics.
    """
    feedback_records = _load_feedback(feedback_path)

    if not feedback_records:
        return {
            "total_feedback": 0,
            "confirmed": 0,
            "dismissed": 0,
            "confirmation_rate": 0.0,
            "suppressed_patterns": 0,
            "active_patterns": 0,
            "rules_with_feedback": 0,
            "message": "No feedback recorded yet",
        }

    total = len(feedback_records)
    confirmed_total = sum(
        1 for r in feedback_records if r.get("action") == FeedbackAction.CONFIRMED.value
    )
    dismissed_total = total - confirmed_total

    tallies = _build_tallies(feedback_records)

    suppressed = 0
    active = 0
    for key, tally in tallies.items():
        if tally["dismissed"] > _SUPPRESS_THRESHOLD and tally["confirmed"] == 0:
            suppressed += 1
        else:
            active += 1

    # Per-rule detail (top 10 most reviewed)
    rule_details: List[Dict[str, Any]] = []
    for key, tally in sorted(
        tallies.items(), key=lambda kv: kv[1]["total"], reverse=True
    )[:10]:
        rule_id, finding_type = key
        is_suppressed = (
            tally["dismissed"] > _SUPPRESS_THRESHOLD and tally["confirmed"] == 0
        )
        rule_details.append({
            "rule_id": rule_id,
            "finding_type": finding_type,
            "confirmed": tally["confirmed"],
            "dismissed": tally["dismissed"],
            "total": tally["total"],
            "suppressed": is_suppressed,
            "net_score": tally["confirmed"] - tally["dismissed"],
        })

    return {
        "total_feedback": total,
        "confirmed": confirmed_total,
        "dismissed": dismissed_total,
        "confirmation_rate": round(confirmed_total / max(total, 1), 4),
        "suppressed_patterns": suppressed,
        "active_patterns": active,
        "rules_with_feedback": len(tallies),
        "top_rules": rule_details,
        "feedback_path": feedback_path,
    }


# -- Internal helpers ---------------------------------------------------------


def _load_feedback(feedback_path: str) -> List[Dict[str, Any]]:
    """Load all feedback records from the JSONL file.

    Returns an empty list if the file does not exist or is empty.
    """
    fb_path = Path(feedback_path)
    if not fb_path.exists():
        return []

    records: List[Dict[str, Any]] = []
    try:
        with open(fb_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError as exc:
        logger.warning("Failed to read feedback file %s: %s", fb_path, exc)

    return records


def _build_tallies(
    records: List[Dict[str, Any]],
) -> Dict[tuple, Dict[str, int]]:
    """Build per-rule feedback tallies from raw records.

    Returns a dict keyed by ``(rule_id, finding_type)`` with counts of
    confirmed, dismissed, and total reviews.
    """
    tallies: Dict[tuple, Dict[str, int]] = defaultdict(
        lambda: {"confirmed": 0, "dismissed": 0, "total": 0}
    )

    for record in records:
        rule_id = record.get("rule_id", "")
        finding_type = record.get("finding_type", "")
        action = record.get("action", "")

        # Skip records without a rule_id (cannot be aggregated)
        if not rule_id:
            continue

        key = (rule_id, finding_type)
        tallies[key]["total"] += 1
        if action == FeedbackAction.CONFIRMED.value:
            tallies[key]["confirmed"] += 1
        else:
            tallies[key]["dismissed"] += 1

    return dict(tallies)
