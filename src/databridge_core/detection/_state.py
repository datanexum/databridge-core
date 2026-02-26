"""LangGraph state definition for the AI verification pipeline."""
from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class VerificationState(TypedDict, total=False):
    """Shared state that flows through the verification graph.

    Keys are updated by each node (triage, verify, reconcile) and
    read by downstream nodes and the conditional router.
    """

    # -- Input (set once before graph invocation) ----------------------------
    file_path: str  # Path to the CSV being analysed
    csv_headers: List[str]  # Column names in the file
    csv_sample_rows: List[Dict[str, str]]  # First N rows for context
    csv_total_rows: int  # Total row count in the file
    candidate_findings: List[Dict[str, Any]]  # GroundedFinding dicts from regex pass
    kb_rules_summary: str  # Human-readable summary of KB rules that fired

    # -- Triage node output --------------------------------------------------
    triage_verdicts: List[Dict[str, Any]]  # Per-finding: keep / dismiss / escalate
    triage_summary: str  # Brief triage explanation
    dismissed_count: int  # Findings dismissed by triage

    # -- Verify node output --------------------------------------------------
    verified_findings: List[Dict[str, Any]]  # Findings that passed AI verification
    verification_notes: List[str]  # Explanations for each verification decision
    numeric_checks: List[Dict[str, Any]]  # Numeric anomaly detections from AI
    new_findings: List[Dict[str, Any]]  # Novel findings the AI discovered

    # -- Reconcile node output -----------------------------------------------
    final_findings: List[Dict[str, Any]]  # De-duplicated, confidence-scored results
    reconciliation_summary: str  # Overall assessment of the file
    confidence_adjustments: Dict[str, float]  # finding_id -> adjusted confidence

    # -- Control flow --------------------------------------------------------
    round_number: int  # Current verification round (starts at 1)
    max_rounds: int  # Max rounds before forced completion (default 2)
    converged: bool  # Set by reconcile node when done
    error: str  # Set if a node encounters a fatal error
