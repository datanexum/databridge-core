"""LangGraph StateGraph assembly for the AI detection verification pipeline.

Graph structure::

    ┌──────────┐     ┌──────────┐     ┌─────────────┐
    │  Triage  │────>│  Verify  │────>│  Reconcile  │
    │  (AI/h)  │     │  (AI/h)  │     │   (AI/h)    │
    └──────────┘     └──────────┘     └──────┬──────┘
                                             │
                                     converged=True → END

Each node runs with Claude AI when available, falling back to
deterministic heuristics otherwise. This ensures the pipeline
always produces results regardless of API availability.

Usage::

    from databridge.detection._graph import run_verification_graph

    result = await run_verification_graph(
        file_path="data/file.csv",
        candidate_findings=[...],
        csv_headers=[...],
        csv_sample_rows=[...],
    )
"""
from __future__ import annotations

import csv
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._state import VerificationState
from ._types import (
    DetectionRule,
    DetectionSummary,
    FindingType,
    GroundedFinding,
    Severity,
)

logger = logging.getLogger(__name__)

# Graceful import — LangGraph is optional
try:
    from langgraph.graph import END, StateGraph

    _LANGGRAPH_AVAILABLE = True
except ImportError:
    _LANGGRAPH_AVAILABLE = False


def _should_continue(state: VerificationState) -> str:
    """Conditional edge: end if converged or errored."""
    if state.get("converged", False):
        return "end"
    if state.get("error"):
        return "end"
    round_num = state.get("round_number", 1)
    max_rounds = state.get("max_rounds", 2)
    if round_num >= max_rounds:
        return "end"
    return "continue"


def build_verification_graph() -> Any:
    """Build and compile the 3-node verification StateGraph.

    Returns:
        Compiled LangGraph ready for ``.ainvoke()``.

    Raises:
        ImportError: If ``langgraph`` is not installed.
    """
    if not _LANGGRAPH_AVAILABLE:
        raise ImportError(
            "langgraph is required for AI verification. "
            "Install it with: pip install langgraph"
        )

    from ._verifier import reconcile_node, triage_node, verify_node

    graph = StateGraph(VerificationState)

    graph.add_node("triage", triage_node)
    graph.add_node("verify", verify_node)
    graph.add_node("reconcile", reconcile_node)

    # Linear pipeline: triage → verify → reconcile
    graph.add_edge("triage", "verify")
    graph.add_edge("verify", "reconcile")

    # Conditional from reconcile: loop or end
    graph.add_conditional_edges(
        "reconcile",
        _should_continue,
        {
            "continue": "triage",  # Re-triage with refined findings
            "end": END,
        },
    )

    graph.set_entry_point("triage")

    logger.info("Built verification graph: triage → verify → reconcile")
    return graph.compile()


async def run_verification_graph(
    file_path: str,
    candidate_findings: List[Dict[str, Any]],
    csv_headers: List[str],
    csv_sample_rows: List[Dict[str, str]],
    csv_total_rows: int = 0,
    kb_rules_summary: str = "",
    max_rounds: int = 2,
) -> Dict[str, Any]:
    """Run the full AI verification pipeline on candidate findings.

    This is the main entry point for AI-powered detection. It takes
    regex-matched candidate findings and filters/verifies/enriches
    them through a 3-stage LangGraph pipeline.

    Args:
        file_path: Path to the CSV file.
        candidate_findings: List of GroundedFinding dicts from the
            regex-based detection pass.
        csv_headers: Column headers from the CSV.
        csv_sample_rows: First N rows of CSV data (as dicts).
        csv_total_rows: Total row count in the CSV.
        kb_rules_summary: Human-readable summary of the KB rules
            that generated the candidates.
        max_rounds: Maximum verification rounds (default 2).

    Returns:
        Dict with ``final_findings``, ``reconciliation_summary``,
        ``verification_notes``, and pipeline metadata.
    """
    if not _LANGGRAPH_AVAILABLE:
        logger.warning(
            "LangGraph not available; running heuristic-only verification"
        )
        return await _run_heuristic_pipeline(
            file_path, candidate_findings, csv_headers,
            csv_sample_rows, csv_total_rows, kb_rules_summary,
        )

    t0 = time.time()
    graph = build_verification_graph()

    initial_state: VerificationState = {
        "file_path": file_path,
        "csv_headers": csv_headers,
        "csv_sample_rows": csv_sample_rows,
        "csv_total_rows": csv_total_rows,
        "candidate_findings": candidate_findings,
        "kb_rules_summary": kb_rules_summary,
        "triage_verdicts": [],
        "triage_summary": "",
        "dismissed_count": 0,
        "verified_findings": [],
        "verification_notes": [],
        "numeric_checks": [],
        "new_findings": [],
        "final_findings": [],
        "reconciliation_summary": "",
        "confidence_adjustments": {},
        "round_number": 1,
        "max_rounds": max_rounds,
        "converged": False,
        "error": "",
    }

    try:
        final_state = await graph.ainvoke(initial_state)
        duration = time.time() - t0

        return {
            "final_findings": final_state.get("final_findings", []),
            "reconciliation_summary": final_state.get("reconciliation_summary", ""),
            "verification_notes": final_state.get("verification_notes", []),
            "numeric_checks": final_state.get("numeric_checks", []),
            "new_findings": final_state.get("new_findings", []),
            "triage_summary": final_state.get("triage_summary", ""),
            "dismissed_by_triage": final_state.get("dismissed_count", 0),
            "original_candidates": len(candidate_findings),
            "verified_count": len(final_state.get("final_findings", [])),
            "duration_seconds": round(duration, 2),
            "pipeline": "langgraph",
        }

    except Exception as exc:
        logger.error("Verification graph failed: %s", exc)
        return {
            "error": str(exc),
            "final_findings": candidate_findings,  # Pass through unfiltered
            "pipeline": "langgraph_error",
        }


async def _run_heuristic_pipeline(
    file_path: str,
    candidate_findings: List[Dict[str, Any]],
    csv_headers: List[str],
    csv_sample_rows: List[Dict[str, str]],
    csv_total_rows: int,
    kb_rules_summary: str,
) -> Dict[str, Any]:
    """Run the heuristic-only pipeline when LangGraph is unavailable.

    Imports the verifier functions directly and calls them sequentially.
    """
    from ._verifier import (
        _reconcile_heuristic,
        _triage_heuristic,
        _verify_heuristic,
    )

    t0 = time.time()

    state: VerificationState = {
        "file_path": file_path,
        "csv_headers": csv_headers,
        "csv_sample_rows": csv_sample_rows,
        "csv_total_rows": csv_total_rows,
        "candidate_findings": candidate_findings,
        "kb_rules_summary": kb_rules_summary,
        "triage_verdicts": [],
        "triage_summary": "",
        "dismissed_count": 0,
        "verified_findings": [],
        "verification_notes": [],
        "numeric_checks": [],
        "new_findings": [],
        "final_findings": [],
        "reconciliation_summary": "",
        "confidence_adjustments": {},
        "round_number": 1,
        "max_rounds": 1,
        "converged": False,
        "error": "",
    }

    # Step 1: Triage
    triage_result = _triage_heuristic(state)
    state.update(triage_result)

    # Step 2: Verify
    survivors = [
        v for v in state.get("triage_verdicts", [])
        if v.get("triage_decision") in ("keep", "escalate")
    ]
    verify_result = _verify_heuristic(state, survivors)
    state.update(verify_result)

    # Step 3: Reconcile
    reconcile_result = _reconcile_heuristic(state)
    state.update(reconcile_result)

    duration = time.time() - t0

    return {
        "final_findings": state.get("final_findings", []),
        "reconciliation_summary": state.get("reconciliation_summary", ""),
        "verification_notes": state.get("verification_notes", []),
        "numeric_checks": state.get("numeric_checks", []),
        "new_findings": [],
        "triage_summary": state.get("triage_summary", ""),
        "dismissed_by_triage": state.get("dismissed_count", 0),
        "original_candidates": len(candidate_findings),
        "verified_count": len(state.get("final_findings", [])),
        "duration_seconds": round(duration, 2),
        "pipeline": "heuristic",
    }


# ── Numeric outlier detection ────────────────────────────────────────────────


def _detect_numeric_outliers(
    rows: List[Dict[str, str]],
    headers: List[str],
) -> List[Dict[str, Any]]:
    """Pre-scan numeric columns for statistical outliers.

    Detects values that are >10x the median in rate/amount columns.
    This catches inverted FX rates, decimal shifts, and sign errors
    that regex patterns cannot detect.
    """
    # Columns likely to contain rates or amounts
    _NUMERIC_COL_KEYWORDS = {
        "rate", "fx_rate", "fx", "amount", "balance", "translated",
        "debit", "credit", "adjustment",
    }

    outliers: List[Dict[str, Any]] = []
    account_col = None
    for h in headers:
        if h.lower() in ("account_name", "account", "name", "description"):
            account_col = h
            break

    for col in headers:
        col_lower = col.lower()
        if not any(kw in col_lower for kw in _NUMERIC_COL_KEYWORDS):
            continue

        # Collect numeric values
        values: List[tuple] = []  # (row_idx, float_val)
        for i, row in enumerate(rows):
            raw = row.get(col, "")
            try:
                val = float(raw.replace(",", ""))
                if val != 0:
                    values.append((i, val))
            except (ValueError, TypeError):
                continue

        if len(values) < 3:
            continue

        # Compute median of absolute values
        abs_vals = sorted(abs(v) for _, v in values)
        median = abs_vals[len(abs_vals) // 2]

        if median == 0:
            continue

        # Flag values that are >10x or <0.1x the median
        for row_idx, val in values:
            abs_val = abs(val)
            ratio = abs_val / median if median > 0 else 0

            if ratio > 10:
                account = rows[row_idx].get(account_col, "") if account_col else ""
                # Check if this looks like an inverted rate (val × median ≈ 1)
                product = abs_val * median
                is_inversion = 0.5 < product < 2.0

                finding_type = "sign_reversal" if is_inversion else "balance_mismatch"
                evidence = (
                    f"Value {val} in column '{col}' at row {row_idx} is "
                    f"{ratio:.1f}x the median ({median:.6f}). "
                )
                if is_inversion:
                    evidence += (
                        f"Product of value × median = {product:.4f} ≈ 1.0, "
                        f"indicating an INVERTED rate (1/rate used instead of rate)."
                    )
                else:
                    evidence += (
                        f"This is a significant outlier that may indicate "
                        f"a data entry error or decimal shift."
                    )

                outliers.append({
                    "finding_type": finding_type,
                    "severity": "critical",
                    "account": account,
                    "row_index": row_idx,
                    "field": col,
                    "value": str(val),
                    "evidence": evidence,
                    "confidence": 0.95 if is_inversion else 0.85,
                })

            elif ratio < 0.1 and col_lower in ("rate", "fx_rate"):
                # Suspiciously small rate — possible decimal shift
                account = rows[row_idx].get(account_col, "") if account_col else ""
                outliers.append({
                    "finding_type": "balance_mismatch",
                    "severity": "high",
                    "account": account,
                    "row_index": row_idx,
                    "field": col,
                    "value": str(val),
                    "evidence": (
                        f"Value {val} in column '{col}' at row {row_idx} is "
                        f"{ratio:.4f}x the median ({median:.6f}). "
                        f"Possible decimal shift or wrong rate type."
                    ),
                    "confidence": 0.75,
                })

    return outliers


# ── Convenience: integrated detect + verify ─────────────────────────────────


async def detect_and_verify(
    file_path: str,
    knowledge_dir: str = "data/knowledge",
    rules: Optional[List[DetectionRule]] = None,
    use_graphrag: bool = False,
    max_rounds: int = 2,
) -> Dict[str, Any]:
    """Full pipeline: KB rules → regex detection → AI verification.

    Combines :func:`detect_grounded` with :func:`run_verification_graph`
    into a single call.

    Args:
        file_path: Path to the CSV file.
        knowledge_dir: Knowledge base directory for rule loading.
        rules: Pre-loaded detection rules (optional).
        use_graphrag: Whether to enrich with GraphRAG.
        max_rounds: Maximum verification rounds.

    Returns:
        Dict with verified findings, summary, and pipeline metadata.
    """
    from ._grounded import _compile_rules, _check_row, _extract_account, _read_csv
    from ._rules import load_detection_rules

    t0 = time.time()
    fp = Path(file_path)

    if not fp.exists():
        return {"error": f"File not found: {file_path}"}

    # 1. Load rules
    if rules is None:
        rules = load_detection_rules(knowledge_dir=knowledge_dir)

    if not rules:
        return {"error": "No detection rules available"}

    # 2. Parse CSV
    rows, headers = _read_csv(file_path)
    if not rows:
        return {"error": "No data rows in file", "file_path": file_path}

    # 3. Regex detection pass
    compiled = _compile_rules(rules, headers)
    raw_findings: List[GroundedFinding] = []

    for row_idx, row in enumerate(rows):
        if len(raw_findings) >= 500:
            break
        row_findings = _check_row(row_idx, row, headers, compiled)
        raw_findings.extend(row_findings)

    # 3b. Numeric outlier pre-scan: detect values that are statistical outliers
    #     in rate/amount columns. These are hard for regex but trivial numerically.
    #     High-confidence numeric outliers bypass AI triage entirely — they are
    #     deterministic mathematical facts, not patterns that need AI judgment.
    numeric_outliers = _detect_numeric_outliers(rows, headers)
    numeric_bypass: List[Dict[str, Any]] = []  # Skip AI, go straight to final
    for outlier in numeric_outliers:
        finding = GroundedFinding(
            finding_type=FindingType(outlier.get("finding_type", "custom")),
            severity=Severity(outlier.get("severity", "critical")),
            account=outlier.get("account", ""),
            row_index=outlier.get("row_index", -1),
            field=outlier.get("field", ""),
            matched_value=outlier.get("value", ""),
            evidence=outlier.get("evidence", ""),
            kb_node_ids=[],
            confidence=outlier.get("confidence", 0.95),
            grounding_context="Numeric outlier detection (statistical)",
            rule_id="numeric_outlier",
            rule_name="Numeric Outlier Detector",
        )
        numeric_bypass.append(finding.model_dump(mode="json"))

    # Convert findings to dicts for the verification pipeline
    candidate_dicts = [f.model_dump(mode="json") for f in raw_findings]

    # Build KB rules summary
    rule_names = list({r.name for r in rules if r.name})
    kb_summary = f"{len(rules)} rules loaded: " + ", ".join(rule_names[:15])
    if len(rule_names) > 15:
        kb_summary += f" ... and {len(rule_names) - 15} more"

    # Pass enough rows for AI to perform numeric analysis (up to 50)
    sample_rows = rows[:50]

    # 4. AI verification pipeline
    verification = await run_verification_graph(
        file_path=file_path,
        candidate_findings=candidate_dicts,
        csv_headers=headers,
        csv_sample_rows=sample_rows,
        csv_total_rows=len(rows),
        kb_rules_summary=kb_summary,
        max_rounds=max_rounds,
    )

    duration = time.time() - t0

    # 5. Merge: numeric bypass findings (deterministic) + AI-verified findings
    #    Numeric outliers are mathematical facts — they don't need AI approval.
    ai_findings = verification.get("final_findings", [])
    # De-duplicate: skip AI findings that overlap with numeric bypass on same row+field
    bypass_keys = {(nb.get("row_index"), nb.get("field")) for nb in numeric_bypass}
    deduped_ai = [
        f for f in ai_findings
        if (f.get("row_index"), f.get("field")) not in bypass_keys
    ]
    merged_findings = numeric_bypass + deduped_ai

    return {
        "file_path": file_path,
        "total_rows": len(rows),
        "regex_candidates": len(raw_findings),
        "numeric_outliers": len(numeric_bypass),
        "verified_findings": merged_findings,
        "verified_count": len(merged_findings),
        "dismissed_by_triage": verification.get("dismissed_by_triage", 0),
        "reconciliation_summary": verification.get("reconciliation_summary", ""),
        "verification_notes": verification.get("verification_notes", []),
        "numeric_checks": verification.get("numeric_checks", []),
        "new_ai_findings": verification.get("new_findings", []),
        "pipeline": verification.get("pipeline", "unknown"),
        "duration_seconds": round(duration, 2),
    }
