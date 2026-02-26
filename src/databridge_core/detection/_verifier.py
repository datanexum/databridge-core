"""AI verification agents for grounded detection.

Each node in the verification graph is an async function that receives
:class:`VerificationState` and returns updates to it. LLM calls use
``langchain-anthropic`` (Claude) with graceful fallback to deterministic
heuristics when the API is unavailable.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from ._prompts import (
    RECONCILE_SYSTEM_PROMPT,
    RECONCILE_TEMPLATE,
    TRIAGE_SYSTEM_PROMPT,
    TRIAGE_TEMPLATE,
    VERIFY_SYSTEM_PROMPT,
    VERIFY_TEMPLATE,
)
from ._state import VerificationState

logger = logging.getLogger(__name__)

# ── LLM availability ────────────────────────────────────────────────────────

_LLM_AVAILABLE = False
_ChatAnthropic: Any = None

try:
    from langchain_anthropic import ChatAnthropic as _CA

    _ChatAnthropic = _CA
    _LLM_AVAILABLE = True
except ImportError:
    pass


def _get_llm(temperature: float = 0.0, max_tokens: int = 4096) -> Any:
    """Create a Claude LLM instance, or None if unavailable."""
    if not _LLM_AVAILABLE or _ChatAnthropic is None:
        return None
    try:
        import os

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return None
        return _ChatAnthropic(
            model="claude-sonnet-4-20250514",
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
        )
    except Exception as exc:
        logger.debug("Failed to create LLM: %s", exc)
        return None


def _parse_json_response(text: str) -> Dict[str, Any]:
    """Extract JSON from an LLM response, tolerating markdown fences."""
    # Strip markdown code fences if present
    cleaned = re.sub(r"^```(?:json)?\s*", "", text.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find JSON object in the response
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return {}


# ── Triage Node ─────────────────────────────────────────────────────────────


async def triage_node(state: VerificationState) -> dict:
    """Triage candidate findings: keep, dismiss, or escalate.

    Uses Claude to evaluate each finding against the CSV context.
    Falls back to heuristic triage when LLM is unavailable.
    """
    candidates = state.get("candidate_findings", [])
    if not candidates:
        return {
            "triage_verdicts": [],
            "triage_summary": "No candidates to triage.",
            "dismissed_count": 0,
        }

    llm = _get_llm()
    if llm is not None:
        return await _triage_with_llm(state, llm)
    else:
        logger.info("LLM unavailable; using heuristic triage")
        return _triage_heuristic(state)


async def _triage_with_llm(state: VerificationState, llm: Any) -> dict:
    """LLM-based triage."""
    candidates = state.get("candidate_findings", [])
    headers = state.get("csv_headers", [])
    sample_rows = state.get("csv_sample_rows", [])
    total_rows = state.get("csv_total_rows", 0)
    kb_summary = state.get("kb_rules_summary", "")

    # Limit to 30 candidates to fit context window
    candidates_trimmed = candidates[:30]

    prompt = TRIAGE_TEMPLATE.format(
        file_path=state.get("file_path", ""),
        headers=", ".join(headers),
        total_rows=total_rows,
        sample_rows=json.dumps(sample_rows[:5], indent=2, default=str),
        finding_count=len(candidates_trimmed),
        findings_json=json.dumps(candidates_trimmed, indent=2, default=str),
        kb_rules_summary=kb_summary,
    )

    try:
        from langchain_core.messages import HumanMessage, SystemMessage

        messages = [
            SystemMessage(content=TRIAGE_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]
        response = await llm.ainvoke(messages)
        result = _parse_json_response(response.content)

        verdicts = result.get("verdicts", [])
        summary = result.get("summary", "")

        # Build verdict lookup
        verdict_map = {v.get("finding_id", ""): v for v in verdicts}
        dismissed = sum(1 for v in verdicts if v.get("decision") == "dismiss")

        # Annotate candidates with triage decisions
        triage_verdicts = []
        for c in candidates:
            fid = c.get("finding_id", "")
            v = verdict_map.get(fid, {"decision": "keep", "reason": "no verdict"})
            triage_verdicts.append({
                **c,
                "triage_decision": v.get("decision", "keep"),
                "triage_reason": v.get("reason", ""),
            })

        return {
            "triage_verdicts": triage_verdicts,
            "triage_summary": summary,
            "dismissed_count": dismissed,
        }

    except Exception as exc:
        logger.warning("LLM triage failed, falling back to heuristic: %s", exc)
        return _triage_heuristic(state)


def _triage_heuristic(state: VerificationState) -> dict:
    """Deterministic triage fallback.

    Dismiss findings that match common false-positive patterns:
    - Keyword rules matching standard account names
    - INFO severity on non-monetary columns
    - Duplicate rule hits on the same row
    """
    candidates = state.get("candidate_findings", [])
    headers = state.get("csv_headers", [])
    header_lower = {h.lower() for h in headers}

    # Fields that contain financial data (more likely real findings)
    _MONETARY_FIELDS = {
        "amount", "balance", "translated_balance", "local_balance",
        "fx_rate", "rate", "debit", "credit", "gaap_balance",
        "ifrs_balance", "ifrs_adjustment",
    }
    monetary_cols = _MONETARY_FIELDS & header_lower

    verdicts: List[Dict[str, Any]] = []
    dismissed = 0
    seen_rows: Dict[int, str] = {}  # row_idx -> first rule_id that hit

    for c in candidates:
        severity = c.get("severity", "medium")
        field = (c.get("field", "") or "").lower()
        row_idx = c.get("row_index", -1)
        finding_type = c.get("finding_type", "custom")
        rule_id = c.get("rule_id", "")
        matched = c.get("matched_value", "")

        decision = "keep"
        reason = ""

        # Dismiss INFO on non-monetary columns
        if severity == "info" and field not in monetary_cols:
            decision = "dismiss"
            reason = "INFO severity on non-monetary column"

        # Dismiss if matched value is just a common account name term (<= 20 chars)
        elif finding_type == "custom" and len(matched) <= 20 and field in (
            "account_name", "description", "name"
        ):
            decision = "dismiss"
            reason = f"Generic keyword match on {field}"

        # Dismiss duplicate rule hits on same row
        elif row_idx in seen_rows and seen_rows[row_idx] != rule_id:
            # Keep the first, dismiss subsequent
            decision = "dismiss"
            reason = f"Duplicate hit on row {row_idx} (kept {seen_rows[row_idx]})"

        # Escalate CRITICAL/HIGH on monetary fields
        elif severity in ("critical", "high") and field in monetary_cols:
            decision = "escalate"
            reason = f"High-severity finding on monetary field {field}"

        if decision == "dismiss":
            dismissed += 1
        if decision != "dismiss" and row_idx not in seen_rows:
            seen_rows[row_idx] = rule_id

        verdicts.append({
            **c,
            "triage_decision": decision,
            "triage_reason": reason,
        })

    return {
        "triage_verdicts": verdicts,
        "triage_summary": (
            f"Heuristic triage: {dismissed}/{len(candidates)} dismissed, "
            f"{len(candidates) - dismissed} kept/escalated"
        ),
        "dismissed_count": dismissed,
    }


# ── Verify Node ─────────────────────────────────────────────────────────────


async def verify_node(state: VerificationState) -> dict:
    """Deep AI verification of findings that survived triage.

    Performs semantic, numeric, and cross-row analysis using Claude.
    Falls back to numeric heuristics when LLM is unavailable.

    Always runs even with zero survivors — the AI can discover novel
    numeric anomalies by scanning the actual CSV data.
    """
    verdicts = state.get("triage_verdicts", [])
    survivors = [
        v for v in verdicts
        if v.get("triage_decision") in ("keep", "escalate")
    ]

    llm = _get_llm(max_tokens=8192)
    if llm is not None:
        return await _verify_with_llm(state, survivors, llm)
    elif survivors:
        logger.info("LLM unavailable; using heuristic verification")
        return _verify_heuristic(state, survivors)
    else:
        return {
            "verified_findings": [],
            "verification_notes": ["No findings survived triage; LLM unavailable for novel detection."],
            "numeric_checks": [],
            "new_findings": [],
        }


async def _verify_with_llm(
    state: VerificationState,
    survivors: List[Dict[str, Any]],
    llm: Any,
) -> dict:
    """LLM-based deep verification."""
    headers = state.get("csv_headers", [])
    sample_rows = state.get("csv_sample_rows", [])
    total_rows = state.get("csv_total_rows", 0)

    escalated = [s for s in survivors if s.get("triage_decision") == "escalate"]
    kept = [s for s in survivors if s.get("triage_decision") == "keep"]

    # Limit to fit context window
    survivors_trimmed = survivors[:25]
    escalated_trimmed = escalated[:10]

    prompt = VERIFY_TEMPLATE.format(
        file_path=state.get("file_path", ""),
        headers=", ".join(headers),
        total_rows=total_rows,
        sample_rows=json.dumps(sample_rows[:10], indent=2, default=str),
        finding_count=len(survivors_trimmed),
        findings_json=json.dumps(survivors_trimmed, indent=2, default=str),
        escalated_json=json.dumps(escalated_trimmed, indent=2, default=str),
    )

    try:
        from langchain_core.messages import HumanMessage, SystemMessage

        messages = [
            SystemMessage(content=VERIFY_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]
        response = await llm.ainvoke(messages)
        result = _parse_json_response(response.content)

        verified = result.get("verified_findings", [])
        numeric_checks = result.get("numeric_checks", [])
        new_findings = result.get("new_findings", [])

        # Build verification notes
        notes = [
            v.get("verification_note", "")
            for v in verified
            if v.get("verification_note")
        ]

        # Filter to only verified=true findings
        verified_findings = [
            v for v in verified if v.get("verified", False)
        ]

        return {
            "verified_findings": verified_findings,
            "verification_notes": notes,
            "numeric_checks": numeric_checks,
            "new_findings": new_findings,
        }

    except Exception as exc:
        logger.warning("LLM verification failed, falling back to heuristic: %s", exc)
        return _verify_heuristic(state, survivors)


def _verify_heuristic(
    state: VerificationState,
    survivors: List[Dict[str, Any]],
) -> dict:
    """Deterministic verification fallback.

    Performs basic numeric range checks on rate/amount columns.
    """
    sample_rows = state.get("csv_sample_rows", [])
    verified: List[Dict[str, Any]] = []
    numeric_checks: List[Dict[str, Any]] = []

    # Known FX rate ranges (to USD)
    _RATE_RANGES = {
        "EUR": (0.8, 1.3), "GBP": (1.1, 1.6), "JPY": (0.005, 0.012),
        "CAD": (0.65, 0.85), "AUD": (0.55, 0.80), "CHF": (0.95, 1.25),
        "CNY": (0.12, 0.18), "INR": (0.010, 0.015), "BRL": (0.15, 0.25),
        "MXN": (0.04, 0.07), "SGD": (0.70, 0.80),
    }

    for s in survivors:
        confidence = s.get("confidence", 0.5)
        field = s.get("field", "")
        matched = s.get("matched_value", "")
        severity = s.get("severity", "medium")

        # Escalated findings get a boost
        if s.get("triage_decision") == "escalate":
            confidence = min(1.0, confidence + 0.1)

        # Try to parse numeric value for rate checks
        try:
            numeric_val = float(matched)
            if "rate" in field.lower() or "fx" in field.lower():
                # Check if rate is in a plausible range
                for ccy, (lo, hi) in _RATE_RANGES.items():
                    if lo <= numeric_val <= hi:
                        # Normal rate range — might be false positive
                        confidence = max(0.3, confidence - 0.2)
                        break
                    elif numeric_val > 10.0 or numeric_val < 0.001:
                        # Very unusual rate — likely inverted or wrong
                        confidence = min(1.0, confidence + 0.2)
                        numeric_checks.append({
                            "row_index": s.get("row_index", -1),
                            "field": field,
                            "value": matched,
                            "expected_range": "currency-dependent",
                            "anomaly_type": "unusual_rate",
                            "confidence": confidence,
                        })
        except (ValueError, TypeError):
            pass

        # Keep findings above threshold
        if confidence >= 0.3:
            verified.append({
                **s,
                "verified": True,
                "adjusted_confidence": round(confidence, 4),
                "verification_note": f"Heuristic: severity={severity}, field={field}",
            })

    return {
        "verified_findings": verified,
        "verification_notes": [f"Heuristic verified {len(verified)}/{len(survivors)} findings"],
        "numeric_checks": numeric_checks,
        "new_findings": [],  # Heuristic mode can't discover novel findings
    }


# ── Reconcile Node ──────────────────────────────────────────────────────────


async def reconcile_node(state: VerificationState) -> dict:
    """Reconcile and produce final de-duplicated findings.

    Combines regex-based detections with AI-verified results, assigns
    final confidence scores, and produces a quality assessment.
    """
    verified = state.get("verified_findings", [])
    numeric_checks = state.get("numeric_checks", [])
    new_findings = state.get("new_findings", [])
    candidates = state.get("candidate_findings", [])

    llm = _get_llm()
    if llm is not None:
        return await _reconcile_with_llm(state, llm)
    else:
        logger.info("LLM unavailable; using heuristic reconciliation")
        return _reconcile_heuristic(state)


async def _reconcile_with_llm(state: VerificationState, llm: Any) -> dict:
    """LLM-based reconciliation."""
    verified = state.get("verified_findings", [])
    numeric_checks = state.get("numeric_checks", [])
    new_findings = state.get("new_findings", [])
    candidates = state.get("candidate_findings", [])
    triage_summary = state.get("triage_summary", "")
    dismissed = state.get("dismissed_count", 0)
    total_rows = state.get("csv_total_rows", 0)

    after_triage = len(candidates) - dismissed

    prompt = RECONCILE_TEMPLATE.format(
        file_path=state.get("file_path", ""),
        total_rows=total_rows,
        triage_summary=triage_summary,
        dismissed_count=dismissed,
        verified_json=json.dumps(verified[:20], indent=2, default=str),
        numeric_checks_json=json.dumps(numeric_checks[:10], indent=2, default=str),
        new_findings_json=json.dumps(new_findings[:10], indent=2, default=str),
        original_count=len(candidates),
        after_triage_count=after_triage,
    )

    try:
        from langchain_core.messages import HumanMessage, SystemMessage

        messages = [
            SystemMessage(content=RECONCILE_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]
        response = await llm.ainvoke(messages)
        result = _parse_json_response(response.content)

        final = result.get("final_findings", [])
        summary = result.get("summary", "")
        quality = result.get("quality_score", 0.0)

        # Build confidence adjustments
        conf_adj = {}
        for f in final:
            fid = f.get("finding_id", "")
            if fid:
                conf_adj[fid] = f.get("confidence", 0.5)

        return {
            "final_findings": final,
            "reconciliation_summary": summary,
            "confidence_adjustments": conf_adj,
            "converged": True,
        }

    except Exception as exc:
        logger.warning("LLM reconciliation failed, falling back to heuristic: %s", exc)
        return _reconcile_heuristic(state)


def _reconcile_heuristic(state: VerificationState) -> dict:
    """Deterministic reconciliation fallback.

    Merges verified findings + numeric checks + novel findings, de-dupes
    by (account, row_index), and sorts by severity/confidence.
    """
    verified = state.get("verified_findings", [])
    numeric_checks = state.get("numeric_checks", [])
    new_findings = state.get("new_findings", [])
    candidates = state.get("candidate_findings", [])
    dismissed = state.get("dismissed_count", 0)

    # Collect all findings into a unified list
    all_findings: List[Dict[str, Any]] = []
    seen_keys: set = set()

    # Priority 1: AI-verified findings
    for v in verified:
        key = (v.get("account", ""), v.get("row_index", -1), v.get("finding_type", ""))
        if key not in seen_keys:
            seen_keys.add(key)
            conf = v.get("adjusted_confidence", v.get("confidence", 0.5))
            all_findings.append({
                "finding_id": v.get("finding_id", ""),
                "finding_type": v.get("finding_type", "custom"),
                "severity": v.get("severity", "medium"),
                "account": v.get("account", ""),
                "row_index": v.get("row_index", -1),
                "field": v.get("field", ""),
                "evidence": v.get("evidence", v.get("verification_note", "")),
                "confidence": round(conf, 4),
                "source": "combined",
                "kb_node_ids": v.get("kb_node_ids", []),
            })

    # Priority 2: Numeric anomalies from AI
    for nc in numeric_checks:
        key = ("", nc.get("row_index", -1), nc.get("anomaly_type", ""))
        if key not in seen_keys:
            seen_keys.add(key)
            all_findings.append({
                "finding_id": "",
                "finding_type": nc.get("anomaly_type", "custom"),
                "severity": "high" if nc.get("confidence", 0) > 0.7 else "medium",
                "account": "",
                "row_index": nc.get("row_index", -1),
                "field": nc.get("field", ""),
                "evidence": f"Numeric anomaly: value {nc.get('value', '')} outside {nc.get('expected_range', '')}",
                "confidence": nc.get("confidence", 0.5),
                "source": "ai",
                "kb_node_ids": [],
            })

    # Priority 3: Novel AI findings
    for nf in new_findings:
        key = (nf.get("account", ""), nf.get("row_index", -1), nf.get("finding_type", ""))
        if key not in seen_keys:
            seen_keys.add(key)
            all_findings.append({
                "finding_id": "",
                "finding_type": nf.get("finding_type", "custom"),
                "severity": nf.get("severity", "medium"),
                "account": nf.get("account", ""),
                "row_index": nf.get("row_index", -1),
                "field": nf.get("field", ""),
                "evidence": nf.get("evidence", ""),
                "confidence": nf.get("confidence", 0.5),
                "source": "ai",
                "kb_node_ids": [],
            })

    # Sort: CRITICAL > HIGH > MEDIUM > LOW > INFO, then by confidence desc
    _SEV_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    all_findings.sort(
        key=lambda f: (
            _SEV_ORDER.get(f.get("severity", "medium"), 2),
            -f.get("confidence", 0),
        )
    )

    total = len(candidates)
    verified_count = len(all_findings)
    summary = (
        f"Processed {total} candidates: {dismissed} dismissed by triage, "
        f"{verified_count} verified findings. "
        f"Detection noise reduction: {round((1 - verified_count / max(total, 1)) * 100, 1)}%"
    )

    conf_adj = {f.get("finding_id", ""): f.get("confidence", 0.5) for f in all_findings}

    return {
        "final_findings": all_findings,
        "reconciliation_summary": summary,
        "confidence_adjustments": conf_adj,
        "converged": True,
    }
