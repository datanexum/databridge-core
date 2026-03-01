"""Grounded detection engine.

Runs KB-grounded detection rules against CSV data files. Every finding
is traceable back to Knowledge Base nodes, providing auditable, explainable
results rather than opaque flags.

The detection pipeline:

1. Load rules from KB (or accept pre-loaded rules)
2. Parse the target CSV file
3. For each row, check each field against KB-grounded rules
4. Optionally query GraphRAG for additional context on flagged accounts
5. Score findings using trust metrics from rule confidence and feedback
6. Apply the feedback filter (suppress consistently dismissed patterns)
7. Return findings with KB citations
"""
from __future__ import annotations

import csv
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._feedback import apply_feedback_filter

try:
    from ._rules_c import load_detection_rules  # Cython compiled
except ImportError:
    from ._rules import load_detection_rules  # Pure Python fallback
from ._types import (
    DetectionContext,
    DetectionRule,
    DetectionSummary,
    FindingType,
    GroundedFinding,
    Severity,
    SPRTCertificate,
)

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "data/detection"
DEFAULT_FEEDBACK_PATH = "data/detection/feedback.jsonl"

# Maximum number of findings per file to prevent runaway output.
_MAX_FINDINGS_PER_FILE = 500

# Maximum CSV rows to scan (safety limit for very large files).
_MAX_ROWS = 100_000

# -- SPRT (Sequential Probability Ratio Test) constants ----------------------
_SPRT_P0 = 0.001       # Clean threshold (null hypothesis error rate)
_SPRT_P1 = 0.01        # Anomalous threshold (alternative hypothesis error rate)
_SPRT_ALPHA = 0.05     # False alarm rate
_SPRT_BETA = 0.05      # Miss rate
_SPRT_MIN_ROWS = 100   # Minimum rows before SPRT can decide
_SPRT_A = math.log((1 - _SPRT_BETA) / _SPRT_ALPHA)   # Upper bound (anomalous)
_SPRT_B = math.log(_SPRT_BETA / (1 - _SPRT_ALPHA))    # Lower bound (clean)


class _SPRTState:
    """Wald's Sequential Probability Ratio Test for early exit.

    Tests H0 (error_rate <= P0, file is clean) against
    H1 (error_rate >= P1, file is anomalous).

    After each row, updates the log-likelihood ratio. If the ratio
    drops below B (lower bound), the file is certified clean and
    scanning stops early. If it exceeds A (upper bound), the file
    is anomalous -- but we do NOT stop scanning because we want all
    findings.
    """

    __slots__ = ("_llr", "_rows_checked", "_findings_count", "_decision")

    def __init__(self):
        self._llr = 0.0
        self._rows_checked = 0
        self._findings_count = 0
        self._decision: Optional[str] = None  # None, "clean", "anomalous"

    def update(self, has_finding: bool) -> None:
        """Update SPRT state after scanning one row."""
        self._rows_checked += 1
        if has_finding:
            self._findings_count += 1
            # Log likelihood increment for finding: log(P1/P0)
            self._llr += math.log(_SPRT_P1 / _SPRT_P0)
        else:
            # Log likelihood increment for no finding: log((1-P1)/(1-P0))
            self._llr += math.log((1 - _SPRT_P1) / (1 - _SPRT_P0))

        # Only decide after minimum rows
        if self._rows_checked >= _SPRT_MIN_ROWS and self._decision is None:
            if self._llr <= _SPRT_B:
                self._decision = "clean"
            elif self._llr >= _SPRT_A:
                self._decision = "anomalous"

    @property
    def decision(self) -> Optional[str]:
        return self._decision

    def to_certificate(self, total_rows: int) -> SPRTCertificate:
        return SPRTCertificate(
            decision=self._decision or "continue",
            rows_scanned=self._rows_checked,
            total_rows=total_rows,
            log_likelihood_ratio=round(self._llr, 6),
            p0=_SPRT_P0,
            p1=_SPRT_P1,
            confidence_level=1 - _SPRT_ALPHA,
        )


def detect_grounded(
    file_path: str,
    rules: Optional[List[DetectionRule]] = None,
    context: Optional[DetectionContext] = None,
    use_graphrag: bool = True,
    feedback_path: str = DEFAULT_FEEDBACK_PATH,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    knowledge_dir: str = "data/knowledge",
    min_confidence: float = 0.0,
    feedback_strategy: str = "thompson",
    early_exit: bool = True,
) -> Dict[str, Any]:
    """Run grounded detection on a single CSV file.

    This is the main entry point for single-file detection. It applies
    KB-grounded rules against every row in the CSV, enriches flagged
    accounts with GraphRAG context, and filters results through the
    feedback learning loop.

    Args:
        file_path: Path to the CSV file to scan.
        rules: Pre-loaded detection rules. If ``None``, rules are loaded
            from the Knowledge Base.
        context: Optional :class:`DetectionContext` with entity maps,
            Logic DNAs, and rate tables for enriched detection.
        use_graphrag: Whether to query GraphRAG for additional context
            on flagged accounts (default ``True``).
        feedback_path: Path to the feedback JSONL file for the learning
            loop.
        output_dir: Directory to write detection results JSON.
        knowledge_dir: Knowledge base directory (used when *rules* is
            ``None``).
        min_confidence: Minimum rule confidence threshold. Rules below
            this value are skipped (default ``0.0`` = no filtering).
        feedback_strategy: Feedback suppression strategy --
            ``"thompson"`` (default, Beta sampling) or ``"threshold"``
            (legacy hard cutoff).
        early_exit: Whether to use SPRT early exit for clean files.
            When ``True``, scanning stops early if the file is
            statistically certified clean (default ``True``).

    Returns:
        Dict with ``summary``, ``findings`` (up to 10 for context limit),
        ``output_file``, and ``total_findings`` count.
    """
    t0 = time.time()
    fp = Path(file_path)

    # Validate input
    if not fp.exists():
        return {"error": f"File not found: {file_path}"}
    if fp.suffix.lower() not in (".csv", ".tsv", ".txt"):
        return {"error": f"Unsupported file type: {fp.suffix}. Expected .csv, .tsv, or .txt"}

    # 1. Load rules
    if rules is None:
        if context and context.rules:
            rules = context.rules
        else:
            rules = load_detection_rules(knowledge_dir=knowledge_dir)

    if not rules:
        logger.warning("No detection rules loaded; returning empty results")
        return {
            "summary": {"file_path": file_path, "total_findings": 0, "rules_applied": 0},
            "findings": [],
            "total_findings": 0,
            "warning": "No detection rules available. Populate data/knowledge/ with rule nodes.",
        }

    # 2. Parse CSV
    rows, headers = _read_csv(file_path)
    if not rows:
        return {
            "summary": {"file_path": file_path, "total_findings": 0, "total_rows": 0},
            "findings": [],
            "total_findings": 0,
        }

    # 3. Compile rule patterns (cache compiled regexes)
    compiled_rules = _compile_rules(rules, headers, min_confidence=min_confidence)

    # 4. Run detection (with optional SPRT early exit)
    raw_findings: List[GroundedFinding] = []
    sprt = _SPRTState() if early_exit else None
    rows_scanned = 0

    for row_idx, row in enumerate(rows):
        if len(raw_findings) >= _MAX_FINDINGS_PER_FILE:
            logger.warning(
                "Hit max findings limit (%d) for %s at row %d",
                _MAX_FINDINGS_PER_FILE,
                file_path,
                row_idx,
            )
            break

        row_findings = _check_row(row_idx, row, headers, compiled_rules)
        raw_findings.extend(row_findings)
        rows_scanned += 1

        if sprt:
            sprt.update(has_finding=len(row_findings) > 0)
            if sprt.decision == "clean":
                logger.info(
                    "SPRT certified %s clean after %d/%d rows",
                    file_path, rows_scanned, len(rows),
                )
                break
            # Note: anomalous decision does NOT stop scanning --
            # we want all findings

    # 5. GraphRAG enrichment (optional)
    graphrag_enriched = 0
    if use_graphrag and raw_findings:
        graphrag_enriched = _enrich_with_graphrag(raw_findings)

    # 5b. Forensic RAG enrichment
    forensic_enriched = 0
    if use_graphrag and raw_findings:
        forensic_enriched = _enrich_with_forensic_rag(raw_findings)

    # 5c. Runtime monitors
    monitor_warnings: list = []
    try:
        from ._monitors import run_all_monitors
        monitor_warnings = run_all_monitors(raw_findings)
        if monitor_warnings:
            from ._counterexamples import capture_counterexample
            capture_counterexample(
                file_path=file_path,
                findings=[f.model_dump(mode="json") for f in raw_findings[:20]],
                monitor_warnings=monitor_warnings,
            )
    except ImportError:
        pass

    # 6. Apply feedback filter
    filtered_findings = apply_feedback_filter(
        raw_findings, feedback_path=feedback_path,
        strategy=feedback_strategy,
    )
    suppressed_count = len(raw_findings) - len(filtered_findings)

    # 7. Build summary
    duration = time.time() - t0
    severity_counts: Dict[str, int] = {}
    type_counts: Dict[str, int] = {}
    confidence_sum = 0.0

    for f in filtered_findings:
        sev = f.severity.value
        severity_counts[sev] = severity_counts.get(sev, 0) + 1
        ft = f.finding_type.value
        type_counts[ft] = type_counts.get(ft, 0) + 1
        confidence_sum += f.confidence

    n = len(filtered_findings) or 1
    sprt_cert = sprt.to_certificate(len(rows)) if sprt else None
    summary = DetectionSummary(
        file_path=file_path,
        total_rows=len(rows),
        rows_scanned=rows_scanned,
        total_findings=len(filtered_findings),
        rules_applied=len(compiled_rules),
        severity_counts=severity_counts,
        type_counts=type_counts,
        avg_confidence=round(confidence_sum / n, 4),
        graphrag_enriched=graphrag_enriched,
        feedback_suppressed=suppressed_count,
        duration_seconds=round(duration, 2),
        sprt_certificate=sprt_cert if sprt_cert and sprt_cert.decision == "clean" else None,
    )

    # 8. Persist results
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = fp.stem
    out_file = out_dir / f"{stem}_detections.json"

    import json
    result_data = {
        "summary": summary.model_dump(mode="json"),
        "findings": [f.model_dump(mode="json") for f in filtered_findings],
    }
    with open(out_file, "w", encoding="utf-8") as fout:
        json.dump(result_data, fout, indent=2, default=str)

    # Return context-limited view (max 10 findings)
    sample_findings = [
        f.model_dump(mode="json") for f in filtered_findings[:10]
    ]

    result = {
        "summary": summary.model_dump(mode="json"),
        "findings": sample_findings,
        "total_findings": len(filtered_findings),
        "output_file": str(out_file),
        "note": (
            f"Showing {len(sample_findings)} of {len(filtered_findings)} findings. "
            f"Full results at {out_file}"
            if len(filtered_findings) > 10
            else None
        ),
    }

    if monitor_warnings:
        result["monitor_warnings"] = monitor_warnings

    return result


def detect_grounded_batch(
    directory: str = ".",
    file_pattern: str = "*.csv",
    rules: Optional[List[DetectionRule]] = None,
    context: Optional[DetectionContext] = None,
    use_graphrag: bool = True,
    feedback_path: str = DEFAULT_FEEDBACK_PATH,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    knowledge_dir: str = "data/knowledge",
    max_files: int = 0,
) -> Dict[str, Any]:
    """Run grounded detection on a batch of CSV files.

    Scans all files matching *file_pattern* in *directory* and runs
    :func:`detect_grounded` on each.

    Args:
        directory: Directory to scan for CSV files.
        file_pattern: Glob pattern for file selection (default ``*.csv``).
        rules: Pre-loaded detection rules. If ``None``, loaded once from KB.
        context: Optional detection context for enrichment.
        use_graphrag: Whether to use GraphRAG enrichment.
        feedback_path: Path to the feedback JSONL file.
        output_dir: Directory to write detection results.
        knowledge_dir: Knowledge base directory.
        max_files: Limit number of files (0 = all).

    Returns:
        Dict with batch summary, per-file results (sample), and errors.
    """
    t0 = time.time()
    scan_dir = Path(directory)

    if not scan_dir.exists():
        return {"error": f"Directory not found: {directory}"}

    # Load rules once for the entire batch
    if rules is None:
        if context and context.rules:
            rules = context.rules
        else:
            rules = load_detection_rules(knowledge_dir=knowledge_dir)

    # Discover files
    csv_files = sorted(scan_dir.glob(file_pattern))
    if max_files > 0:
        csv_files = csv_files[:max_files]

    if not csv_files:
        return {
            "summary": {"total_files": 0, "message": f"No {file_pattern} files found in {directory}"},
            "results": [],
            "errors": [],
        }

    total = len(csv_files)
    completed: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    total_findings = 0
    total_rows = 0
    severity_counts: Dict[str, int] = {}
    type_counts: Dict[str, int] = {}
    confidence_sum = 0.0

    for i, csv_file in enumerate(csv_files):
        logger.info("[%d/%d] Scanning %s", i + 1, total, csv_file.name)

        try:
            result = detect_grounded(
                file_path=str(csv_file),
                rules=rules,
                context=context,
                use_graphrag=use_graphrag,
                feedback_path=feedback_path,
                output_dir=output_dir,
                knowledge_dir=knowledge_dir,
            )

            if result.get("error"):
                errors.append({"file_path": str(csv_file), "error": result["error"]})
            else:
                summary = result.get("summary", {})
                total_findings += summary.get("total_findings", 0)
                total_rows += summary.get("total_rows", 0)

                # Aggregate severity and type counts
                for sev, count in summary.get("severity_counts", {}).items():
                    severity_counts[sev] = severity_counts.get(sev, 0) + count
                for ft, count in summary.get("type_counts", {}).items():
                    type_counts[ft] = type_counts.get(ft, 0) + count

                n_findings = summary.get("total_findings", 0)
                confidence_sum += summary.get("avg_confidence", 0.0) * n_findings

                completed.append({
                    "file_path": str(csv_file),
                    "total_findings": n_findings,
                    "total_rows": summary.get("total_rows", 0),
                    "output_file": result.get("output_file", ""),
                })

        except Exception as exc:
            logger.error("Detection failed for %s: %s", csv_file.name, exc)
            errors.append({"file_path": str(csv_file), "error": str(exc)})

    duration = time.time() - t0

    from ._types import DetectionBatchSummary

    batch_summary = DetectionBatchSummary(
        total_files=total,
        completed=len(completed),
        failed=len(errors),
        total_findings=total_findings,
        total_rows_scanned=total_rows,
        severity_counts=severity_counts,
        type_counts=type_counts,
        avg_confidence=round(confidence_sum / max(total_findings, 1), 4),
        rules_loaded=len(rules) if rules else 0,
        duration_seconds=round(duration, 2),
    )

    return {
        "summary": batch_summary.model_dump(mode="json"),
        "results": completed[:10],
        "errors": errors[:10],
        "output_dir": str(output_dir),
        "note": (
            f"Showing {min(len(completed), 10)} of {len(completed)} results."
            if len(completed) > 10
            else None
        ),
    }


# -- Internal helpers ---------------------------------------------------------


def _read_csv(file_path: str) -> tuple:
    """Read a CSV file and return (rows, headers).

    Skips comment lines (starting with ``#``) at the top of the file,
    which are common in generated training data. Each row is a dict
    keyed by column header. Returns at most :data:`_MAX_ROWS` rows.

    Returns:
        Tuple of (list of row dicts, list of header strings).
    """
    rows: List[Dict[str, str]] = []
    headers: List[str] = []

    try:
        fp = Path(file_path)
        delimiter = "\t" if fp.suffix.lower() == ".tsv" else ","

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            # Read all lines, strip comment/blank lines at the top
            lines = f.readlines()

        # Skip leading comment lines (# ...) and blank lines
        data_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#") or not stripped:
                continue
            data_lines.append(line)

        if not data_lines:
            return rows, headers

        # Detect delimiter from data lines (not comments)
        sample = "".join(data_lines[:10])
        if delimiter == "," and "\t" in sample and "," not in sample:
            delimiter = "\t"

        import io
        reader = csv.DictReader(io.StringIO("".join(data_lines)), delimiter=delimiter)
        headers = reader.fieldnames or []

        for i, row in enumerate(reader):
            if i >= _MAX_ROWS:
                logger.warning(
                    "Hit max rows limit (%d) for %s", _MAX_ROWS, file_path
                )
                break
            rows.append(row)

    except Exception as exc:
        logger.error("Failed to read CSV %s: %s", file_path, exc)

    return rows, headers


def _compile_rules(
    rules: List[DetectionRule],
    headers: List[str],
    min_confidence: float = 0.0,
) -> List[tuple]:
    """Compile rule regex patterns and resolve field targets.

    For each rule, the field_targets are resolved against the actual CSV
    headers. Rules whose field_targets are non-empty but none resolve
    to actual columns are skipped entirely (they are irrelevant to this
    file and would cause false-positive noise via all-column fallback).

    Args:
        rules: Detection rules to compile.
        headers: CSV column headers from the target file.
        min_confidence: Skip rules with confidence below this threshold.

    Returns:
        List of (rule, compiled_regex, resolved_fields) tuples.
    """
    compiled: List[tuple] = []
    header_lower = {h.lower(): h for h in headers}

    for rule in rules:
        if not rule.pattern:
            continue

        if rule.confidence < min_confidence:
            continue

        try:
            regex = re.compile(rule.pattern)
        except re.error as exc:
            logger.warning(
                "Skipping rule %s: invalid regex %r: %s",
                rule.rule_id,
                rule.pattern,
                exc,
            )
            continue

        # Resolve field targets to actual column names
        if rule.field_targets:
            resolved = []
            for target in rule.field_targets:
                # Try exact match first, then case-insensitive
                if target in headers:
                    resolved.append(target)
                elif target.lower() in header_lower:
                    resolved.append(header_lower[target.lower()])
            # If none of the rule's field_targets exist in this file,
            # the rule is irrelevant -- skip it.  Falling back to every
            # column causes massive false-positive noise.
            if not resolved:
                continue
            fields = resolved
        else:
            fields = headers

        compiled.append((rule, regex, fields))

    return compiled


def _check_row(
    row_idx: int,
    row: Dict[str, str],
    headers: List[str],
    compiled_rules: List[tuple],
) -> List[GroundedFinding]:
    """Check a single CSV row against all compiled rules.

    Returns a list of findings for this row.
    """
    findings: List[GroundedFinding] = []

    # Pre-extract common account identifier for the finding
    account = _extract_account(row, headers)

    for rule, regex, fields in compiled_rules:
        for field in fields:
            value = row.get(field, "")
            if not value:
                continue

            match = regex.search(str(value))
            if match:
                finding = GroundedFinding(
                    finding_type=rule.finding_type,
                    severity=rule.severity,
                    account=account,
                    row_index=row_idx,
                    field=field,
                    matched_value=match.group(0),
                    evidence=(
                        f"Rule '{rule.name}' matched pattern "
                        f"/{rule.pattern}/ in field '{field}' "
                        f"(value: '{_truncate(value, 80)}')"
                    ),
                    kb_node_ids=rule.evidence_nodes[:],
                    confidence=rule.confidence,
                    grounding_context=rule.description,
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                )
                findings.append(finding)
                # One match per rule per row to avoid duplicates
                break

    return findings


def _extract_account(row: Dict[str, str], headers: List[str]) -> str:
    """Extract the most likely account identifier from a row.

    Looks for common account column names in priority order.
    """
    _ACCOUNT_COLUMNS = [
        "account", "account_code", "account_name", "acct", "acct_code",
        "account_id", "acct_id", "gl_account", "account_number",
        "name", "description", "label",
    ]
    header_lower = {h.lower(): h for h in headers}

    for col in _ACCOUNT_COLUMNS:
        actual = header_lower.get(col)
        if actual:
            val = row.get(actual, "")
            if val:
                return _truncate(val, 120)

    # Fallback: use first non-empty value
    for h in headers:
        val = row.get(h, "")
        if val:
            return _truncate(val, 120)

    return ""


def _truncate(text: str, max_len: int) -> str:
    """Truncate text to max_len, appending ellipsis if truncated."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _enrich_with_graphrag(findings: List[GroundedFinding]) -> int:
    """Query GraphRAG for additional context on flagged accounts.

    Enriches each finding's ``grounding_context`` and ``graphrag_results``
    fields with relevant knowledge graph nodes.

    Returns the number of findings successfully enriched.
    """
    try:
        from databridge_core.graphrag import search  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("GraphRAG search not available; skipping enrichment")
        return 0

    enriched = 0
    # Deduplicate queries by account to avoid redundant searches
    seen_accounts: Dict[str, Dict[str, Any]] = {}

    for finding in findings:
        account = finding.account
        if not account:
            continue

        if account in seen_accounts:
            result = seen_accounts[account]
        else:
            try:
                result = search(
                    query=account,
                    top_k=3,
                    include_evidence=True,
                )
                seen_accounts[account] = result
            except Exception as exc:
                logger.debug("GraphRAG search failed for '%s': %s", account, exc)
                seen_accounts[account] = {}
                continue

        if result and not result.get("error"):
            matches = result.get("results", result.get("matches", []))
            if matches:
                # Add GraphRAG context
                top_match = matches[0] if isinstance(matches, list) and matches else {}
                context_text = top_match.get("name", top_match.get("text", ""))
                if context_text:
                    finding.grounding_context = (
                        f"{finding.grounding_context} | "
                        f"GraphRAG: {_truncate(context_text, 200)}"
                    )
                finding.graphrag_results = (
                    matches[:3] if isinstance(matches, list) else []
                )
                enriched += 1

    if enriched > 0:
        logger.info("GraphRAG enriched %d of %d findings", enriched, len(findings))

    return enriched


def _enrich_with_forensic_rag(findings: List[GroundedFinding]) -> int:
    """Query Forensic Evidence RAG for similar past findings.

    For each finding, searches the forensic evidence index for similar
    historical detections and appends outcome context to the finding's
    ``grounding_context`` field.

    Returns the number of findings successfully enriched.
    """
    try:
        from databridge_core.graphrag.unified import dispatch_rag_search  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("Forensic RAG search not available; skipping enrichment")
        return 0

    # Need settings for the dispatch call
    try:
        from databridge_core.config import get_settings  # type: ignore[import-not-found]
        settings = get_settings()
    except Exception:
        logger.debug("Settings not available for forensic RAG; skipping")
        return 0

    enriched = 0
    # Deduplicate queries to avoid redundant searches
    seen_queries: Dict[str, Dict[str, Any]] = {}

    for finding in findings:
        # Build a query string from finding attributes
        query_parts = [
            finding.finding_type.value if finding.finding_type else "",
            finding.severity.value if finding.severity else "",
            finding.rule_name or "",
            finding.account or "",
            finding.field or "",
        ]
        query_key = " ".join(p for p in query_parts if p).strip()
        if not query_key:
            continue

        if query_key in seen_queries:
            result = seen_queries[query_key]
        else:
            try:
                result = dispatch_rag_search(
                    settings,
                    action="search_forensic_evidence",
                    query=query_key,
                    top_k=3,
                )
                seen_queries[query_key] = result
            except Exception as exc:
                logger.debug("Forensic RAG search failed for '%s': %s", query_key, exc)
                seen_queries[query_key] = {}
                continue

        if result and not result.get("error"):
            similar = result.get("similar_findings", [])
            if similar:
                top = similar[0]
                context_text = (
                    f"ForensicRAG: Similar case: {top.get('finding_type', '')} "
                    f"in {top.get('source_file', '')} was "
                    f"{top.get('outcome', 'pending')} "
                    f"({_truncate(top.get('feedback_notes', ''), 100)})"
                )
                finding.grounding_context = (
                    f"{finding.grounding_context} | {context_text}"
                )
                enriched += 1

    if enriched > 0:
        logger.info("ForensicRAG enriched %d of %d findings", enriched, len(findings))

    return enriched
