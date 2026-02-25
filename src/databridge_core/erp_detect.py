"""DataBridge ERP Detection — Identify source ERP system from COA file fingerprints.

Analyzes column names, delimiters, account number formats, and metadata patterns
to determine whether a file originated from SAP, Oracle, NetSuite, Dynamics 365,
or Workday.

Public API:
    detect_erp       — Detect ERP system from a single file
    detect_erp_batch — Detect ERP for all files in a directory
"""

__all__ = ["detect_erp", "detect_erp_batch"]

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# ERP Fingerprint Definitions
# ---------------------------------------------------------------------------

ERP_SIGNATURES = {
    "SAP": {
        "columns": {"BUKRS", "SAKNR", "KTOPL", "TXT20", "TXT50", "KTOKS", "XBILK", "WAERS", "ERDAT", "USNAM"},
        "strong_columns": {"SAKNR", "BUKRS", "KTOPL", "KTOKS"},
        "metadata_patterns": [r"SAP\s*S/4", r"Table:\s*SKA1", r"SKAT", r"Client:\s*\d{3}"],
        "account_pattern": r"^\d{10}$",
        "delimiter": "\t",
        "header_prefix": "*",
    },
    "Oracle": {
        "columns": {"LEDGER_ID", "CODE_COMBINATION_ID", "SEGMENT1", "SEGMENT2", "SEGMENT3", "SEGMENT4",
                     "SEGMENT5", "ACCOUNT_TYPE", "ENABLED_FLAG", "SUMMARY_FLAG", "START_DATE_ACTIVE"},
        "strong_columns": {"CODE_COMBINATION_ID", "SEGMENT1", "SEGMENT2", "ENABLED_FLAG"},
        "metadata_patterns": [r"Oracle\s*EBS", r"Fusion\s*Cloud", r"GL_CODE_COMBINATIONS", r"Set of Books"],
        "account_pattern": r"^\d{2}-\d{3}-\d{4}-\d{3}-\d{3}$",
        "delimiter": ",",
        "header_prefix": "--",
    },
    "NetSuite": {
        "columns": {"Internal ID", "External ID", "Account Number", "Account Name", "Is Inactive",
                     "Is Summary", "Parent", "General Rate Type", "Eliminate", "Revalue"},
        "strong_columns": {"Internal ID", "External ID", "Is Inactive", "General Rate Type", "Eliminate"},
        "metadata_patterns": [r"NetSuite", r"Subsidiary:", r"Format:\s*CSV"],
        "account_pattern": r"^GL-\d+$",
        "delimiter": ",",
        "header_prefix": None,
    },
    "Dynamics365": {
        "columns": {"MainAccountId", "MainAccountName", "MainAccountCategory", "DebitCreditDefault",
                     "FinancialStatementGroup", "BalanceControl", "PostingType", "IsBlockedForManualEntry",
                     "IsSuspended", "ChartOfAccountsId"},
        "strong_columns": {"MainAccountId", "MainAccountCategory", "FinancialStatementGroup", "ChartOfAccountsId"},
        "metadata_patterns": [r"Microsoft\s*Dynamics\s*365", r"Legal\s*Entity:", r"USMF|DEMF|GBMF"],
        "account_pattern": r"^\d{6}$",
        "delimiter": ",",
        "header_prefix": "#",
    },
    "Workday": {
        "columns": {"Ledger_Account_ID", "Ledger_Account_Name", "Account_Type", "Account_Subtype",
                     "Worktag_Organization", "Revenue_Category", "Spend_Category", "Balance_Sheet_Category",
                     "Rollup_Account"},
        "strong_columns": {"Ledger_Account_ID", "Worktag_Organization", "Revenue_Category", "Spend_Category"},
        "metadata_patterns": [r"Workday", r"Tenant:", r"Financial\s*Management", r"Ledger\s*Account\s*Extract"],
        "account_pattern": r"^\d{5}$",
        "delimiter": ",",
        "header_prefix": None,
    },
}


def _read_file_head(path: str, max_lines: int = 50) -> List[str]:
    """Read first N lines of a file, trying common encodings."""
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                lines = []
                for i, line in enumerate(f):
                    if i >= max_lines:
                        break
                    lines.append(line.rstrip("\n\r"))
                return lines
        except (UnicodeDecodeError, UnicodeError):
            continue
    return []


def _detect_delimiter(lines: List[str]) -> str:
    """Detect the most likely delimiter from data lines."""
    # Skip metadata/comment lines
    data_lines = [l for l in lines if l and not l.startswith(("*", "#", "--", "//"))]
    if not data_lines:
        return ","

    for delim in ("\t", ",", ";", "|"):
        counts = [l.count(delim) for l in data_lines[:10]]
        if counts and min(counts) > 0 and max(counts) - min(counts) <= 2:
            return delim
    return ","


def _find_header_line(lines: List[str]) -> Optional[int]:
    """Find the header row index (first non-comment, non-empty line)."""
    for i, line in enumerate(lines):
        if not line or line.startswith(("*", "#", "--", "//")):
            continue
        # Check if it looks like a header (has letters, minimal digits)
        alpha_ratio = sum(1 for c in line if c.isalpha()) / max(len(line), 1)
        if alpha_ratio > 0.3:
            return i
    return None


def _extract_columns(lines: List[str], delimiter: str) -> List[str]:
    """Extract column names from the header line."""
    header_idx = _find_header_line(lines)
    if header_idx is None:
        return []
    return [col.strip().strip('"').strip("'") for col in lines[header_idx].split(delimiter)]


def _score_erp(columns: List[str], lines: List[str], delimiter: str, erp_name: str, sig: dict) -> Dict[str, Any]:
    """Score how well a file matches an ERP signature."""
    col_set = set(columns)
    score = 0.0
    signals = []
    max_score = 100.0

    # 1. Column name matching (40 points max)
    all_matches = col_set & sig["columns"]
    strong_matches = col_set & sig["strong_columns"]

    if sig["columns"]:
        col_pct = len(all_matches) / len(sig["columns"])
        score += col_pct * 25
        if all_matches:
            signals.append(f"Column matches: {', '.join(sorted(all_matches))}")

    if sig["strong_columns"]:
        strong_pct = len(strong_matches) / len(sig["strong_columns"])
        score += strong_pct * 15
        if strong_matches:
            signals.append(f"Strong signals: {', '.join(sorted(strong_matches))}")

    # 2. Metadata pattern matching (25 points max)
    metadata_lines = "\n".join(lines[:15])
    pattern_hits = 0
    for pattern in sig["metadata_patterns"]:
        if re.search(pattern, metadata_lines, re.IGNORECASE):
            pattern_hits += 1
            signals.append(f"Metadata match: {pattern}")
    if sig["metadata_patterns"]:
        score += (pattern_hits / len(sig["metadata_patterns"])) * 25

    # 3. Delimiter matching (15 points)
    if sig["delimiter"] == delimiter:
        score += 15
        signals.append(f"Delimiter match: {repr(delimiter)}")

    # 4. Account number format (10 points)
    if sig["account_pattern"]:
        # Check first few data values in likely account column
        header_idx = _find_header_line(lines)
        if header_idx is not None:
            data_lines = lines[header_idx + 1: header_idx + 20]
            matches = 0
            total = 0
            for dl in data_lines:
                parts = dl.split(delimiter)
                if len(parts) > 1:
                    # Check first 3 columns for account-like values
                    for val in parts[:3]:
                        val = val.strip().strip('"')
                        if val and re.match(sig["account_pattern"], val):
                            matches += 1
                        total += 1
            if total > 0 and matches > 0:
                score += (matches / total) * 10
                signals.append(f"Account format matches: {matches}/{total}")

    # 5. Header prefix (5 points)
    if sig["header_prefix"] and lines:
        prefix_hits = sum(1 for l in lines[:10] if l.startswith(sig["header_prefix"]))
        if prefix_hits >= 2:
            score += 5
            signals.append(f"Header prefix '{sig['header_prefix']}': {prefix_hits} lines")

    # 6. Bonus: case-insensitive column matching for partial names
    col_lower = {c.lower().replace(" ", "_") for c in columns}
    sig_lower = {c.lower().replace(" ", "_") for c in sig["columns"]}
    fuzzy_matches = col_lower & sig_lower - {c.lower().replace(" ", "_") for c in all_matches}
    if fuzzy_matches:
        score += min(len(fuzzy_matches) * 2, 5)

    confidence = round(min(score / max_score, 1.0), 3)

    return {
        "erp": erp_name,
        "score": round(score, 1),
        "confidence": confidence,
        "signals": signals,
        "column_matches": len(all_matches),
        "strong_matches": len(strong_matches),
    }


def detect_erp(
    file_path: str,
    return_all_scores: bool = False,
) -> Dict[str, Any]:
    """Detect the source ERP system for a single COA file.

    Args:
        file_path: Path to the COA/GL file to analyze.
        return_all_scores: If True, include scores for all ERP systems.

    Returns:
        Dict with detected_erp, confidence, signals, and optionally all_scores.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "detected_erp": "UNKNOWN"}

    lines = _read_file_head(str(path), max_lines=50)
    if not lines:
        return {"error": "Could not read file", "detected_erp": "UNKNOWN"}

    delimiter = _detect_delimiter(lines)
    columns = _extract_columns(lines, delimiter)

    scores = []
    for erp_name, sig in ERP_SIGNATURES.items():
        result = _score_erp(columns, lines, delimiter, erp_name, sig)
        scores.append(result)

    scores.sort(key=lambda x: x["score"], reverse=True)

    best = scores[0] if scores else {"erp": "UNKNOWN", "confidence": 0, "signals": []}
    runner_up = scores[1] if len(scores) > 1 else None

    # Require minimum confidence threshold
    if best["confidence"] < 0.15:
        detected = "UNKNOWN"
        confidence = 0.0
    else:
        detected = best["erp"]
        confidence = best["confidence"]

    # Ambiguity check: if top 2 are very close, flag it
    ambiguous = False
    if runner_up and best["score"] > 0 and runner_up["score"] / best["score"] > 0.85:
        ambiguous = True

    result = {
        "file": path.name,
        "detected_erp": detected,
        "confidence": confidence,
        "signals": best.get("signals", []),
        "delimiter": repr(delimiter),
        "columns_found": len(columns),
        "ambiguous": ambiguous,
    }

    if return_all_scores:
        result["all_scores"] = scores

    return result


def detect_erp_batch(
    directory: str = "data/COA_Training",
    pattern: str = "*.csv",
    limit: int = 0,
) -> Dict[str, Any]:
    """Detect ERP systems for all files in a directory.

    Args:
        directory: Path to directory containing COA files.
        pattern: Glob pattern for files to scan.
        limit: Max files to process (0 = unlimited).

    Returns:
        Dict with summary stats and per-ERP counts.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return {"error": f"Directory not found: {directory}"}

    # Collect both CSV and TXT files
    files = sorted(dir_path.glob(pattern))
    if pattern == "*.csv":
        files.extend(sorted(dir_path.glob("*.txt")))

    if limit:
        files = files[:limit]

    results = []
    erp_counts: Dict[str, int] = {}
    confidence_sum: Dict[str, float] = {}
    errors = 0

    for f in files:
        det = detect_erp(str(f))
        erp = det["detected_erp"]
        erp_counts[erp] = erp_counts.get(erp, 0) + 1
        confidence_sum[erp] = confidence_sum.get(erp, 0) + det.get("confidence", 0)

        if det.get("error"):
            errors += 1

        results.append({
            "file": det["file"],
            "detected_erp": erp,
            "confidence": det.get("confidence", 0),
        })

    avg_confidence = {}
    for erp, total in confidence_sum.items():
        avg_confidence[erp] = round(total / max(erp_counts[erp], 1), 3)

    return {
        "total_files": len(files),
        "erp_distribution": erp_counts,
        "avg_confidence": avg_confidence,
        "errors": errors,
        "sample_results": results[:10],
    }
