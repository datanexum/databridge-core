"""DataBridge FX Translation Validator — Detect FX rate errors in multi-currency TBs.

Validates translation rates, checks for inversions, stale rates, wrong rate types,
and CTA miscalculations in multi-currency trial balances.

Public API:
    validate_fx       — Validate a single multi-currency TB file
    validate_fx_batch — Validate all files in a directory
"""

__all__ = ["validate_fx", "validate_fx_batch"]

import csv
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# FX Rate reference data (approximate market rates for validation)
# ---------------------------------------------------------------------------

# Rates to USD as of reference date
REFERENCE_RATES_TO_USD = {
    "USD": 1.0, "EUR": 1.09, "GBP": 1.27, "JPY": 0.0067,
    "CAD": 0.74, "AUD": 0.65, "CHF": 1.12, "CNY": 0.138,
    "INR": 0.012, "BRL": 0.20, "MXN": 0.059, "SGD": 0.74,
}

# Translation rules by account type
EXPECTED_RATE_TYPES = {
    "Asset": "closing",
    "Liability": "closing",
    "Equity": "historical",
    "Revenue": "average",
    "Expense": "average",
    "Other": "average",
    "COGS": "average",
    "Operating Expense": "average",
}

# Tolerance for rate validation
RATE_TOLERANCE = 0.15  # 15% deviation from reference
INVERSION_THRESHOLD = 3.0  # If rate is >3x or <0.33x expected, likely inverted


def _read_csv(path: str) -> List[Dict[str, str]]:
    """Read CSV, skipping comment lines."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                lines = f.readlines()
            data_start = 0
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith("#") or not stripped:
                    data_start = i + 1
                    continue
                break
            reader = csv.DictReader(lines[data_start:])
            return list(reader)
        except (UnicodeDecodeError, csv.Error):
            continue
    return []


def _parse_float(val: str) -> Optional[float]:
    """Parse a float from string, returning None on failure."""
    if not val or val.strip() == "":
        return None
    val = val.strip().strip('"').replace(",", "")
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _get_cross_rate(from_ccy: str, to_ccy: str) -> Optional[float]:
    """Get approximate cross rate between two currencies."""
    from_rate = REFERENCE_RATES_TO_USD.get(from_ccy)
    to_rate = REFERENCE_RATES_TO_USD.get(to_ccy)
    if from_rate is None or to_rate is None or to_rate == 0:
        return None
    return from_rate / to_rate


def validate_fx(
    file_path: str,
) -> Dict[str, Any]:
    """Validate FX translations in a multi-currency trial balance file.

    Args:
        file_path: Path to CSV with multi-currency TB data.

    Returns:
        Dict with findings, TB balance check, and rate analysis.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "findings": []}

    rows = _read_csv(str(path))
    if not rows:
        return {"error": "Could not parse file", "findings": []}

    findings = []
    func_ccy = None
    rpt_ccy = None
    total_translated = 0.0
    accounts_checked = 0

    for row in rows:
        acct_id = row.get("Account_ID", "")
        acct_type = row.get("Account_Type", "")
        acct_name = row.get("Account_Name", "")

        fc = row.get("Functional_Currency", "")
        rc = row.get("Reporting_Currency", "")
        if fc:
            func_ccy = fc
        if rc:
            rpt_ccy = rc

        local_bal = _parse_float(row.get("Local_Balance", ""))
        translated_bal = _parse_float(row.get("Translated_Balance", ""))
        stated_rate = _parse_float(row.get("FX_Rate", ""))
        stated_rate_type = row.get("FX_Rate_Type", "").strip().lower()

        if local_bal is None or translated_bal is None:
            continue

        accounts_checked += 1
        total_translated += translated_bal

        # Skip CTA (it's a balancing plug)
        if "CTA" in acct_name or "Translation" in acct_name:
            continue

        # 1. Check rate type correctness
        expected_type = EXPECTED_RATE_TYPES.get(acct_type, "average")
        if stated_rate_type and stated_rate_type != expected_type:
            findings.append({
                "type": "WRONG_RATE_TYPE",
                "severity": "HIGH",
                "account": acct_id,
                "account_name": acct_name,
                "expected_rate_type": expected_type,
                "actual_rate_type": stated_rate_type,
                "evidence": f"Account type '{acct_type}' should use {expected_type} rate, got {stated_rate_type}",
            })

        # 2. Check for inverted rate
        if stated_rate is not None and local_bal != 0 and func_ccy and rpt_ccy:
            expected_rate = _get_cross_rate(func_ccy, rpt_ccy)
            if expected_rate is not None and expected_rate > 0:
                ratio = stated_rate / expected_rate
                if ratio > INVERSION_THRESHOLD or ratio < (1 / INVERSION_THRESHOLD):
                    findings.append({
                        "type": "INVERTED_RATE",
                        "severity": "CRITICAL",
                        "account": acct_id,
                        "account_name": acct_name,
                        "stated_rate": stated_rate,
                        "expected_rate": round(expected_rate, 6),
                        "ratio": round(ratio, 2),
                        "evidence": f"Rate {stated_rate} is {ratio:.1f}x expected {expected_rate:.6f} — likely inverted",
                    })

        # 3. Verify rate x balance = translated
        if stated_rate is not None and local_bal != 0:
            expected_translated = local_bal * stated_rate
            if translated_bal != 0:
                diff_pct = abs(expected_translated - translated_bal) / abs(translated_bal)
                if diff_pct > 0.01:  # More than 1% off
                    findings.append({
                        "type": "TRANSLATION_MATH_ERROR",
                        "severity": "HIGH",
                        "account": acct_id,
                        "account_name": acct_name,
                        "local_balance": local_bal,
                        "fx_rate": stated_rate,
                        "expected_translated": round(expected_translated, 2),
                        "actual_translated": translated_bal,
                        "difference": round(expected_translated - translated_bal, 2),
                        "evidence": f"Local({local_bal}) × Rate({stated_rate}) = {expected_translated:.2f}, but got {translated_bal}",
                    })

        # 4. Check for stale rate (significant deviation from reference)
        if stated_rate is not None and func_ccy and rpt_ccy:
            ref_rate = _get_cross_rate(func_ccy, rpt_ccy)
            if ref_rate is not None and ref_rate > 0:
                deviation = abs(stated_rate - ref_rate) / ref_rate
                if RATE_TOLERANCE < deviation < INVERSION_THRESHOLD:
                    findings.append({
                        "type": "STALE_RATE",
                        "severity": "MEDIUM",
                        "account": acct_id,
                        "account_name": acct_name,
                        "stated_rate": stated_rate,
                        "reference_rate": round(ref_rate, 6),
                        "deviation_pct": round(deviation * 100, 1),
                        "evidence": f"Rate {stated_rate} deviates {deviation*100:.1f}% from reference {ref_rate:.6f}",
                    })

    # 5. CTA / TB balance check
    if abs(total_translated) > 1.0:
        findings.append({
            "type": "TB_IMBALANCE",
            "severity": "HIGH",
            "total_translated": round(total_translated, 2),
            "evidence": f"Translated TB does not balance: net {total_translated:,.2f} (should be ~0)",
        })

    risk = 0
    for f in findings:
        if f["severity"] == "CRITICAL":
            risk += 30
        elif f["severity"] == "HIGH":
            risk += 15
        elif f["severity"] == "MEDIUM":
            risk += 5
    risk = min(risk, 100)

    return {
        "file": path.name,
        "functional_currency": func_ccy,
        "reporting_currency": rpt_ccy,
        "accounts_checked": accounts_checked,
        "findings_count": len(findings),
        "risk_score": risk,
        "findings": findings,
    }


def validate_fx_batch(
    directory: str = "data/COA_Training/multicurrency",
    limit: int = 0,
) -> Dict[str, Any]:
    """Validate FX translations for all files in a directory.

    Args:
        directory: Path to directory with multi-currency TB files.
        limit: Max files to process (0 = unlimited).

    Returns:
        Dict with summary across all files.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return {"error": f"Directory not found: {directory}"}

    files = sorted(dir_path.glob("MULTICCY_*.csv"))
    if not files:
        files = sorted(dir_path.glob("*.csv"))
    if limit:
        files = files[:limit]

    total_findings = 0
    by_type = Counter()
    by_severity = Counter()
    problem_files = []

    for f in files:
        result = validate_fx(str(f))
        n = result.get("findings_count", 0)
        total_findings += n
        for finding in result.get("findings", []):
            by_type[finding["type"]] += 1
            by_severity[finding.get("severity", "UNKNOWN")] += 1

        if n > 0:
            problem_files.append({
                "file": result["file"],
                "findings": n,
                "risk_score": result.get("risk_score", 0),
                "currencies": f"{result.get('functional_currency', '?')}→{result.get('reporting_currency', '?')}",
            })

    return {
        "total_files": len(files),
        "files_with_issues": len(problem_files),
        "total_findings": total_findings,
        "by_type": dict(by_type),
        "by_severity": dict(by_severity),
        "problem_files": problem_files[:10],
    }
