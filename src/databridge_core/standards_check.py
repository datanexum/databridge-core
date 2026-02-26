"""DataBridge Standards Compliance Checker — Flag GAAP/IFRS/J-GAAP violations.

Analyzes COA files for accounting standards compliance issues including:
  - LIFO usage under IFRS (prohibited by IAS 2)
  - PPE revaluation under US GAAP (not permitted by ASC 360)
  - Capitalized development costs under US GAAP (should be expensed per ASC 730)
  - Operating lease expense under IFRS 16 (should be ROU asset + lease liability)
  - Superseded standards references (IAS 17, old ASC references)
  - Dual-reporting reconciliation errors

Public API:
    check_standards       — Check a single file for standards compliance
    check_standards_batch — Check all files in a directory
"""

__all__ = ["check_standards", "check_standards_batch"]

import csv
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Standards rules
# ---------------------------------------------------------------------------

# Accounts that indicate specific standard treatments
GAAP_VIOLATIONS_UNDER_IFRS = [
    {
        "pattern": r"LIFO",
        "field": "Account_Name",
        "issue": "LIFO inventory method used — LIFO is prohibited under IFRS (IAS 2)",
        "type": "STANDARD_VIOLATION",
        "severity": "CRITICAL",
        "standard": "IFRS",
        "reference": "IAS 2",
    },
]

IFRS_VIOLATIONS_UNDER_GAAP = [
    {
        "pattern": r"(?i)revaluation\s*surplus|revaluation\s*reserve",
        "field": "Account_Name",
        "issue": "Revaluation surplus under US GAAP — PPE revaluation not permitted (ASC 360)",
        "type": "STANDARD_VIOLATION",
        "severity": "CRITICAL",
        "standard": "US_GAAP",
        "reference": "ASC 360",
    },
    {
        "pattern": r"(?i)capitaliz.*development|development\s*costs.*capitaliz",
        "field": "Account_Name",
        "issue": "Capitalized development costs under US GAAP — should be expensed (ASC 730)",
        "type": "STANDARD_VIOLATION",
        "severity": "CRITICAL",
        "standard": "US_GAAP",
        "reference": "ASC 730",
    },
]

SUPERSEDED_STANDARDS = [
    {
        "pattern": r"IAS\s*17",
        "field": "Standard_Reference",
        "issue": "Reference to IAS 17 (superseded by IFRS 16 effective 2019)",
        "type": "SUPERSEDED_STANDARD",
        "severity": "HIGH",
        "reference": "IFRS 16",
    },
    {
        "pattern": r"(?i)operating\s*lease\s*expense",
        "field": "Account_Name",
        "issue": "Operating lease as simple expense — should be ROU asset + lease liability under IFRS 16/ASC 842",
        "type": "SUPERSEDED_STANDARD",
        "severity": "HIGH",
        "reference": "IFRS 16 / ASC 842",
    },
    {
        "pattern": r"FAS\s*\d+|SFAS\s*\d+",
        "field": "Standard_Reference",
        "issue": "Reference to pre-ASC FASB standards (FAS/SFAS superseded by ASC codification in 2009)",
        "type": "SUPERSEDED_STANDARD",
        "severity": "MEDIUM",
        "reference": "ASC",
    },
]

# Classification rules by account type
CLASSIFICATION_RULES = {
    "Asset": {
        "expected_sign": "Debit",
        "balance_positive": True,
    },
    "Liability": {
        "expected_sign": "Credit",
        "balance_positive": False,
    },
    "Equity": {
        "expected_sign": "Credit",
        "balance_positive": False,
    },
    "Revenue": {
        "expected_sign": "Credit",
        "balance_positive": False,
    },
    "Expense": {
        "expected_sign": "Debit",
        "balance_positive": True,
    },
}

# GAAP-specific account patterns that shouldn't appear under other standards
GAAP_SPECIFIC = [
    (r"(?i)APIC|Additional\s*Paid.in\s*Capital", "US_GAAP", "APIC is US GAAP terminology; IFRS uses 'Share Premium'"),
    (r"(?i)Treasury\s*Stock", "US_GAAP", "Treasury Stock is US GAAP; IFRS uses 'Treasury Shares'"),
    (r"(?i)CECL|Current\s*Expected\s*Credit\s*Loss", "US_GAAP", "CECL is ASC 326; IFRS uses ECL model (IFRS 9)"),
]

IFRS_SPECIFIC = [
    (r"(?i)Share\s*Premium", "IFRS", "Share Premium is IFRS terminology; US GAAP uses 'Additional Paid-in Capital'"),
    (r"(?i)Investment\s*Property", "IFRS", "Investment Property (IAS 40) is an IFRS classification"),
    (r"(?i)Revaluation\s*Surplus", "IFRS", "Revaluation surplus only exists under IFRS revaluation model"),
]


def _read_csv(path: str) -> tuple:
    """Read CSV, detect standard from header comments, return (rows, detected_standard)."""
    detected_standard = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                lines = f.readlines()

            # Scan header comments for standard
            # Collect all header lines first, then determine standard with priority
            data_start = 0
            header_text = ""
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith("#") or not stripped:
                    data_start = i + 1
                    header_text += stripped + "\n"
                    continue
                break

            # Dual takes priority (dual files mention both GAAP and IFRS)
            if "Dual" in header_text or "Reconciliation:" in header_text:
                detected_standard = "DUAL"
            elif "J-GAAP" in header_text or "Kigyo Kaikei" in header_text:
                detected_standard = "JGAAP"
            elif "US GAAP" in header_text or ("ASC" in header_text and "IFRS" not in header_text) or "FASB" in header_text:
                detected_standard = "US_GAAP"
            elif "IFRS" in header_text:
                detected_standard = "IFRS"

            reader = csv.DictReader(lines[data_start:])
            rows = list(reader)
            return rows, detected_standard
        except (UnicodeDecodeError, csv.Error):
            continue
    return [], None


def _parse_float(val: str) -> Optional[float]:
    if not val or val.strip() == "":
        return None
    val = val.strip().strip('"').replace(",", "")
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _check_rule(row: Dict, rule: Dict) -> Optional[Dict]:
    """Check a single rule against a row."""
    field = rule["field"]
    value = row.get(field, "")
    if not value:
        return None

    if re.search(rule["pattern"], value):
        return {
            "type": rule["type"],
            "severity": rule["severity"],
            "account": row.get("Account_ID", ""),
            "account_name": row.get("Account_Name", ""),
            "issue": rule["issue"],
            "reference": rule.get("reference", ""),
            "field_value": value,
        }
    return None


def check_standards(
    file_path: str,
    target_standard: Optional[str] = None,
) -> Dict[str, Any]:
    """Check a single COA file for accounting standards compliance.

    Args:
        file_path: Path to CSV file with COA data.
        target_standard: Override standard detection (US_GAAP, IFRS, JGAAP, DUAL).
            If None, auto-detected from file header.

    Returns:
        Dict with findings, detected standard, and compliance score.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "findings": []}

    rows, detected = _read_csv(str(path))
    if not rows:
        return {"error": "Could not parse file", "findings": []}

    standard = target_standard or detected or "UNKNOWN"
    findings = []

    for row in rows:
        # 1. Check standard-specific violations
        if standard == "IFRS":
            for rule in GAAP_VIOLATIONS_UNDER_IFRS:
                finding = _check_rule(row, rule)
                if finding:
                    findings.append(finding)

        elif standard == "US_GAAP":
            for rule in IFRS_VIOLATIONS_UNDER_GAAP:
                finding = _check_rule(row, rule)
                if finding:
                    findings.append(finding)

        # 2. Check superseded standards (applies to all)
        for rule in SUPERSEDED_STANDARDS:
            finding = _check_rule(row, rule)
            if finding:
                findings.append(finding)

        # 3. Check terminology mismatches
        acct_name = row.get("Account_Name", "")
        if standard == "IFRS":
            for pattern, origin, note in GAAP_SPECIFIC:
                if re.search(pattern, acct_name):
                    findings.append({
                        "type": "TERMINOLOGY_MISMATCH",
                        "severity": "LOW",
                        "account": row.get("Account_ID", ""),
                        "account_name": acct_name,
                        "issue": f"US GAAP terminology used in IFRS COA: {note}",
                        "reference": origin,
                    })
        elif standard == "US_GAAP":
            for pattern, origin, note in IFRS_SPECIFIC:
                if re.search(pattern, acct_name):
                    findings.append({
                        "type": "TERMINOLOGY_MISMATCH",
                        "severity": "LOW",
                        "account": row.get("Account_ID", ""),
                        "account_name": acct_name,
                        "issue": f"IFRS terminology used in US GAAP COA: {note}",
                        "reference": origin,
                    })

        # 4. Check standard reference in the data
        std_ref = row.get("Standard_Reference", "")
        if std_ref:
            if standard == "US_GAAP" and re.search(r"IAS|IFRS", std_ref):
                findings.append({
                    "type": "WRONG_STANDARD_REF",
                    "severity": "MEDIUM",
                    "account": row.get("Account_ID", ""),
                    "account_name": acct_name,
                    "issue": f"IFRS reference '{std_ref}' in US GAAP file",
                    "reference": std_ref,
                })
            elif standard == "IFRS" and re.search(r"ASC\s*\d+", std_ref):
                findings.append({
                    "type": "WRONG_STANDARD_REF",
                    "severity": "MEDIUM",
                    "account": row.get("Account_ID", ""),
                    "account_name": acct_name,
                    "issue": f"US GAAP reference '{std_ref}' in IFRS file",
                    "reference": std_ref,
                })

        # 5. Misclassification checks
        acct_type = row.get("Account_Type", "")
        if standard == "US_GAAP":
            # Capitalized development costs should be expensed under GAAP (ASC 730)
            if re.search(r"(?i)capitaliz.*development|development.*capitaliz", acct_name):
                findings.append({
                    "type": "STANDARD_MISCLASSIFICATION",
                    "severity": "CRITICAL",
                    "account": row.get("Account_ID", ""),
                    "account_name": acct_name,
                    "issue": "Development costs capitalized under US GAAP — should be expensed per ASC 730",
                    "reference": "ASCComponentModel730",
                })

        # 6. Opportunity checks (things that could be treated differently)
        if standard == "IFRS":
            # R&D fully expensed when development portion could be capitalized
            if re.search(r"(?i)^Research and Development$|^R&D Expense$", acct_name):
                if acct_type == "Expense":
                    findings.append({
                        "type": "STANDARD_OPPORTUNITY",
                        "severity": "MEDIUM",
                        "account": row.get("Account_ID", ""),
                        "account_name": acct_name,
                        "issue": "All R&D expensed under IFRS — eligible development costs should be capitalized per IAS 38.57",
                        "reference": "IAS 38",
                    })

    # 5. Dual-reporting reconciliation check
    if standard == "DUAL":
        for row in rows:
            gaap_bal = _parse_float(row.get("GAAP_Balance", ""))
            ifrs_adj = _parse_float(row.get("IFRS_Adjustment", ""))
            ifrs_bal = _parse_float(row.get("IFRS_Balance", ""))

            if gaap_bal is not None and ifrs_adj is not None and ifrs_bal is not None:
                expected = round(gaap_bal + ifrs_adj, 2)
                if abs(expected - ifrs_bal) > 0.01:
                    findings.append({
                        "type": "RECONCILIATION_ERROR",
                        "severity": "CRITICAL",
                        "account": row.get("Account_ID", ""),
                        "account_name": row.get("Account_Name", ""),
                        "gaap_balance": gaap_bal,
                        "ifrs_adjustment": ifrs_adj,
                        "expected_ifrs": expected,
                        "actual_ifrs": ifrs_bal,
                        "difference": round(ifrs_bal - expected, 2),
                        "issue": f"GAAP({gaap_bal}) + Adj({ifrs_adj}) = {expected}, but IFRS shows {ifrs_bal}",
                    })

    # Deduplication: suppress secondary findings when primary violation exists.
    # WRONG_STANDARD_REF and TERMINOLOGY_MISMATCH are redundant when a
    # STANDARD_VIOLATION or STANDARD_MISCLASSIFICATION already covers the account.
    violation_accounts = {
        f["account"] for f in findings
        if f["type"] in ("STANDARD_VIOLATION", "STANDARD_MISCLASSIFICATION")
    }
    if violation_accounts:
        findings = [
            f for f in findings
            if not (
                f["type"] in ("WRONG_STANDARD_REF", "TERMINOLOGY_MISMATCH")
                and f.get("account") in violation_accounts
            )
        ]
    # STANDARD_MISCLASSIFICATION is more specific than STANDARD_VIOLATION for the
    # same account — remove the less-specific duplicate.
    misclass_accounts = {
        f["account"] for f in findings
        if f["type"] == "STANDARD_MISCLASSIFICATION"
    }
    if misclass_accounts:
        findings = [
            f for f in findings
            if not (
                f["type"] == "STANDARD_VIOLATION"
                and f.get("account") in misclass_accounts
            )
        ]

    # Compliance score (100 = fully compliant)
    penalty = 0
    for f in findings:
        if f.get("severity") == "CRITICAL":
            penalty += 15
        elif f.get("severity") == "HIGH":
            penalty += 10
        elif f.get("severity") == "MEDIUM":
            penalty += 5
        elif f.get("severity") == "LOW":
            penalty += 2
    compliance_score = max(0, 100 - penalty)

    return {
        "file": path.name,
        "detected_standard": standard,
        "accounts_checked": len(rows),
        "findings_count": len(findings),
        "compliance_score": compliance_score,
        "findings": findings,
    }


def check_standards_batch(
    directory: str = "data/COA_Training/accounting_standards",
    target_standard: Optional[str] = None,
    limit: int = 0,
) -> Dict[str, Any]:
    """Check all COA files in a directory for standards compliance.

    Args:
        directory: Path to directory with COA files.
        target_standard: Override standard for all files (auto-detect if None).
        limit: Max files to process (0 = unlimited).

    Returns:
        Dict with summary across all files.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return {"error": f"Directory not found: {directory}"}

    files = sorted(dir_path.glob("STANDARDS_*.csv"))
    if not files:
        files = sorted(dir_path.glob("*.csv"))
    if limit:
        files = files[:limit]

    total_findings = 0
    by_type = Counter()
    by_standard = Counter()
    non_compliant = []
    total_score = 0

    for f in files:
        result = check_standards(str(f), target_standard=target_standard)
        n = result.get("findings_count", 0)
        total_findings += n
        total_score += result.get("compliance_score", 100)
        std = result.get("detected_standard", "UNKNOWN")
        by_standard[std] += 1

        for finding in result.get("findings", []):
            by_type[finding.get("type", "UNKNOWN")] += 1

        if n > 0:
            non_compliant.append({
                "file": result["file"],
                "standard": std,
                "findings": n,
                "compliance_score": result.get("compliance_score", 0),
            })

    avg_score = round(total_score / max(len(files), 1), 1)

    return {
        "total_files": len(files),
        "non_compliant_files": len(non_compliant),
        "total_findings": total_findings,
        "avg_compliance_score": avg_score,
        "by_type": dict(by_type),
        "by_standard": dict(by_standard),
        "non_compliant_files_detail": non_compliant[:10],
    }
