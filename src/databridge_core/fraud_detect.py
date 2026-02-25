"""DataBridge Fraud Detection — Detect financial fraud patterns in transaction data.

Analyzes journal entries, revenue data, and expense records for 6 fraud types:
  1. Round-tripping (circular transactions via shell entities)
  2. Channel stuffing (quarter-end revenue spikes with returns)
  3. Cookie jar reserves (income smoothing via reserve manipulation)
  4. Capitalization abuse (OpEx reclassified as CapEx)
  5. Related party transactions (off-market pricing with shells)
  6. Journal entry fraud (round amounts, off-hours, self-approval)

Public API:
    detect_fraud       — Scan a single file for fraud indicators
    detect_fraud_batch — Scan all files in a directory
"""

__all__ = ["detect_fraud", "detect_fraud_batch"]

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Offshore / Shell entity indicators
# ---------------------------------------------------------------------------

SHELL_INDICATORS = {
    "LLC", "Ltd", "AG", "GmbH", "FZE", "Pte", "Pty", "KK", "Inc",
    "Consulting", "Advisory", "Services", "Holdings", "Management",
    "Offshore", "Trade", "Procurement", "Bridge", "Horizon", "Silk",
    "Cayman", "Bermuda", "Isle", "Labuan", "Nordic", "Sahara", "Atlantic",
    "Pacific", "Eastern", "Westlake", "Everest",
}

QUARTER_END_MONTHS = {"03", "06", "09", "12"}


def _read_csv(path: str) -> List[Dict[str, str]]:
    """Read CSV file, handling encoding variations."""
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                # Skip comment/metadata lines
                lines = f.readlines()
            data_start = 0
            for i, line in enumerate(lines):
                if line.startswith(("#", "*", "--", "//")):
                    data_start = i + 1
                    continue
                break

            clean_lines = lines[data_start:]
            reader = csv.DictReader(clean_lines)
            return list(reader)
        except (UnicodeDecodeError, csv.Error):
            continue
    return []


def _is_shell_entity(name: str) -> bool:
    """Check if a counterparty name looks like a shell company."""
    if not name:
        return False
    words = set(name.replace(",", " ").replace(".", " ").split())
    hits = words & SHELL_INDICATORS
    return len(hits) >= 2


def _detect_round_tripping(rows: List[Dict]) -> List[Dict]:
    """Detect circular revenue through shell entities."""
    findings = []
    rev_from_shells = []
    exp_to_shells = []

    for row in rows:
        cp = row.get("Counterparty", "")
        amount = _parse_amount(row.get("Amount", "0"))
        source = row.get("Source", "")
        acct = row.get("Account", "")

        if _is_shell_entity(cp):
            if acct.startswith("4") and amount > 0:  # Revenue accounts
                rev_from_shells.append({"counterparty": cp, "amount": amount, "period": row.get("Period", ""), "source": source})
            elif acct.startswith(("5", "6")) and amount > 0:  # Expense accounts
                exp_to_shells.append({"counterparty": cp, "amount": amount, "period": row.get("Period", ""), "source": source})

    # Cross-reference: revenue from shell + expense to different shell in same period
    rev_periods = defaultdict(list)
    for r in rev_from_shells:
        rev_periods[r["period"]].append(r)

    exp_periods = defaultdict(list)
    for e in exp_to_shells:
        exp_periods[e["period"]].append(e)

    for period in rev_periods:
        if period in exp_periods:
            rev_shells = {r["counterparty"] for r in rev_periods[period]}
            exp_shells = {e["counterparty"] for e in exp_periods[period]}
            if rev_shells and exp_shells:
                total_rev = sum(r["amount"] for r in rev_periods[period])
                findings.append({
                    "type": "ROUND_TRIPPING",
                    "severity": "CRITICAL",
                    "period": period,
                    "revenue_shells": list(rev_shells),
                    "expense_shells": list(exp_shells),
                    "revenue_amount": round(total_rev, 2),
                    "evidence": f"Revenue from shell entities AND expenses to shells in {period}",
                })

    return findings


def _detect_channel_stuffing(rows: List[Dict]) -> List[Dict]:
    """Detect quarter-end revenue spikes with subsequent returns."""
    findings = []
    monthly_rev = defaultdict(float)
    monthly_returns = defaultdict(float)

    for row in rows:
        period = row.get("Period", "")
        acct = row.get("Account", "")
        amount = _parse_amount(row.get("Amount", "0"))

        if acct == "4000" and amount > 0:
            monthly_rev[period] += amount
        elif acct in ("4010", "4100") and amount < 0:
            monthly_returns[period] += abs(amount)

    periods = sorted(monthly_rev.keys())
    if len(periods) < 4:
        return findings

    avg_rev = sum(monthly_rev.values()) / max(len(monthly_rev), 1)

    for period in periods:
        month = period.split("-")[-1] if "-" in period else ""
        if month in QUARTER_END_MONTHS:
            rev = monthly_rev[period]
            if avg_rev > 0 and rev / avg_rev > 1.5:
                # Check for returns in next month
                idx = periods.index(period)
                next_period = periods[idx + 1] if idx + 1 < len(periods) else None
                return_amt = monthly_returns.get(next_period, 0) if next_period else 0
                return_pct = return_amt / rev if rev > 0 else 0

                findings.append({
                    "type": "CHANNEL_STUFFING",
                    "severity": "HIGH",
                    "period": period,
                    "revenue": round(rev, 2),
                    "avg_monthly": round(avg_rev, 2),
                    "spike_ratio": round(rev / avg_rev, 2),
                    "next_period_returns": round(return_amt, 2),
                    "return_pct": round(return_pct * 100, 1),
                    "evidence": f"Quarter-end revenue {rev/avg_rev:.1f}x average; {return_pct*100:.0f}% returns next month",
                })

    return findings


def _detect_cookie_jar(rows: List[Dict]) -> List[Dict]:
    """Detect reserve build/release pattern for income smoothing."""
    findings = []
    reserve_entries = []

    for row in rows:
        acct = row.get("Account", "")
        desc = row.get("Description", "").lower()
        amount = _parse_amount(row.get("Amount", "0"))
        source = row.get("Source", "")

        if acct in ("2800", "6800") or "reserve" in desc or "provision" in desc or "contingenc" in desc:
            reserve_entries.append({
                "period": row.get("Period", ""),
                "account": acct,
                "amount": amount,
                "description": row.get("Description", ""),
                "source": source,
                "approved_by": row.get("Approved_By", ""),
            })

    # Check for build/release pattern
    builds = [e for e in reserve_entries if e["amount"] > 0 and e["account"].startswith("6")]
    releases = [e for e in reserve_entries if e["amount"] < 0 and e["account"].startswith("6")]

    if builds and releases:
        build_total = sum(e["amount"] for e in builds)
        release_total = sum(abs(e["amount"]) for e in releases)
        all_manual = all(e["source"].lower() == "manual" for e in reserve_entries)

        findings.append({
            "type": "COOKIE_JAR_RESERVES",
            "severity": "HIGH",
            "build_count": len(builds),
            "release_count": len(releases),
            "total_built": round(build_total, 2),
            "total_released": round(release_total, 2),
            "all_manual_entries": all_manual,
            "evidence": f"Reserve built ({len(builds)} entries, ${build_total:,.0f}) then released ({len(releases)} entries, ${release_total:,.0f})",
        })

    return findings


def _detect_capitalization_abuse(rows: List[Dict]) -> List[Dict]:
    """Detect OpEx improperly reclassified to CapEx."""
    findings = []
    reclass_entries = []

    for row in rows:
        desc = row.get("Description", "").lower()
        acct = row.get("Account", "")
        amount = _parse_amount(row.get("Amount", "0"))
        source = row.get("Source", "")

        # Look for reclassification to asset accounts
        if ("reclassif" in desc or "capitaliz" in desc) and acct in ("1800", "1700", "1600"):
            reclass_entries.append({
                "period": row.get("Period", ""),
                "account": acct,
                "amount": amount,
                "description": row.get("Description", ""),
                "source": source,
                "approved_by": row.get("Approved_By", ""),
            })

        # Look for negative expense entries that reduce OpEx
        if acct.startswith("6") and amount < 0 and "reclassif" in desc:
            reclass_entries.append({
                "period": row.get("Period", ""),
                "account": acct,
                "amount": amount,
                "description": row.get("Description", ""),
                "source": source,
                "approved_by": row.get("Approved_By", ""),
            })

    if reclass_entries:
        total_reclass = sum(abs(e["amount"]) for e in reclass_entries if e["account"].startswith(("1", "17", "18")))
        findings.append({
            "type": "CAPITALIZATION_ABUSE",
            "severity": "CRITICAL",
            "reclass_count": len(reclass_entries),
            "total_capitalized": round(total_reclass, 2),
            "entries": reclass_entries[:5],
            "evidence": f"{len(reclass_entries)} OpEx-to-CapEx reclassifications totaling ${total_reclass:,.0f}",
        })

    return findings


def _detect_related_party(rows: List[Dict]) -> List[Dict]:
    """Detect off-market transactions with shell entities."""
    findings = []
    shell_txns = []

    for row in rows:
        cp = row.get("Counterparty", "")
        if _is_shell_entity(cp):
            amount = _parse_amount(row.get("Amount", "0"))
            source = row.get("Source", "")
            shell_txns.append({
                "period": row.get("Period", ""),
                "counterparty": cp,
                "amount": amount,
                "account": row.get("Account", ""),
                "source": source,
                "approved_by": row.get("Approved_By", ""),
            })

    if shell_txns:
        total = sum(t["amount"] for t in shell_txns)
        unique_shells = list(set(t["counterparty"] for t in shell_txns))
        all_cfo = all(t["approved_by"].upper() in ("CFO", "CEO") for t in shell_txns if t["approved_by"])

        findings.append({
            "type": "RELATED_PARTY",
            "severity": "CRITICAL",
            "transaction_count": len(shell_txns),
            "total_amount": round(total, 2),
            "shell_entities": unique_shells[:5],
            "all_exec_approved": all_cfo,
            "evidence": f"{len(shell_txns)} transactions with {len(unique_shells)} shell entities totaling ${total:,.0f}",
        })

    return findings


def _detect_journal_entry_fraud(rows: List[Dict]) -> List[Dict]:
    """Detect suspicious manual journal entry patterns."""
    findings = []
    suspicious = []

    for row in rows:
        source = row.get("Source", "")
        if source.lower() != "manual":
            continue

        flags = []
        amount = _parse_amount(row.get("Amount", "0"))
        timestamp = row.get("Timestamp", "")
        preparer = row.get("Prepared_By", "")
        approver = row.get("Approved_By", "")
        override = row.get("Override", "")
        desc = row.get("Description", "")

        # Round amount (no cents)
        if amount != 0 and amount == int(amount) and abs(amount) >= 1000:
            flags.append("ROUND_AMOUNT")

        # Off-hours
        if timestamp:
            hour_match = re.search(r"T(\d{2}):", timestamp)
            if hour_match:
                hour = int(hour_match.group(1))
                if hour < 7 or hour >= 21:
                    flags.append("OFF_HOURS")

        # Period-end
        if timestamp:
            day_match = re.search(r"-(\d{2})T", timestamp)
            if day_match and int(day_match.group(1)) >= 28:
                flags.append("PERIOD_END")

        # Self-approved
        if preparer and approver and preparer.strip().lower() == approver.strip().lower():
            flags.append("SELF_APPROVED")

        # Control override
        if override.upper() == "Y":
            flags.append("CONTROL_OVERRIDE")

        # Description mismatch
        if "miscoded" in desc.lower():
            flags.append("DESC_MISMATCH")

        if flags:
            severity = "CRITICAL" if len(flags) >= 3 else "HIGH" if len(flags) >= 2 else "MEDIUM"
            suspicious.append({
                "journal_id": row.get("Journal_ID", ""),
                "flags": flags,
                "amount": amount,
                "period": row.get("Period", ""),
                "severity": severity,
            })

    if suspicious:
        flag_counts = Counter()
        for s in suspicious:
            for f in s["flags"]:
                flag_counts[f] += 1

        findings.append({
            "type": "JOURNAL_ENTRY_FRAUD",
            "severity": "CRITICAL" if any(s["severity"] == "CRITICAL" for s in suspicious) else "HIGH",
            "suspicious_count": len(suspicious),
            "flag_distribution": dict(flag_counts),
            "sample_entries": suspicious[:5],
            "evidence": f"{len(suspicious)} suspicious JEs with flags: {dict(flag_counts)}",
        })

    return findings


def _parse_amount(val: str) -> float:
    """Parse an amount string to float."""
    if not val:
        return 0.0
    val = val.strip().strip('"').replace(",", "")
    # Handle accounting brackets
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def detect_fraud(
    file_path: str,
    checks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Scan a single file for fraud indicators.

    Args:
        file_path: Path to CSV file with transaction data.
        checks: Optional list of specific checks to run. Default: all 6.
            Options: round_tripping, channel_stuffing, cookie_jar,
            capitalization_abuse, related_party, journal_entry

    Returns:
        Dict with findings list, summary stats, and risk score.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "findings": []}

    rows = _read_csv(str(path))
    if not rows:
        return {"error": "Could not parse file", "findings": []}

    all_checks = {
        "round_tripping": _detect_round_tripping,
        "channel_stuffing": _detect_channel_stuffing,
        "cookie_jar": _detect_cookie_jar,
        "capitalization_abuse": _detect_capitalization_abuse,
        "related_party": _detect_related_party,
        "journal_entry": _detect_journal_entry_fraud,
    }

    if checks:
        active = {k: v for k, v in all_checks.items() if k in checks}
    else:
        active = all_checks

    findings = []
    for check_name, check_func in active.items():
        try:
            results = check_func(rows)
            findings.extend(results)
        except Exception as e:
            findings.append({"type": f"ERROR_{check_name.upper()}", "error": str(e)})

    # Risk score (0-100)
    risk = 0
    for f in findings:
        if f.get("severity") == "CRITICAL":
            risk += 30
        elif f.get("severity") == "HIGH":
            risk += 20
        elif f.get("severity") == "MEDIUM":
            risk += 10
    risk = min(risk, 100)

    return {
        "file": path.name,
        "rows_analyzed": len(rows),
        "findings_count": len(findings),
        "risk_score": risk,
        "findings": findings,
        "checks_run": list(active.keys()),
    }


def detect_fraud_batch(
    directory: str = "data/COA_Training/fraud_scenarios",
    limit: int = 0,
) -> Dict[str, Any]:
    """Scan all CSV files in a directory for fraud indicators.

    Args:
        directory: Path to directory with transaction files.
        limit: Max files to process (0 = unlimited).

    Returns:
        Dict with summary across all files.
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return {"error": f"Directory not found: {directory}"}

    files = sorted(dir_path.glob("*.csv"))
    if limit:
        files = files[:limit]

    total_findings = 0
    by_type = Counter()
    by_severity = Counter()
    high_risk_files = []

    for f in files:
        result = detect_fraud(str(f))
        n = result.get("findings_count", 0)
        total_findings += n
        for finding in result.get("findings", []):
            by_type[finding.get("type", "UNKNOWN")] += 1
            by_severity[finding.get("severity", "UNKNOWN")] += 1

        if result.get("risk_score", 0) >= 50:
            high_risk_files.append({
                "file": result["file"],
                "risk_score": result["risk_score"],
                "findings": n,
            })

    return {
        "total_files": len(files),
        "total_findings": total_findings,
        "by_type": dict(by_type),
        "by_severity": dict(by_severity),
        "high_risk_files": high_risk_files[:10],
        "avg_findings_per_file": round(total_findings / max(len(files), 1), 1),
    }
