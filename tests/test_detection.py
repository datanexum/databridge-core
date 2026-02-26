"""Tests for the 4 detection modules: erp_detect, fraud_detect, fx_validate, standards_check."""

import csv
from pathlib import Path

import pytest


# ============================================================================
# ERP Detection
# ============================================================================


class TestErpDetect:
    """Tests for the ERP detection module."""

    def test_detect_erp_sap_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "sap_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["BUKRS", "SAKNR", "KTOPL", "TXT20", "TXT50", "KTOKS"])
            writer.writerow(["1000", "0010000001", "CAUS", "Cash", "Cash and equivalents", "BALA"])
            writer.writerow(["1000", "0010000002", "CAUS", "Bank", "Bank accounts", "BALA"])

        result = detect_erp(str(path))
        assert result["detected_erp"] == "SAP"
        assert result["confidence"] > 0
        assert not result.get("error")

    def test_detect_erp_oracle_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "oracle_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["LEDGER_ID", "CODE_COMBINATION_ID", "SEGMENT1", "SEGMENT2", "ACCOUNT_TYPE", "ENABLED_FLAG"])
            writer.writerow(["1", "10001", "01", "1000", "A", "Y"])

        result = detect_erp(str(path))
        assert result["detected_erp"] == "Oracle"
        assert result["confidence"] > 0

    def test_detect_erp_netsuite_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "netsuite_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Internal ID", "External ID", "Account Number", "Account Name", "Is Inactive", "General Rate Type", "Eliminate"])
            writer.writerow(["1", "GL-1000", "GL-1000", "Cash", "No", "Current", "No"])

        result = detect_erp(str(path))
        assert result["detected_erp"] == "NetSuite"
        assert result["confidence"] > 0

    def test_detect_erp_dynamics_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "dynamics_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["MainAccountId", "MainAccountName", "MainAccountCategory", "FinancialStatementGroup", "ChartOfAccountsId"])
            writer.writerow(["100000", "Cash", "Asset", "Balance Sheet", "USMF"])

        result = detect_erp(str(path))
        assert result["detected_erp"] == "Dynamics365"
        assert result["confidence"] > 0

    def test_detect_erp_workday_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "workday_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Ledger_Account_ID", "Ledger_Account_Name", "Account_Type", "Worktag_Organization", "Revenue_Category", "Spend_Category"])
            writer.writerow(["10000", "Cash", "Asset", "Org1", "", ""])

        result = detect_erp(str(path))
        assert result["detected_erp"] == "Workday"
        assert result["confidence"] > 0

    def test_detect_erp_unknown_file(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        # Use pipe-delimited with non-ERP column names to avoid false positives
        path = tmp_path / "random.csv"
        with open(path, "w") as f:
            f.write("color|shape|weight\n")
            f.write("red|circle|heavy\n")
            f.write("blue|square|light\n")

        result = detect_erp(str(path))
        assert result["detected_erp"] == "UNKNOWN"

    def test_detect_erp_nonexistent(self):
        from databridge_core.erp_detect import detect_erp

        result = detect_erp("/nonexistent/file.csv")
        assert result["detected_erp"] == "UNKNOWN"
        assert result.get("error")

    def test_detect_erp_all_scores(self, tmp_path):
        from databridge_core.erp_detect import detect_erp

        path = tmp_path / "sap_coa.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["BUKRS", "SAKNR", "KTOPL", "TXT20"])
            writer.writerow(["1000", "0010000001", "CAUS", "Cash"])

        result = detect_erp(str(path), return_all_scores=True)
        assert "all_scores" in result
        assert len(result["all_scores"]) == 5

    def test_detect_erp_batch(self, tmp_path):
        from databridge_core.erp_detect import detect_erp_batch

        for i in range(3):
            path = tmp_path / f"file_{i}.csv"
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["foo", "bar"])
                writer.writerow(["1", "2"])

        result = detect_erp_batch(str(tmp_path))
        assert result["total_files"] == 3
        assert "erp_distribution" in result

    def test_detect_erp_batch_nonexistent(self):
        from databridge_core.erp_detect import detect_erp_batch

        result = detect_erp_batch("/nonexistent/dir")
        assert result.get("error")

    def test_detect_erp_batch_limit(self, tmp_path):
        from databridge_core.erp_detect import detect_erp_batch

        for i in range(5):
            path = tmp_path / f"file_{i}.csv"
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["col1"])
                writer.writerow(["val"])

        result = detect_erp_batch(str(tmp_path), limit=2)
        assert result["total_files"] == 2


# ============================================================================
# Fraud Detection
# ============================================================================


class TestFraudDetect:
    """Tests for the fraud detection module."""

    def _write_csv(self, path, rows, fieldnames):
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_detect_fraud_clean_file(self, tmp_path):
        from databridge_core.fraud_detect import detect_fraud

        path = tmp_path / "clean.csv"
        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source", "Description",
                       "Counterparty", "Prepared_By", "Approved_By", "Override", "Timestamp"]
        rows = [
            {"Journal_ID": "J1", "Account": "4000", "Amount": "1000", "Period": "2025-01",
             "Source": "System", "Description": "Monthly revenue", "Counterparty": "Customer A",
             "Prepared_By": "Analyst1", "Approved_By": "Manager1", "Override": "N", "Timestamp": "2025-01-15T10:00:00"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = detect_fraud(str(path))
        assert result["rows_analyzed"] == 1
        assert result["risk_score"] == 0
        assert result["findings_count"] == 0

    def test_detect_fraud_journal_entry(self, tmp_path):
        from databridge_core.fraud_detect import detect_fraud

        path = tmp_path / "suspicious.csv"
        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source", "Description",
                       "Counterparty", "Prepared_By", "Approved_By", "Override", "Timestamp"]
        rows = [
            {"Journal_ID": "J1", "Account": "6000", "Amount": "50000", "Period": "2025-03",
             "Source": "Manual", "Description": "Miscoded expense", "Counterparty": "",
             "Prepared_By": "CFO", "Approved_By": "CFO", "Override": "Y",
             "Timestamp": "2025-03-31T23:30:00"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = detect_fraud(str(path), checks=["journal_entry"])
        assert result["findings_count"] > 0
        journal_finding = [f for f in result["findings"] if f["type"] == "JOURNAL_ENTRY_FRAUD"]
        assert len(journal_finding) == 1

    def test_detect_fraud_related_party(self, tmp_path):
        from databridge_core.fraud_detect import detect_fraud

        path = tmp_path / "related.csv"
        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source", "Description",
                       "Counterparty", "Prepared_By", "Approved_By", "Override", "Timestamp"]
        rows = [
            {"Journal_ID": "J1", "Account": "5000", "Amount": "100000", "Period": "2025-01",
             "Source": "Manual", "Description": "Consulting fees", "Counterparty": "Silk Horizon Consulting LLC",
             "Prepared_By": "Analyst", "Approved_By": "CFO", "Override": "N", "Timestamp": ""},
        ]
        self._write_csv(path, rows, fieldnames)

        result = detect_fraud(str(path), checks=["related_party"])
        related = [f for f in result["findings"] if f["type"] == "RELATED_PARTY"]
        assert len(related) == 1

    def test_detect_fraud_nonexistent(self):
        from databridge_core.fraud_detect import detect_fraud

        result = detect_fraud("/nonexistent/file.csv")
        assert result.get("error")
        assert result["findings"] == []

    def test_detect_fraud_selective_checks(self, tmp_path):
        from databridge_core.fraud_detect import detect_fraud

        path = tmp_path / "simple.csv"
        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source"]
        rows = [{"Journal_ID": "J1", "Account": "4000", "Amount": "1000", "Period": "2025-01", "Source": "System"}]
        self._write_csv(path, rows, fieldnames)

        result = detect_fraud(str(path), checks=["round_tripping", "channel_stuffing"])
        assert set(result["checks_run"]) == {"round_tripping", "channel_stuffing"}

    def test_detect_fraud_batch(self, tmp_path):
        from databridge_core.fraud_detect import detect_fraud_batch

        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source"]
        for i in range(3):
            path = tmp_path / f"txn_{i}.csv"
            rows = [{"Journal_ID": f"J{i}", "Account": "4000", "Amount": "1000",
                     "Period": "2025-01", "Source": "System"}]
            self._write_csv(path, rows, fieldnames)

        result = detect_fraud_batch(str(tmp_path))
        assert result["total_files"] == 3
        assert "by_type" in result

    def test_detect_fraud_batch_nonexistent(self):
        from databridge_core.fraud_detect import detect_fraud_batch

        result = detect_fraud_batch("/nonexistent/dir")
        assert result.get("error")

    def test_detect_fraud_risk_capping(self, tmp_path):
        """Risk score should be capped at 100."""
        from databridge_core.fraud_detect import detect_fraud

        path = tmp_path / "risky.csv"
        fieldnames = ["Journal_ID", "Account", "Amount", "Period", "Source", "Description",
                       "Counterparty", "Prepared_By", "Approved_By", "Override", "Timestamp"]
        # Create multiple suspicious entries
        rows = []
        for i in range(10):
            rows.append({
                "Journal_ID": f"J{i}", "Account": "6000", "Amount": "50000",
                "Period": "2025-03", "Source": "Manual", "Description": "Miscoded",
                "Counterparty": "Silk Horizon Consulting LLC",
                "Prepared_By": "CFO", "Approved_By": "CFO", "Override": "Y",
                "Timestamp": "2025-03-31T23:30:00",
            })
        self._write_csv(path, rows, fieldnames)

        result = detect_fraud(str(path))
        assert result["risk_score"] <= 100


# ============================================================================
# FX Validation
# ============================================================================


class TestFxValidate:
    """Tests for the FX validation module."""

    def _write_csv(self, path, rows, fieldnames):
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_validate_fx_clean(self, tmp_path):
        from databridge_core.fx_validate import validate_fx

        path = tmp_path / "clean_fx.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Functional_Currency",
                       "Reporting_Currency", "Local_Balance", "FX_Rate", "FX_Rate_Type", "Translated_Balance"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset",
             "Functional_Currency": "EUR", "Reporting_Currency": "USD",
             "Local_Balance": "1000", "FX_Rate": "1.09", "FX_Rate_Type": "closing",
             "Translated_Balance": "1090"},
            {"Account_ID": "2000", "Account_Name": "Payables", "Account_Type": "Liability",
             "Functional_Currency": "EUR", "Reporting_Currency": "USD",
             "Local_Balance": "-1000", "FX_Rate": "1.09", "FX_Rate_Type": "closing",
             "Translated_Balance": "-1090"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = validate_fx(str(path))
        assert result["accounts_checked"] == 2
        assert result["functional_currency"] == "EUR"
        assert result["reporting_currency"] == "USD"
        # Clean file should have few or no findings (maybe TB imbalance check)
        assert not result.get("error")

    def test_validate_fx_wrong_rate_type(self, tmp_path):
        from databridge_core.fx_validate import validate_fx

        path = tmp_path / "wrong_rate.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Functional_Currency",
                       "Reporting_Currency", "Local_Balance", "FX_Rate", "FX_Rate_Type", "Translated_Balance"]
        # Use the exact closing rate (1.0920) so it clearly matches closing period,
        # not average (1.0885). Revenue accounts should use average rate.
        rows = [
            {"Account_ID": "4000", "Account_Name": "Revenue", "Account_Type": "Revenue",
             "Functional_Currency": "EUR", "Reporting_Currency": "USD",
             "Local_Balance": "10000", "FX_Rate": "1.092", "FX_Rate_Type": "closing",
             "Translated_Balance": "10920"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = validate_fx(str(path))
        wrong_type = [f for f in result["findings"] if f["type"] == "WRONG_RATE_TYPE"]
        assert len(wrong_type) == 1
        assert wrong_type[0]["expected_rate_type"] == "average"
        assert wrong_type[0]["actual_rate_type"] == "closing"

    def test_validate_fx_inverted_rate(self, tmp_path):
        from databridge_core.fx_validate import validate_fx

        path = tmp_path / "inverted.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Functional_Currency",
                       "Reporting_Currency", "Local_Balance", "FX_Rate", "FX_Rate_Type", "Translated_Balance"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset",
             "Functional_Currency": "JPY", "Reporting_Currency": "USD",
             "Local_Balance": "100000", "FX_Rate": "149.5", "FX_Rate_Type": "closing",
             "Translated_Balance": "14950000"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = validate_fx(str(path))
        inverted = [f for f in result["findings"] if f["type"] == "INVERTED_RATE"]
        assert len(inverted) >= 1

    def test_validate_fx_math_error(self, tmp_path):
        from databridge_core.fx_validate import validate_fx

        path = tmp_path / "math_error.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Functional_Currency",
                       "Reporting_Currency", "Local_Balance", "FX_Rate", "FX_Rate_Type", "Translated_Balance"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset",
             "Functional_Currency": "EUR", "Reporting_Currency": "USD",
             "Local_Balance": "1000", "FX_Rate": "1.09", "FX_Rate_Type": "closing",
             "Translated_Balance": "500"},  # Wrong: should be 1090
        ]
        self._write_csv(path, rows, fieldnames)

        result = validate_fx(str(path))
        math_errors = [f for f in result["findings"] if f["type"] == "TRANSLATION_MATH_ERROR"]
        assert len(math_errors) == 1

    def test_validate_fx_nonexistent(self):
        from databridge_core.fx_validate import validate_fx

        result = validate_fx("/nonexistent/file.csv")
        assert result.get("error")

    def test_validate_fx_batch(self, tmp_path):
        from databridge_core.fx_validate import validate_fx_batch

        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Functional_Currency",
                       "Reporting_Currency", "Local_Balance", "FX_Rate", "FX_Rate_Type", "Translated_Balance"]
        for i in range(2):
            path = tmp_path / f"fx_{i}.csv"
            rows = [{"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset",
                     "Functional_Currency": "EUR", "Reporting_Currency": "USD",
                     "Local_Balance": "1000", "FX_Rate": "1.09", "FX_Rate_Type": "closing",
                     "Translated_Balance": "1090"}]
            self._write_csv(path, rows, fieldnames)

        result = validate_fx_batch(str(tmp_path))
        assert result["total_files"] == 2
        assert "by_type" in result

    def test_validate_fx_batch_nonexistent(self):
        from databridge_core.fx_validate import validate_fx_batch

        result = validate_fx_batch("/nonexistent/dir")
        assert result.get("error")

    def test_validate_fx_reference_rates(self):
        from databridge_core.fx_validate import REFERENCE_RATES_TO_USD

        assert REFERENCE_RATES_TO_USD["USD"] == 1.0
        assert "EUR" in REFERENCE_RATES_TO_USD
        assert "JPY" in REFERENCE_RATES_TO_USD
        assert len(REFERENCE_RATES_TO_USD) == 12

    def test_validate_fx_cross_rate(self):
        from databridge_core.fx_validate import _get_cross_rate

        rate = _get_cross_rate("EUR", "USD")
        assert rate is not None
        assert rate > 0

        rate_unknown = _get_cross_rate("EUR", "XYZ")
        assert rate_unknown is None


# ============================================================================
# Standards Compliance
# ============================================================================


class TestStandardsCheck:
    """Tests for the standards compliance checker."""

    def _write_csv(self, path, rows, fieldnames, header_comment=None):
        with open(path, "w", newline="") as f:
            if header_comment:
                f.write(header_comment + "\n")
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_check_standards_clean_gaap(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "gaap_clean.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Cash and Equivalents", "Account_Type": "Asset", "Standard_Reference": "ASC 305"},
            {"Account_ID": "4000", "Account_Name": "Revenue", "Account_Type": "Revenue", "Standard_Reference": "ASC 606"},
        ]
        self._write_csv(path, rows, fieldnames, header_comment="# US GAAP Chart of Accounts")

        result = check_standards(str(path))
        assert result["detected_standard"] == "US_GAAP"
        assert result["accounts_checked"] == 2
        assert result["compliance_score"] <= 100

    def test_check_standards_lifo_under_ifrs(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "ifrs_lifo.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [
            {"Account_ID": "1500", "Account_Name": "Inventory - LIFO Reserve", "Account_Type": "Asset", "Standard_Reference": ""},
        ]
        self._write_csv(path, rows, fieldnames, header_comment="# IFRS Chart of Accounts")

        result = check_standards(str(path))
        assert result["detected_standard"] == "IFRS"
        violations = [f for f in result["findings"] if f["type"] == "STANDARD_VIOLATION"]
        assert len(violations) == 1
        assert "LIFO" in violations[0]["issue"]

    def test_check_standards_revaluation_under_gaap(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "gaap_reval.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [
            {"Account_ID": "3200", "Account_Name": "Revaluation Surplus", "Account_Type": "Equity", "Standard_Reference": ""},
        ]
        self._write_csv(path, rows, fieldnames, header_comment="# US GAAP Chart of Accounts")

        result = check_standards(str(path))
        violations = [f for f in result["findings"] if f["type"] == "STANDARD_VIOLATION"]
        assert len(violations) >= 1
        assert any("revaluation" in v["issue"].lower() for v in violations)

    def test_check_standards_superseded(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "old_standards.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Operating Lease Expense", "Account_Type": "Expense", "Standard_Reference": "IAS 17"},
        ]
        self._write_csv(path, rows, fieldnames)

        result = check_standards(str(path), target_standard="IFRS")
        superseded = [f for f in result["findings"] if f["type"] == "SUPERSEDED_STANDARD"]
        assert len(superseded) >= 1

    def test_check_standards_terminology_mismatch(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "ifrs_with_gaap_terms.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [
            {"Account_ID": "3100", "Account_Name": "Additional Paid-in Capital (APIC)", "Account_Type": "Equity", "Standard_Reference": ""},
        ]
        self._write_csv(path, rows, fieldnames)

        result = check_standards(str(path), target_standard="IFRS")
        term = [f for f in result["findings"] if f["type"] == "TERMINOLOGY_MISMATCH"]
        assert len(term) >= 1

    def test_check_standards_dual_reconciliation_error(self, tmp_path):
        from databridge_core.standards_check import check_standards

        path = tmp_path / "dual_recon.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference",
                       "GAAP_Balance", "IFRS_Adjustment", "IFRS_Balance"]
        rows = [
            {"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset",
             "Standard_Reference": "", "GAAP_Balance": "1000", "IFRS_Adjustment": "50",
             "IFRS_Balance": "1100"},  # Error: 1000+50=1050, not 1100
        ]
        self._write_csv(path, rows, fieldnames, header_comment="# Dual Reporting File")

        result = check_standards(str(path))
        assert result["detected_standard"] == "DUAL"
        recon_errors = [f for f in result["findings"] if f["type"] == "RECONCILIATION_ERROR"]
        assert len(recon_errors) == 1
        assert recon_errors[0]["difference"] == 50.0

    def test_check_standards_nonexistent(self):
        from databridge_core.standards_check import check_standards

        result = check_standards("/nonexistent/file.csv")
        assert result.get("error")

    def test_check_standards_batch(self, tmp_path):
        from databridge_core.standards_check import check_standards_batch

        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        for i in range(3):
            path = tmp_path / f"coa_{i}.csv"
            rows = [{"Account_ID": f"{i}000", "Account_Name": "Cash", "Account_Type": "Asset", "Standard_Reference": ""}]
            self._write_csv(path, rows, fieldnames)

        result = check_standards_batch(str(tmp_path))
        assert result["total_files"] == 3
        assert "avg_compliance_score" in result

    def test_check_standards_batch_nonexistent(self):
        from databridge_core.standards_check import check_standards_batch

        result = check_standards_batch("/nonexistent/dir")
        assert result.get("error")

    def test_check_standards_compliance_score_range(self, tmp_path):
        """Compliance score should be 0-100."""
        from databridge_core.standards_check import check_standards

        path = tmp_path / "many_issues.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = []
        for i in range(20):
            rows.append({"Account_ID": str(i), "Account_Name": f"LIFO Reserve {i}",
                         "Account_Type": "Asset", "Standard_Reference": "IAS 17"})
        self._write_csv(path, rows, fieldnames)

        result = check_standards(str(path), target_standard="IFRS")
        assert 0 <= result["compliance_score"] <= 100

    def test_check_standards_override_detection(self, tmp_path):
        """Target standard should override auto-detection."""
        from databridge_core.standards_check import check_standards

        path = tmp_path / "override.csv"
        fieldnames = ["Account_ID", "Account_Name", "Account_Type", "Standard_Reference"]
        rows = [{"Account_ID": "1000", "Account_Name": "Cash", "Account_Type": "Asset", "Standard_Reference": ""}]
        self._write_csv(path, rows, fieldnames, header_comment="# US GAAP COA")

        result = check_standards(str(path), target_standard="IFRS")
        assert result["detected_standard"] == "IFRS"


# ============================================================================
# Import tests
# ============================================================================


class TestImports:
    """Test that all detection functions are accessible from the top-level package."""

    def test_import_erp_detect(self):
        from databridge_core import detect_erp, detect_erp_batch
        assert callable(detect_erp)
        assert callable(detect_erp_batch)

    def test_import_fraud_detect(self):
        from databridge_core import detect_fraud, detect_fraud_batch
        assert callable(detect_fraud)
        assert callable(detect_fraud_batch)

    def test_import_fx_validate(self):
        from databridge_core import validate_fx, validate_fx_batch
        assert callable(validate_fx)
        assert callable(validate_fx_batch)

    def test_import_standards_check(self):
        from databridge_core import check_standards, check_standards_batch
        assert callable(check_standards)
        assert callable(check_standards_batch)

    def test_version_updated(self):
        from databridge_core import __version__
        assert __version__ == "1.4.0"

    def test_all_exports(self):
        import databridge_core
        assert "detect_erp" in databridge_core.__all__
        assert "detect_erp_batch" in databridge_core.__all__
        assert "detect_fraud" in databridge_core.__all__
        assert "detect_fraud_batch" in databridge_core.__all__
        assert "validate_fx" in databridge_core.__all__
        assert "validate_fx_batch" in databridge_core.__all__
        assert "check_standards" in databridge_core.__all__
        assert "check_standards_batch" in databridge_core.__all__
