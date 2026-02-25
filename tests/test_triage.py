"""Tests for the triage module."""

import json
from pathlib import Path

import pytest

# Skip all tests if openpyxl not installed
openpyxl = pytest.importorskip("openpyxl")


class TestTriageTypes:
    """Test triage Pydantic models."""

    def test_archetype_enum(self):
        from databridge_core.triage import Archetype

        assert Archetype.FINANCIAL_REPORT == "Financial Report"
        assert Archetype.DATA_EXTRACT == "Data Extract"
        assert Archetype.MODEL_TEMPLATE == "Model/Template"
        assert Archetype.UNKNOWN == "Unknown"

    def test_scan_status_enum(self):
        from databridge_core.triage import ScanStatus

        assert ScanStatus.OK == "ok"
        assert ScanStatus.ERROR == "error"
        assert ScanStatus.SKIPPED == "skipped"

    def test_file_triage_result_defaults(self):
        from databridge_core.triage import FileTriageResult

        result = FileTriageResult(file_path="test.xlsx", file_name="test.xlsx")
        assert result.formula_count == 0
        assert result.sheet_count == 0
        assert result.archetype.value == "Unknown"

    def test_sheet_metadata(self):
        from databridge_core.triage import SheetMetadata

        sheet = SheetMetadata(name="Sheet1", row_count=100, col_count=10, formula_count=50)
        assert sheet.name == "Sheet1"
        assert not sheet.is_empty

    def test_batch_summary(self):
        from databridge_core.triage import BatchTriageSummary

        summary = BatchTriageSummary(
            total_files=10,
            scanned=8,
            errors=1,
            skipped=1,
            duration_seconds=5.0,
            files_per_second=2.0,
        )
        assert summary.total_files == 10


class TestArchetypeClassifier:
    """Test the heuristic archetype classifier."""

    def test_classify_empty_file(self):
        from databridge_core.triage import ArchetypeClassifier, FileTriageResult, ScanStatus

        classifier = ArchetypeClassifier()
        result = FileTriageResult(
            file_path="test.xlsx",
            file_name="test.xlsx",
            scan_status=ScanStatus.ERROR,
        )
        classified = classifier.classify(result)
        assert classified.archetype.value == "Unknown"

    def test_classify_financial_report(self):
        from databridge_core.triage import ArchetypeClassifier, FileTriageResult

        classifier = ArchetypeClassifier()
        result = FileTriageResult(
            file_path="budget_2025.xlsx",
            file_name="budget_2025.xlsx",
            file_extension=".xlsx",
            sheet_count=3,
            sheet_names=["P&L", "Balance Sheet", "Summary"],
            total_row_count=200,
            formula_count=50,
            named_range_count=10,
            has_sumif_pattern=True,
            dominant_formula_functions=["SUM", "SUMIF", "VLOOKUP"],
        )

        classified = classifier.classify(result)
        assert classified.archetype.value == "Financial Report"
        assert classified.archetype_confidence > 0.3

    def test_classify_data_extract(self):
        from databridge_core.triage import ArchetypeClassifier, FileTriageResult

        classifier = ArchetypeClassifier()
        result = FileTriageResult(
            file_path="sales_export.xlsx",
            file_name="sales_export.xlsx",
            file_extension=".xlsx",
            sheet_count=1,
            total_row_count=5000,
            formula_count=0,
        )

        classified = classifier.classify(result)
        assert classified.archetype.value == "Data Extract"

    def test_classify_academic(self):
        from databridge_core.triage import ArchetypeClassifier, FileTriageResult

        classifier = ArchetypeClassifier()
        result = FileTriageResult(
            file_path="homework_chapter3.xlsx",
            file_name="homework_chapter3.xlsx",
            file_extension=".xlsx",
            sheet_count=1,
            sheet_names=["#1", "#2", "#3", "#4"],
            total_row_count=30,
            formula_count=2,
        )

        classified = classifier.classify(result)
        assert classified.archetype.value == "Academic/Exercise"

    def test_classify_batch(self):
        from databridge_core.triage import ArchetypeClassifier, FileTriageResult

        classifier = ArchetypeClassifier()
        results = [
            FileTriageResult(
                file_path=f"file{i}.xlsx",
                file_name=f"file{i}.xlsx",
                file_extension=".xlsx",
                total_row_count=100,
                formula_count=5,
            )
            for i in range(5)
        ]

        classified = classifier.classify_batch(results)
        assert len(classified) == 5


class TestReportGenerator:
    """Test JSONL and JSON summary report generation."""

    def test_generate_report(self, tmp_path):
        from databridge_core.triage import (
            Archetype,
            FileTriageResult,
            ReportGenerator,
        )

        generator = ReportGenerator()
        results = [
            FileTriageResult(
                file_path="a.xlsx",
                file_name="a.xlsx",
                file_extension=".xlsx",
                file_size_bytes=1024,
                sheet_count=2,
                formula_count=10,
                total_row_count=100,
                archetype=Archetype.FINANCIAL_REPORT,
            ),
            FileTriageResult(
                file_path="b.xlsx",
                file_name="b.xlsx",
                file_extension=".xlsx",
                file_size_bytes=2048,
                sheet_count=1,
                formula_count=0,
                total_row_count=500,
                archetype=Archetype.DATA_EXTRACT,
            ),
        ]

        report = generator.generate(results, "test_dir", 1.5, output_dir=str(tmp_path))

        assert report.summary.total_files == 2
        assert report.summary.scanned == 2
        assert report.summary.duration_seconds == 1.5

        # Check JSONL file
        jsonl_path = tmp_path / "triage_report.jsonl"
        assert jsonl_path.exists()
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) == 2

        # Check summary JSON
        summary_path = tmp_path / "triage_summary.json"
        assert summary_path.exists()
        summary_data = json.loads(summary_path.read_text())
        assert summary_data["summary"]["total_files"] == 2


class TestBatchExcelScanner:
    """Test the Excel file scanner."""

    def _create_xlsx(self, path: Path, sheets: dict = None):
        """Helper to create a minimal .xlsx file."""
        wb = openpyxl.Workbook()
        if sheets:
            for name, data in sheets.items():
                if name == "Sheet":
                    ws = wb.active
                    ws.title = name
                else:
                    ws = wb.create_sheet(name)
                for row in data:
                    ws.append(row)
        wb.save(str(path))

    def test_scan_empty_directory(self, tmp_path):
        from databridge_core.triage import BatchExcelScanner

        scanner = BatchExcelScanner()
        results = scanner.scan_directory(str(tmp_path))
        assert results == []

    def test_scan_single_file(self, tmp_path):
        from databridge_core.triage import BatchExcelScanner

        xlsx_path = tmp_path / "test.xlsx"
        self._create_xlsx(xlsx_path, {
            "Sheet": [
                ["Name", "Amount", "Date"],
                ["Alice", 1000, "2025-01-01"],
                ["Bob", 2000, "2025-01-02"],
            ],
        })

        scanner = BatchExcelScanner()
        results = scanner.scan_directory(str(tmp_path))

        assert len(results) == 1
        assert results[0].file_name == "test.xlsx"
        assert results[0].sheet_count == 1
        assert results[0].scan_status.value == "ok"

    def test_scan_file_with_formulas(self, tmp_path):
        from databridge_core.triage import BatchExcelScanner

        xlsx_path = tmp_path / "formulas.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws["A1"] = "Revenue"
        ws["B1"] = 1000
        ws["A2"] = "Cost"
        ws["B2"] = 500
        ws["A3"] = "Profit"
        ws["B3"] = "=B1-B2"
        ws["A4"] = "Total"
        ws["B4"] = "=SUM(B1:B3)"
        wb.save(str(xlsx_path))

        scanner = BatchExcelScanner()
        result = scanner.scan_file(str(xlsx_path))

        assert result.formula_count >= 2
        assert result.scan_status.value == "ok"

    def test_scan_skips_xls_files(self, tmp_path):
        from databridge_core.triage import BatchExcelScanner

        # Create a fake .xls file
        xls_path = tmp_path / "old_format.xls"
        xls_path.write_bytes(b"fake xls content")

        scanner = BatchExcelScanner()
        result = scanner.scan_file(str(xls_path))
        assert result.scan_status.value == "skipped"

    def test_scan_nonexistent_directory(self):
        from databridge_core.triage import BatchExcelScanner

        scanner = BatchExcelScanner()
        with pytest.raises(FileNotFoundError):
            scanner.scan_directory("/nonexistent/path")

    def test_scan_with_progress_callback(self, tmp_path):
        from databridge_core.triage import BatchExcelScanner

        xlsx_path = tmp_path / "test.xlsx"
        self._create_xlsx(xlsx_path, {"Sheet": [["A", "B"], [1, 2]]})

        progress_calls = []

        def callback(completed, total, filename):
            progress_calls.append((completed, total, filename))

        scanner = BatchExcelScanner()
        scanner.scan_directory(str(tmp_path), progress_callback=callback)

        assert len(progress_calls) == 1
        assert progress_calls[0][0] == 1  # completed
        assert progress_calls[0][1] == 1  # total


class TestScanAndClassify:
    """Test the high-level scan_and_classify function."""

    def test_full_pipeline(self, tmp_path):
        from databridge_core.triage import scan_and_classify

        # Create test Excel files
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "P&L"
        ws["A1"] = "Revenue"
        ws["B1"] = "=SUM(B2:B10)"
        for i in range(2, 11):
            ws[f"A{i}"] = f"Product {i}"
            ws[f"B{i}"] = i * 1000
        wb.save(str(tmp_path / "financial_report.xlsx"))

        wb2 = openpyxl.Workbook()
        ws2 = wb2.active
        for i in range(1, 100):
            ws2.append([f"row{i}", i, f"data_{i}"])
        wb2.save(str(tmp_path / "data_export.xlsx"))

        output_dir = str(tmp_path / "reports")
        result = scan_and_classify(
            directory=str(tmp_path),
            output_dir=output_dir,
            max_workers=2,
        )

        assert result["summary"]["total_files"] == 2
        assert result["summary"]["scanned"] == 2
        assert len(result["sample_results"]) == 2
        assert Path(result["report_jsonl"]).exists()
        assert Path(result["summary_json"]).exists()
