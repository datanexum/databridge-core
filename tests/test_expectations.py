"""Tests for expectation suite generation and validation."""

import json
import tempfile
from pathlib import Path

import pytest

from databridge_core.profiler.expectations import (
    generate_expectation_suite,
    list_expectation_suites,
)
from databridge_core.profiler.validation import validate, get_validation_results


@pytest.fixture
def sample_csv(tmp_path):
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n")
    return str(csv_file)


@pytest.fixture
def suite_dir(tmp_path):
    return str(tmp_path / "suites")


@pytest.fixture
def validation_dir(tmp_path):
    return str(tmp_path / "validations")


def test_generate_suite(sample_csv, suite_dir):
    result = generate_expectation_suite(sample_csv, name="test_suite", output_dir=suite_dir)
    assert result["suite_name"] == "test_suite"
    assert result["expectations_count"] > 0
    assert Path(result["output_file"]).exists()


def test_generate_suite_auto_name(sample_csv, suite_dir):
    result = generate_expectation_suite(sample_csv, output_dir=suite_dir)
    assert result["suite_name"] == "test_data"


def test_list_suites(sample_csv, suite_dir):
    generate_expectation_suite(sample_csv, name="suite_a", output_dir=suite_dir)
    generate_expectation_suite(sample_csv, name="suite_b", output_dir=suite_dir)
    suites = list_expectation_suites(suite_dir)
    assert len(suites) == 2
    names = {s["name"] for s in suites}
    assert "suite_a" in names
    assert "suite_b" in names


def test_list_suites_empty(tmp_path):
    empty_dir = str(tmp_path / "empty")
    suites = list_expectation_suites(empty_dir)
    assert suites == []


def test_validate_pass(sample_csv, suite_dir, validation_dir):
    generate_expectation_suite(sample_csv, name="test_suite", output_dir=suite_dir)
    result = validate(
        sample_csv,
        suite_name="test_suite",
        suite_dir=suite_dir,
        output_dir=validation_dir,
    )
    assert result["status"] == "passed"
    assert result["failed"] == 0
    assert result["success_percent"] == 100.0


def test_validate_drift(sample_csv, suite_dir, validation_dir, tmp_path):
    """Validate a file that has drifted from the expectations."""
    generate_expectation_suite(sample_csv, name="test_suite", output_dir=suite_dir)

    # Create drifted file — missing 'name' column, different types
    drifted = tmp_path / "drifted.csv"
    drifted.write_text("id,value,extra\n1,100,x\n2,200,y\n")

    result = validate(
        str(drifted),
        suite_name="test_suite",
        suite_dir=suite_dir,
        output_dir=validation_dir,
    )
    assert result["status"] == "failed"
    assert result["failed"] > 0


def test_validate_suite_not_found(sample_csv, suite_dir, validation_dir):
    with pytest.raises(FileNotFoundError):
        validate(
            sample_csv,
            suite_name="nonexistent",
            suite_dir=suite_dir,
            output_dir=validation_dir,
        )


def test_get_validation_results(sample_csv, suite_dir, validation_dir):
    generate_expectation_suite(sample_csv, name="test_suite", output_dir=suite_dir)
    validate(sample_csv, suite_name="test_suite", suite_dir=suite_dir, output_dir=validation_dir)
    validate(sample_csv, suite_name="test_suite", suite_dir=suite_dir, output_dir=validation_dir)

    results = get_validation_results("test_suite", output_dir=validation_dir)
    assert len(results) == 2
    assert all(r["status"] == "passed" for r in results)
