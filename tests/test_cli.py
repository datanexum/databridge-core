"""Tests for the CLI module."""

from click.testing import CliRunner

from databridge_core.cli import cli


class TestCli:
    def setup_method(self):
        self.runner = CliRunner()

    def test_version(self):
        result = self.runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "1.2.0" in result.output

    def test_help(self):
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "profile" in result.output
        assert "compare" in result.output
        assert "fuzzy" in result.output
        assert "diff" in result.output
        assert "drift" in result.output
        assert "transform" in result.output
        assert "merge" in result.output
        assert "find" in result.output
        assert "parse" in result.output
        assert "triage" in result.output

    def test_profile(self, customers_a):
        result = self.runner.invoke(cli, ["profile", customers_a])
        assert result.exit_code == 0
        assert "Profile Summary" in result.output

    def test_compare(self, customers_a, customers_b):
        result = self.runner.invoke(cli, [
            "compare", customers_a, customers_b, "--keys", "id"
        ])
        assert result.exit_code == 0
        assert "Comparison" in result.output
        assert "Match rate" in result.output

    def test_drift_no_drift(self, customers_a, customers_b):
        result = self.runner.invoke(cli, ["drift", customers_a, customers_b])
        assert result.exit_code == 0
        assert "No schema drift" in result.output

    def test_transform(self, customers_a):
        result = self.runner.invoke(cli, [
            "transform", customers_a, "--column", "name", "--op", "upper"
        ])
        assert result.exit_code == 0
        assert "Transform" in result.output

    def test_merge(self, customers_a, customers_b):
        result = self.runner.invoke(cli, [
            "merge", customers_a, customers_b, "--keys", "id"
        ])
        assert result.exit_code == 0
        assert "Merged" in result.output

    def test_find(self):
        result = self.runner.invoke(cli, ["find", "*.py"])
        assert result.exit_code == 0
        assert "Found" in result.output

    def test_parse(self):
        result = self.runner.invoke(cli, ["parse", "Name\tAge\nAlice\t30"])
        assert result.exit_code == 0
        assert "Parsed" in result.output
