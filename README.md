# DataBridge Core

[![PyPI version](https://img.shields.io/pypi/v/databridge-core)](https://pypi.org/project/databridge-core/)
[![Python](https://img.shields.io/pypi/pyversions/databridge-core)](https://pypi.org/project/databridge-core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Your finance team just spent 4 hours on VLOOKUP. This takes 5 seconds.**

DataBridge Core is a Python toolkit for data reconciliation, profiling, ingestion, and Excel triage. Compare CSV files, find fuzzy matches, detect schema drift, scan Excel workbooks, and send results to Slack -- from the command line or Python.

```bash
pip install databridge-core
```

## 5-Second Demo

```bash
# Profile a file
databridge profile sales.csv

# Compare two sources -- find orphans, conflicts, match rate
databridge compare source.csv target.csv --keys id

# Fuzzy match names across systems
databridge fuzzy erp_accounts.csv gl_accounts.csv --column name --threshold 80

# Scan Excel files and classify by archetype
pip install 'databridge-core[triage]'
databridge triage ./excel_files/
```

## Python API

```python
from databridge_core import compare_hashes, profile_data, load_csv

# Profile your data
profile = profile_data("chart_of_accounts.csv")
print(f"{profile['rows']} rows, {profile['columns']} columns")
print(f"Potential keys: {profile['potential_key_columns']}")

# Compare two sources
result = compare_hashes("source.csv", "target.csv", key_columns="account_id")
stats = result["statistics"]
print(f"Match rate: {stats['match_rate_percent']}%")
print(f"Conflicts: {stats['conflicts']}, Orphans: {stats['total_orphans']}")
```

### Templates

```python
from databridge_core.templates import TemplateService

svc = TemplateService(templates_dir="templates")
templates = svc.list_templates(domain="accounting")
rec = svc.get_template_recommendations(industry="manufacturing", statement_type="pl")
```

### Slack Integration

```python
from databridge_core.integrations import SlackClient

slack = SlackClient(bot_token="xoxb-...")
slack.send_message("#data-ops", "Reconciliation complete: 99.5% match rate")
slack.post_reconciliation_report("#data-ops", result)
```

### Excel Triage

```python
from databridge_core.triage import scan_and_classify

result = scan_and_classify("./excel_files/", output_dir="./reports/")
print(f"Scanned {result['summary']['total_files']} files")
print(f"Archetypes: {result['summary']['archetype_counts']}")
```

## Commands

| Command | Description |
|---------|-------------|
| `databridge profile <file>` | Profile data: structure, quality, cardinality |
| `databridge compare <a> <b> --keys <col>` | Hash comparison: orphans, conflicts, match rate |
| `databridge fuzzy <a> <b> -c <col>` | Fuzzy match columns across two files |
| `databridge diff <a> <b>` | Text diff between two files |
| `databridge drift <old> <new>` | Detect schema drift between CSVs |
| `databridge transform <file> -c <col> --op upper` | Clean a column (upper/lower/strip/trim/remove_special) |
| `databridge merge <a> <b> --keys <col>` | Merge two CSVs on key columns |
| `databridge find "*.csv"` | Find files matching a pattern |
| `databridge parse <text>` | Parse tabular data from messy text |
| `databridge triage <dir>` | Scan Excel files and classify by archetype |

## Optional Extras

```bash
pip install 'databridge-core[fuzzy]'    # Fuzzy matching (rapidfuzz)
pip install 'databridge-core[pdf]'      # PDF text extraction (pypdf)
pip install 'databridge-core[ocr]'      # OCR image extraction (pytesseract)
pip install 'databridge-core[sql]'      # Database queries (sqlalchemy)
pip install 'databridge-core[triage]'   # Excel triage scanning (openpyxl)
pip install 'databridge-core[all]'      # Everything
pip install 'databridge-core[dev]'      # Development tools (pytest, ruff, build)
```

## Modules

| Module | Description | Extra Required |
|--------|-------------|----------------|
| `reconciler` | Hash comparison, fuzzy matching, diffing, merging | - |
| `profiler` | Data profiling, schema drift detection | - |
| `ingestion` | CSV, JSON, PDF, OCR loading | `[pdf]`, `[ocr]` |
| `templates` | Industry hierarchy templates, skills, knowledge base | - |
| `integrations` | Slack client (BaseClient + SlackClient) | - |
| `triage` | Batch Excel scanning and archetype classification | `[triage]` |

## Built for Finance

DataBridge Core is the open-source foundation of [DataBridge AI](https://github.com/datanexum/databridge-ai) -- a full platform for financial hierarchy management, dbt model generation, and enterprise data reconciliation with 268 MCP tools.

**How it works:** Upload your Chart of Accounts. Get a production-ready financial hierarchy and dbt models. Zero config.

## What's Next?

DataBridge Core provides the SDK foundation. For the full platform experience:

- **MCP Server** (268 tools): `pip install databridge-ai` -- headless AI-native data engine
- **Docker**: `docker run -p 786:786 ghcr.io/datanexum/databridge-mcp:latest`
- **Claude Code Plugin**: `claude plugin install datanexum/databridge-plugin`

See the [full documentation](https://github.com/datanexum/DATABRIDGE_AI) for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for full version history.

## License

MIT
