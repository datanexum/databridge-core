# DataBridge Core

**Your finance team just spent 4 hours on VLOOKUP. This takes 5 seconds.**

DataBridge Core is a Python toolkit for data reconciliation, profiling, and ingestion. Compare CSV files, find fuzzy matches, detect schema drift, and clean messy data -- from the command line or Python.

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

## Optional Extras

```bash
pip install 'databridge-core[fuzzy]'   # Fuzzy matching (rapidfuzz)
pip install 'databridge-core[pdf]'     # PDF text extraction (pypdf)
pip install 'databridge-core[ocr]'     # OCR image extraction (pytesseract)
pip install 'databridge-core[sql]'     # Database queries (sqlalchemy)
pip install 'databridge-core[all]'     # Everything
pip install 'databridge-core[dev]'     # Development tools (pytest, ruff, build)
```

## Built for Finance

DataBridge Core is the open-source foundation of [DataBridge AI](https://github.com/datanexum/databridge-ai) -- a full platform for financial hierarchy management, dbt model generation, and enterprise data reconciliation.

**How it works:** Upload your Chart of Accounts. Get a production-ready financial hierarchy and dbt models. Zero config.

## License

MIT
