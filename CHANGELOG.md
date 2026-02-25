# Changelog

All notable changes to `databridge-core` will be documented in this file.

## [1.3.0] - 2026-02-25

### Added
- **ERP Detection module** (`databridge_core.erp_detect`): Identify source ERP system from COA file fingerprints
  - `detect_erp()` -- fingerprint-based scoring across 5 ERP systems (SAP, Oracle, NetSuite, Dynamics 365, Workday)
  - `detect_erp_batch()` -- batch detection across a directory of COA files
  - Scoring: column name matching (40pts), metadata patterns (25pts), delimiter (15pts), account format (10pts), header prefix (5pts), fuzzy bonus (5pts)
  - New CLI command: `databridge erp-detect <file_or_dir>`
- **Fraud Detection module** (`databridge_core.fraud_detect`): Detect 6 financial fraud patterns in transaction data
  - `detect_fraud()` -- scan a single file for fraud indicators with 0-100 risk scoring
  - `detect_fraud_batch()` -- batch scanning across a directory
  - 6 detection engines: round-tripping, channel stuffing, cookie jar reserves, capitalization abuse, related party transactions, journal entry fraud
  - New CLI command: `databridge fraud-detect <file_or_dir>`
- **FX Validation module** (`databridge_core.fx_validate`): Validate FX translation rates in multi-currency trial balances
  - `validate_fx()` -- detect inverted rates, stale rates, wrong rate types, math errors, and TB imbalances
  - `validate_fx_batch()` -- batch validation across a directory
  - Reference rates for 12 currencies, GAAP/IFRS rate type rules (closing/average/historical)
  - New CLI command: `databridge fx-validate <file_or_dir>`
- **Standards Compliance module** (`databridge_core.standards_check`): Flag GAAP/IFRS/J-GAAP violations
  - `check_standards()` -- check a single COA file with auto-detected or overridden standard
  - `check_standards_batch()` -- batch compliance checking across a directory
  - Rules: LIFO under IFRS, revaluation under US GAAP, capitalized dev costs, operating lease expense, superseded standards, terminology mismatches, dual-reporting reconciliation errors
  - New CLI command: `databridge standards-check <file_or_dir>`
- 8 new exports in `__init__.py`: `detect_erp`, `detect_erp_batch`, `detect_fraud`, `detect_fraud_batch`, `validate_fx`, `validate_fx_batch`, `check_standards`, `check_standards_batch`
- 4 new CLI commands: `erp-detect`, `fraud-detect`, `fx-validate`, `standards-check`
- All 4 modules are pure stdlib (csv, re, pathlib, collections, math) -- no new dependencies

### Changed
- Total exported functions: 22 -> 30
- Total CLI commands: 10 -> 14

## [1.2.0] - 2026-02-25

### Added
- **Triage module** (`databridge_core.triage`): Batch Excel scanning and archetype classification
  - `scan_and_classify()` — scan directories of .xlsx files, classify by archetype (Financial Report, Data Extract, Model/Template, etc.)
  - `BatchExcelScanner` — concurrent file scanning via ThreadPoolExecutor
  - `ArchetypeClassifier` — heuristic scoring with 7 archetype categories
  - `ReportGenerator` — JSONL and JSON summary output
  - New `[triage]` optional extra: `pip install 'databridge-core[triage]'`
  - New CLI command: `databridge triage <directory>`
- **Templates module** (`databridge_core.templates`): Industry-specific hierarchy templates
  - `TemplateService` — CRUD for templates, skills, and client knowledge profiles
  - `FinancialTemplate` — complete hierarchy template model with 20+ hierarchy types
  - `SkillDefinition` — AI expertise skill definitions by domain
  - `ClientKnowledge` — client-specific knowledge base (COA patterns, ERP systems, preferences)
- **Integrations module** (`databridge_core.integrations`): Third-party client connectors
  - `BaseClient` — lightweight HTTP client using stdlib only (no requests dependency)
  - `SlackClient` — post messages, reconciliation reports, and workflow alerts to Slack
- 6 new exports in `__init__.py`: `TemplateService`, `FinancialTemplate`, `BaseClient`, `SlackClient`, `scan_and_classify`
- 10 new CLI command: `databridge triage`

### Changed
- Bumped minimum `pandas` requirement from `>=1.5` to `>=2.0`
- Total exported functions: 16 → 22

## [1.1.0] - 2026-02-24

### Added
- Interactive guided tour with realistic finance data (`databridge-tour` CLI command)
- 9 tour data files: SAP COA, bank statements, GL extracts, CRM sales, ERP invoices, vendor records
- 4 corporate scenarios: SAP migration, bank-GL reconciliation, vendor dedup, schema drift

## [1.0.0] - 2026-02-24

### Added
- Initial public release on PyPI
- 9 CLI commands: `profile`, `compare`, `fuzzy`, `diff`, `drift`, `transform`, `merge`, `find`, `parse`
- 16 Python API functions across 3 modules (reconciler, profiler, ingestion)
- 5 optional extras: `[fuzzy]`, `[pdf]`, `[ocr]`, `[sql]`, `[all]`
- Python 3.10 - 3.13
