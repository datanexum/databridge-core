# Changelog

All notable changes to `databridge-core` will be documented in this file.

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
