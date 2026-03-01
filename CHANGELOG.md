# Changelog

All notable changes to `databridge-core` will be documented in this file.

## [1.5.1] - 2026-02-28

### Changed
- Server-side (not in this package): 5 new evaluation tools — falsification search, property checks, failure clustering, failure rate estimation, release dashboard (Build 3 validation harness). CE tools 297→302, Enterprise 362→367
- Server-side (not in this package): planner gains online decision-making — VOI clarifications, Thompson Sampling strategy selection, Monte Carlo rollout planning
- No library code changes; version bump for changelog only

## [1.5.0] - 2026-02-26

### Added
- **Grounded Detection module** (`databridge_core.detection`): KB-grounded anomaly detection for financial data
  - `detect_grounded()` — run KB-grounded regex detection on a single CSV file with Knowledge Base citations
  - `detect_grounded_batch()` — batch detection across a directory of CSV files
  - `record_feedback()` — record user feedback (confirm/dismiss) on detection findings for the learning loop
  - `get_detection_stats()` — compute detection performance and learning statistics
  - `detect_and_verify()` — full AI pipeline: KB rules → regex detection → LangGraph 3-node verification (Triage → Verify → Reconcile)
  - `run_verification_graph()` — run AI verification on pre-computed candidate findings
  - `load_detection_rules()` — load detection rules from Knowledge Base JSON files
  - `apply_feedback_filter()` — filter findings based on historical feedback (auto-suppress false positives)
  - 10 finding types: sign reversal, rounding discrepancy, missing/duplicate account, hierarchy break, naming violation, balance mismatch, formula anomaly, classification error, custom
  - 5 severity levels: critical, high, medium, low, info
  - Numeric outlier detection: statistical outlier analysis for rate/amount columns (inverted FX rates, decimal shifts)
  - GraphRAG enrichment: optional Knowledge Graph context for flagged accounts
  - Forensic RAG enrichment: optional historical finding similarity search
  - Graceful fallback: deterministic heuristics when LangGraph/Claude API unavailable
  - New optional dependency group: `pip install 'databridge-core[detection]'` for AI verification pipeline

## [1.4.0] - 2026-02-25

### Added
- **Entity Linker module** (`databridge_core.linker`): Cross-file entity resolution with financial synonym awareness
  - `link_entities()` — resolve entities across Logic DNA files using composite scoring and Union-Find clustering
  - `get_entity_map()` — retrieve a previously generated entity map
  - `get_entity_cluster()` — retrieve a single cluster by ID
  - `get_link_summary()` — summary statistics
  - `find_entity()` — fuzzy search across linked clusters
  - Composite scoring: name similarity (40%) + formula similarity (25%) + business meaning (25%) + archetype compatibility (10%)
  - 60+ financial synonym sets covering GAAP/IFRS terminology (revenue/net sales, COGS/cost of sales, APIC/share premium, etc.)
  - Jaccard token overlap for fallback names to prevent over-linking of descriptive texts
  - Domain inference (revenue, expense, balance, margin, tax, intercompany, headcount)
  - Conflict detection: formula mismatch, sign reversal
  - Oversized cluster filtering (>100 mentions)
- **Profiler enhancements** (`databridge_core.profiler`):
  - `generate_expectation_suite()` — auto-generate data quality expectations from profiled data
  - `list_expectation_suites()` — list persisted expectation suites
  - `validate()` — validate data files against expectation suites
  - `get_validation_results()` — get historical validation results
  - Multi-format support: profiler and drift detection now support CSV, Excel (.xlsx/.xls/.xlsb), JSON, and Parquet
- **Connectors module** (`databridge_core.connectors`): Local DuckDB SQL engine
  - `query_local()` — execute SQL against local CSV/Parquet/JSON/Excel files
  - `register_table()` — register a file as a named DuckDB table
  - `list_tables()` — list registered tables in the DuckDB session
  - `export_to_parquet()` — export query results or tables to Parquet format
  - New `[duckdb]` optional extra: `pip install 'databridge-core[duckdb]'`
  - New `[excel]` optional extra: `pip install 'databridge-core[excel]'`
- 8 new exports in `__init__.py`: `generate_expectation_suite`, `list_expectation_suites`, `validate`, `get_validation_results`, `link_entities`, `find_entity`, `query_local`, `export_to_parquet`

### Fixed
- FX Validation: replaced tolerance-based stale detection with rate-period matching (closing/opening/average/historical)
- FX Validation: added inversion product check (stated × expected ≈ 1.0) for more accurate inverted rate detection
- FX Validation: renamed TB_IMBALANCE to CTA_MISCALCULATION for clarity
- Standards Check: fixed dual-reporting detection priority (Dual > J-GAAP > US GAAP > IFRS)
- Standards Check: added STANDARD_MISCLASSIFICATION for capitalized dev costs under GAAP (ASC 730)
- Standards Check: added STANDARD_OPPORTUNITY for R&D expensing under IFRS (IAS 38.57)
- Standards Check: deduplicate secondary findings when primary violation exists

### Changed
- Total exported functions: 30 → 38
- New optional extras: `[duckdb]`, `[excel]`

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
