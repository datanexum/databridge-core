# DataBridge Core: Enterprise Guided Tour

This guide walks you through four high-value corporate scenarios using the DataBridge Core toolkit. We process **thousands of rows of mock data** simulating real-world financial messy files.

## Scenario 1: The "Messy" SAP Export (Discovery)
**Corporate Challenge:** Your ERP (SAP) exports a Chart of Accounts (COA) with internal metadata, disclaimers, and filtering info at the top of the file. This breaks most automated ingestion pipelines.

**DataBridge Solution:** 
Our discovery engine identifies the "Anchor Row" and automatically skips the garbage.
- **Input:** `sap_coa_messy.csv` (800 accounts + 5 lines of metadata).
- **Execution:** Automated skip and parse.
- **Outcome:** Clean tabular data with summarized Account Types (Asset, Liability, etc.) ready for modeling.

## Scenario 2: Vendor Master Consolidation (Mapping)
**Corporate Challenge:** You are merging two companies. Legacy System A calls a vendor "Amazon Web Svcs" while System B calls it "Amazon Web Services (AWS)". You need to link them without manual lookup.

**DataBridge Solution:** 
Fuzzy matching calculates the "Edit Distance" between names.
- **Input:** `legacy_vendors.csv` vs `new_erp_vendors.csv` (600 records each).
- **Execution:** `fuzzy_match_columns` with a 70% confidence threshold.
- **Outcome:** A mapping table linking messy legacy names to clean global targets instantly.

## Scenario 3: Cash & Bank Audit (Reconciliation)
**Corporate Challenge:** Your General Ledger (GL) says you paid 1,200 vendors, but your bank statement only shows 1,195 transactions. Finding the 5 missing ones manually is a "needle in a haystack" problem.

**DataBridge Solution:** 
High-speed hashing creates a digital fingerprint for every transaction.
- **Input:** `gl_extract.csv` (1,200 rows) vs `bank_statement.csv` (1,195 rows).
- **Execution:** `compare_hashes` on the `amount` column.
- **Outcome:** Instant flagging of the exact 5 "Orphan" transactions missing from the bank.

## Scenario 4: Revenue Integrity (CRM vs ERP)
**Corporate Challenge:** Sales reps in Salesforce (CRM) close deals for one price, but Billing in NetSuite (ERP) sometimes applies manual discounts. You need to find unbilled deals and price discrepancies.

**DataBridge Solution:** 
Conflict detection surfaces row-level differences in specific columns.
- **Input:** `crm_sales.csv` (1,500 rows) vs `erp_invoices.csv`.
- **Execution:** `get_conflict_details` comparing `amount`.
- **Outcome:** 
    1.  List of **Unbilled Opportunities** (Sales in CRM but not in ERP).
    2.  List of **Revenue Mismatches** (Price differences where CRM != ERP).

---

## How to Run the Tour
1.  Navigate to the project root.
2.  Run the tour script:
    ```bash
    python databridge-core/examples/guided_tour.py
    ```

## Why this matters
The work demonstrated in this tour usually requires **4-8 hours of manual Excel work** by a mid-level financial analyst. DataBridge Core handles it in **under 10 seconds** with zero human error.
