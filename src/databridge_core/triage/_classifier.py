"""Heuristic archetype classifier for Excel files.

Scores each file against a set of signal-based rules to assign
an archetype (Financial Report, Data Extract, Consolidation, etc.).
No AI/LLM required — pure heuristic scoring.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from ._types import Archetype, FileTriageResult


# ---------------------------------------------------------------------------
# Filename keyword patterns (compiled once)
# ---------------------------------------------------------------------------
_TEMPLATE_RE = re.compile(r"\b(template|blank|form)\b", re.IGNORECASE)
_ACADEMIC_RE = re.compile(
    r"\b(exam|homework|practice|exercise|quiz|test|assignment|class"
    r"|chapter|edition|textbook|midterm|final\b.*\bsolution"
    r"|survey|poll|question|session|work\d|topic\d"
    r"|case[\s_]|solutions?\.xlsx|tutorial|reading|mgmt\s*\d"
    r"|emba|capm|student|scorecard)\b",
    re.IGNORECASE,
)
_FINANCIAL_RE = re.compile(
    r"\b(p&l|p\.l|revenue|balance|financial|budget\w*|valuation|income"
    r"|forecast|cashflow|cash.?flow|profit|margin|cost|checking"
    r"|retirement|mortgage|loan|investment\w*|rate|price.?structure"
    r"|salary|calculat\w*|cal\s*sheet|personal\s*budget)",
    re.IGNORECASE,
)
_DATA_EXTRACT_RE = re.compile(
    r"\b(data|extract|export|dump|download|database|report(?:ing)?)\b",
    re.IGNORECASE,
)
_CONSOLIDATION_RE = re.compile(
    r"\b(consolidat\w*|consol\w*|combined|merged|interco\w*)\b",
    re.IGNORECASE,
)
_REFERENCE_RE = re.compile(
    r"\b(reference|lookup|master|mapping|codes|chart.of.accounts|coa"
    r"|schedule|roster|contact|list|rank|directory|hours|timesheet"
    r"|status.?report|timeline|timing|inspection|estimate|spr\b)\b",
    re.IGNORECASE,
)
_MODEL_RE = re.compile(
    r"\b(model|simulat\w+|monte.?carlo|probabilit\w+"
    r"|sampling|experiment|decision.?tree|portfolio"
    r"|optimi\w+|solver|allocation|scenario)\b",
    re.IGNORECASE,
)

# Financial Excel function names (TVM, valuation, fixed-income)
_FINANCIAL_FUNCTIONS = frozenset([
    "pv", "npv", "irr", "xirr", "xnpv", "pmt", "fv", "rate",
    "yield", "price", "duration", "effect", "nominal",
])

# Statistical/analytical function names
_STATISTICAL_FUNCTIONS = frozenset([
    "average", "stdev", "sqrt", "intercept", "slope", "correl",
    "normsdist", "norminv", "ln", "exp", "sumproduct",
])


class ArchetypeClassifier:
    """Score-based archetype classifier for triaged Excel files."""

    def classify(self, result: FileTriageResult) -> FileTriageResult:
        """Assign archetype, confidence, and reasons to *result* in place.

        Returns the same FileTriageResult for convenience chaining.
        """
        if result.scan_status.value != "ok":
            result.archetype = Archetype.UNKNOWN
            result.archetype_confidence = 0.0
            return result

        scores: Dict[Archetype, float] = {a: 0.0 for a in Archetype}
        reasons: Dict[Archetype, List[str]] = {a: [] for a in Archetype}

        self._score_formulas(result, scores, reasons)
        self._score_structure(result, scores, reasons)
        self._score_filename(result, scores, reasons)
        self._score_sheet_names(result, scores, reasons)

        # Pick winner
        best = max(scores, key=lambda a: scores[a])
        best_score = scores[best]

        if best_score < 0.15:
            result.archetype = Archetype.UNKNOWN
            result.archetype_confidence = best_score
            result.archetype_reasons = ["No strong signal detected"]
        else:
            result.archetype = best
            result.archetype_confidence = min(best_score, 1.0)
            result.archetype_reasons = reasons[best]

        return result

    def classify_batch(self, results: List[FileTriageResult]) -> List[FileTriageResult]:
        """Classify a list of results in place."""
        for r in results:
            self.classify(r)
        return results

    # ------------------------------------------------------------------
    # Scoring rules
    # ------------------------------------------------------------------

    def _score_formulas(
        self,
        r: FileTriageResult,
        scores: Dict[Archetype, float],
        reasons: Dict[Archetype, List[str]],
    ) -> None:
        """Score based on formula patterns and counts."""
        fc = r.formula_count
        nr = r.named_range_count
        rc = r.total_row_count
        dom_lower = [f.lower() for f in r.dominant_formula_functions]

        # Financial Report: lots of formulas + SUMIF or named ranges
        if fc > 20 and (r.has_sumif_pattern or nr > 5):
            scores[Archetype.FINANCIAL_REPORT] += 0.4
            reasons[Archetype.FINANCIAL_REPORT].append(
                f"High formula count ({fc}) with SUMIF/named ranges ({nr})"
            )

        # Financial Report: VLOOKUP with moderate formulas
        if r.has_vlookup_pattern and fc > 10:
            scores[Archetype.FINANCIAL_REPORT] += 0.3
            reasons[Archetype.FINANCIAL_REPORT].append(
                f"VLOOKUP pattern with {fc} formulas"
            )

        # Financial Report: many named ranges + formulas
        if nr > 10 and fc > 50:
            scores[Archetype.FINANCIAL_REPORT] += 0.2
            reasons[Archetype.FINANCIAL_REPORT].append(
                f"Rich structure: {nr} named ranges, {fc} formulas"
            )

        # Financial Report: TVM/valuation functions (PV, NPV, IRR, PMT, etc.)
        fin_funcs = [f for f in dom_lower if f in _FINANCIAL_FUNCTIONS]
        if fin_funcs:
            scores[Archetype.FINANCIAL_REPORT] += 0.25
            reasons[Archetype.FINANCIAL_REPORT].append(
                f"Financial functions detected: {fin_funcs}"
            )

        # Financial Report: high formula density on single sheet (personal finance, ledgers)
        if r.sheet_count == 1 and fc > 100 and rc > 0 and (fc / rc) > 2.0:
            scores[Archetype.FINANCIAL_REPORT] += 0.2
            reasons[Archetype.FINANCIAL_REPORT].append(
                f"High formula density ({fc / rc:.1f} formulas/row) on single sheet"
            )

        # Data Extract: few formulas, lots of rows
        if fc < 5 and rc > 500:
            scores[Archetype.DATA_EXTRACT] += 0.4
            reasons[Archetype.DATA_EXTRACT].append(
                f"Low formulas ({fc}) with high row count ({rc})"
            )

        # Data Extract: many rows with mostly statistical/transform formulas
        if rc > 500 and fc > 0 and fc < rc:
            stat_funcs = [f for f in dom_lower if f in _STATISTICAL_FUNCTIONS]
            if stat_funcs and len(stat_funcs) >= len(dom_lower) * 0.5:
                scores[Archetype.DATA_EXTRACT] += 0.2
                reasons[Archetype.DATA_EXTRACT].append(
                    f"High row count ({rc}) with analytical formulas: {stat_funcs}"
                )

        # Model/Template: high formula density (>2 formulas/row) on multi-sheet
        if r.sheet_count >= 2 and fc > 50 and rc > 0 and (fc / rc) > 2.0:
            scores[Archetype.MODEL_TEMPLATE] += 0.3
            reasons[Archetype.MODEL_TEMPLATE].append(
                f"High formula density ({fc / rc:.1f} formulas/row) — computational model"
            )

        # Academic/Exercise: few formulas, few rows, few sheets
        if fc < 5 and rc < 50 and r.sheet_count <= 2:
            scores[Archetype.ACADEMIC_EXERCISE] += 0.3
            reasons[Archetype.ACADEMIC_EXERCISE].append(
                f"Small file: {fc} formulas, {rc} rows, {r.sheet_count} sheets"
            )

        # Academic/Exercise: single sheet with modest rows and statistical formulas
        if r.sheet_count == 1 and 5 < fc <= 200 and 10 < rc <= 200:
            stat_funcs = [f for f in dom_lower if f in _STATISTICAL_FUNCTIONS]
            if stat_funcs:
                scores[Archetype.ACADEMIC_EXERCISE] += 0.2
                reasons[Archetype.ACADEMIC_EXERCISE].append(
                    f"Small dataset ({rc} rows) with statistical formulas: {stat_funcs}"
                )

        # Model/Template: single sheet, small dataset, moderate formulas (calc worksheets)
        if r.sheet_count == 1 and 10 < fc and rc < 100 and fc > rc * 0.5:
            scores[Archetype.MODEL_TEMPLATE] += 0.15
            reasons[Archetype.MODEL_TEMPLATE].append(
                f"Small calc sheet: {fc} formulas across {rc} rows"
            )

        # Model/Template: single sheet, ~1:1 formula/row ratio (each row is a calculation)
        if r.sheet_count == 1 and fc > 50 and rc > 50 and 0.5 < (fc / rc) < 2.0:
            scores[Archetype.MODEL_TEMPLATE] += 0.15
            reasons[Archetype.MODEL_TEMPLATE].append(
                f"Near 1:1 formula-to-row ratio ({fc / rc:.1f}) — calculation worksheet"
            )

        # Model/Template: multi-sheet, very high formula density (>1.5 formulas/row)
        if r.sheet_count >= 2 and fc > 100 and rc > 0 and (fc / rc) > 1.5:
            scores[Archetype.MODEL_TEMPLATE] += 0.2
            reasons[Archetype.MODEL_TEMPLATE].append(
                f"Multi-sheet with {fc / rc:.1f} formulas/row — computational model"
            )

        # Data Extract: large dataset (>1000 rows) regardless of formulas
        if rc > 1000 and r.sheet_count >= 2 and fc < rc:
            scores[Archetype.DATA_EXTRACT] += 0.15
            reasons[Archetype.DATA_EXTRACT].append(
                f"Large multi-sheet dataset: {rc} rows across {r.sheet_count} sheets"
            )

    def _score_structure(
        self,
        r: FileTriageResult,
        scores: Dict[Archetype, float],
        reasons: Dict[Archetype, List[str]],
    ) -> None:
        """Score based on sheet structure and counts."""
        # Consolidation: 3+ sheets with similar row counts
        if r.sheet_count >= 3 and len(r.sheets) >= 3:
            non_empty = [s for s in r.sheets if not s.is_empty and s.row_count > 0]
            if len(non_empty) >= 3:
                row_counts = [s.row_count for s in non_empty]
                avg_rows = sum(row_counts) / len(row_counts)
                if avg_rows > 0:
                    variance = sum((rc - avg_rows) ** 2 for rc in row_counts) / len(row_counts)
                    cv = (variance ** 0.5) / avg_rows  # coefficient of variation
                    if cv < 0.5:
                        scores[Archetype.CONSOLIDATION] += 0.35
                        reasons[Archetype.CONSOLIDATION].append(
                            f"{len(non_empty)} sheets with similar row counts (CV={cv:.2f})"
                        )

        # Model/Template: many empty or sparse sheets
        if r.sheet_count >= 2:
            empty_or_sparse = sum(
                1 for s in r.sheets
                if s.is_empty or (s.row_count <= 5 and s.formula_count <= 2)
            )
            if empty_or_sparse >= r.sheet_count * 0.5:
                scores[Archetype.MODEL_TEMPLATE] += 0.3
                reasons[Archetype.MODEL_TEMPLATE].append(
                    f"{empty_or_sparse}/{r.sheet_count} sheets are empty or sparse"
                )

        # Reference Data: moderate rows, no formulas, few sheets
        if r.formula_count == 0 and 10 < r.total_row_count <= 500 and r.sheet_count <= 3:
            scores[Archetype.REFERENCE_DATA] += 0.3
            reasons[Archetype.REFERENCE_DATA].append(
                f"No formulas, {r.total_row_count} rows — looks like reference data"
            )

        # Reference Data: multi-sheet with no formulas and moderate data
        if r.sheet_count >= 2 and r.formula_count == 0 and r.total_row_count > 10:
            scores[Archetype.REFERENCE_DATA] += 0.2
            reasons[Archetype.REFERENCE_DATA].append(
                f"Multi-sheet, no formulas, {r.total_row_count} rows"
            )

        # Data Extract: many sheets with lots of rows and few formulas per row
        if r.sheet_count >= 5 and r.total_row_count > 1000 and r.formula_count < r.total_row_count:
            scores[Archetype.DATA_EXTRACT] += 0.25
            reasons[Archetype.DATA_EXTRACT].append(
                f"{r.sheet_count} sheets, {r.total_row_count} rows, low formula ratio — data extract"
            )

    def _score_filename(
        self,
        r: FileTriageResult,
        scores: Dict[Archetype, float],
        reasons: Dict[Archetype, List[str]],
    ) -> None:
        """Score based on filename keywords."""
        name = r.file_name

        if _TEMPLATE_RE.search(name):
            scores[Archetype.MODEL_TEMPLATE] += 0.2
            reasons[Archetype.MODEL_TEMPLATE].append("Filename suggests template/form")

        if _MODEL_RE.search(name):
            scores[Archetype.MODEL_TEMPLATE] += 0.2
            reasons[Archetype.MODEL_TEMPLATE].append("Filename suggests model/simulation")

        if _ACADEMIC_RE.search(name):
            scores[Archetype.ACADEMIC_EXERCISE] += 0.25
            reasons[Archetype.ACADEMIC_EXERCISE].append("Filename suggests academic/exercise")

        if _FINANCIAL_RE.search(name):
            scores[Archetype.FINANCIAL_REPORT] += 0.15
            reasons[Archetype.FINANCIAL_REPORT].append("Filename suggests financial content")

        if _DATA_EXTRACT_RE.search(name):
            scores[Archetype.DATA_EXTRACT] += 0.15
            reasons[Archetype.DATA_EXTRACT].append("Filename suggests data extract")

        if _CONSOLIDATION_RE.search(name):
            scores[Archetype.CONSOLIDATION] += 0.2
            reasons[Archetype.CONSOLIDATION].append("Filename suggests consolidation")

        if _REFERENCE_RE.search(name):
            scores[Archetype.REFERENCE_DATA] += 0.2
            reasons[Archetype.REFERENCE_DATA].append("Filename suggests reference data")

    def _score_sheet_names(
        self,
        r: FileTriageResult,
        scores: Dict[Archetype, float],
        reasons: Dict[Archetype, List[str]],
    ) -> None:
        """Score based on worksheet names."""
        if not r.sheet_names:
            return

        names_lower = [s.lower().strip() for s in r.sheet_names]

        # Numbered sheets (#1, #2, ...) — textbook exercise pattern
        numbered = sum(1 for s in names_lower if re.match(r"^#\d+$", s))
        if numbered >= 3:
            scores[Archetype.ACADEMIC_EXERCISE] += 0.3
            reasons[Archetype.ACADEMIC_EXERCISE].append(
                f"{numbered} numbered sheets (#1, #2, ...) — textbook exercises"
            )

        # Sheet named "model" or "assumptions" — model/template
        model_names = {"model", "assumptions", "inputs", "parameters", "dashboard"}
        if any(s in model_names for s in names_lower):
            scores[Archetype.MODEL_TEMPLATE] += 0.15
            reasons[Archetype.MODEL_TEMPLATE].append(
                "Sheet named 'model'/'assumptions'/'inputs' — computational model"
            )

        # Sheet named "data" with other analysis sheets — data extract
        if "data" in names_lower and r.sheet_count >= 2:
            scores[Archetype.DATA_EXTRACT] += 0.15
            reasons[Archetype.DATA_EXTRACT].append(
                "Sheet named 'data' in multi-sheet workbook"
            )

        # Financial sheet names
        fin_names = {"p&l", "balance sheet", "income", "bs", "pl", "is", "cf",
                     "revenue", "expenses", "budget", "forecast"}
        if any(s in fin_names for s in names_lower):
            scores[Archetype.FINANCIAL_REPORT] += 0.2
            reasons[Archetype.FINANCIAL_REPORT].append(
                "Sheet name suggests financial report"
            )
