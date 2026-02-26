"""Composite confidence scoring for entity mention pairs."""
from __future__ import annotations

import difflib
from typing import Dict, FrozenSet, Set, Tuple

from ._types import EntityMention, MatchStrategy

# ── Scoring weights ──────────────────────────────────────────────────────────

WEIGHT_NAME = 0.40
WEIGHT_FORMULA = 0.25
WEIGHT_MEANING = 0.25
WEIGHT_ARCHETYPE = 0.10

# Minimum composite score to consider a pair linked
LINK_THRESHOLD = 0.65

# ── Financial synonym map ────────────────────────────────────────────────────
# Each frozenset contains terms that are semantically equivalent across
# GAAP / IFRS / common accounting usage.  Used to boost name_similarity
# when two different labels refer to the same financial concept.

FINANCIAL_SYNONYMS: Set[FrozenSet[str]] = {
    # Revenue synonyms
    frozenset({"revenue", "net sales", "sales revenue", "turnover", "net revenue", "gross revenue", "total revenue"}),
    # Expense synonyms
    frozenset({"cost of sales", "cost of goods sold", "cogs", "cost of revenue"}),
    frozenset({"sg&a", "selling general and administrative", "operating expenses", "opex"}),
    # Balance sheet — GAAP vs IFRS terminology
    frozenset({"additional paid-in capital", "apic", "share premium", "capital surplus"}),
    frozenset({"treasury stock", "treasury shares", "own shares held"}),
    frozenset({"accounts receivable", "trade receivables", "trade and other receivables", "a/r", "ar"}),
    frozenset({"accounts payable", "trade payables", "trade and other payables", "a/p", "ap"}),
    frozenset({"retained earnings", "accumulated profits", "revenue reserves"}),
    frozenset({"goodwill", "goodwill on acquisition"}),
    frozenset({"inventory", "inventories", "stock", "merchandise"}),
    frozenset({"property plant and equipment", "ppe", "fixed assets", "tangible assets"}),
    frozenset({"intangible assets", "intangibles"}),
    frozenset({"depreciation", "depreciation expense", "depreciation and amortisation", "d&a"}),
    frozenset({"amortization", "amortisation", "amortization expense"}),
    # Credit loss models
    frozenset({"cecl", "current expected credit loss", "expected credit loss", "ecl", "allowance for credit losses"}),
    # Lease terminology
    frozenset({"right-of-use asset", "rou asset", "lease asset", "operating lease rou"}),
    frozenset({"lease liability", "operating lease liability", "finance lease liability"}),
    # Cash flow
    frozenset({"cash and cash equivalents", "cash", "cash & equivalents", "liquid assets"}),
    frozenset({"operating cash flow", "cash from operations", "cfo"}),
    # Tax
    frozenset({"income tax expense", "tax expense", "provision for income taxes", "tax provision"}),
    frozenset({"deferred tax asset", "dta", "deferred tax"}),
    frozenset({"deferred tax liability", "dtl"}),
    # Equity
    frozenset({"common stock", "ordinary shares", "share capital", "capital stock"}),
    frozenset({"preferred stock", "preference shares"}),
    # Intercompany
    frozenset({"intercompany receivable", "ic receivable", "due from affiliate", "due from related party"}),
    frozenset({"intercompany payable", "ic payable", "due to affiliate", "due to related party"}),
    # Profit metrics
    frozenset({"net income", "net profit", "profit for the period", "net earnings", "bottom line"}),
    frozenset({"gross profit", "gross margin"}),
    frozenset({"operating income", "operating profit", "ebit"}),
    frozenset({"ebitda", "earnings before interest taxes depreciation amortization"}),
}

# Inverted index: normalized term → the frozenset it belongs to (O(1) lookup)
_SYNONYM_LOOKUP: Dict[str, FrozenSet[str]] = {}
for _syn_set in FINANCIAL_SYNONYMS:
    for _term in _syn_set:
        _SYNONYM_LOOKUP[_term] = _syn_set


def _synonym_score(name_a: str, name_b: str) -> float:
    """Check whether two names are financial synonyms.

    Args:
        name_a: First entity name (raw or normalized).
        name_b: Second entity name (raw or normalized).

    Returns:
        1.0 if both names belong to the same synonym set, 0.0 otherwise.
    """
    a = name_a.lower().strip()
    b = name_b.lower().strip()

    if not a or not b:
        return 0.0

    # Fast path: identical after normalization
    if a == b:
        return 1.0

    syn_set_a = _SYNONYM_LOOKUP.get(a)
    if syn_set_a is None:
        return 0.0

    return 1.0 if b in syn_set_a else 0.0


# ── Archetype compatibility matrix ───────────────────────────────────────────

_ARCHETYPE_GROUPS = {
    "Financial Report": "finance",
    "Financial Statement": "finance",
    "Budget": "finance",
    "Forecast": "finance",
    "Consolidation": "finance",
    "Data Extract": "data",
    "Data Export": "data",
    "Database Extract": "data",
    "Model/Template": "model",
    "Template": "model",
    "Calculation Model": "model",
    "Dashboard": "reporting",
    "Report": "reporting",
    "Analysis": "reporting",
    "Unknown": "unknown",
}


def _archetype_group(archetype: str) -> str:
    """Map an archetype to its compatibility group."""
    return _ARCHETYPE_GROUPS.get(archetype, "unknown")


def _is_fallback_name(normalized: str) -> bool:
    """Detect if a normalized name is a long fallback (sentence-derived, not a crisp identifier)."""
    # Fallback names are multi-word descriptions (>5 tokens, >40 chars)
    tokens = normalized.split("_")
    return len(tokens) > 5 and len(normalized) > 40


def _name_similarity(a: EntityMention, b: EntityMention) -> float:
    """Name similarity using normalized names.

    Takes the maximum of:
    - difflib.SequenceMatcher character-level ratio
    - Financial synonym score (1.0 if both names map to the same synonym set)

    For long fallback names (sentence-derived descriptions), uses Jaccard
    token overlap instead of character-level SequenceMatcher to prevent
    over-linking of descriptive texts that share common words.
    """
    if not a.normalized_name or not b.normalized_name:
        return 0.0

    a_is_fallback = _is_fallback_name(a.normalized_name)
    b_is_fallback = _is_fallback_name(b.normalized_name)

    if a_is_fallback or b_is_fallback:
        # Use Jaccard (token overlap) for fallback names to avoid
        # SequenceMatcher inflating scores on shared common words.
        tokens_a = set(a.normalized_name.split("_")) - _STOP_TOKENS
        tokens_b = set(b.normalized_name.split("_")) - _STOP_TOKENS
        if not tokens_a or not tokens_b:
            return 0.0
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0.0
        # Apply 0.8 penalty to discourage linking different intents
        return jaccard * 0.8

    seq_score = difflib.SequenceMatcher(
        None, a.normalized_name, b.normalized_name
    ).ratio()

    # Check synonyms using both raw_name (preserves original casing/spacing)
    # and normalized_name (underscore-delimited lowercase).
    # raw_name is checked first since synonym keys use spaces, not underscores.
    syn_score = _synonym_score(a.raw_name, b.raw_name)
    if syn_score == 0.0:
        # Try normalized names with underscores replaced by spaces
        syn_score = _synonym_score(
            a.normalized_name.replace("_", " "),
            b.normalized_name.replace("_", " "),
        )

    return max(seq_score, syn_score)


# Common words to ignore when computing Jaccard for fallback names
_STOP_TOKENS = frozenset({
    "the", "a", "an", "of", "for", "in", "on", "to", "and", "or", "is", "are",
    "was", "by", "from", "with", "as", "at", "this", "that", "it", "its",
    "calculate", "calculates", "calculated", "calculation", "calculations",
    "compute", "computes", "computed",
    "determine", "determines", "determined",
    "value", "values", "data", "based", "used", "using",
})


def _formula_similarity(a: EntityMention, b: EntityMention) -> float:
    """Formula similarity — compare raw formula strings."""
    if not a.formula or not b.formula:
        return 0.0
    return difflib.SequenceMatcher(None, a.formula, b.formula).ratio()


def _meaning_similarity(a: EntityMention, b: EntityMention) -> float:
    """Business meaning similarity — compare the meaning text."""
    if not a.business_meaning or not b.business_meaning:
        return 0.0
    return difflib.SequenceMatcher(
        None,
        a.business_meaning.lower(),
        b.business_meaning.lower(),
    ).ratio()


def _archetype_compatibility(a: EntityMention, b: EntityMention) -> float:
    """Archetype compatibility score.

    - Same group → 1.0
    - finance ↔ reporting → 0.7
    - finance ↔ model → 0.6
    - Any ↔ unknown → 0.3
    - Otherwise → 0.1
    """
    ga = _archetype_group(a.archetype)
    gb = _archetype_group(b.archetype)

    if ga == gb:
        return 1.0

    pair = frozenset({ga, gb})
    if pair == frozenset({"finance", "reporting"}):
        return 0.7
    if pair == frozenset({"finance", "model"}):
        return 0.6
    if pair == frozenset({"reporting", "model"}):
        return 0.5
    if "unknown" in pair:
        return 0.3
    return 0.1


def score_pair(
    a: EntityMention,
    b: EntityMention,
) -> Tuple[float, Dict[str, float], list]:
    """Compute composite confidence score for a pair of entity mentions.

    Args:
        a: First entity mention.
        b: Second entity mention.

    Returns:
        Tuple of (composite_score, component_scores dict, strategies_used list).
    """
    name_score = _name_similarity(a, b)
    formula_score = _formula_similarity(a, b)
    meaning_score = _meaning_similarity(a, b)
    archetype_score = _archetype_compatibility(a, b)

    composite = (
        WEIGHT_NAME * name_score
        + WEIGHT_FORMULA * formula_score
        + WEIGHT_MEANING * meaning_score
        + WEIGHT_ARCHETYPE * archetype_score
    )

    components = {
        MatchStrategy.NAME_SIMILARITY.value: round(name_score, 4),
        MatchStrategy.FORMULA_SIMILARITY.value: round(formula_score, 4),
        MatchStrategy.BUSINESS_MEANING.value: round(meaning_score, 4),
        MatchStrategy.ARCHETYPE_COMPATIBILITY.value: round(archetype_score, 4),
    }

    strategies = [
        s
        for s, v in components.items()
        if v > 0.3  # Only list strategies that contributed meaningfully
    ]

    return round(composite, 4), components, strategies
