"""Extract EntityMention objects from Logic DNA files."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from ._types import EntityMention

logger = logging.getLogger(__name__)


def _normalize_name(name: str) -> str:
    """Normalize a name: lowercase, replace non-alnum with underscore, collapse."""
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", name.lower())).strip("_")


def _extract_names_from_text(text: str) -> List[str]:
    """Extract candidate entity names from business meaning or description text.

    Looks for:
    - Quoted strings: "Revenue West"
    - CamelCase identifiers: RevenueWest
    - Underscored identifiers: Revenue_West, Acct_101
    - Known accounting patterns: Total X, Net X, Gross X, Revenue, EBITDA, etc.
    """
    names: List[str] = []

    # Quoted strings
    for match in re.finditer(r'"([^"]{2,50})"', text):
        names.append(match.group(1).strip())

    # Underscored identifiers (e.g., Revenue_West, Acct_101)
    for match in re.finditer(r'\b[A-Za-z][A-Za-z0-9]*(?:_[A-Za-z0-9]+)+\b', text):
        names.append(match.group(0))

    # CamelCase identifiers (at least 2 words)
    for match in re.finditer(r'\b[A-Z][a-z]+(?:[A-Z][a-z]+)+\b', text):
        names.append(match.group(0))

    # Known accounting / financial patterns
    for pattern in [
        r'\b(Total\s+\w[\w\s]{1,30})',
        r'\b(Net\s+\w[\w\s]{1,30})',
        r'\b(Gross\s+\w[\w\s]{1,30})',
        r'\b(Operating\s+\w[\w\s]{1,30})',
        r'\b(Capital\s+\w[\w\s]{1,30})',
        r'\b(Cash\s+[Ff]low[\w\s]{0,20})',
        r'\b(EBITDA[\w\s]{0,20})',
        r'\b(Revenue[\w\s]{0,20})',
        r'\b(Cost\s+of\s+\w[\w\s]{1,20})',
        r'\b(Depreciation[\w\s]{0,20})',
        r'\b(Amortization[\w\s]{0,20})',
        r'\b(Interest\s+\w[\w\s]{1,20})',
        r'\b(Retained\s+[Ee]arnings[\w\s]{0,15})',
        r'\b(Working\s+[Cc]apital[\w\s]{0,15})',
        r'\b(Accounts\s+[RPpr]\w+[\w\s]{0,15})',
        r'\b(Profit\s+\w[\w\s]{1,20})',
        r'\b(Loss\s+\w[\w\s]{1,20})',
        r'\b(Margin[\w\s]{0,20})',
        r'\b(Variance[\w\s]{0,20})',
        r'\b(Budget[\w\s]{0,20})',
        r'\b(Forecast[\w\s]{0,20})',
    ]:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            candidate = match.group(1).strip()
            # Trim trailing common words
            candidate = re.sub(r'\s+(is|are|was|for|the|of|in|and|or)\s*$', '', candidate, flags=re.IGNORECASE)
            if len(candidate) > 3:
                names.append(candidate)

    return names


def extract_mentions_from_logic_dna(
    dna: Dict[str, Any],
    max_mentions: int = 50,
) -> List[EntityMention]:
    """Extract EntityMention objects from a single Logic DNA dict.

    Args:
        dna: A LogicDNA dict (as loaded from JSON).
        max_mentions: Cap mentions per file to prevent O(n²) explosion.

    Returns:
        List of EntityMention objects.
    """
    mentions: List[EntityMention] = []
    file_path = dna.get("file_path", "")
    archetype = dna.get("archetype", "Unknown")

    # Extract from formula_intents
    for intent in dna.get("formula_intents", []):
        bm = intent.get("business_meaning", "")
        cell_ref = intent.get("cell_ref", "")
        formula = intent.get("raw_formula", "")

        if not bm:
            continue

        names = _extract_names_from_text(bm)
        if not names:
            # Use the business_meaning as a fallback entity name.
            # For short texts, use as-is. For longer texts, take the
            # first clause (up to first comma, period, or dash).
            fallback = bm
            if len(bm) > 120:
                # Extract first clause
                clause_match = re.match(r'^(.{20,120}?)(?:[,.\-;:]|$)', bm)
                fallback = clause_match.group(1).strip() if clause_match else bm[:100]
            if 3 < len(fallback) <= 150:
                names = [fallback]

        for name in names:
            if len(mentions) >= max_mentions:
                break
            mentions.append(
                EntityMention(
                    file_path=file_path,
                    archetype=archetype,
                    raw_name=name,
                    normalized_name=_normalize_name(name),
                    cell_ref=cell_ref,
                    formula=formula,
                    business_meaning=bm,
                    source_type="formula_intent",
                )
            )

        if len(mentions) >= max_mentions:
            break

    # Extract from cross_references
    for xref in dna.get("cross_references", []):
        if len(mentions) >= max_mentions:
            break

        rel = xref.get("relationship", "")
        src = xref.get("source_cell", "")
        tgt = xref.get("target_cell", "")

        # Cross-references often name entities in the relationship text
        names = _extract_names_from_text(rel)
        for name in names:
            if len(mentions) >= max_mentions:
                break
            mentions.append(
                EntityMention(
                    file_path=file_path,
                    archetype=archetype,
                    raw_name=name,
                    normalized_name=_normalize_name(name),
                    cell_ref=f"{src} → {tgt}",
                    formula="",
                    business_meaning=rel,
                    source_type="cross_reference",
                )
            )

    return mentions


def load_and_extract_mentions(
    logic_dna_dir: str = "data/debate",
    max_mentions_per_file: int = 50,
) -> List[EntityMention]:
    """Load all Logic DNA files from a directory and extract mentions.

    Args:
        logic_dna_dir: Directory containing *_logic_dna.json files.
        max_mentions_per_file: Cap per file.

    Returns:
        Aggregated list of EntityMention objects from all files.
    """
    dna_dir = Path(logic_dna_dir)
    if not dna_dir.exists():
        logger.warning("Logic DNA directory not found: %s", logic_dna_dir)
        return []

    all_mentions: List[EntityMention] = []
    files = sorted(dna_dir.glob("*_logic_dna.json"))

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                dna = json.load(f)
            mentions = extract_mentions_from_logic_dna(
                dna, max_mentions=max_mentions_per_file
            )
            all_mentions.extend(mentions)
            logger.debug("Extracted %d mentions from %s", len(mentions), fp.name)
        except Exception as exc:
            logger.warning("Failed to extract mentions from %s: %s", fp.name, exc)

    logger.info(
        "Extracted %d total mentions from %d Logic DNA files",
        len(all_mentions),
        len(files),
    )
    return all_mentions
