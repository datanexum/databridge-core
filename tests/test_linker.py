"""Tests for the entity linker module."""

import pytest

from databridge_core.linker._types import (
    EntityMention,
    EntityLink,
    EntityCluster,
    EntityMap,
    MatchStrategy,
    ConflictType,
)
from databridge_core.linker._scorer import (
    _synonym_score,
    _name_similarity,
    _archetype_compatibility,
    score_pair,
    LINK_THRESHOLD,
)
from databridge_core.linker._extractor import (
    _normalize_name,
    _extract_names_from_text,
    extract_mentions_from_logic_dna,
)
from databridge_core.linker._linker import EntityLinker


# ── Types ────────────────────────────────────────────────────────────────────


def test_entity_mention_defaults():
    m = EntityMention()
    assert m.mention_id.startswith("mention_")
    assert m.raw_name == ""


def test_entity_map_defaults():
    em = EntityMap()
    assert em.total_clusters == 0
    assert em.map_id.startswith("map_")


# ── Scorer ───────────────────────────────────────────────────────────────────


def test_synonym_exact():
    assert _synonym_score("revenue", "revenue") == 1.0


def test_synonym_match():
    assert _synonym_score("revenue", "net sales") == 1.0
    assert _synonym_score("cogs", "cost of goods sold") == 1.0


def test_synonym_no_match():
    assert _synonym_score("revenue", "expense") == 0.0


def test_name_similarity_identical():
    a = EntityMention(raw_name="Revenue", normalized_name="revenue")
    b = EntityMention(raw_name="Revenue", normalized_name="revenue")
    assert _name_similarity(a, b) == 1.0


def test_archetype_same_group():
    a = EntityMention(archetype="Financial Report")
    b = EntityMention(archetype="Financial Statement")
    assert _archetype_compatibility(a, b) == 1.0


def test_archetype_cross_group():
    a = EntityMention(archetype="Financial Report")
    b = EntityMention(archetype="Dashboard")
    assert _archetype_compatibility(a, b) == 0.7


def test_score_pair_returns_tuple():
    a = EntityMention(raw_name="Revenue", normalized_name="revenue", archetype="Financial Report")
    b = EntityMention(raw_name="Net Sales", normalized_name="net_sales", archetype="Financial Statement")
    composite, components, strategies = score_pair(a, b)
    assert isinstance(composite, float)
    assert isinstance(components, dict)
    assert isinstance(strategies, list)
    assert composite > 0


def test_link_threshold():
    assert LINK_THRESHOLD == 0.65


# ── Extractor ────────────────────────────────────────────────────────────────


def test_normalize_name():
    assert _normalize_name("Revenue_West") == "revenue_west"
    assert _normalize_name("Total Q1 Revenue") == "total_q1_revenue"
    assert _normalize_name("  spaces  ") == "spaces"


def test_extract_quoted():
    names = _extract_names_from_text('Calculates "Revenue West" for Q1')
    assert "Revenue West" in names


def test_extract_camelcase():
    names = _extract_names_from_text("Uses RevenueWest metric")
    assert "RevenueWest" in names


def test_extract_accounting_patterns():
    names = _extract_names_from_text("Total Revenue for all regions")
    assert any("Total Revenue" in n for n in names)


def test_extract_mentions_from_dna():
    dna = {
        "file_path": "test.json",
        "archetype": "Financial Report",
        "formula_intents": [
            {
                "cell_ref": "B2",
                "raw_formula": "=SUM(B3:B10)",
                "business_meaning": 'Calculates "Total Revenue" for the period',
            }
        ],
        "cross_references": [],
    }
    mentions = extract_mentions_from_logic_dna(dna)
    assert len(mentions) > 0
    assert mentions[0].source_type == "formula_intent"


# ── Linker ───────────────────────────────────────────────────────────────────


def test_linker_empty():
    linker = EntityLinker()
    result = linker.link([])
    assert result.total_clusters == 0


def test_linker_cross_file():
    """Two mentions with identical names from different files should cluster."""
    m1 = EntityMention(
        file_path="file_a.json",
        archetype="Financial Report",
        raw_name="Revenue",
        normalized_name="revenue",
        business_meaning="Total revenue for the period",
    )
    m2 = EntityMention(
        file_path="file_b.json",
        archetype="Financial Statement",
        raw_name="Revenue",
        normalized_name="revenue",
        business_meaning="Total revenue for the period",
    )
    linker = EntityLinker(threshold=0.5)
    result = linker.link([m1, m2])
    assert result.total_clusters >= 1
    assert result.clusters[0].canonical_name == "Revenue"


def test_linker_no_within_file():
    """Two mentions from the SAME file should not cluster."""
    m1 = EntityMention(
        file_path="same_file.json",
        raw_name="Revenue",
        normalized_name="revenue",
    )
    m2 = EntityMention(
        file_path="same_file.json",
        raw_name="Revenue Copy",
        normalized_name="revenue_copy",
    )
    linker = EntityLinker()
    result = linker.link([m1, m2])
    assert result.total_clusters == 0
