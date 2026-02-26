"""Pydantic models for the cross-file entity linker."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


def _new_id(prefix: str = "link") -> str:
    """Generate a short unique ID with the given prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


# ── Enums ────────────────────────────────────────────────────────────────────


class MatchStrategy(str, Enum):
    """Strategy used to score a pair of entity mentions."""

    NAME_SIMILARITY = "name_similarity"
    FORMULA_SIMILARITY = "formula_similarity"
    BUSINESS_MEANING = "business_meaning"
    ARCHETYPE_COMPATIBILITY = "archetype_compatibility"


class ConflictType(str, Enum):
    """Types of conflicts that can occur within an entity cluster."""

    FORMULA_MISMATCH = "formula_mismatch"
    SIGN_REVERSAL = "sign_reversal"
    AGGREGATION_MISMATCH = "aggregation_mismatch"
    DOMAIN_MISMATCH = "domain_mismatch"


# ── Core models ──────────────────────────────────────────────────────────────


class EntityMention(BaseModel):
    """A single mention of an entity in a Logic DNA file."""

    mention_id: str = Field(default_factory=lambda: _new_id("mention"))
    file_path: str = ""
    archetype: str = ""
    raw_name: str = ""  # e.g. "Revenue_West" or "Acct_101"
    normalized_name: str = ""  # lowered, non-alnum → underscore
    cell_ref: str = ""  # e.g. "Sheet1!B12"
    formula: str = ""  # e.g. "=SUM(B2:B11)"
    business_meaning: str = ""  # from FormulaIntent
    source_type: str = ""  # "formula_intent" | "cross_reference"


class EntityLink(BaseModel):
    """A scored link between two entity mentions."""

    link_id: str = Field(default_factory=lambda: _new_id("link"))
    mention_a_id: str = ""
    mention_b_id: str = ""
    composite_score: float = 0.0
    component_scores: Dict[str, float] = Field(default_factory=dict)
    strategies_used: List[str] = Field(default_factory=list)


class EntityConflict(BaseModel):
    """A conflict detected within an entity cluster."""

    conflict_id: str = Field(default_factory=lambda: _new_id("conflict"))
    cluster_id: str = ""
    conflict_type: ConflictType = ConflictType.FORMULA_MISMATCH
    description: str = ""
    mention_ids: List[str] = Field(default_factory=list)
    severity: str = "medium"  # "low" | "medium" | "high"


class EntityCluster(BaseModel):
    """A cluster of linked entity mentions representing the same concept."""

    cluster_id: str = Field(default_factory=lambda: _new_id("cluster"))
    canonical_name: str = ""
    domain: str = ""  # "revenue", "expense", "balance", etc.
    mentions: List[EntityMention] = Field(default_factory=list)
    links: List[EntityLink] = Field(default_factory=list)
    conflicts: List[EntityConflict] = Field(default_factory=list)
    avg_confidence: float = 0.0
    file_count: int = 0


class EntityMap(BaseModel):
    """Complete entity map produced by the linker."""

    map_id: str = Field(default_factory=lambda: _new_id("map"))
    clusters: List[EntityCluster] = Field(default_factory=list)
    total_mentions: int = 0
    total_clusters: int = 0
    total_links: int = 0
    total_conflicts: int = 0
    files_processed: int = 0
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class LinkBatchSummary(BaseModel):
    """Summary statistics for an entity linking run."""

    total_mentions: int = 0
    total_clusters: int = 0
    total_links: int = 0
    total_conflicts: int = 0
    files_processed: int = 0
    domain_counts: Dict[str, int] = Field(default_factory=dict)
    avg_cluster_size: float = 0.0
    largest_cluster_size: int = 0
    duration_seconds: float = 0.0
