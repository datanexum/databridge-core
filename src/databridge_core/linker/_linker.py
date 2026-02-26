"""EntityLinker — cross-file entity linking with Union-Find clustering."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from ._scorer import LINK_THRESHOLD, score_pair
from ._types import (
    ConflictType,
    EntityCluster,
    EntityConflict,
    EntityLink,
    EntityMap,
    EntityMention,
)

logger = logging.getLogger(__name__)

# ── Domain inference keywords ────────────────────────────────────────────────

_DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "revenue": ["revenue", "sales", "income", "turnover", "top_line"],
    "expense": ["expense", "cost", "spend", "opex", "cogs", "overhead"],
    "balance": ["balance", "asset", "liability", "equity", "net_worth"],
    "margin": ["margin", "gross_margin", "net_margin", "ebitda", "profit"],
    "account": ["account", "acct", "gl_", "ledger", "chart_of_accounts", "coa"],
    "tax": ["tax", "vat", "gst", "withholding"],
    "intercompany": ["interco", "intercompany", "elimination", "consolidation"],
    "headcount": ["headcount", "fte", "employee", "personnel", "salary", "wage"],
}


def _infer_domain(mentions: List[EntityMention]) -> str:
    """Infer the business domain from a cluster's mention names/meanings."""
    text = " ".join(
        f"{m.normalized_name} {m.business_meaning.lower()}" for m in mentions
    )

    scores: Dict[str, int] = defaultdict(int)
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                scores[domain] += 1

    if not scores:
        return "general"
    return max(scores, key=scores.get)


# ── Union-Find ───────────────────────────────────────────────────────────────


class _UnionFind:
    """Disjoint-set / Union-Find with path compression and union by rank."""

    def __init__(self) -> None:
        self._parent: Dict[str, str] = {}
        self._rank: Dict[str, int] = {}

    def make_set(self, x: str) -> None:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0

    def find(self, x: str) -> str:
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])  # path compression
        return self._parent[x]

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        # Union by rank
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def clusters(self) -> Dict[str, Set[str]]:
        """Return mapping of root → set of member IDs."""
        groups: Dict[str, Set[str]] = defaultdict(set)
        for x in self._parent:
            groups[self.find(x)].add(x)
        return dict(groups)


# ── Conflict detection ───────────────────────────────────────────────────────


def _detect_conflicts(
    cluster_id: str,
    mentions: List[EntityMention],
) -> List[EntityConflict]:
    """Detect conflicts within a cluster of entity mentions."""
    conflicts: List[EntityConflict] = []

    # Group by formula (skip empty)
    formula_groups: Dict[str, List[EntityMention]] = defaultdict(list)
    for m in mentions:
        if m.formula:
            formula_groups[m.formula].append(m)

    # If more than one distinct formula pattern, flag formula mismatch
    if len(formula_groups) > 1:
        # Check if formulas are meaningfully different (not just different ranges)
        unique_patterns = set()
        for formula in formula_groups:
            # Normalize: strip cell references, keep function structure
            pattern = re.sub(r'[A-Z]+\d+(?::[A-Z]+\d+)?', 'REF', formula)
            unique_patterns.add(pattern)

        if len(unique_patterns) > 1:
            conflicts.append(
                EntityConflict(
                    cluster_id=cluster_id,
                    conflict_type=ConflictType.FORMULA_MISMATCH,
                    description=f"Cluster has {len(formula_groups)} distinct formula patterns across files",
                    mention_ids=[m.mention_id for ms in formula_groups.values() for m in ms],
                    severity="medium",
                )
            )

    # Check for sign reversal (one file has SUM, another has negation)
    has_positive = any("SUM" in m.formula.upper() for m in mentions if m.formula)
    has_negative = any(
        m.formula.startswith("-") or "SUBTRACT" in m.business_meaning.upper()
        for m in mentions
        if m.formula or m.business_meaning
    )
    if has_positive and has_negative:
        conflicts.append(
            EntityConflict(
                cluster_id=cluster_id,
                conflict_type=ConflictType.SIGN_REVERSAL,
                description="Potential sign reversal: some mentions are additive, others subtractive",
                mention_ids=[m.mention_id for m in mentions],
                severity="high",
            )
        )

    return conflicts


# ── Main linker ──────────────────────────────────────────────────────────────


class EntityLinker:
    """Cross-file entity linker using pairwise scoring and Union-Find clustering."""

    def __init__(
        self,
        threshold: float = LINK_THRESHOLD,
        max_mentions_per_file: int = 50,
    ) -> None:
        self.threshold = threshold
        self.max_mentions_per_file = max_mentions_per_file

    def link(self, mentions: List[EntityMention]) -> EntityMap:
        """Link entity mentions into clusters.

        Performs cross-file pairwise comparison, builds Union-Find clusters,
        detects conflicts, and selects canonical names.

        Args:
            mentions: List of EntityMention objects from all files.

        Returns:
            EntityMap with clusters, links, and conflict information.
        """
        if not mentions:
            return EntityMap()

        # Index mentions by ID
        by_id: Dict[str, EntityMention] = {m.mention_id: m for m in mentions}

        # Group by file for cross-file pairing
        by_file: Dict[str, List[EntityMention]] = defaultdict(list)
        for m in mentions:
            if len(by_file[m.file_path]) < self.max_mentions_per_file:
                by_file[m.file_path].append(m)

        files = sorted(by_file.keys())
        file_count = len(files)

        # Union-Find
        uf = _UnionFind()
        for m in mentions:
            uf.make_set(m.mention_id)

        # Collect all links above threshold
        all_links: List[EntityLink] = []

        # Cross-file pairwise comparison only
        for i in range(file_count):
            for j in range(i + 1, file_count):
                for ma in by_file[files[i]]:
                    for mb in by_file[files[j]]:
                        composite, components, strategies = score_pair(ma, mb)
                        if composite >= self.threshold:
                            link = EntityLink(
                                mention_a_id=ma.mention_id,
                                mention_b_id=mb.mention_id,
                                composite_score=composite,
                                component_scores=components,
                                strategies_used=strategies,
                            )
                            all_links.append(link)
                            uf.union(ma.mention_id, mb.mention_id)

        # Build clusters from Union-Find
        uf_clusters = uf.clusters()
        clusters: List[EntityCluster] = []

        for root, member_ids in uf_clusters.items():
            if len(member_ids) < 2:
                # Skip singletons (no cross-file links)
                continue

            cluster_mentions = [by_id[mid] for mid in member_ids if mid in by_id]
            cluster_links = [
                lnk for lnk in all_links
                if lnk.mention_a_id in member_ids or lnk.mention_b_id in member_ids
            ]

            # Canonical name: from the mention with the highest link score
            canonical = _select_canonical_name(cluster_mentions, cluster_links, by_id)

            # Detect conflicts
            cluster_id = f"cluster_{root[:12]}"
            conflicts = _detect_conflicts(cluster_id, cluster_mentions)

            # Domain
            domain = _infer_domain(cluster_mentions)

            # Confidence: average of link scores
            avg_conf = (
                sum(lnk.composite_score for lnk in cluster_links) / len(cluster_links)
                if cluster_links
                else 0.0
            )

            # File count
            unique_files = len({m.file_path for m in cluster_mentions})

            cluster = EntityCluster(
                cluster_id=cluster_id,
                canonical_name=canonical,
                domain=domain,
                mentions=cluster_mentions,
                links=cluster_links,
                conflicts=conflicts,
                avg_confidence=round(avg_conf, 4),
                file_count=unique_files,
            )
            clusters.append(cluster)

        # Filter out oversized clusters (likely over-linked)
        MAX_CLUSTER_SIZE = 100
        oversized = [c for c in clusters if len(c.mentions) > MAX_CLUSTER_SIZE]
        if oversized:
            logger.warning(
                "Removed %d oversized clusters (>%d mentions): %s",
                len(oversized),
                MAX_CLUSTER_SIZE,
                ", ".join(f"{c.canonical_name[:40]}({len(c.mentions)})" for c in oversized),
            )
            clusters = [c for c in clusters if len(c.mentions) <= MAX_CLUSTER_SIZE]

        # Sort clusters by size (descending) then by canonical name
        clusters.sort(key=lambda c: (-len(c.mentions), c.canonical_name))

        entity_map = EntityMap(
            clusters=clusters,
            total_mentions=sum(len(c.mentions) for c in clusters),
            total_clusters=len(clusters),
            total_links=sum(len(c.links) for c in clusters),
            total_conflicts=sum(len(c.conflicts) for c in clusters),
            files_processed=file_count,
        )

        logger.info(
            "Entity linking complete: %d clusters, %d links, %d conflicts from %d mentions",
            entity_map.total_clusters,
            entity_map.total_links,
            entity_map.total_conflicts,
            len(mentions),
        )

        return entity_map


def _select_canonical_name(
    mentions: List[EntityMention],
    links: List[EntityLink],
    by_id: Dict[str, EntityMention],
) -> str:
    """Select the canonical name for a cluster.

    Strategy: pick the mention that participates in the highest-scoring link.
    Tie-break by shortest raw_name (prefer concise names).
    """
    if not mentions:
        return ""

    if not links:
        # No links — pick the shortest non-empty raw_name
        named = [m for m in mentions if m.raw_name]
        if named:
            return min(named, key=lambda m: len(m.raw_name)).raw_name
        return mentions[0].normalized_name

    # Find the mention involved in the highest-scoring link
    best_score = 0.0
    best_mention_id = mentions[0].mention_id
    for lnk in links:
        if lnk.composite_score > best_score:
            best_score = lnk.composite_score
            best_mention_id = lnk.mention_a_id

    m = by_id.get(best_mention_id)
    return m.raw_name if m and m.raw_name else mentions[0].raw_name
