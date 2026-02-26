"""Cross-file entity linker for Logic DNA outputs.

Public API
----------
- ``link_entities(logic_dna_dir, ...)`` — resolve entities across all Logic DNA files
- ``get_entity_map(output_dir)`` — retrieve a previously generated entity map
- ``get_entity_cluster(cluster_id, output_dir)`` — retrieve a single cluster
- ``get_link_summary(output_dir)`` — summary statistics
"""
from __future__ import annotations

import difflib
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._extractor import load_and_extract_mentions
from ._linker import EntityLinker
from ._types import (
    ConflictType,
    EntityCluster,
    EntityConflict,
    EntityLink,
    EntityMap,
    EntityMention,
    LinkBatchSummary,
    MatchStrategy,
)

logger = logging.getLogger(__name__)

__all__ = [
    # Public functions
    "link_entities",
    "get_entity_map",
    "get_entity_cluster",
    "get_link_summary",
    "find_entity",
    # Types (re-exported for convenience)
    "EntityMention",
    "EntityLink",
    "EntityCluster",
    "EntityConflict",
    "EntityMap",
    "LinkBatchSummary",
    "MatchStrategy",
    "ConflictType",
]


def link_entities(
    logic_dna_dir: str = "data/debate",
    output_dir: str = "data/linker",
    threshold: float = 0.60,
    max_mentions_per_file: int = 50,
) -> Dict[str, Any]:
    """Resolve entities across all Logic DNA files in a directory.

    Args:
        logic_dna_dir: Directory containing *_logic_dna.json files.
        output_dir: Where to write the entity map and clusters.
        threshold: Minimum composite score to link two mentions (default 0.60).
        max_mentions_per_file: Cap mentions per file (default 50).

    Returns:
        Dict with summary, entity_map path, and sample clusters (≤10).
    """
    t0 = time.time()

    # 1. Extract mentions
    mentions = load_and_extract_mentions(
        logic_dna_dir=logic_dna_dir,
        max_mentions_per_file=max_mentions_per_file,
    )

    if not mentions:
        return {
            "error": f"No entity mentions found in {logic_dna_dir}",
            "total_mentions": 0,
        }

    # 2. Link
    linker = EntityLinker(threshold=threshold, max_mentions_per_file=max_mentions_per_file)
    entity_map = linker.link(mentions)

    duration = time.time() - t0

    # 3. Persist
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Main entity map
    map_file = out_dir / "entity_map.json"
    map_data = entity_map.model_dump(mode="json")
    with open(map_file, "w", encoding="utf-8") as f:
        json.dump(map_data, f, indent=2, default=str)

    # Per-cluster JSONL for large-scale querying
    clusters_file = out_dir / "entity_clusters.jsonl"
    with open(clusters_file, "w", encoding="utf-8") as f:
        for cluster in entity_map.clusters:
            f.write(json.dumps(cluster.model_dump(mode="json"), default=str) + "\n")

    # 4. Build summary
    domain_counts: Dict[str, int] = {}
    largest = 0
    for cluster in entity_map.clusters:
        domain_counts[cluster.domain] = domain_counts.get(cluster.domain, 0) + 1
        if len(cluster.mentions) > largest:
            largest = len(cluster.mentions)

    summary = LinkBatchSummary(
        total_mentions=entity_map.total_mentions,
        total_clusters=entity_map.total_clusters,
        total_links=entity_map.total_links,
        total_conflicts=entity_map.total_conflicts,
        files_processed=entity_map.files_processed,
        domain_counts=domain_counts,
        avg_cluster_size=round(
            entity_map.total_mentions / max(entity_map.total_clusters, 1), 2
        ),
        largest_cluster_size=largest,
        duration_seconds=round(duration, 2),
    )

    # Return summary + sample (≤10 clusters per context limit)
    sample_clusters = [
        {
            "cluster_id": c.cluster_id,
            "canonical_name": c.canonical_name,
            "domain": c.domain,
            "mention_count": len(c.mentions),
            "file_count": c.file_count,
            "avg_confidence": c.avg_confidence,
            "conflict_count": len(c.conflicts),
        }
        for c in entity_map.clusters[:10]
    ]

    return {
        "summary": summary.model_dump(mode="json"),
        "sample_clusters": sample_clusters,
        "output_files": {
            "entity_map": str(map_file),
            "entity_clusters": str(clusters_file),
        },
    }


def get_entity_map(output_dir: str = "data/linker") -> Dict[str, Any]:
    """Retrieve a previously generated entity map.

    Args:
        output_dir: Directory containing entity_map.json.

    Returns:
        Entity map dict, or error dict if not found.
    """
    map_file = Path(output_dir) / "entity_map.json"
    if not map_file.exists():
        return {"error": f"Entity map not found at {map_file}"}

    with open(map_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Context-limit: return summary + first 10 clusters (abridged)
    clusters = data.get("clusters", [])
    abridged_clusters = []
    for c in clusters[:10]:
        abridged_clusters.append({
            "cluster_id": c.get("cluster_id"),
            "canonical_name": c.get("canonical_name"),
            "domain": c.get("domain"),
            "mention_count": len(c.get("mentions", [])),
            "file_count": c.get("file_count", 0),
            "avg_confidence": c.get("avg_confidence", 0),
            "conflict_count": len(c.get("conflicts", [])),
        })

    return {
        "map_id": data.get("map_id"),
        "total_mentions": data.get("total_mentions", 0),
        "total_clusters": data.get("total_clusters", 0),
        "total_links": data.get("total_links", 0),
        "total_conflicts": data.get("total_conflicts", 0),
        "files_processed": data.get("files_processed", 0),
        "timestamp": data.get("timestamp"),
        "clusters": abridged_clusters,
        "note": f"Showing {len(abridged_clusters)} of {len(clusters)} clusters. Use linker_get_cluster for full details.",
    }


def get_entity_cluster(
    cluster_id: str,
    output_dir: str = "data/linker",
) -> Dict[str, Any]:
    """Retrieve a single cluster by ID.

    Args:
        cluster_id: The cluster_id to look up.
        output_dir: Directory containing entity_map.json.

    Returns:
        Full cluster dict, or error dict if not found.
    """
    map_file = Path(output_dir) / "entity_map.json"
    if not map_file.exists():
        return {"error": f"Entity map not found at {map_file}"}

    with open(map_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    for cluster in data.get("clusters", []):
        if cluster.get("cluster_id") == cluster_id:
            return cluster

    return {"error": f"Cluster not found: {cluster_id}"}


def get_link_summary(output_dir: str = "data/linker") -> Dict[str, Any]:
    """Compute summary statistics from the entity map.

    Args:
        output_dir: Directory containing entity_map.json.

    Returns:
        Summary dict with aggregate statistics.
    """
    map_file = Path(output_dir) / "entity_map.json"
    if not map_file.exists():
        return {"error": f"Entity map not found at {map_file}", "total_clusters": 0}

    with open(map_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    clusters = data.get("clusters", [])
    if not clusters:
        return {"total_clusters": 0, "message": "No entity clusters found"}

    domain_counts: Dict[str, int] = {}
    total_mentions = 0
    total_conflicts = 0
    largest = 0

    for c in clusters:
        d = c.get("domain", "general")
        domain_counts[d] = domain_counts.get(d, 0) + 1
        mc = len(c.get("mentions", []))
        total_mentions += mc
        total_conflicts += len(c.get("conflicts", []))
        if mc > largest:
            largest = mc

    return {
        "total_clusters": len(clusters),
        "total_mentions": total_mentions,
        "total_links": data.get("total_links", 0),
        "total_conflicts": total_conflicts,
        "files_processed": data.get("files_processed", 0),
        "domain_counts": domain_counts,
        "avg_cluster_size": round(total_mentions / max(len(clusters), 1), 2),
        "largest_cluster_size": largest,
    }


def find_entity(
    query: str,
    output_dir: str = "data/linker",
    top_k: int = 5,
) -> Dict[str, Any]:
    """Fuzzy search for an entity across clusters.

    Args:
        query: The entity name or description to search for.
        output_dir: Directory containing entity_map.json.
        top_k: Number of results to return (default 5).

    Returns:
        Dict with matching clusters and their similarity scores.
    """
    map_file = Path(output_dir) / "entity_map.json"
    if not map_file.exists():
        return {"error": f"Entity map not found at {map_file}"}

    with open(map_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    query_lower = query.lower()
    results: List[Dict[str, Any]] = []

    for cluster in data.get("clusters", []):
        canonical = cluster.get("canonical_name", "")
        # Score against canonical name
        score = difflib.SequenceMatcher(
            None, query_lower, canonical.lower()
        ).ratio()

        # Also check individual mention names for better match
        for mention in cluster.get("mentions", []):
            raw = mention.get("raw_name", "")
            m_score = difflib.SequenceMatcher(
                None, query_lower, raw.lower()
            ).ratio()
            if m_score > score:
                score = m_score

        if score > 0.3:  # liberal threshold for fuzzy search
            results.append({
                "cluster_id": cluster.get("cluster_id"),
                "canonical_name": canonical,
                "domain": cluster.get("domain"),
                "similarity": round(score, 4),
                "mention_count": len(cluster.get("mentions", [])),
                "file_count": cluster.get("file_count", 0),
            })

    results.sort(key=lambda r: -r["similarity"])
    return {
        "query": query,
        "results": results[:top_k],
        "total_matches": len(results),
    }
