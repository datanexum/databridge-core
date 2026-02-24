"""DataBridge Reconciler -- Hashing, diffing, fuzzy matching, and merging.

Public API:
    compare_hashes       -- Row-level hash comparison between two CSVs
    get_orphan_details   -- Retrieve orphan records
    get_conflict_details -- Retrieve conflicting records with diff analysis
    fuzzy_match_columns  -- RapidFuzz matching between two columns
    fuzzy_deduplicate    -- Find duplicate values within a column
    merge_sources        -- Merge two CSVs on key columns
    diff_lists / diff_dicts -- Text and data comparison
    explain_diff         -- Human-readable diff explanation
    find_similar_strings -- Find similar strings from candidates
    compute_similarity   -- String similarity ratio (0.0-1.0)
    transform_column     -- Apply string transformation to a CSV column
"""

from .differ import (
    compute_similarity,
    get_matching_blocks,
    get_opcodes,
    unified_diff,
    context_diff,
    ndiff_text,
    diff_lists,
    diff_dicts,
    diff_values_paired,
    explain_diff,
    find_close_matches,
    quick_ratio,
    real_quick_ratio,
    # Types
    DiffOpcode,
    MatchingBlock,
    ListDiffResult,
    DictDiffResult,
    DictValueDiff,
    SimilarStringMatch,
    TransformDiff,
)

from .hasher import (
    compare_hashes,
    get_orphan_details,
    get_conflict_details,
)

from .fuzzy import (
    fuzzy_match_columns,
    fuzzy_deduplicate,
)

from .merger import merge_sources

from .transform import transform_column

# Aliases matching the MCP tool names
find_similar_strings = find_close_matches

__all__ = [
    # Hasher
    "compare_hashes",
    "get_orphan_details",
    "get_conflict_details",
    # Differ
    "compute_similarity",
    "get_matching_blocks",
    "get_opcodes",
    "unified_diff",
    "context_diff",
    "ndiff_text",
    "diff_lists",
    "diff_dicts",
    "diff_values_paired",
    "explain_diff",
    "find_close_matches",
    "find_similar_strings",
    "quick_ratio",
    "real_quick_ratio",
    # Fuzzy
    "fuzzy_match_columns",
    "fuzzy_deduplicate",
    # Merge
    "merge_sources",
    # Transform
    "transform_column",
    # Types
    "DiffOpcode",
    "MatchingBlock",
    "ListDiffResult",
    "DictDiffResult",
    "DictValueDiff",
    "SimilarStringMatch",
    "TransformDiff",
]
