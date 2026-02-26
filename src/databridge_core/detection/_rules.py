"""Load detection rules from the Knowledge Base.

Converts KB nodes of type 'rule', 'standard', 'pattern', 'gap',
'fact', 'guardrail', or any node with a ``detection_rule`` property
block into executable :class:`DetectionRule` objects.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._types import DetectionRule, FindingType, Severity

logger = logging.getLogger(__name__)

DEFAULT_KNOWLEDGE_DIR = "data/knowledge"

# KB node types that are candidates for rule extraction.
_RULE_NODE_TYPES = frozenset({
    "rule",
    "standard",
    "pattern",
    "gap",
    "capability_gap",
    "fact",
    "guardrail",
    "formula_domain",
    "hierarchy_type",
    "complexity_pattern",
})

# Maps KB property keys to FindingType (case-insensitive substring match).
_FINDING_TYPE_MAP: Dict[str, FindingType] = {
    "sign_reversal": FindingType.SIGN_REVERSAL,
    "sign_convention": FindingType.SIGN_REVERSAL,
    "rounding": FindingType.ROUNDING_DISCREPANCY,
    "rounding_tolerance": FindingType.ROUNDING_DISCREPANCY,
    "missing": FindingType.MISSING_ACCOUNT,
    "missing_account": FindingType.MISSING_ACCOUNT,
    "duplicate": FindingType.DUPLICATE_ACCOUNT,
    "duplicate_account": FindingType.DUPLICATE_ACCOUNT,
    "hierarchy": FindingType.HIERARCHY_BREAK,
    "hierarchy_break": FindingType.HIERARCHY_BREAK,
    "naming": FindingType.NAMING_VIOLATION,
    "naming_convention": FindingType.NAMING_VIOLATION,
    "balance": FindingType.BALANCE_MISMATCH,
    "trial_balance": FindingType.BALANCE_MISMATCH,
    "formula": FindingType.FORMULA_ANOMALY,
    "formula_anomaly": FindingType.FORMULA_ANOMALY,
    "classification": FindingType.CLASSIFICATION_ERROR,
    "classification_error": FindingType.CLASSIFICATION_ERROR,
}

# Maps KB severity strings (case-insensitive) to Severity enum.
_SEVERITY_MAP: Dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "high": Severity.HIGH,
    "medium": Severity.MEDIUM,
    "low": Severity.LOW,
    "info": Severity.INFO,
}


def load_detection_rules(
    knowledge_dir: str = DEFAULT_KNOWLEDGE_DIR,
    rule_types: Optional[List[str]] = None,
) -> List[DetectionRule]:
    """Load detection rules from Knowledge Base JSON files.

    Scans all ``*.json`` files in *knowledge_dir* and converts KB nodes
    whose ``type`` is in :data:`_RULE_NODE_TYPES` (or whose ``properties``
    contain a ``detection_rule`` dict) into :class:`DetectionRule` objects.

    Args:
        knowledge_dir: Path to the knowledge base directory.
        rule_types: Optional list of KB node types to include.
            If ``None``, uses :data:`_RULE_NODE_TYPES`.

    Returns:
        List of :class:`DetectionRule` instances ready for detection.
    """
    kb_dir = Path(knowledge_dir)
    if not kb_dir.exists():
        logger.warning(
            "Knowledge directory %s does not exist; returning empty rules", kb_dir
        )
        return []

    json_files = sorted(kb_dir.glob("*.json"))
    if not json_files:
        logger.warning("No JSON files found in %s; returning empty rules", kb_dir)
        return []

    accepted_types = frozenset(rule_types) if rule_types else _RULE_NODE_TYPES
    all_rules: List[DetectionRule] = []

    for path in json_files:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", path, exc)
            continue

        if not isinstance(raw, dict):
            continue

        nodes_list = raw.get("nodes")
        if not isinstance(nodes_list, list):
            continue

        source_file = str(path).replace("\\", "/")

        for node in nodes_list:
            if not isinstance(node, dict) or "id" not in node:
                continue

            node_type = node.get("type", "")
            props = node.get("properties", {}) or {}

            # Check explicit detection_rule block first (highest priority)
            explicit_rule = props.get("detection_rule")
            if isinstance(explicit_rule, dict):
                rule = _build_rule_from_explicit(node, explicit_rule, source_file)
                if rule is not None:
                    all_rules.append(rule)
                continue

            # Otherwise check if the node type qualifies
            if node_type not in accepted_types:
                continue

            rule = _build_rule_from_node(node, source_file)
            if rule is not None:
                all_rules.append(rule)

    logger.info(
        "Loaded %d detection rules from %d KB files in %s",
        len(all_rules),
        len(json_files),
        kb_dir,
    )
    return all_rules


# -- Internal builders --------------------------------------------------------


def _build_rule_from_explicit(
    node: Dict[str, Any],
    rule_block: Dict[str, Any],
    source_file: str,
) -> Optional[DetectionRule]:
    """Build a DetectionRule from an explicit ``detection_rule`` property block.

    Expected block schema::

        {
            "pattern": "<regex>",
            "field_targets": ["col_a", "col_b"],
            "finding_type": "sign_reversal",
            "severity": "high",
            "standard": "GAAP"
        }
    """
    pattern = rule_block.get("pattern", "")
    if not pattern:
        return None

    # Validate regex
    try:
        re.compile(pattern)
    except re.error as exc:
        logger.warning(
            "Invalid regex in explicit rule for node %s: %s",
            node.get("id"),
            exc,
        )
        return None

    return DetectionRule(
        rule_id=f"rule_{node.get('id', '')}",
        name=node.get("name", ""),
        standard=rule_block.get("standard", ""),
        finding_type=_resolve_finding_type(rule_block.get("finding_type", "")),
        pattern=pattern,
        field_targets=rule_block.get("field_targets", []),
        severity=_resolve_severity(rule_block.get("severity", "medium")),
        description=rule_block.get("description", node.get("name", "")),
        evidence_nodes=[node.get("id", "")],
        kb_source_file=source_file,
        confidence=node.get("confidence", 0.9),
        tags=node.get("tags", []) or [],
        properties=node.get("properties", {}) or {},
    )


def _build_rule_from_node(
    node: Dict[str, Any],
    source_file: str,
) -> Optional[DetectionRule]:
    """Build a DetectionRule by inferring a pattern from a KB node.

    For nodes without an explicit ``detection_rule`` block, we synthesize
    a rule from the node metadata:

    - ``gap`` / ``capability_gap``: Build a regex from ``gap_id`` and
      missing-component keywords.
    - ``hierarchy_type``: Build a pattern from sheet-name patterns.
    - ``formula_domain``: Build a pattern from function names.
    - ``fact`` / ``pattern`` / ``standard`` / ``rule``: Build from
      description keywords.
    """
    node_type = node.get("type", "")
    props = node.get("properties", {}) or {}
    node_id = node.get("id", "")
    name = node.get("name", "")
    tags = node.get("tags", []) or []

    # Infer pattern and finding type based on node type
    pattern = ""
    field_targets: List[str] = []
    finding_type = FindingType.CUSTOM
    severity = Severity.MEDIUM
    description = props.get("description", name)

    if node_type in ("gap", "capability_gap"):
        # Gap nodes: build pattern from gap_id or missing_components
        gap_id = props.get("gap_id", "")
        severity = _resolve_severity(props.get("severity", "medium"))
        if gap_id:
            # Create a pattern that flags accounts or fields mentioning this gap
            keywords = _extract_keywords(name, description)
            if keywords:
                pattern = r"(?i)\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b"
                finding_type = FindingType.CLASSIFICATION_ERROR
                field_targets = ["account_name", "description", "category"]

    elif node_type == "hierarchy_type":
        # Hierarchy nodes: detect expected sheet/field patterns
        sheet_patterns = props.get("sheet_name_patterns", [])
        if sheet_patterns:
            escaped = [re.escape(p) for p in sheet_patterns]
            pattern = r"(?i)\b(?:" + "|".join(escaped) + r")\b"
            finding_type = FindingType.HIERARCHY_BREAK
            field_targets = ["sheet_name", "hierarchy_type", "category"]
            severity = Severity.INFO  # informational match

    elif node_type == "formula_domain":
        # Formula nodes: detect function usage patterns
        functions = props.get("functions", [])
        if functions:
            escaped = [re.escape(fn) for fn in functions]
            pattern = r"(?i)\b(?:" + "|".join(escaped) + r")\b"
            finding_type = FindingType.FORMULA_ANOMALY
            field_targets = ["formula", "formula_text", "expression"]
            severity = Severity.INFO

    elif node_type == "fact":
        # Facts with gap category indicate detectable issues
        category = props.get("category", "")
        if category == "gap":
            keywords = _extract_keywords(name, description)
            if keywords:
                pattern = r"(?i)\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b"
                finding_type = FindingType.CLASSIFICATION_ERROR
                field_targets = ["account_name", "description"]

    elif node_type in ("rule", "standard", "pattern", "guardrail", "terminology"):
        # Direct rule nodes: prefer detection_pattern/detection_field from KB
        rule_pattern = (
            props.get("detection_pattern", "")
            or props.get("pattern", "")
        )
        if rule_pattern:
            try:
                re.compile(rule_pattern)
                pattern = rule_pattern
            except re.error:
                pass
        if not pattern:
            keywords = _extract_keywords(name, description)
            if keywords:
                pattern = r"(?i)\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b"

        finding_type = _infer_finding_type_from_tags(tags, name)
        # Use detection_field from KB (exact CSV column name)
        detection_field = props.get("detection_field", "")
        if detection_field:
            field_targets = [detection_field]
        else:
            field_targets = props.get("field_targets", ["account_name", "description"])
        severity = _resolve_severity(props.get("severity", "medium"))

    elif node_type == "complexity_pattern":
        # Complexity pattern: detect complexity indicators
        keywords = _extract_keywords(name, description)
        if keywords:
            pattern = r"(?i)\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b"
            finding_type = FindingType.FORMULA_ANOMALY
            field_targets = ["formula", "expression", "description"]
            severity = Severity.LOW

    if not pattern:
        return None

    return DetectionRule(
        rule_id=f"rule_{node_id}",
        name=name,
        standard=props.get("standard", ""),
        finding_type=finding_type,
        pattern=pattern,
        field_targets=field_targets,
        severity=severity,
        description=str(description),
        evidence_nodes=[node_id],
        kb_source_file=source_file,
        confidence=node.get("confidence", 0.9),
        tags=tags,
        properties=props,
    )


# -- Helpers ------------------------------------------------------------------


def _resolve_finding_type(raw: str) -> FindingType:
    """Resolve a raw string to a FindingType enum value."""
    if not raw:
        return FindingType.CUSTOM
    key = raw.lower().strip()
    if key in _FINDING_TYPE_MAP:
        return _FINDING_TYPE_MAP[key]
    # Try direct enum match
    try:
        return FindingType(key)
    except ValueError:
        return FindingType.CUSTOM


def _resolve_severity(raw: str) -> Severity:
    """Resolve a raw string to a Severity enum value."""
    if not raw:
        return Severity.MEDIUM
    key = raw.lower().strip()
    return _SEVERITY_MAP.get(key, Severity.MEDIUM)


def _infer_finding_type_from_tags(
    tags: List[str], name: str
) -> FindingType:
    """Infer a FindingType from tags and name using keyword matching."""
    combined = " ".join(tags).lower() + " " + name.lower()
    for keyword, finding_type in _FINDING_TYPE_MAP.items():
        if keyword in combined:
            return finding_type
    return FindingType.CUSTOM


def _extract_keywords(name: str, description: str) -> List[str]:
    """Extract meaningful keywords from name and description for regex building.

    Filters out generic stopwords and returns at most 6 keywords that are
    3+ characters long.
    """
    _STOPWORDS = frozenset({
        "the", "and", "for", "with", "from", "that", "this", "are", "was",
        "has", "have", "been", "not", "but", "its", "any", "all", "can",
        "only", "into", "per", "via", "each", "use", "new", "one", "two",
    })

    words: List[str] = []
    for text in (name, str(description)):
        # Split on non-alphanumeric
        tokens = re.split(r"[^a-zA-Z0-9]+", text)
        for token in tokens:
            token_lower = token.lower()
            if (
                len(token_lower) >= 3
                and token_lower not in _STOPWORDS
                and token_lower not in words
            ):
                words.append(token_lower)

    # Return most distinctive keywords (first 6)
    return words[:6]
