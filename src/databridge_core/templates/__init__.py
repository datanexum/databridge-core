"""DataBridge Templates — Template, skill, and knowledge base management."""

from ._types import (
    ClientKnowledge,
    ClientMetadata,
    CustomPrompt,
    FinancialTemplate,
    HierarchyType,
    MappingHint,
    SkillDefinition,
    SkillDomain,
    TemplateCategory,
    TemplateDomain,
    TemplateHierarchy,
    TemplateMetadata,
    TemplateRecommendation,
)
from ._service import TemplateService

__all__ = [
    "TemplateService",
    "FinancialTemplate",
    "TemplateHierarchy",
    "MappingHint",
    "TemplateMetadata",
    "TemplateDomain",
    "HierarchyType",
    "SkillDefinition",
    "SkillDomain",
    "TemplateCategory",
    "ClientKnowledge",
    "ClientMetadata",
    "CustomPrompt",
    "TemplateRecommendation",
]
