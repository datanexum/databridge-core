"""Pydantic models for Templates, Skills, and Knowledge Base."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from enum import Enum


class TemplateDomain(str, Enum):
    """High-level domain categories for templates."""
    ACCOUNTING = "accounting"      # COA, GL structures, statutory reporting
    FINANCE = "finance"            # Budgeting, cost centers, management reporting
    OPERATIONS = "operations"      # Geographic, departmental, asset-based
    CUSTOM = "custom"              # User-created templates


class HierarchyType(str, Enum):
    """Types of hierarchies that can be built."""
    # Accounting hierarchies
    CHART_OF_ACCOUNTS = "chart_of_accounts"
    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"
    TRIAL_BALANCE = "trial_balance"

    # Finance hierarchies
    COST_CENTER = "cost_center"
    PROFIT_CENTER = "profit_center"
    BUDGET = "budget"
    FORECAST = "forecast"
    MANAGEMENT_REPORTING = "management_reporting"

    # Operations hierarchies
    GEOGRAPHIC = "geographic"
    DEPARTMENT = "department"
    LEGAL_ENTITY = "legal_entity"
    ASSET = "asset"
    ACQUISITION = "acquisition"
    PRODUCT = "product"
    PROJECT = "project"

    # Extensions
    SUBSTITUTE = "substitute"      # Replacement for another hierarchy
    EXTENSION = "extension"        # Extension of another hierarchy
    CUSTOM = "custom"


class SkillDomain(str, Enum):
    """Domain categories for AI skills."""
    ACCOUNTING = "accounting"
    FINANCE = "finance"
    OPERATIONS = "operations"
    GENERAL = "general"
    DATA_ENGINEERING = "data-engineering"


# Keep old enum for backward compatibility
class TemplateCategory(str, Enum):
    """Categories of financial statement templates (legacy, use HierarchyType instead)."""
    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"
    CUSTOM = "custom"


class MappingHint(BaseModel):
    """Hint for mapping source data to a hierarchy node."""
    pattern: str = Field(..., description="Pattern to match (e.g., '4*' for accounts starting with 4)")
    description: str = Field(..., description="Description of what this pattern matches")
    examples: List[str] = Field(default_factory=list, description="Example values")
    source_type: str = Field("gl_account", description="Type of source: gl_account, cost_center, department, region, etc.")


class TemplateHierarchy(BaseModel):
    """A hierarchy node within a template."""
    hierarchy_id: str = Field(..., description="Unique identifier for this node")
    hierarchy_name: str = Field(..., description="Display name")
    parent_id: Optional[str] = Field(None, description="Parent node ID (None for root)")
    level: int = Field(1, description="Depth level in the hierarchy (1=root)")
    sort_order: int = Field(0, description="Display order among siblings")
    is_calculated: bool = Field(False, description="Whether this is a calculated/rollup node")
    formula_hint: Optional[str] = Field(None, description="Formula description (e.g., 'SUM of children')")
    mapping_hints: List[MappingHint] = Field(default_factory=list, description="Hints for source mapping")
    description: Optional[str] = Field(None, description="Description of this hierarchy node")
    node_type: Optional[str] = Field(None, description="Type of node: category, account, rollup, detail, etc.")
    flags: Dict[str, bool] = Field(default_factory=lambda: {
        "include_flag": True,
        "calculation_flag": False,
        "is_leaf_node": False
    })


class TemplateMetadata(BaseModel):
    """Minimal template info for listing."""
    id: str
    name: str
    # Use str instead of Enum to accept values from JSON index
    domain: str = "accounting"
    hierarchy_type: str = "custom"
    industry: str = "general"
    description: str
    hierarchy_count: int = 0
    # Legacy field for backward compatibility
    category: Optional[str] = None


class FinancialTemplate(BaseModel):
    """Complete hierarchy template (not just financial - covers all domains)."""
    id: str = Field(..., description="Unique template identifier")
    name: str = Field(..., description="Template display name")
    domain: TemplateDomain = Field(TemplateDomain.ACCOUNTING, description="Domain: accounting, finance, operations")
    hierarchy_type: HierarchyType = Field(HierarchyType.CUSTOM, description="Type of hierarchy")
    industry: str = Field("general", description="Target industry (general, oil_gas, manufacturing, etc.)")
    description: str = Field(..., description="Template description")
    hierarchies: List[TemplateHierarchy] = Field(default_factory=list, description="Pre-defined structure")
    recommended_mappings: List[MappingHint] = Field(default_factory=list, description="Suggested source patterns")
    extends_template: Optional[str] = Field(None, description="Template ID this extends/substitutes")
    relationship_type: Optional[str] = Field(None, description="extension, substitute, or None")
    tags: List[str] = Field(default_factory=list, description="Searchable tags")
    version: str = Field("1.0", description="Template version")
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Legacy field for backward compatibility
    category: Optional[TemplateCategory] = None

    def to_metadata(self) -> TemplateMetadata:
        """Convert to minimal metadata for listing."""
        return TemplateMetadata(
            id=self.id,
            name=self.name,
            domain=self.domain,
            hierarchy_type=self.hierarchy_type,
            category=self.category,
            industry=self.industry,
            description=self.description,
            hierarchy_count=len(self.hierarchies)
        )


class SkillDefinition(BaseModel):
    """AI expertise skill definition."""
    id: str = Field(..., description="Unique skill identifier")
    name: str = Field(..., description="Skill display name")
    description: str = Field(..., description="What this skill provides")
    domain: SkillDomain = Field(SkillDomain.GENERAL, description="Primary domain: accounting, finance, operations")
    industries: List[str] = Field(default_factory=lambda: ["general"], description="Target industries")
    prompt_file: str = Field(..., description="Path to .txt prompt file")
    documentation_file: str = Field(..., description="Path to .md documentation file")
    capabilities: List[str] = Field(default_factory=list, description="List of capabilities")
    hierarchy_types: List[str] = Field(default_factory=list, description="Hierarchy types this skill handles")
    tags: List[str] = Field(default_factory=list, description="Searchable tags")
    version: str = Field("1.0", description="Skill version")


class CustomPrompt(BaseModel):
    """Client-specific custom prompt."""
    id: str = Field(..., description="Unique prompt identifier")
    name: str = Field(..., description="Prompt display name")
    trigger: str = Field(..., description="When to use this prompt (e.g., 'When building P&L')")
    content: str = Field(..., description="The prompt content")
    domain: str = Field("general", description="Domain: accounting, finance, operations, general")
    category: str = Field("general", description="Prompt category within domain")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ClientKnowledge(BaseModel):
    """Client-specific knowledge base profile."""
    client_id: str = Field(..., description="Unique client identifier")
    client_name: str = Field(..., description="Client display name")
    industry: str = Field("general", description="Client industry")

    # Accounting-specific
    chart_of_accounts_pattern: Optional[str] = Field(None, description="COA pattern (e.g., '4-digit', 'segment-based')")
    gl_patterns: Dict[str, str] = Field(default_factory=dict, description="Known GL to hierarchy mappings")

    # Finance-specific
    cost_center_pattern: Optional[str] = Field(None, description="Cost center code pattern")
    profit_center_pattern: Optional[str] = Field(None, description="Profit center code pattern")
    budget_structure: Optional[str] = Field(None, description="Budget hierarchy structure description")

    # Operations-specific
    geographic_structure: Optional[str] = Field(None, description="Geographic hierarchy (global/regional/local)")
    department_structure: Optional[str] = Field(None, description="Departmental hierarchy description")
    legal_entity_structure: Optional[str] = Field(None, description="Legal entity structure")

    # Systems
    erp_system: Optional[str] = Field(None, description="ERP system (e.g., 'SAP', 'Oracle', 'NetSuite')")
    reporting_system: Optional[str] = Field(None, description="Reporting system (e.g., 'Hyperion', 'Anaplan', 'Workday Adaptive')")

    # Preferences
    custom_prompts: List[CustomPrompt] = Field(default_factory=list, description="Client-specific prompts")
    preferred_templates: Dict[str, str] = Field(default_factory=dict, description="Preferred template by domain/type")
    preferred_skills: Dict[str, str] = Field(default_factory=dict, description="Preferred skill by domain")

    notes: Optional[str] = Field(None, description="Free-form notes about this client")

    # Legacy fields
    preferred_template_id: Optional[str] = Field(None, description="Default template (legacy)")
    preferred_skill_id: Optional[str] = Field(None, description="Default skill (legacy)")

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        extra = "allow"


class ClientMetadata(BaseModel):
    """Minimal client info for listing."""
    client_id: str
    client_name: str
    industry: str
    erp_system: Optional[str] = None
    prompt_count: int = 0


class TemplateRecommendation(BaseModel):
    """A template recommendation with reasoning."""
    template_id: str
    template_name: str
    domain: TemplateDomain
    hierarchy_type: HierarchyType
    score: float = Field(..., description="Relevance score (0-100)")
    reason: str = Field(..., description="Why this template is recommended")
    industry_match: bool = False
    type_match: bool = False
    # Legacy
    category_match: bool = False
