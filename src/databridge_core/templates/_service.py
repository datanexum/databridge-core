"""Template, Skills, and Knowledge Base Service - Core business logic."""
import json
import uuid
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from ._types import (
    FinancialTemplate,
    TemplateHierarchy,
    TemplateMetadata,
    TemplateCategory,
    MappingHint,
    SkillDefinition,
    ClientKnowledge,
    ClientMetadata,
    CustomPrompt,
    TemplateRecommendation,
)


class TemplateService:
    """Service for managing templates, skills, and knowledge base."""

    def __init__(self, templates_dir: str = "templates",
                 skills_dir: str = "skills",
                 kb_dir: str = "knowledge_base"):
        self.templates_dir = Path(templates_dir)
        self.skills_dir = Path(skills_dir)
        self.kb_dir = Path(kb_dir)

        # mtime-based cache: {str(path): (mtime, data)}
        self._json_cache: dict = {}

        # Ensure directories exist
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.skills_dir.mkdir(parents=True, exist_ok=True)
        self.kb_dir.mkdir(parents=True, exist_ok=True)
        (self.kb_dir / "clients").mkdir(parents=True, exist_ok=True)

        # Initialize index files if they don't exist
        self._init_indices()

    def _init_indices(self):
        """Initialize index files if they don't exist."""
        templates_index = self.templates_dir / "index.json"
        if not templates_index.exists():
            self._save_json(templates_index, {"templates": [], "version": "1.0"})

        skills_index = self.skills_dir / "index.json"
        if not skills_index.exists():
            self._save_json(skills_index, {"skills": [], "version": "1.0"})

        kb_index = self.kb_dir / "index.json"
        if not kb_index.exists():
            self._save_json(kb_index, {"clients": [], "version": "1.0"})

    def _load_json(self, path: Path) -> dict:
        """Load JSON from file with mtime-based caching."""
        if not path.exists():
            return {}
        key = str(path)
        try:
            current_mtime = path.stat().st_mtime
        except OSError:
            current_mtime = None

        if key in self._json_cache:
            cached_mtime, cached_data = self._json_cache[key]
            if current_mtime is not None and cached_mtime == current_mtime:
                return cached_data

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if current_mtime is not None:
            self._json_cache[key] = (current_mtime, data)
        return data

    def _save_json(self, path: Path, data: dict):
        """Save JSON to file and invalidate cache."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        # Invalidate cache so next _load_json re-reads with new mtime
        self._json_cache.pop(str(path), None)

    def _generate_id(self) -> str:
        """Generate a UUID."""
        return str(uuid.uuid4())

    def _slugify(self, text: str) -> str:
        """Convert text to a slug."""
        slug = re.sub(r"[^a-z0-9]+", "_", text.lower())
        return re.sub(r"^_+|_+$", "", slug)

    # =========================================================================
    # Template Operations
    # =========================================================================

    def list_templates(self, category: Optional[str] = None,
                       industry: Optional[str] = None,
                       domain: Optional[str] = None,
                       hierarchy_type: Optional[str] = None) -> List[TemplateMetadata]:
        """List available templates with optional filtering.

        Args:
            category: Legacy filter by category (income_statement, balance_sheet, etc.)
            industry: Filter by industry (general, oil_gas_upstream, manufacturing, saas, etc.)
            domain: Filter by domain (accounting, finance, operations)
            hierarchy_type: Filter by hierarchy type (income_statement, cost_center, geographic, etc.)

        Returns:
            List of matching template metadata
        """
        index = self._load_json(self.templates_dir / "index.json")
        templates = []

        for entry in index.get("templates", []):
            # Apply filters
            if category and entry.get("category") != category:
                continue
            if domain and entry.get("domain") != domain:
                continue
            if hierarchy_type and entry.get("hierarchy_type") != hierarchy_type:
                continue

            # Industry filter: include general templates or exact matches
            # Also include parent industry matches (e.g., oil_gas matches oil_gas_upstream)
            if industry:
                entry_industry = entry.get("industry", "general")
                if entry_industry != industry and entry_industry != "general":
                    # Check if it's a sub-industry match
                    if not (industry.startswith(entry_industry + "_") or entry_industry.startswith(industry + "_")):
                        continue

            templates.append(TemplateMetadata(
                id=entry["id"],
                name=entry["name"],
                category=entry.get("category", "custom"),
                industry=entry.get("industry", "general"),
                description=entry.get("description", ""),
                hierarchy_count=entry.get("hierarchy_count", 0),
                domain=entry.get("domain", "accounting"),
                hierarchy_type=entry.get("hierarchy_type", "custom")
            ))

        return templates

    def get_template(self, template_id: str) -> Optional[FinancialTemplate]:
        """Get full details of a template."""
        index = self._load_json(self.templates_dir / "index.json")

        for entry in index.get("templates", []):
            if entry["id"] == template_id:
                # Try loading from file path in index first
                if "file" in entry:
                    template_path = self.templates_dir / entry["file"]
                    if template_path.exists():
                        data = self._load_json(template_path)
                        return FinancialTemplate(**data)

                # Try domain-based directory (new structure)
                domain = entry.get("domain", "accounting")
                template_path = self.templates_dir / domain / f"{template_id}.json"
                if template_path.exists():
                    data = self._load_json(template_path)
                    return FinancialTemplate(**data)

                # Try category-based directory (legacy structure)
                template_path = self.templates_dir / entry.get("category", "custom") / f"{template_id}.json"
                if template_path.exists():
                    data = self._load_json(template_path)
                    return FinancialTemplate(**data)

                # Try without subfolder
                template_path = self.templates_dir / f"{template_id}.json"
                if template_path.exists():
                    data = self._load_json(template_path)
                    return FinancialTemplate(**data)

        return None

    def save_template(self, template: FinancialTemplate) -> FinancialTemplate:
        """Save a template to disk."""
        # Ensure category directory exists
        category_dir = self.templates_dir / template.category.value
        category_dir.mkdir(parents=True, exist_ok=True)

        # Set timestamps
        if not template.created_at:
            template.created_at = datetime.now()
        template.updated_at = datetime.now()

        # Save template file
        template_path = category_dir / f"{template.id}.json"
        self._save_json(template_path, template.model_dump())

        # Update index
        index = self._load_json(self.templates_dir / "index.json")
        templates = index.get("templates", [])

        # Remove old entry if exists
        templates = [t for t in templates if t["id"] != template.id]

        # Add new entry
        templates.append({
            "id": template.id,
            "name": template.name,
            "category": template.category.value,
            "industry": template.industry,
            "description": template.description,
            "hierarchy_count": len(template.hierarchies),
            "file": f"{template.category.value}/{template.id}.json"
        })

        index["templates"] = templates
        self._save_json(self.templates_dir / "index.json", index)

        return template

    def get_template_recommendations(self, industry: Optional[str] = None,
                                     statement_type: Optional[str] = None) -> List[TemplateRecommendation]:
        """Get AI-powered template recommendations based on context."""
        templates = self.list_templates()
        recommendations = []

        for template in templates:
            score = 50  # Base score
            reasons = []
            industry_match = False
            category_match = False

            # Industry matching
            if industry:
                if template.industry.lower() == industry.lower():
                    score += 30
                    reasons.append(f"Matches {industry} industry")
                    industry_match = True
                elif template.industry == "general":
                    score += 10
                    reasons.append("General-purpose template suitable for all industries")

            # Statement type matching
            if statement_type:
                type_map = {
                    "pl": "income_statement",
                    "p&l": "income_statement",
                    "profit_loss": "income_statement",
                    "income": "income_statement",
                    "bs": "balance_sheet",
                    "balance": "balance_sheet",
                    "cf": "cash_flow",
                    "cash": "cash_flow",
                }
                normalized_type = type_map.get(statement_type.lower(), statement_type.lower())
                cat_val = template.category.value if hasattr(template.category, "value") else template.category
                if cat_val == normalized_type:
                    score += 30
                    reasons.append(f"Matches requested {statement_type} statement type")
                    category_match = True

            # Hierarchy count bonus (more complete templates score higher)
            if template.hierarchy_count > 10:
                score += 10
                reasons.append(f"Comprehensive template with {template.hierarchy_count} hierarchy nodes")

            if reasons:
                recommendations.append(TemplateRecommendation(
                    template_id=template.id,
                    template_name=template.name,
                    domain=template.domain,
                    hierarchy_type=template.hierarchy_type,
                    score=min(score, 100),
                    reason="; ".join(reasons),
                    industry_match=industry_match,
                    category_match=category_match
                ))

        # Sort by score descending
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:5]  # Return top 5

    # =========================================================================
    # Skill Operations
    # =========================================================================

    def list_skills(self, domain: Optional[str] = None,
                    industry: Optional[str] = None) -> List[SkillDefinition]:
        """List all available AI expertise skills.

        Args:
            domain: Filter by domain (accounting, finance, operations)
            industry: Filter by industry (general, oil_gas, manufacturing, saas, etc.)

        Returns:
            List of matching skill definitions
        """
        index = self._load_json(self.skills_dir / "index.json")
        skills = []

        for entry in index.get("skills", []):
            # Apply domain filter
            if domain and entry.get("domain") != domain:
                continue

            # Apply industry filter
            if industry:
                skill_industries = entry.get("industries", ["general"])
                if industry not in skill_industries and "all" not in skill_industries and "general" not in skill_industries:
                    # Check sub-industry match
                    matched = False
                    for skill_ind in skill_industries:
                        if industry.startswith(skill_ind) or skill_ind.startswith(industry):
                            matched = True
                            break
                    if not matched:
                        continue

            skills.append(SkillDefinition(**entry))

        return skills

    def get_skills_for_industry(self, industry: str) -> List[SkillDefinition]:
        """Get recommended skills for a specific industry."""
        index = self._load_json(self.skills_dir / "index.json")
        industry_map = index.get("industries", {})

        skill_ids = industry_map.get(industry, [])
        if not skill_ids:
            skill_ids = industry_map.get("general", [])

        skills = []
        for skill in self.list_skills():
            if skill.id in skill_ids:
                skills.append(skill)

        return skills

    def get_skill(self, skill_id: str) -> Optional[SkillDefinition]:
        """Get detailed information about a specific skill."""
        skills = self.list_skills()
        for skill in skills:
            if skill.id == skill_id:
                return skill
        return None

    def get_skill_prompt(self, skill_id: str) -> Optional[str]:
        """Get the system prompt content for a skill."""
        skill = self.get_skill(skill_id)
        if not skill:
            return None

        prompt_path = self.skills_dir / skill.prompt_file
        if prompt_path.exists():
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        return None

    def get_skill_documentation(self, skill_id: str) -> Optional[str]:
        """Get the documentation content for a skill."""
        skill = self.get_skill(skill_id)
        if not skill:
            return None

        doc_path = self.skills_dir / skill.documentation_file
        if doc_path.exists():
            with open(doc_path, "r", encoding="utf-8") as f:
                return f.read()
        return None

    # =========================================================================
    # Knowledge Base Operations
    # =========================================================================

    def list_clients(self) -> List[ClientMetadata]:
        """List all client knowledge base profiles."""
        index = self._load_json(self.kb_dir / "index.json")
        clients = []

        for entry in index.get("clients", []):
            clients.append(ClientMetadata(
                client_id=entry["client_id"],
                client_name=entry["client_name"],
                industry=entry.get("industry", "general"),
                erp_system=entry.get("erp_system"),
                prompt_count=entry.get("prompt_count", 0)
            ))

        return clients

    def get_client_knowledge(self, client_id: str) -> Optional[ClientKnowledge]:
        """Get full client knowledge base profile."""
        client_dir = self.kb_dir / "clients" / client_id
        config_path = client_dir / "config.json"

        if not config_path.exists():
            return None

        config = self._load_json(config_path)

        # Load prompts
        prompts_path = client_dir / "prompts.json"
        if prompts_path.exists():
            prompts_data = self._load_json(prompts_path)
            config["custom_prompts"] = [CustomPrompt(**p) for p in prompts_data.get("prompts", [])]

        # Load mappings
        mappings_path = client_dir / "mappings.json"
        if mappings_path.exists():
            config["gl_patterns"] = self._load_json(mappings_path).get("mappings", {})

        # Load notes
        notes_path = client_dir / "notes.md"
        if notes_path.exists():
            with open(notes_path, "r", encoding="utf-8") as f:
                config["notes"] = f.read()

        return ClientKnowledge(**config)

    def create_client(self, client_id: str, client_name: str,
                      industry: str = "general", erp_system: Optional[str] = None) -> ClientKnowledge:
        """Create a new client knowledge base profile."""
        # Create client directory
        client_dir = self.kb_dir / "clients" / client_id
        client_dir.mkdir(parents=True, exist_ok=True)

        # Create client config
        client = ClientKnowledge(
            client_id=client_id,
            client_name=client_name,
            industry=industry,
            erp_system=erp_system,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        self._save_json(client_dir / "config.json", {
            "client_id": client.client_id,
            "client_name": client.client_name,
            "industry": client.industry,
            "erp_system": client.erp_system,
            "chart_of_accounts_pattern": client.chart_of_accounts_pattern,
            "preferred_template_id": client.preferred_template_id,
            "preferred_skill_id": client.preferred_skill_id,
            "created_at": client.created_at,
            "updated_at": client.updated_at
        })

        # Create empty prompts file
        self._save_json(client_dir / "prompts.json", {"prompts": []})

        # Create empty mappings file
        self._save_json(client_dir / "mappings.json", {"mappings": {}})

        # Create empty notes file
        with open(client_dir / "notes.md", "w", encoding="utf-8") as f:
            f.write(f"# {client_name} - Knowledge Base Notes\n\n")
            f.write(f"Industry: {industry}\n")
            if erp_system:
                f.write(f"ERP System: {erp_system}\n")
            f.write("\n## Notes\n\n")

        # Update index
        index = self._load_json(self.kb_dir / "index.json")
        clients = index.get("clients", [])

        # Remove old entry if exists
        clients = [c for c in clients if c["client_id"] != client_id]

        # Add new entry
        clients.append({
            "client_id": client_id,
            "client_name": client_name,
            "industry": industry,
            "erp_system": erp_system,
            "prompt_count": 0
        })

        index["clients"] = clients
        self._save_json(self.kb_dir / "index.json", index)

        return client

    def update_client_knowledge(self, client_id: str, field: str, value: Any) -> Optional[ClientKnowledge]:
        """Update a specific field in client knowledge base."""
        client_dir = self.kb_dir / "clients" / client_id
        config_path = client_dir / "config.json"

        if not config_path.exists():
            return None

        config = self._load_json(config_path)

        # Update the field
        allowed_fields = [
            "client_name", "industry", "erp_system", "chart_of_accounts_pattern",
            "preferred_template_id", "preferred_skill_id"
        ]

        if field in allowed_fields:
            config[field] = value
            config["updated_at"] = datetime.now().isoformat()
            self._save_json(config_path, config)

            # Update index if needed
            if field in ["client_name", "industry", "erp_system"]:
                index = self._load_json(self.kb_dir / "index.json")
                for client in index.get("clients", []):
                    if client["client_id"] == client_id:
                        client[field] = value
                        break
                self._save_json(self.kb_dir / "index.json", index)
        elif field == "notes":
            # Save notes to markdown file
            with open(client_dir / "notes.md", "w", encoding="utf-8") as f:
                f.write(value)
        elif field == "gl_patterns":
            # Update mappings
            mappings = self._load_json(client_dir / "mappings.json")
            if isinstance(value, dict):
                mappings["mappings"].update(value)
            self._save_json(client_dir / "mappings.json", mappings)

        return self.get_client_knowledge(client_id)

    def add_client_prompt(self, client_id: str, prompt: CustomPrompt) -> Optional[ClientKnowledge]:
        """Add a custom prompt to a client's knowledge base."""
        client_dir = self.kb_dir / "clients" / client_id
        prompts_path = client_dir / "prompts.json"

        if not client_dir.exists():
            return None

        prompts_data = self._load_json(prompts_path) if prompts_path.exists() else {"prompts": []}

        # Set ID and timestamps if not set
        if not prompt.id:
            prompt.id = self._generate_id()
        if not prompt.created_at:
            prompt.created_at = datetime.now()
        prompt.updated_at = datetime.now()

        # Add prompt
        prompts_data["prompts"].append(prompt.model_dump())
        self._save_json(prompts_path, prompts_data)

        # Update index prompt count
        index = self._load_json(self.kb_dir / "index.json")
        for client in index.get("clients", []):
            if client["client_id"] == client_id:
                client["prompt_count"] = len(prompts_data["prompts"])
                break
        self._save_json(self.kb_dir / "index.json", index)

        return self.get_client_knowledge(client_id)

    def get_client_prompts(self, client_id: str) -> List[CustomPrompt]:
        """Get all custom prompts for a client."""
        client_dir = self.kb_dir / "clients" / client_id
        prompts_path = client_dir / "prompts.json"

        if not prompts_path.exists():
            return []

        prompts_data = self._load_json(prompts_path)
        return [CustomPrompt(**p) for p in prompts_data.get("prompts", [])]

    # =========================================================================
    # Integration with Hierarchy Service
    # =========================================================================

    def create_project_from_template(self, template_id: str, project_name: str,
                                     hierarchy_service: Any) -> Optional[Dict[str, Any]]:
        """Create a new hierarchy project from a template.

        Args:
            template_id: The template to use
            project_name: Name for the new project
            hierarchy_service: Instance of HierarchyService

        Returns:
            Dict with project info and created hierarchies
        """
        template = self.get_template(template_id)
        if not template:
            return None

        # Create the project
        project = hierarchy_service.create_project(
            name=project_name,
            description=f"Created from template: {template.name}"
        )

        # Create hierarchies from template
        created_hierarchies = []
        id_mapping = {}  # Map template hierarchy IDs to actual IDs

        # Sort hierarchies by level to ensure parents are created first
        sorted_hierarchies = sorted(template.hierarchies, key=lambda h: h.level)

        for th in sorted_hierarchies:
            # Map parent ID if it exists
            parent_id = None
            if th.parent_id and th.parent_id in id_mapping:
                parent_id = id_mapping[th.parent_id]

            # Create the hierarchy
            hierarchy = hierarchy_service.create_hierarchy(
                project_id=project.id,
                name=th.hierarchy_name,
                parent_id=parent_id,
                description=th.description,
                flags=th.flags
            )

            if hierarchy:
                id_mapping[th.hierarchy_id] = hierarchy.hierarchy_id
                created_hierarchies.append({
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "level": th.level,
                    "is_calculated": th.is_calculated
                })

        return {
            "project_id": project.id,
            "project_name": project.name,
            "template_used": template.name,
            "hierarchies_created": len(created_hierarchies),
            "hierarchies": created_hierarchies
        }

    def save_project_as_template(self, project_id: str, template_name: str,
                                 category: str, description: str,
                                 hierarchy_service: Any,
                                 industry: str = "general") -> Optional[FinancialTemplate]:
        """Save an existing hierarchy project as a reusable template.

        Args:
            project_id: The project to convert
            template_name: Name for the new template
            category: Template category (income_statement, balance_sheet, etc.)
            description: Template description
            hierarchy_service: Instance of HierarchyService
            industry: Target industry

        Returns:
            The created FinancialTemplate
        """
        # Get project and its hierarchies
        project = hierarchy_service.get_project(project_id)
        if not project:
            return None

        hierarchies = hierarchy_service.list_hierarchies(project_id)

        # Convert to template hierarchies
        template_hierarchies = []
        for h in hierarchies:
            level = self._calculate_hierarchy_level(h, hierarchies)

            template_hierarchies.append(TemplateHierarchy(
                hierarchy_id=h.get("hierarchy_id", ""),
                hierarchy_name=h.get("hierarchy_name", ""),
                parent_id=h.get("parent_id"),
                level=level,
                sort_order=h.get("sort_order", 0),
                is_calculated=h.get("flags", {}).get("calculation_flag", False),
                formula_hint=self._get_formula_hint(h),
                description=h.get("description"),
                flags=h.get("flags", {})
            ))

        # Create template
        template_id = self._slugify(template_name)
        template = FinancialTemplate(
            id=template_id,
            name=template_name,
            category=TemplateCategory(category),
            industry=industry,
            description=description,
            hierarchies=template_hierarchies,
            created_at=datetime.now()
        )

        return self.save_template(template)

    def _calculate_hierarchy_level(self, hierarchy: Dict, all_hierarchies: List[Dict]) -> int:
        """Calculate the depth level of a hierarchy node."""
        level = 1
        parent_id = hierarchy.get("parent_id")

        while parent_id:
            level += 1
            parent = next((h for h in all_hierarchies if h.get("hierarchy_id") == parent_id), None)
            if parent:
                parent_id = parent.get("parent_id")
            else:
                break

        return level

    def _get_formula_hint(self, hierarchy: Dict) -> Optional[str]:
        """Extract formula hint from hierarchy if it has calculations."""
        formula_config = hierarchy.get("formula_config")
        if formula_config:
            if formula_config.get("formula_group"):
                return f"Calculated: {formula_config['formula_group'].get('group_name', 'Custom formula')}"
            elif formula_config.get("formula_text"):
                return f"Formula: {formula_config['formula_text'][:50]}..."
        return None
