"""Tests for the templates module."""

import json
from pathlib import Path

import pytest


class TestTemplateTypes:
    """Test template Pydantic models."""

    def test_template_domain_enum(self):
        from databridge_core.templates import TemplateDomain

        assert TemplateDomain.ACCOUNTING == "accounting"
        assert TemplateDomain.FINANCE == "finance"
        assert TemplateDomain.OPERATIONS == "operations"
        assert TemplateDomain.CUSTOM == "custom"

    def test_hierarchy_type_enum(self):
        from databridge_core.templates import HierarchyType

        assert HierarchyType.CHART_OF_ACCOUNTS == "chart_of_accounts"
        assert HierarchyType.INCOME_STATEMENT == "income_statement"
        assert HierarchyType.COST_CENTER == "cost_center"

    def test_financial_template_creation(self):
        from databridge_core.templates import FinancialTemplate, TemplateHierarchy

        template = FinancialTemplate(
            id="test_template",
            name="Test Template",
            description="A test template",
            hierarchies=[
                TemplateHierarchy(
                    hierarchy_id="h1",
                    hierarchy_name="Revenue",
                    level=1,
                ),
                TemplateHierarchy(
                    hierarchy_id="h2",
                    hierarchy_name="Product Revenue",
                    parent_id="h1",
                    level=2,
                ),
            ],
        )

        assert template.id == "test_template"
        assert len(template.hierarchies) == 2
        meta = template.to_metadata()
        assert meta.hierarchy_count == 2

    def test_client_knowledge_creation(self):
        from databridge_core.templates import ClientKnowledge

        client = ClientKnowledge(
            client_id="acme",
            client_name="ACME Corp",
            industry="manufacturing",
            erp_system="SAP",
        )

        assert client.client_id == "acme"
        assert client.erp_system == "SAP"

    def test_skill_definition(self):
        from databridge_core.templates import SkillDefinition, SkillDomain

        skill = SkillDefinition(
            id="accounting_expert",
            name="Accounting Expert",
            description="Expert in GAAP/IFRS",
            domain=SkillDomain.ACCOUNTING,
            prompt_file="prompts/accounting.txt",
            documentation_file="docs/accounting.md",
        )

        assert skill.domain == SkillDomain.ACCOUNTING

    def test_mapping_hint(self):
        from databridge_core.templates import MappingHint

        hint = MappingHint(
            pattern="4*",
            description="Revenue accounts starting with 4",
            examples=["4000", "4100", "4200"],
        )

        assert hint.pattern == "4*"
        assert len(hint.examples) == 3


class TestTemplateService:
    """Test TemplateService CRUD operations."""

    def test_init_creates_directories(self, tmp_path):
        from databridge_core.templates import TemplateService

        svc = TemplateService(
            templates_dir=str(tmp_path / "templates"),
            skills_dir=str(tmp_path / "skills"),
            kb_dir=str(tmp_path / "kb"),
        )

        assert (tmp_path / "templates").exists()
        assert (tmp_path / "skills").exists()
        assert (tmp_path / "kb").exists()
        assert (tmp_path / "templates" / "index.json").exists()

    def test_list_templates_empty(self, tmp_path):
        from databridge_core.templates import TemplateService

        svc = TemplateService(
            templates_dir=str(tmp_path / "templates"),
            skills_dir=str(tmp_path / "skills"),
            kb_dir=str(tmp_path / "kb"),
        )

        templates = svc.list_templates()
        assert templates == []

    def test_save_and_get_template(self, tmp_path):
        from databridge_core.templates import (
            FinancialTemplate,
            TemplateCategory,
            TemplateService,
        )

        svc = TemplateService(
            templates_dir=str(tmp_path / "templates"),
            skills_dir=str(tmp_path / "skills"),
            kb_dir=str(tmp_path / "kb"),
        )

        template = FinancialTemplate(
            id="pl_general",
            name="General P&L",
            category=TemplateCategory.INCOME_STATEMENT,
            description="Standard income statement",
        )

        saved = svc.save_template(template)
        assert saved.id == "pl_general"

        retrieved = svc.get_template("pl_general")
        assert retrieved is not None
        assert retrieved.name == "General P&L"

        listed = svc.list_templates()
        assert len(listed) == 1

    def test_create_and_get_client(self, tmp_path):
        from databridge_core.templates import TemplateService

        svc = TemplateService(
            templates_dir=str(tmp_path / "templates"),
            skills_dir=str(tmp_path / "skills"),
            kb_dir=str(tmp_path / "kb"),
        )

        client = svc.create_client("acme", "ACME Corp", "manufacturing", "SAP")
        assert client.client_id == "acme"

        retrieved = svc.get_client_knowledge("acme")
        assert retrieved is not None
        assert retrieved.client_name == "ACME Corp"

        clients = svc.list_clients()
        assert len(clients) == 1

    def test_get_template_recommendations(self, tmp_path):
        from databridge_core.templates import (
            FinancialTemplate,
            TemplateCategory,
            TemplateService,
        )

        svc = TemplateService(
            templates_dir=str(tmp_path / "templates"),
            skills_dir=str(tmp_path / "skills"),
            kb_dir=str(tmp_path / "kb"),
        )

        svc.save_template(FinancialTemplate(
            id="pl_mfg",
            name="Manufacturing P&L",
            category=TemplateCategory.INCOME_STATEMENT,
            industry="manufacturing",
            description="P&L for manufacturing companies",
        ))

        recs = svc.get_template_recommendations(
            industry="manufacturing",
            statement_type="pl",
        )

        assert len(recs) >= 1
        assert recs[0].template_name == "Manufacturing P&L"
        assert recs[0].industry_match is True
