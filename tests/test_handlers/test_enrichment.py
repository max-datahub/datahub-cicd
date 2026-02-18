from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import (
    DomainsClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    TagAssociationClass,
)

from src.handlers.enrichment import (
    DatasetEnrichmentHandler,
    GenericEnrichmentHandler,
)
from src.scope import ScopeConfig
from src.urn_mapper import PassthroughMapper


class TestDatasetEnrichmentHandler:
    @pytest.fixture
    def governance_urns(self):
        return {
            "urn:li:tag:PII",
            "urn:li:glossaryTerm:CustomerData",
            "urn:li:domain:marketing",
        }

    @pytest.fixture
    def handler(self, governance_urns):
        return DatasetEnrichmentHandler(governance_urns=governance_urns)

    def test_entity_type(self, handler):
        assert handler.entity_type == "enrichment"

    def test_dependencies(self, handler):
        deps = handler.dependencies
        assert "tag" in deps
        assert "glossaryNode" in deps
        assert "glossaryTerm" in deps
        assert "domain" in deps

    def test_export_filters_by_governance_urns(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:ds1",
        ]

        tags = MagicMock(spec=GlobalTagsClass)
        gov_tag = MagicMock()
        gov_tag.tag = "urn:li:tag:PII"
        other_tag = MagicMock()
        other_tag.tag = "urn:li:tag:other-tag"
        tags.tags = [gov_tag, other_tag]
        mock_graph.get_tags.return_value = tags

        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert len(entities[0]["globalTags"]) == 1
        assert entities[0]["globalTags"][0]["tag"] == "urn:li:tag:PII"

    def test_export_no_enrichment_skips_dataset(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:empty",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

        entities = handler.export(mock_graph)
        assert len(entities) == 0

    def test_export_includes_ownership(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = ["urn:li:dataset:ds1"]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_aspect.return_value = None

        ownership = MagicMock(spec=OwnershipClass)
        owner = MagicMock(spec=OwnerClass)
        owner.owner = "urn:li:corpuser:alice@test.com"
        owner.type = "TECHNICAL_OWNER"
        ownership.owners = [owner]
        mock_graph.get_ownership.return_value = ownership

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert "ownership" in entities[0]
        assert entities[0]["ownership"][0]["owner"] == "urn:li:corpuser:alice@test.com"
        assert entities[0]["ownership"][0]["type"] == "TECHNICAL_OWNER"

    def test_export_field_level_enrichment(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:ds2",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None

        field_info = MagicMock(spec=EditableSchemaFieldInfoClass)
        field_info.fieldPath = "email"
        field_tag = MagicMock()
        field_tag.tag = "urn:li:tag:PII"
        field_info.globalTags = MagicMock(spec=GlobalTagsClass)
        field_info.globalTags.tags = [field_tag]
        field_info.glossaryTerms = None

        esm = MagicMock(spec=EditableSchemaMetadataClass)
        esm.editableSchemaFieldInfo = [field_info]
        mock_graph.get_aspect.return_value = esm

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert "editableSchemaMetadata" in entities[0]
        assert entities[0]["editableSchemaMetadata"][0]["fieldPath"] == "email"

    def test_build_mcps_global_tags(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "globalTags": [{"tag": "urn:li:tag:PII"}],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, GlobalTagsClass)
        assert len(mcps[0].aspect.tags) == 1
        assert mcps[0].aspect.tags[0].tag == "urn:li:tag:PII"

    def test_build_mcps_glossary_terms(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "glossaryTerms": [{"urn": "urn:li:glossaryTerm:CustomerData"}],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, GlossaryTermsClass)

    def test_build_mcps_domains(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "domains": ["urn:li:domain:marketing"],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, DomainsClass)
        assert "urn:li:domain:marketing" in mcps[0].aspect.domains

    def test_build_mcps_ownership(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "ownership": [
                {"owner": "urn:li:corpuser:alice", "type": "TECHNICAL_OWNER"},
            ],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, OwnershipClass)
        assert len(mcps[0].aspect.owners) == 1
        assert mcps[0].aspect.owners[0].owner == "urn:li:corpuser:alice"
        assert mcps[0].aspect.owners[0].type == "TECHNICAL_OWNER"

    def test_build_mcps_field_level(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "editableSchemaMetadata": [
                {
                    "fieldPath": "email",
                    "globalTags": [{"tag": "urn:li:tag:PII"}],
                }
            ],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, EditableSchemaMetadataClass)
        fields = mcps[0].aspect.editableSchemaFieldInfo
        assert len(fields) == 1
        assert fields[0].fieldPath == "email"

    def test_build_mcps_all_enrichment_types(self, handler):
        entity = {
            "dataset_urn": "urn:li:dataset:ds1",
            "globalTags": [{"tag": "urn:li:tag:PII"}],
            "glossaryTerms": [{"urn": "urn:li:glossaryTerm:CustomerData"}],
            "domains": ["urn:li:domain:marketing"],
            "ownership": [
                {"owner": "urn:li:corpuser:alice", "type": "TECHNICAL_OWNER"},
            ],
            "editableSchemaMetadata": [
                {
                    "fieldPath": "email",
                    "globalTags": [{"tag": "urn:li:tag:PII"}],
                }
            ],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        # tags + terms + domains + ownership + editableSchemaMetadata = 5
        assert len(mcps) == 5


class TestGenericEnrichmentHandler:
    @pytest.fixture
    def governance_urns(self):
        return {
            "urn:li:tag:PII",
            "urn:li:domain:marketing",
        }

    @pytest.fixture
    def chart_handler(self, governance_urns):
        return GenericEnrichmentHandler("chart", governance_urns)

    @pytest.fixture
    def container_handler(self, governance_urns):
        return GenericEnrichmentHandler("container", governance_urns)

    def test_entity_type_includes_kind(self, chart_handler):
        assert chart_handler.entity_type == "chartEnrichment"

    def test_dependencies(self, chart_handler):
        deps = chart_handler.dependencies
        assert "tag" in deps
        assert "domain" in deps

    def test_export_chart_ownership(self, chart_handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:chart:(looker,test.chart1)",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_aspect.return_value = None

        ownership = MagicMock(spec=OwnershipClass)
        owner = MagicMock(spec=OwnerClass)
        owner.owner = "urn:li:corpuser:bob@test.com"
        owner.type = "BUSINESS_OWNER"
        ownership.owners = [owner]
        mock_graph.get_ownership.return_value = ownership

        entities = chart_handler.export(mock_graph)
        assert len(entities) == 1
        assert entities[0]["entity_urn"] == "urn:li:chart:(looker,test.chart1)"
        assert entities[0]["ownership"][0]["owner"] == "urn:li:corpuser:bob@test.com"

    def test_export_container_domain(self, container_handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:container:test_db",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

        domain = MagicMock(spec=DomainsClass)
        domain.domains = ["urn:li:domain:marketing"]
        mock_graph.get_domain.return_value = domain

        entities = container_handler.export(mock_graph)
        assert len(entities) == 1
        assert entities[0]["domains"] == ["urn:li:domain:marketing"]

    def test_export_skips_empty(self, chart_handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:chart:(looker,no_enrichment)",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

        entities = chart_handler.export(mock_graph)
        assert len(entities) == 0

    def test_build_mcps_ownership(self, chart_handler):
        entity = {
            "entity_urn": "urn:li:chart:(looker,test)",
            "ownership": [
                {"owner": "urn:li:corpuser:bob", "type": "BUSINESS_OWNER"},
            ],
        }
        mapper = PassthroughMapper()
        mcps = chart_handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, OwnershipClass)
        assert mcps[0].entityUrn == "urn:li:chart:(looker,test)"

    def test_build_mcps_domains_and_tags(self, chart_handler):
        entity = {
            "entity_urn": "urn:li:container:test",
            "globalTags": [{"tag": "urn:li:tag:PII"}],
            "domains": ["urn:li:domain:marketing"],
            "ownership": [
                {"owner": "urn:li:corpuser:alice", "type": "TECHNICAL_OWNER"},
            ],
        }
        mapper = PassthroughMapper()
        mcps = chart_handler.build_mcps(entity, mapper)

        # tags + domains + ownership = 3
        assert len(mcps) == 3
        aspect_types = {type(m.aspect).__name__ for m in mcps}
        assert "GlobalTagsClass" in aspect_types
        assert "DomainsClass" in aspect_types
        assert "OwnershipClass" in aspect_types

    def test_build_mcps_maps_owner_urns(self, chart_handler):
        entity = {
            "entity_urn": "urn:li:chart:test",
            "ownership": [
                {"owner": "urn:li:corpuser:alice", "type": "TECHNICAL_OWNER"},
            ],
        }
        mapper = MagicMock()
        mapper.map.side_effect = lambda u: u.replace("alice", "bob")

        mcps = chart_handler.build_mcps(entity, mapper)
        assert mcps[0].aspect.owners[0].owner == "urn:li:corpuser:bob"


class TestDatasetEnrichmentScoped:
    @pytest.fixture
    def governance_urns(self):
        return {"urn:li:tag:PII"}

    def _stub_graph_no_enrichment(self, mock_graph):
        mock_graph.get_urns_by_filter.return_value = []
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

    def test_export_with_domain_scope(self, governance_urns, mock_graph):
        scope = ScopeConfig(domains=["urn:li:domain:marketing"])
        handler = DatasetEnrichmentHandler(governance_urns=governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        extra_filters = call_kwargs.kwargs["extraFilters"]
        assert extra_filters is not None
        assert len(extra_filters) == 1
        assert extra_filters[0]["field"] == "domains"
        assert extra_filters[0]["values"] == ["urn:li:domain:marketing"]

    def test_export_with_platform_scope(self, governance_urns, mock_graph):
        scope = ScopeConfig(platforms=["snowflake"])
        handler = DatasetEnrichmentHandler(governance_urns=governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["platform"] == ["snowflake"]

    def test_export_with_env_scope(self, governance_urns, mock_graph):
        scope = ScopeConfig(env="PROD")
        handler = DatasetEnrichmentHandler(governance_urns=governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["env"] == "PROD"

    def test_export_with_combined_scope(self, governance_urns, mock_graph):
        scope = ScopeConfig(
            domains=["urn:li:domain:marketing"],
            platforms=["snowflake", "bigquery"],
            env="PROD",
        )
        handler = DatasetEnrichmentHandler(governance_urns=governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["platform"] == ["snowflake", "bigquery"]
        assert call_kwargs.kwargs["env"] == "PROD"
        assert call_kwargs.kwargs["extraFilters"] is not None

    def test_export_no_scope_unchanged(self, governance_urns, mock_graph):
        handler = DatasetEnrichmentHandler(governance_urns=governance_urns)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["platform"] is None
        assert call_kwargs.kwargs["env"] is None
        assert call_kwargs.kwargs["extraFilters"] is None


class TestGenericEnrichmentScoped:
    @pytest.fixture
    def governance_urns(self):
        return {"urn:li:tag:PII"}

    def _stub_graph_no_enrichment(self, mock_graph):
        mock_graph.get_urns_by_filter.return_value = []
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
        mock_graph.get_ownership.return_value = None
        mock_graph.get_aspect.return_value = None

    def test_export_chart_env_not_passed(self, governance_urns, mock_graph):
        """env should NOT be passed for charts (no environment field)."""
        scope = ScopeConfig(env="PROD", platforms=["looker"])
        handler = GenericEnrichmentHandler("chart", governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["env"] is None
        assert call_kwargs.kwargs["platform"] == ["looker"]

    def test_export_dashboard_env_not_passed(self, governance_urns, mock_graph):
        """env should NOT be passed for dashboards."""
        scope = ScopeConfig(env="PROD")
        handler = GenericEnrichmentHandler("dashboard", governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["env"] is None

    def test_export_container_env_passed(self, governance_urns, mock_graph):
        """env SHOULD be passed for containers (they have environment)."""
        scope = ScopeConfig(env="PROD")
        handler = GenericEnrichmentHandler("container", governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["env"] == "PROD"

    def test_export_with_domain_scope(self, governance_urns, mock_graph):
        scope = ScopeConfig(domains=["urn:li:domain:finance"])
        handler = GenericEnrichmentHandler("chart", governance_urns, scope=scope)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        extra_filters = call_kwargs.kwargs["extraFilters"]
        assert extra_filters is not None
        assert extra_filters[0]["field"] == "domains"
        assert extra_filters[0]["values"] == ["urn:li:domain:finance"]

    def test_export_no_scope_unchanged(self, governance_urns, mock_graph):
        handler = GenericEnrichmentHandler("chart", governance_urns)
        self._stub_graph_no_enrichment(mock_graph)

        handler.export(mock_graph)

        call_kwargs = mock_graph.get_urns_by_filter.call_args
        assert call_kwargs.kwargs["platform"] is None
        assert call_kwargs.kwargs["env"] is None
        assert call_kwargs.kwargs["extraFilters"] is None
