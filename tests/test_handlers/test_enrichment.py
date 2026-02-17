from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import (
    DomainsClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    TagAssociationClass,
)

from src.handlers.enrichment import DatasetEnrichmentHandler
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

        # Dataset has both governance and non-governance tags
        tags = MagicMock(spec=GlobalTagsClass)
        gov_tag = MagicMock()
        gov_tag.tag = "urn:li:tag:PII"
        other_tag = MagicMock()
        other_tag.tag = "urn:li:tag:other-tag"
        tags.tags = [gov_tag, other_tag]
        mock_graph.get_tags.return_value = tags

        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None
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
        mock_graph.get_aspect.return_value = None

        entities = handler.export(mock_graph)
        assert len(entities) == 0

    def test_export_field_level_enrichment(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:ds2",
        ]
        mock_graph.get_tags.return_value = None
        mock_graph.get_glossary_terms.return_value = None
        mock_graph.get_domain.return_value = None

        # Field-level tags
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
            "editableSchemaMetadata": [
                {
                    "fieldPath": "email",
                    "globalTags": [{"tag": "urn:li:tag:PII"}],
                }
            ],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        # Should produce 4 MCPs: tags, terms, domains, editableSchemaMetadata
        assert len(mcps) == 4
