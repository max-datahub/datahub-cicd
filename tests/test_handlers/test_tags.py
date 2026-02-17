from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import TagPropertiesClass

from src.handlers.tags import TagHandler
from src.urn_mapper import PassthroughMapper


class TestTagHandler:
    @pytest.fixture
    def handler(self):
        return TagHandler()

    def test_entity_type(self, handler):
        assert handler.entity_type == "tag"

    def test_no_dependencies(self, handler):
        assert handler.dependencies == []

    def test_is_system_entity_default_tag(self, handler):
        assert handler.is_system_entity("urn:li:tag:__default_tag_123")
        assert handler.is_system_entity("urn:li:tag:some__default_abc")

    def test_is_not_system_entity(self, handler):
        assert not handler.is_system_entity("urn:li:tag:PII")
        assert not handler.is_system_entity("urn:li:tag:customer-data")

    def test_export_filters_system_tags(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:tag:PII",
            "urn:li:tag:__default_tag_123",
            "urn:li:tag:custom",
        ]

        pii_props = MagicMock(spec=TagPropertiesClass)
        pii_props.name = "PII"
        pii_props.description = "Personally identifiable"
        pii_props.colorHex = "#FF0000"

        custom_props = MagicMock(spec=TagPropertiesClass)
        custom_props.name = "Custom"
        custom_props.description = None
        custom_props.colorHex = None

        def get_aspect_side_effect(urn, cls):
            if urn == "urn:li:tag:PII":
                return pii_props
            if urn == "urn:li:tag:custom":
                return custom_props
            return None

        mock_graph.get_aspect.side_effect = get_aspect_side_effect

        entities = handler.export(mock_graph)
        assert len(entities) == 2
        urns = [e["urn"] for e in entities]
        assert "urn:li:tag:PII" in urns
        assert "urn:li:tag:custom" in urns
        assert "urn:li:tag:__default_tag_123" not in urns

    def test_export_skips_missing_props(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = ["urn:li:tag:orphan"]
        mock_graph.get_aspect.return_value = None

        entities = handler.export(mock_graph)
        assert len(entities) == 0

    def test_build_mcps(self, handler):
        entity = {
            "urn": "urn:li:tag:PII",
            "name": "PII",
            "description": "Personally identifiable information",
            "colorHex": "#FF0000",
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        mcp = mcps[0]
        assert mcp.entityUrn == "urn:li:tag:PII"
        assert isinstance(mcp.aspect, TagPropertiesClass)
        assert mcp.aspect.name == "PII"
        assert mcp.aspect.description == "Personally identifiable information"
        assert mcp.aspect.colorHex == "#FF0000"

    def test_build_mcps_minimal(self, handler):
        entity = {
            "urn": "urn:li:tag:simple",
            "name": "Simple",
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert mcps[0].aspect.name == "Simple"
        assert mcps[0].aspect.description is None
