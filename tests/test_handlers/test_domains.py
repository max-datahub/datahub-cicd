from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import DomainPropertiesClass

from src.handlers.domains import DomainHandler
from src.urn_mapper import PassthroughMapper


class TestDomainHandler:
    @pytest.fixture
    def handler(self):
        return DomainHandler()

    def test_entity_type(self, handler):
        assert handler.entity_type == "domain"

    def test_no_dependencies(self, handler):
        assert handler.dependencies == []

    def test_is_system_entity(self, handler):
        assert handler.is_system_entity("urn:li:domain:__system__default")
        assert not handler.is_system_entity("urn:li:domain:marketing")

    def test_export_with_hierarchy(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:domain:subdomain",
            "urn:li:domain:root",
        ]

        root_props = MagicMock(spec=DomainPropertiesClass)
        root_props.name = "Root"
        root_props.description = "Root domain"
        root_props.parentDomain = None

        sub_props = MagicMock(spec=DomainPropertiesClass)
        sub_props.name = "Subdomain"
        sub_props.description = "Child domain"
        sub_props.parentDomain = "urn:li:domain:root"

        def get_aspect_side_effect(urn, cls):
            if urn == "urn:li:domain:root":
                return root_props
            if urn == "urn:li:domain:subdomain":
                return sub_props
            return None

        mock_graph.get_aspect.side_effect = get_aspect_side_effect

        entities = handler.export(mock_graph)
        assert len(entities) == 2
        # Root must come before subdomain
        assert entities[0]["urn"] == "urn:li:domain:root"
        assert entities[1]["urn"] == "urn:li:domain:subdomain"

    def test_build_mcps_with_parent(self, handler):
        entity = {
            "urn": "urn:li:domain:subdomain",
            "name": "Subdomain",
            "description": "Child domain",
            "parentDomain": "urn:li:domain:root",
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        mcp = mcps[0]
        assert mcp.entityUrn == "urn:li:domain:subdomain"
        assert isinstance(mcp.aspect, DomainPropertiesClass)
        assert mcp.aspect.name == "Subdomain"
        assert mcp.aspect.parentDomain == "urn:li:domain:root"

    def test_build_mcps_root_domain(self, handler):
        entity = {
            "urn": "urn:li:domain:root",
            "name": "Root",
            "description": "Top level",
            "parentDomain": None,
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert mcps[0].aspect.parentDomain is None
