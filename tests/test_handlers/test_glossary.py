from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import GlossaryNodeInfoClass, GlossaryTermInfoClass

from src.handlers.glossary import GlossaryNodeHandler, GlossaryTermHandler
from src.urn_mapper import PassthroughMapper


class TestGlossaryNodeHandler:
    @pytest.fixture
    def handler(self):
        return GlossaryNodeHandler()

    def test_entity_type(self, handler):
        assert handler.entity_type == "glossaryNode"

    def test_no_dependencies(self, handler):
        assert handler.dependencies == []

    def test_is_system_entity(self, handler):
        assert handler.is_system_entity("urn:li:glossaryNode:__system__root")
        assert not handler.is_system_entity("urn:li:glossaryNode:my-node")

    def test_export_with_hierarchy(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:glossaryNode:child",
            "urn:li:glossaryNode:parent",
        ]

        parent_info = MagicMock(spec=GlossaryNodeInfoClass)
        parent_info.name = "Parent"
        parent_info.definition = "Parent node"
        parent_info.parentNode = None

        child_info = MagicMock(spec=GlossaryNodeInfoClass)
        child_info.name = "Child"
        child_info.definition = "Child node"
        child_info.parentNode = "urn:li:glossaryNode:parent"

        def get_aspect_side_effect(urn, cls):
            if urn == "urn:li:glossaryNode:parent":
                return parent_info
            if urn == "urn:li:glossaryNode:child":
                return child_info
            return None

        mock_graph.get_aspect.side_effect = get_aspect_side_effect

        entities = handler.export(mock_graph)
        assert len(entities) == 2
        # Parent must come before child (topological sort)
        assert entities[0]["urn"] == "urn:li:glossaryNode:parent"
        assert entities[1]["urn"] == "urn:li:glossaryNode:child"

    def test_build_mcps_with_parent(self, handler):
        entity = {
            "urn": "urn:li:glossaryNode:child",
            "name": "Child",
            "definition": "Child node",
            "parentNode": "urn:li:glossaryNode:parent",
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        mcp = mcps[0]
        assert mcp.entityUrn == "urn:li:glossaryNode:child"
        assert isinstance(mcp.aspect, GlossaryNodeInfoClass)
        assert mcp.aspect.parentNode == "urn:li:glossaryNode:parent"

    def test_build_mcps_root_node(self, handler):
        entity = {
            "urn": "urn:li:glossaryNode:root",
            "name": "Root",
            "definition": "Root node",
            "parentNode": None,
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert mcps[0].aspect.parentNode is None


class TestGlossaryTermHandler:
    @pytest.fixture
    def handler(self):
        return GlossaryTermHandler()

    def test_entity_type(self, handler):
        assert handler.entity_type == "glossaryTerm"

    def test_depends_on_nodes(self, handler):
        assert "glossaryNode" in handler.dependencies

    def test_export(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:glossaryTerm:term1",
        ]

        term_info = MagicMock(spec=GlossaryTermInfoClass)
        term_info.name = "Term1"
        term_info.definition = "First term"
        term_info.termSource = "INTERNAL"
        term_info.parentNode = "urn:li:glossaryNode:node1"

        mock_graph.get_aspect.return_value = term_info

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert entities[0]["name"] == "Term1"
        assert entities[0]["parentNode"] == "urn:li:glossaryNode:node1"

    def test_build_mcps(self, handler):
        entity = {
            "urn": "urn:li:glossaryTerm:term1",
            "name": "Term1",
            "definition": "A term",
            "termSource": "INTERNAL",
            "parentNode": "urn:li:glossaryNode:node1",
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        mcp = mcps[0]
        assert mcp.entityUrn == "urn:li:glossaryTerm:term1"
        assert isinstance(mcp.aspect, GlossaryTermInfoClass)
        assert mcp.aspect.termSource == "INTERNAL"
        assert mcp.aspect.parentNode == "urn:li:glossaryNode:node1"

    def test_build_mcps_no_parent(self, handler):
        entity = {
            "urn": "urn:li:glossaryTerm:orphan",
            "name": "Orphan",
            "definition": "No parent",
            "parentNode": None,
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert mcps[0].aspect.parentNode is None
