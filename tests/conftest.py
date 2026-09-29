from unittest.mock import MagicMock

import pytest
from datahub.metadata.schema_classes import ASPECT_NAME_MAP

from src.interfaces import UrnMapper
from src.registry import HandlerRegistry
from src.urn_mapper import PassthroughMapper


@pytest.fixture
def mock_graph():
    """Mock DataHubGraph client."""
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.return_value = None
    graph.get_tags.return_value = None
    graph.get_glossary_terms.return_value = None
    graph.get_domain.return_value = None
    graph.emit_mcp.return_value = None
    graph.soft_delete_entity.return_value = None
    graph.get_entity_as_mcps.return_value = []

    # Batched reads answer from the get_aspect stub, so tests set up aspects one way.
    def get_entities(entity_name, urns, aspects=None, **_):
        found = {}
        for urn in urns:
            bag = {name: graph.get_aspect(urn, ASPECT_NAME_MAP[name]) for name in aspects or []}
            found[urn] = {name: (v, None) for name, v in bag.items() if v is not None}
        return found

    graph.get_entities.side_effect = get_entities
    return graph


@pytest.fixture
def passthrough_mapper() -> UrnMapper:
    return PassthroughMapper()


@pytest.fixture
def registry() -> HandlerRegistry:
    return HandlerRegistry()
