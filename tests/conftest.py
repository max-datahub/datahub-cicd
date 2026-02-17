from unittest.mock import MagicMock

import pytest

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
    return graph


@pytest.fixture
def passthrough_mapper() -> UrnMapper:
    return PassthroughMapper()


@pytest.fixture
def registry() -> HandlerRegistry:
    return HandlerRegistry()
