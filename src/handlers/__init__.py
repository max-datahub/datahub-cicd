from src.handlers.data_products import DataProductHandler
from src.handlers.domains import DomainHandler
from src.handlers.glossary import GlossaryNodeHandler, GlossaryTermHandler
from src.handlers.logical_models import LogicalModelHandler
from src.handlers.tags import TagHandler
from src.registry import HandlerRegistry


def create_default_registry(logical_platforms: list[str] | None = None) -> HandlerRegistry:
    """Create a registry with all governance handlers plus logical model definitions.

    logical_platforms scopes LogicalModelHandler's export (None = every platform
    with dataPlatformInfo.logical == true). Enrichment handlers are registered
    separately because they need governance_urns populated after export.
    """
    registry = HandlerRegistry()
    registry.register(TagHandler())
    registry.register(GlossaryNodeHandler())
    registry.register(GlossaryTermHandler())
    registry.register(DomainHandler())
    registry.register(DataProductHandler())
    registry.register(LogicalModelHandler(platforms=logical_platforms))
    return registry
