from src.handlers.domains import DomainHandler
from src.handlers.glossary import GlossaryNodeHandler, GlossaryTermHandler
from src.handlers.tags import TagHandler
from src.registry import HandlerRegistry


def create_default_registry() -> HandlerRegistry:
    """Create a registry with all default governance handlers.

    Note: DatasetEnrichmentHandler is registered separately because
    it needs governance_urns populated after export.
    """
    registry = HandlerRegistry()
    registry.register(TagHandler())
    registry.register(GlossaryNodeHandler())
    registry.register(GlossaryTermHandler())
    registry.register(DomainHandler())
    return registry
