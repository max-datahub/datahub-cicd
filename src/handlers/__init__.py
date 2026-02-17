from src.handlers.data_products import DataProductHandler
from src.handlers.domains import DomainHandler
from src.handlers.glossary import GlossaryNodeHandler, GlossaryTermHandler
from src.handlers.tags import TagHandler
from src.registry import HandlerRegistry


def create_default_registry() -> HandlerRegistry:
    """Create a registry with all governance handlers.

    Note: Enrichment handlers (DatasetEnrichmentHandler, GenericEnrichmentHandler)
    are registered separately because they need governance_urns populated after export.
    """
    registry = HandlerRegistry()
    registry.register(TagHandler())
    registry.register(GlossaryNodeHandler())
    registry.register(GlossaryTermHandler())
    registry.register(DomainHandler())
    registry.register(DataProductHandler())
    return registry
