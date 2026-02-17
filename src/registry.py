import logging

from src.interfaces import EntityHandler

logger = logging.getLogger(__name__)


class HandlerRegistry:
    """Registry for entity handlers. Resolves dependency ordering."""

    def __init__(self) -> None:
        self._handlers: dict[str, EntityHandler] = {}

    def register(self, handler: EntityHandler) -> None:
        entity_type = handler.entity_type
        if entity_type in self._handlers:
            raise ValueError(f"Handler already registered for '{entity_type}'")
        self._handlers[entity_type] = handler
        logger.debug(f"Registered handler for '{entity_type}'")

    def get_handler(self, entity_type: str) -> EntityHandler:
        if entity_type not in self._handlers:
            raise KeyError(f"No handler registered for '{entity_type}'")
        return self._handlers[entity_type]

    def get_all_handlers(self) -> list[EntityHandler]:
        return list(self._handlers.values())

    def get_sync_order(self) -> list[EntityHandler]:
        """Return handlers in dependency-resolved order.
        Uses topological sort on handler.dependencies."""
        resolved: list[str] = []
        visited: set[str] = set()
        in_stack: set[str] = set()

        def visit(entity_type: str) -> None:
            if entity_type in visited:
                return
            if entity_type in in_stack:
                raise ValueError(
                    f"Circular dependency detected involving '{entity_type}'"
                )
            in_stack.add(entity_type)

            handler = self._handlers.get(entity_type)
            if handler is None:
                raise KeyError(
                    f"Dependency '{entity_type}' not registered"
                )

            for dep in handler.dependencies:
                visit(dep)

            in_stack.remove(entity_type)
            visited.add(entity_type)
            resolved.append(entity_type)

        for entity_type in self._handlers:
            visit(entity_type)

        return [self._handlers[et] for et in resolved]
