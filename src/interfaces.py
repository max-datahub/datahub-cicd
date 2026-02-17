from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph


@dataclass
class SyncResult:
    entity_type: str
    urn: str
    status: str  # "success" | "failed" | "skipped"
    error: Optional[str] = None


class UrnMapper(ABC):
    """Maps source URNs to target URNs. Passthrough by default."""

    def map(self, urn: str) -> str:
        return urn

    def map_all(self, urns: list[str]) -> list[str]:
        return [self.map(u) for u in urns]


class WriteStrategy(ABC):
    """Controls how MCPs are written to the target DataHub.

    Performance: Implementations should batch MCPs where possible.
    The emit() method receives all MCPs for a single entity;
    emit_batch() receives all MCPs for a handler phase and can
    optimize across entities (e.g., via graph.emit_mcps()).
    """

    @abstractmethod
    def emit(
        self, graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]
    ) -> list[SyncResult]:
        """Emit MCPs for a single entity."""
        ...

    def emit_batch(
        self,
        graph: DataHubGraph,
        mcp_groups: list[tuple[str, list[MetadataChangeProposalWrapper]]],
    ) -> list[SyncResult]:
        """Emit MCPs for multiple entities in a batch.

        Args:
            mcp_groups: List of (urn, mcps) tuples, one per entity.

        Default implementation delegates to emit() per entity.
        Override for batch-optimized writes.
        """
        results = []
        for _urn, mcps in mcp_groups:
            results.extend(self.emit(graph, mcps))
        return results


class EntityHandler(ABC):
    """Base class for all entity type handlers.

    To add a new entity type:
    1. Subclass EntityHandler
    2. Implement entity_type, export(), build_mcps()
    3. Register in src/handlers/__init__.py

    Performance notes:
    - export() should log progress for large entity counts
    - Handlers with no inter-dependencies can run concurrently
      (see SyncOrchestrator)
    """

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """Unique string identifier (e.g., 'tag', 'glossaryNode', 'domain')."""
        ...

    @property
    def dependencies(self) -> list[str]:
        """Entity types that must be synced before this one.
        Return entity_type strings of handlers that must run first."""
        return []

    @abstractmethod
    def export(self, graph: DataHubGraph) -> list[dict]:
        """Export all entities of this type from a DataHub instance.
        Must handle pagination internally (SDK handles this via
        get_urns_by_filter's scroll-based iteration).
        Must filter system entities via is_system_entity()."""
        ...

    @abstractmethod
    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        """Build MCPs to sync a single entity to the target.
        Use urn_mapper.map() on all URN references."""
        ...

    def is_system_entity(self, urn: str) -> bool:
        """Return True to skip this entity during export."""
        return False

    def validate(self, entities: list[dict]) -> list[str]:
        """Optional pre-sync validation. Returns list of error messages."""
        return []
