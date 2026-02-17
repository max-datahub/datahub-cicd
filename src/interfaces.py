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
    """Controls how MCPs are written to the target DataHub."""

    @abstractmethod
    def emit(
        self, graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]
    ) -> list[SyncResult]:
        ...


class EntityHandler(ABC):
    """Base class for all entity type handlers.

    To add a new entity type:
    1. Subclass EntityHandler
    2. Implement entity_type, export(), build_mcps()
    3. Register in src/handlers/__init__.py
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
        Must handle pagination internally.
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
