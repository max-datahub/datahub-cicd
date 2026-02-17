import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DomainPropertiesClass

from src.interfaces import EntityHandler, UrnMapper
from src.utils import topological_sort

logger = logging.getLogger(__name__)


class DomainHandler(EntityHandler):

    @property
    def entity_type(self) -> str:
        return "domain"

    @property
    def dependencies(self) -> list[str]:
        return []

    def export(self, graph: DataHubGraph) -> list[dict]:
        entities = []
        for urn in graph.get_urns_by_filter(entity_types=["domain"]):
            if self.is_system_entity(urn):
                logger.debug(f"Skipping system domain: {urn}")
                continue
            props = graph.get_aspect(urn, DomainPropertiesClass)
            if props:
                entities.append(
                    {
                        "urn": urn,
                        "name": props.name,
                        "description": props.description,
                        "parentDomain": props.parentDomain,
                    }
                )
        sorted_entities = topological_sort(entities, parent_key="parentDomain")
        logger.info(f"Exported {len(sorted_entities)} domains")
        return sorted_entities

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        target_urn = urn_mapper.map(entity["urn"])
        parent = (
            urn_mapper.map(entity["parentDomain"])
            if entity.get("parentDomain")
            else None
        )
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=DomainPropertiesClass(
                    name=entity["name"],
                    description=entity.get("description"),
                    parentDomain=parent,
                ),
            )
        ]

    def is_system_entity(self, urn: str) -> bool:
        return "__system__" in urn
