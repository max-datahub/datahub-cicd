import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import TagPropertiesClass

from src.interfaces import EntityHandler, UrnMapper

logger = logging.getLogger(__name__)


class TagHandler(EntityHandler):

    @property
    def entity_type(self) -> str:
        return "tag"

    @property
    def dependencies(self) -> list[str]:
        return []

    def export(self, graph: DataHubGraph) -> list[dict]:
        entities = []
        for urn in graph.get_urns_by_filter(entity_types=["tag"]):
            if self.is_system_entity(urn):
                logger.debug(f"Skipping system tag: {urn}")
                continue
            props = graph.get_aspect(urn, TagPropertiesClass)
            if props:
                entities.append(
                    {
                        "urn": urn,
                        "name": props.name,
                        "description": props.description,
                        "colorHex": props.colorHex,
                    }
                )
        logger.info(f"Exported {len(entities)} tags")
        return entities

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        target_urn = urn_mapper.map(entity["urn"])
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=TagPropertiesClass(
                    name=entity["name"],
                    description=entity.get("description"),
                    colorHex=entity.get("colorHex"),
                ),
            )
        ]

    def is_system_entity(self, urn: str) -> bool:
        return "__default_" in urn
