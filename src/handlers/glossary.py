import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
)

from src.interfaces import EntityHandler, UrnMapper
from src.utils import topological_sort

logger = logging.getLogger(__name__)


class GlossaryNodeHandler(EntityHandler):

    @property
    def entity_type(self) -> str:
        return "glossaryNode"

    @property
    def dependencies(self) -> list[str]:
        return []

    def export(self, graph: DataHubGraph) -> list[dict]:
        entities = []
        for urn in graph.get_urns_by_filter(entity_types=["glossaryNode"]):
            if self.is_system_entity(urn):
                logger.debug(f"Skipping system glossary node: {urn}")
                continue
            info = graph.get_aspect(urn, GlossaryNodeInfoClass)
            if info:
                entities.append(
                    {
                        "urn": urn,
                        "name": info.name,
                        "definition": info.definition,
                        "parentNode": info.parentNode,
                    }
                )
        sorted_entities = topological_sort(entities, parent_key="parentNode")
        logger.info(f"Exported {len(sorted_entities)} glossary nodes")
        return sorted_entities

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        target_urn = urn_mapper.map(entity["urn"])
        parent = (
            urn_mapper.map(entity["parentNode"])
            if entity.get("parentNode")
            else None
        )
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=GlossaryNodeInfoClass(
                    definition=entity.get("definition", ""),
                    name=entity.get("name"),
                    parentNode=parent,
                ),
            )
        ]

    def is_system_entity(self, urn: str) -> bool:
        return "__system__" in urn


class GlossaryTermHandler(EntityHandler):

    @property
    def entity_type(self) -> str:
        return "glossaryTerm"

    @property
    def dependencies(self) -> list[str]:
        return ["glossaryNode"]

    def export(self, graph: DataHubGraph) -> list[dict]:
        entities = []
        for urn in graph.get_urns_by_filter(entity_types=["glossaryTerm"]):
            if self.is_system_entity(urn):
                logger.debug(f"Skipping system glossary term: {urn}")
                continue
            info = graph.get_aspect(urn, GlossaryTermInfoClass)
            if info:
                entities.append(
                    {
                        "urn": urn,
                        "name": info.name,
                        "definition": info.definition,
                        "termSource": info.termSource,
                        "parentNode": info.parentNode,
                    }
                )
        sorted_entities = topological_sort(entities, parent_key="parentNode")
        logger.info(f"Exported {len(sorted_entities)} glossary terms")
        return sorted_entities

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        target_urn = urn_mapper.map(entity["urn"])
        parent = (
            urn_mapper.map(entity["parentNode"])
            if entity.get("parentNode")
            else None
        )
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=GlossaryTermInfoClass(
                    definition=entity.get("definition", ""),
                    name=entity.get("name"),
                    termSource=entity.get("termSource", "INTERNAL"),
                    parentNode=parent,
                ),
            )
        ]

    def is_system_entity(self, urn: str) -> bool:
        return "__system__" in urn
