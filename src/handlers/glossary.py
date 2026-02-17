import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
)

from src.interfaces import EntityHandler, UrnMapper
from src.utils import name_from_urn, topological_sort

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
                name = info.name or name_from_urn(urn)
                entities.append(
                    {
                        "urn": urn,
                        "name": name,
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
                name = info.name or name_from_urn(urn)
                # termSource may be stored as a URN (e.g. parent node URN)
                # in some DataHub instances. Pass through as-is to preserve
                # fidelity with the source.
                entities.append(
                    {
                        "urn": urn,
                        "name": name,
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
        # Map termSource through urn_mapper if it looks like a URN
        term_source = entity.get("termSource", "INTERNAL")
        if term_source and term_source.startswith("urn:"):
            term_source = urn_mapper.map(term_source)
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=GlossaryTermInfoClass(
                    definition=entity.get("definition", ""),
                    name=entity.get("name"),
                    termSource=term_source,
                    parentNode=parent,
                ),
            )
        ]

    def is_system_entity(self, urn: str) -> bool:
        return "__system__" in urn
