import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
)

from src.interfaces import EntityHandler, UrnMapper

logger = logging.getLogger(__name__)


class DataProductHandler(EntityHandler):

    @property
    def entity_type(self) -> str:
        return "dataProduct"

    @property
    def dependencies(self) -> list[str]:
        return ["domain"]

    def export(self, graph: DataHubGraph) -> list[dict]:
        entities = []
        for urn in graph.get_urns_by_filter(entity_types=["dataProduct"]):
            if self.is_system_entity(urn):
                logger.debug(f"Skipping system data product: {urn}")
                continue
            props = graph.get_aspect(urn, DataProductPropertiesClass)
            if props:
                assets = []
                for a in props.assets or []:
                    assets.append({"destinationUrn": str(a.destinationUrn)})
                entities.append(
                    {
                        "urn": urn,
                        "name": props.name,
                        "description": props.description,
                        "customProperties": props.customProperties or {},
                        "assets": assets,
                    }
                )
        logger.info(f"Exported {len(entities)} data products")
        return entities

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        target_urn = urn_mapper.map(entity["urn"])
        assets = []
        for a in entity.get("assets", []):
            assets.append(
                DataProductAssociationClass(
                    sourceUrn=target_urn,
                    destinationUrn=urn_mapper.map(a["destinationUrn"]),
                    created=AuditStampClass(
                        time=0, actor="urn:li:corpuser:datahub"
                    ),
                )
            )
        return [
            MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=DataProductPropertiesClass(
                    name=entity["name"],
                    description=entity.get("description"),
                    customProperties=entity.get("customProperties", {}),
                    assets=assets,
                ),
            )
        ]
