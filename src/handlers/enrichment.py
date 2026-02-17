import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DomainsClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    TagAssociationClass,
)

from src.interfaces import EntityHandler, UrnMapper

logger = logging.getLogger(__name__)


class DatasetEnrichmentHandler(EntityHandler):

    def __init__(self, governance_urns: set[str] | None = None) -> None:
        self.governance_urns = governance_urns or set()

    @property
    def entity_type(self) -> str:
        return "enrichment"

    @property
    def dependencies(self) -> list[str]:
        return ["tag", "glossaryNode", "glossaryTerm", "domain"]

    def export(self, graph: DataHubGraph) -> list[dict]:
        """Export tag/term/domain assignments on datasets + field-level metadata."""
        enriched = []
        for urn in graph.get_urns_by_filter(entity_types=["dataset"]):
            entry: dict = {"dataset_urn": urn}
            has_enrichment = False

            # Dataset-level tags
            tags = graph.get_tags(urn)
            if tags and tags.tags:
                filtered = [
                    t for t in tags.tags if str(t.tag) in self.governance_urns
                ]
                if filtered:
                    entry["globalTags"] = [
                        {"tag": str(t.tag)} for t in filtered
                    ]
                    has_enrichment = True

            # Dataset-level glossary terms
            terms = graph.get_glossary_terms(urn)
            if terms and terms.terms:
                filtered = [
                    t for t in terms.terms if str(t.urn) in self.governance_urns
                ]
                if filtered:
                    entry["glossaryTerms"] = [
                        {"urn": str(t.urn)} for t in filtered
                    ]
                    has_enrichment = True

            # Dataset-level domain
            domain = graph.get_domain(urn)
            if domain and domain.domains:
                filtered = [
                    d for d in domain.domains if d in self.governance_urns
                ]
                if filtered:
                    entry["domains"] = filtered
                    has_enrichment = True

            # Field-level tags/terms (editableSchemaMetadata)
            esm = graph.get_aspect(urn, EditableSchemaMetadataClass)
            if esm and esm.editableSchemaFieldInfo:
                field_entries = []
                for field_info in esm.editableSchemaFieldInfo:
                    field_entry: dict = {"fieldPath": field_info.fieldPath}
                    field_has = False
                    if field_info.globalTags and field_info.globalTags.tags:
                        ft = [
                            t
                            for t in field_info.globalTags.tags
                            if str(t.tag) in self.governance_urns
                        ]
                        if ft:
                            field_entry["globalTags"] = [
                                {"tag": str(t.tag)} for t in ft
                            ]
                            field_has = True
                    if (
                        field_info.glossaryTerms
                        and field_info.glossaryTerms.terms
                    ):
                        ft = [
                            t
                            for t in field_info.glossaryTerms.terms
                            if str(t.urn) in self.governance_urns
                        ]
                        if ft:
                            field_entry["glossaryTerms"] = [
                                {"urn": str(t.urn)} for t in ft
                            ]
                            field_has = True
                    if field_has:
                        field_entries.append(field_entry)
                if field_entries:
                    entry["editableSchemaMetadata"] = field_entries
                    has_enrichment = True

            if has_enrichment:
                enriched.append(entry)

        logger.info(f"Exported enrichment for {len(enriched)} datasets")
        return enriched

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        mcps: list[MetadataChangeProposalWrapper] = []
        dataset_urn = urn_mapper.map(entity["dataset_urn"])

        if "globalTags" in entity:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(tag=urn_mapper.map(t["tag"]))
                            for t in entity["globalTags"]
                        ]
                    ),
                )
            )

        if "glossaryTerms" in entity:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlossaryTermsClass(
                        terms=[
                            GlossaryTermAssociationClass(
                                urn=urn_mapper.map(t["urn"])
                            )
                            for t in entity["glossaryTerms"]
                        ],
                        auditStamp=AuditStampClass(
                            time=0, actor="urn:li:corpuser:datahub"
                        ),
                    ),
                )
            )

        if "domains" in entity:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DomainsClass(
                        domains=[urn_mapper.map(d) for d in entity["domains"]]
                    ),
                )
            )

        if "editableSchemaMetadata" in entity:
            field_infos = []
            for fe in entity["editableSchemaMetadata"]:
                fi = EditableSchemaFieldInfoClass(fieldPath=fe["fieldPath"])
                if "globalTags" in fe:
                    fi.globalTags = GlobalTagsClass(
                        tags=[
                            TagAssociationClass(tag=urn_mapper.map(t["tag"]))
                            for t in fe["globalTags"]
                        ]
                    )
                if "glossaryTerms" in fe:
                    fi.glossaryTerms = GlossaryTermsClass(
                        terms=[
                            GlossaryTermAssociationClass(
                                urn=urn_mapper.map(t["urn"])
                            )
                            for t in fe["glossaryTerms"]
                        ],
                        auditStamp=AuditStampClass(
                            time=0, actor="urn:li:corpuser:datahub"
                        ),
                    )
                field_infos.append(fi)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=field_infos
                    ),
                )
            )

        return mcps
