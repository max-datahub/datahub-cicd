import logging
import traceback

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
    OwnerClass,
    OwnershipClass,
    TagAssociationClass,
)

from src.interfaces import EntityHandler, UrnMapper
from src.retry import retry_transient
from src.scope import ENV_SUPPORTED_ENTITY_TYPES, ScopeConfig

logger = logging.getLogger(__name__)

# Entity types whose enrichment (tags, terms, domains, ownership) we export.
# editableSchemaMetadata is only on datasets.
# dataJob is excluded by default: typically high count with no user-authored enrichment.
# Add it via --enrichment-entity-types if needed.
ENRICHABLE_ENTITY_TYPES = [
    "dataset",
    "chart",
    "dashboard",
    "container",
    "dataFlow",
    "dataProduct",
]


def _export_common_enrichment(
    graph: DataHubGraph,
    urn: str,
    governance_urns: set[str],
    entry: dict,
) -> bool:
    """Export tags, terms, domains, ownership for a single entity.

    Mutates entry dict in place. Returns True if any enrichment was found.
    API calls are wrapped with retry for transient failures (Amendment 3).
    """
    has_enrichment = False

    # Tags
    @retry_transient(max_retries=3, base_delay=1.0)
    def _get_tags():
        return graph.get_tags(urn)

    tags = _get_tags()
    if tags and tags.tags:
        filtered = [t for t in tags.tags if str(t.tag) in governance_urns]
        if filtered:
            entry["globalTags"] = [{"tag": str(t.tag)} for t in filtered]
            has_enrichment = True

    # Glossary terms
    @retry_transient(max_retries=3, base_delay=1.0)
    def _get_terms():
        return graph.get_glossary_terms(urn)

    terms = _get_terms()
    if terms and terms.terms:
        filtered = [t for t in terms.terms if str(t.urn) in governance_urns]
        if filtered:
            entry["glossaryTerms"] = [{"urn": str(t.urn)} for t in filtered]
            has_enrichment = True

    # Domains
    @retry_transient(max_retries=3, base_delay=1.0)
    def _get_domain():
        return graph.get_domain(urn)

    domain = _get_domain()
    if domain and domain.domains:
        filtered = [d for d in domain.domains if d in governance_urns]
        if filtered:
            entry["domains"] = filtered
            has_enrichment = True

    # Ownership (not filtered by governance URNs -- owner URNs are identity-based)
    @retry_transient(max_retries=3, base_delay=1.0)
    def _get_ownership():
        return graph.get_ownership(urn)

    ownership = _get_ownership()
    if ownership and ownership.owners:
        entry["ownership"] = [
            {
                "owner": str(o.owner),
                "type": str(o.type),
            }
            for o in ownership.owners
        ]
        has_enrichment = True

    return has_enrichment


def _build_common_mcps(
    entity: dict,
    entity_urn: str,
    urn_mapper: UrnMapper,
) -> list[MetadataChangeProposalWrapper]:
    """Build MCPs for tags, terms, domains, ownership on any entity."""
    mcps: list[MetadataChangeProposalWrapper] = []

    if "globalTags" in entity:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
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
                entityUrn=entity_urn,
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
                entityUrn=entity_urn,
                aspect=DomainsClass(
                    domains=[urn_mapper.map(d) for d in entity["domains"]]
                ),
            )
        )

    if "ownership" in entity:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=urn_mapper.map(o["owner"]),
                            type=o["type"],
                        )
                        for o in entity["ownership"]
                    ]
                ),
            )
        )

    return mcps


def _progress_interval(total: int) -> int:
    """Adaptive progress reporting interval."""
    if total > 1000:
        return 100
    if total < 100:
        return 25
    return 50


class DatasetEnrichmentHandler(EntityHandler):

    def __init__(
        self,
        governance_urns: set[str] | None = None,
        scope: ScopeConfig | None = None,
    ) -> None:
        self.governance_urns = governance_urns or set()
        self.scope = scope

    @property
    def entity_type(self) -> str:
        return "enrichment"

    @property
    def dependencies(self) -> list[str]:
        return ["tag", "glossaryNode", "glossaryTerm", "domain"]

    def export(self, graph: DataHubGraph) -> list[dict]:
        """Export tag/term/domain/ownership assignments on datasets + field-level metadata."""
        enriched = []
        scanned = 0
        errors = 0
        dataset_urns = list(graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=self.scope.platforms if self.scope else None,
            env=self.scope.env if self.scope else None,
            extraFilters=self.scope.build_extra_filters() if self.scope else None,
        ))
        total = len(dataset_urns)
        interval = _progress_interval(total)
        logger.info(f"Scanning {total} datasets for enrichment...")
        for i, urn in enumerate(dataset_urns):
            try:
                entry: dict = {"dataset_urn": urn}
                has_enrichment = _export_common_enrichment(
                    graph, urn, self.governance_urns, entry
                )

                # Field-level tags/terms (editableSchemaMetadata) -- dataset only
                @retry_transient(max_retries=3, base_delay=1.0)
                def _get_esm():
                    return graph.get_aspect(urn, EditableSchemaMetadataClass)

                esm = _get_esm()
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
                scanned += 1
            except Exception as e:
                errors += 1
                logger.debug(
                    f"Error exporting enrichment for dataset {urn}",
                    exc_info=True,
                )
                logger.error(
                    f"Error exporting enrichment for dataset {urn}: {e}"
                )

            if (i + 1) % interval == 0 or (i + 1) == total:
                pct = (i + 1) / total * 100 if total else 0
                logger.info(
                    f"  Enrichment scan progress: {i + 1}/{total} datasets "
                    f"({pct:.1f}%, {len(enriched)} enriched)"
                )

        if errors:
            logger.warning(
                f"Enrichment export completed with {errors} errors "
                f"out of {total} datasets"
            )
        logger.info(f"Exported enrichment for {len(enriched)} datasets")
        return enriched

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        dataset_urn = urn_mapper.map(entity["dataset_urn"])
        mcps = _build_common_mcps(entity, dataset_urn, urn_mapper)

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


class GenericEnrichmentHandler(EntityHandler):
    """Handles tag/term/domain/ownership enrichment for non-dataset entity types.

    Supports: chart, dashboard, container, dataFlow, dataJob, dataProduct.
    """

    def __init__(
        self,
        datahub_entity_type: str,
        governance_urns: set[str] | None = None,
        scope: ScopeConfig | None = None,
    ) -> None:
        self._datahub_entity_type = datahub_entity_type
        self.governance_urns = governance_urns or set()
        self.scope = scope

    @property
    def entity_type(self) -> str:
        return f"{self._datahub_entity_type}Enrichment"

    @property
    def dependencies(self) -> list[str]:
        return ["tag", "glossaryNode", "glossaryTerm", "domain"]

    def export(self, graph: DataHubGraph) -> list[dict]:
        enriched = []
        errors = 0
        # env only applies to entity types with an environment field —
        # passing it for charts/dashboards/etc. would exclude all results.
        env_filter = (
            self.scope.env
            if self.scope and self._datahub_entity_type in ENV_SUPPORTED_ENTITY_TYPES
            else None
        )
        urns = list(
            graph.get_urns_by_filter(
                entity_types=[self._datahub_entity_type],
                platform=self.scope.platforms if self.scope else None,
                env=env_filter,
                extraFilters=self.scope.build_extra_filters() if self.scope else None,
            )
        )
        total = len(urns)
        interval = _progress_interval(total)
        logger.info(
            f"Scanning {total} {self._datahub_entity_type} entities "
            f"for enrichment..."
        )
        for i, urn in enumerate(urns):
            try:
                entry: dict = {"entity_urn": urn}
                has_enrichment = _export_common_enrichment(
                    graph, urn, self.governance_urns, entry
                )
                if has_enrichment:
                    enriched.append(entry)
            except Exception as e:
                errors += 1
                logger.debug(
                    f"Error exporting enrichment for "
                    f"{self._datahub_entity_type} {urn}",
                    exc_info=True,
                )
                logger.error(
                    f"Error exporting enrichment for "
                    f"{self._datahub_entity_type} {urn}: {e}"
                )

            if (i + 1) % interval == 0 or (i + 1) == total:
                pct = (i + 1) / total * 100 if total else 0
                logger.info(
                    f"  {self._datahub_entity_type} scan progress: "
                    f"{i + 1}/{total} ({pct:.1f}%, {len(enriched)} enriched)"
                )

        if errors:
            logger.warning(
                f"Enrichment export for {self._datahub_entity_type} "
                f"completed with {errors} errors out of {total} entities"
            )
        logger.info(
            f"Exported enrichment for {len(enriched)} "
            f"{self._datahub_entity_type} entities"
        )
        return enriched

    def build_mcps(
        self, entity: dict, urn_mapper: UrnMapper
    ) -> list[MetadataChangeProposalWrapper]:
        entity_urn = urn_mapper.map(entity["entity_urn"])
        return _build_common_mcps(entity, entity_urn, urn_mapper)
