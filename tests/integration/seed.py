"""Seed a DataHub instance with all entity types and aspects for integration testing.

Creates a deterministic set of governance entities, data assets, and enrichment
assignments that cover every supported entity type and aspect.

Includes edge-case entities for testing:
- Non-governance tag references that should be stripped from enrichment
- Tag assigned to a dataset then soft-deleted (enrichment should exclude)
- Glossary term with null name (pipeline derives name from URN)
- Glossary term with URN in termSource (pipeline passes through, maps if URN)
- Dataset with empty tag/ownership aspects (should not produce enrichment)
- Dataset on a second platform (snowflake, for multi-platform scope testing)
"""

import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataFlowInfoClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainsClass,
    DomainPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryNodeInfoClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    TagAssociationClass,
    TagPropertiesClass,
)

logger = logging.getLogger(__name__)

# ── URN constants ──────────────────────────────────────────────────────────

# Tags
TAG_PII = "urn:li:tag:integration-pii"
TAG_FINANCIAL = "urn:li:tag:integration-financial"
TAG_SYSTEM = "urn:li:tag:__default_integration_test"

# Tag that will be assigned to a dataset then soft-deleted.
# Tests that enrichment filtering strips references to deleted governance.
TAG_ASSIGNED_THEN_DELETED = "urn:li:tag:integration-assigned-then-deleted"

# Soft-deleted entities (created then deleted, for deletion propagation tests).
# NOTE: only entity types with a registered `status` aspect can be soft-deleted
# via soft_delete_entity(). In current DataHub head, `domain` does NOT support
# the status aspect — soft_delete_entity() returns 422. Tags work fine.
TAG_DELETED = "urn:li:tag:integration-deleted-tag"
TERM_DELETED = "urn:li:glossaryTerm:integration-deleted-term"

# Glossary nodes (hierarchy: root -> child)
NODE_ROOT = "urn:li:glossaryNode:integration-root-node"
NODE_CHILD = "urn:li:glossaryNode:integration-child-node"

# Glossary terms (under nodes)
TERM_A = "urn:li:glossaryTerm:integration-term-a"
TERM_B = "urn:li:glossaryTerm:integration-term-b"

# Glossary term with null name -- DataHub sometimes stores null for
# GlossaryTermInfo.name. Pipeline should derive name from URN.
TERM_NULL_NAME = "urn:li:glossaryTerm:integration-term-null-name"

# Glossary term with URN stored in termSource field instead of INTERNAL/EXTERNAL.
# Some DataHub instances store the parent node URN in termSource.
# Pipeline passes through as-is and maps via UrnMapper if it looks like a URN.
TERM_URN_TERMSOURCE = "urn:li:glossaryTerm:integration-term-urn-source"

# Domains (hierarchy: root -> child)
DOMAIN_ROOT = "urn:li:domain:integration-root-domain"
DOMAIN_CHILD = "urn:li:domain:integration-child-domain"

# Data products
DATA_PRODUCT = "urn:li:dataProduct:integration-product"

# Datasets
DATASET_1 = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.users,PROD)"
DATASET_2 = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.orders,PROD)"
DATASET_NO_ENRICHMENT = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.logs,PROD)"

# Dataset with a mix of governance + non-governance tags.
# System tag (__default_*) should be stripped from enrichment.
DATASET_MIXED_TAGS = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.mixed_tags,PROD)"

# Dataset on a different platform (snowflake) for multi-platform scope testing.
DATASET_SNOWFLAKE = "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.public.events,PROD)"

# Dataset with the "assigned then deleted" tag. After the tag is soft-deleted,
# enrichment should not include the tag reference.
DATASET_WITH_DELETED_TAG = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.deleted_tag_ref,PROD)"

# Dataset with empty aspects (GlobalTags with tags=[], Ownership with owners=[]).
# Tests that the pipeline handles empty-but-present aspects gracefully.
DATASET_EMPTY_ASPECTS = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.empty_aspects,PROD)"

# Charts
CHART_1 = "urn:li:chart:(looker,integration-chart-1)"

# Dashboards
DASHBOARD_1 = "urn:li:dashboard:(looker,integration-dashboard-1)"

# Containers
CONTAINER_1 = "urn:li:container:integration-container-1"

# DataFlows
DATAFLOW_1 = "urn:li:dataFlow:(airflow,integration-flow,prod)"

# Users
USER_ALICE = "urn:li:corpuser:alice@integration-test.com"
USER_BOB = "urn:li:corpuser:bob@integration-test.com"

# ── All governance URNs (used by enrichment filtering) ─────────────────────

ALL_GOVERNANCE_URNS = {
    TAG_PII,
    TAG_FINANCIAL,
    NODE_ROOT,
    NODE_CHILD,
    TERM_A,
    TERM_B,
    TERM_NULL_NAME,
    TERM_URN_TERMSOURCE,
    DOMAIN_ROOT,
    DOMAIN_CHILD,
    DATA_PRODUCT,
}


def _emit(graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]) -> None:
    for mcp in mcps:
        graph.emit_mcp(mcp)


def seed_tags(graph: DataHubGraph) -> None:
    """Create tag definitions, including a system tag that should be filtered."""
    logger.info("Seeding tags...")
    _emit(
        graph,
        [
            MetadataChangeProposalWrapper(
                entityUrn=TAG_PII,
                aspect=TagPropertiesClass(
                    name="Integration PII",
                    description="PII tag for integration test",
                    colorHex="#FF0000",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=TAG_FINANCIAL,
                aspect=TagPropertiesClass(
                    name="Integration Financial",
                    description="Financial data tag",
                ),
            ),
            # System tag (should be filtered by export)
            MetadataChangeProposalWrapper(
                entityUrn=TAG_SYSTEM,
                aspect=TagPropertiesClass(
                    name="__default_integration_test",
                    description="System tag, should not be exported",
                ),
            ),
            # Tag that will be assigned to a dataset then soft-deleted.
            # Created here so it exists before enrichment is seeded.
            MetadataChangeProposalWrapper(
                entityUrn=TAG_ASSIGNED_THEN_DELETED,
                aspect=TagPropertiesClass(
                    name="Integration Assigned Then Deleted",
                    description="Will be assigned to a dataset, then soft-deleted",
                ),
            ),
        ],
    )


def seed_glossary(graph: DataHubGraph) -> None:
    """Create glossary nodes (nested) and terms (with parent refs)."""
    logger.info("Seeding glossary nodes and terms...")
    _emit(
        graph,
        [
            # Root node
            MetadataChangeProposalWrapper(
                entityUrn=NODE_ROOT,
                aspect=GlossaryNodeInfoClass(
                    definition="Root node for integration tests",
                    name="Integration Root",
                ),
            ),
            # Child node (parent = root)
            MetadataChangeProposalWrapper(
                entityUrn=NODE_CHILD,
                aspect=GlossaryNodeInfoClass(
                    definition="Child node under root",
                    name="Integration Child",
                    parentNode=NODE_ROOT,
                ),
            ),
            # Term A (parent = root node)
            MetadataChangeProposalWrapper(
                entityUrn=TERM_A,
                aspect=GlossaryTermInfoClass(
                    definition="Term A: first integration term",
                    name="Integration Term A",
                    termSource="INTERNAL",
                    parentNode=NODE_ROOT,
                ),
            ),
            # Term B (parent = child node)
            MetadataChangeProposalWrapper(
                entityUrn=TERM_B,
                aspect=GlossaryTermInfoClass(
                    definition="Term B: under child node",
                    name="Integration Term B",
                    termSource="INTERNAL",
                    parentNode=NODE_CHILD,
                ),
            ),
            # ── Data model quirk: term with null name ──
            # DataHub does not always populate GlossaryTermInfo.name.
            # Terms created via certain code paths or older SDK versions
            # may have null names. Pipeline should derive name from URN.
            MetadataChangeProposalWrapper(
                entityUrn=TERM_NULL_NAME,
                aspect=GlossaryTermInfoClass(
                    definition="Term with null name for quirk testing",
                    name=None,
                    termSource="INTERNAL",
                    parentNode=NODE_ROOT,
                ),
            ),
            # ── Data model quirk: URN in termSource ──
            # Some DataHub instances store the parent node URN in
            # termSource instead of the expected "INTERNAL"/"EXTERNAL".
            # Pipeline passes through as-is and maps via UrnMapper
            # if it looks like a URN.
            MetadataChangeProposalWrapper(
                entityUrn=TERM_URN_TERMSOURCE,
                aspect=GlossaryTermInfoClass(
                    definition="Term with URN in termSource",
                    name="Integration Term URN Source",
                    termSource=NODE_CHILD,  # URN instead of enum!
                    parentNode=NODE_CHILD,
                ),
            ),
        ],
    )


def seed_domains(graph: DataHubGraph) -> None:
    """Create domains with nested hierarchy."""
    logger.info("Seeding domains...")
    _emit(
        graph,
        [
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_ROOT,
                aspect=DomainPropertiesClass(
                    name="Integration Root Domain",
                    description="Root domain for integration tests",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DOMAIN_CHILD,
                aspect=DomainPropertiesClass(
                    name="Integration Child Domain",
                    description="Nested under root domain",
                    parentDomain=DOMAIN_ROOT,
                ),
            ),
        ],
    )


def seed_data_products(graph: DataHubGraph) -> None:
    """Create a data product with asset references."""
    logger.info("Seeding data products...")
    _emit(
        graph,
        [
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT,
                aspect=DataProductPropertiesClass(
                    name="Integration Product",
                    description="Data product for integration tests",
                    assets=[
                        DataProductAssociationClass(
                            sourceUrn=DATA_PRODUCT,
                            destinationUrn=DATASET_1,
                            created=AuditStampClass(
                                time=0, actor="urn:li:corpuser:datahub"
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )


def seed_data_assets(graph: DataHubGraph) -> None:
    """Create datasets, charts, dashboards, containers, dataflows."""
    logger.info("Seeding data assets...")
    _emit(
        graph,
        [
            # Datasets
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=DatasetPropertiesClass(
                    name="users",
                    description="Users table",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_2,
                aspect=DatasetPropertiesClass(
                    name="orders",
                    description="Orders table",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_NO_ENRICHMENT,
                aspect=DatasetPropertiesClass(
                    name="logs",
                    description="Logs table (no enrichment)",
                ),
            ),
            # Dataset with mixed governance/non-governance tags
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_MIXED_TAGS,
                aspect=DatasetPropertiesClass(
                    name="mixed_tags",
                    description="Dataset with governance + system tags",
                ),
            ),
            # Dataset on snowflake (different platform for scope testing)
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_SNOWFLAKE,
                aspect=DatasetPropertiesClass(
                    name="events",
                    description="Snowflake events table",
                ),
            ),
            # Dataset referencing a tag that will be soft-deleted
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_WITH_DELETED_TAG,
                aspect=DatasetPropertiesClass(
                    name="deleted_tag_ref",
                    description="Dataset with tag that gets soft-deleted",
                ),
            ),
            # Dataset with empty aspects
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_EMPTY_ASPECTS,
                aspect=DatasetPropertiesClass(
                    name="empty_aspects",
                    description="Dataset with empty tag/owner lists",
                ),
            ),
            # Chart
            MetadataChangeProposalWrapper(
                entityUrn=CHART_1,
                aspect=ChartInfoClass(
                    title="Integration Chart",
                    description="Chart for integration tests",
                    lastModified=ChangeAuditStampsClass(),
                ),
            ),
            # Dashboard
            MetadataChangeProposalWrapper(
                entityUrn=DASHBOARD_1,
                aspect=DashboardInfoClass(
                    title="Integration Dashboard",
                    description="Dashboard for integration tests",
                    lastModified=ChangeAuditStampsClass(),
                ),
            ),
            # Container
            MetadataChangeProposalWrapper(
                entityUrn=CONTAINER_1,
                aspect=ContainerPropertiesClass(
                    name="integration-container",
                    description="Container for integration tests",
                ),
            ),
            # DataFlow
            MetadataChangeProposalWrapper(
                entityUrn=DATAFLOW_1,
                aspect=DataFlowInfoClass(
                    name="Integration Flow",
                    description="DataFlow for integration tests",
                ),
            ),
        ],
    )


def seed_enrichment(graph: DataHubGraph) -> None:
    """Apply tags, terms, domains, and ownership to data assets."""
    logger.info("Seeding enrichment on data assets...")

    audit = AuditStampClass(time=0, actor="urn:li:corpuser:datahub")

    _emit(
        graph,
        [
            # ── Dataset 1: full enrichment (tags, terms, domains, ownership, field-level) ──
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=TAG_PII),
                        TagAssociationClass(tag=TAG_FINANCIAL),
                    ]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=TERM_A)],
                    auditStamp=audit,
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=DomainsClass(domains=[DOMAIN_ROOT]),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_ALICE, type="TECHNICAL_OWNER"),
                        OwnerClass(owner=USER_BOB, type="BUSINESS_OWNER"),
                    ]
                ),
            ),
            # Field-level tags on dataset 1
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_1,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfoClass(
                            fieldPath="email",
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag=TAG_PII)]
                            ),
                        ),
                        EditableSchemaFieldInfoClass(
                            fieldPath="name",
                            glossaryTerms=GlossaryTermsClass(
                                terms=[
                                    GlossaryTermAssociationClass(urn=TERM_A)
                                ],
                                auditStamp=audit,
                            ),
                        ),
                    ]
                ),
            ),
            # ── Dataset 2: partial enrichment (domain + ownership only) ──
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_2,
                aspect=DomainsClass(domains=[DOMAIN_CHILD]),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_2,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_ALICE, type="TECHNICAL_OWNER"),
                    ]
                ),
            ),
            # ── Chart: ownership + domain ──
            MetadataChangeProposalWrapper(
                entityUrn=CHART_1,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_BOB, type="TECHNICAL_OWNER"),
                    ]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=CHART_1,
                aspect=DomainsClass(domains=[DOMAIN_ROOT]),
            ),
            # ── Dashboard: ownership ──
            MetadataChangeProposalWrapper(
                entityUrn=DASHBOARD_1,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_ALICE, type="BUSINESS_OWNER"),
                    ]
                ),
            ),
            # ── Container: domain + ownership ──
            MetadataChangeProposalWrapper(
                entityUrn=CONTAINER_1,
                aspect=DomainsClass(domains=[DOMAIN_ROOT]),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=CONTAINER_1,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_ALICE, type="TECHNICAL_OWNER"),
                    ]
                ),
            ),
            # ── DataFlow: ownership ──
            MetadataChangeProposalWrapper(
                entityUrn=DATAFLOW_1,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_ALICE, type="TECHNICAL_OWNER"),
                    ]
                ),
            ),
            # ── DataProduct: domain + ownership ──
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT,
                aspect=DomainsClass(domains=[DOMAIN_CHILD]),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATA_PRODUCT,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(owner=USER_BOB, type="BUSINESS_OWNER"),
                    ]
                ),
            ),
            # ── Edge case: mixed governance + non-governance tags ──
            # TAG_PII is governance (exported), TAG_SYSTEM is system (filtered).
            # Enrichment should include TAG_PII but NOT TAG_SYSTEM.
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_MIXED_TAGS,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=TAG_PII),
                        TagAssociationClass(tag=TAG_SYSTEM),
                    ]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_MIXED_TAGS,
                aspect=DomainsClass(domains=[DOMAIN_ROOT]),
            ),
            # Field-level: one field with only non-governance tag (should be excluded),
            # one field with governance tag (should be included).
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_MIXED_TAGS,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        # Field with only system tag -> should NOT appear in export
                        EditableSchemaFieldInfoClass(
                            fieldPath="system_only_field",
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag=TAG_SYSTEM)]
                            ),
                        ),
                        # Field with governance tag -> should appear
                        EditableSchemaFieldInfoClass(
                            fieldPath="pii_field",
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag=TAG_PII)]
                            ),
                        ),
                        # Field with mixed tags -> only PII should survive
                        EditableSchemaFieldInfoClass(
                            fieldPath="mixed_field",
                            globalTags=GlobalTagsClass(
                                tags=[
                                    TagAssociationClass(tag=TAG_PII),
                                    TagAssociationClass(tag=TAG_SYSTEM),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
            # ── Edge case: tag reference to tag that will be soft-deleted ──
            # TAG_ASSIGNED_THEN_DELETED exists now but will be soft-deleted
            # in seed_soft_deleted(). After deletion, the tag won't be in
            # the exported governance set, so enrichment should strip this.
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_WITH_DELETED_TAG,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=TAG_PII),
                        TagAssociationClass(tag=TAG_ASSIGNED_THEN_DELETED),
                    ]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_WITH_DELETED_TAG,
                aspect=DomainsClass(domains=[DOMAIN_ROOT]),
            ),
            # ── Edge case: snowflake dataset with enrichment ──
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_SNOWFLAKE,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=TAG_FINANCIAL)]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_SNOWFLAKE,
                aspect=DomainsClass(domains=[DOMAIN_CHILD]),
            ),
            # ── Edge case: empty aspects ──
            # Write empty tag and ownership lists. DataHub may or may not
            # persist these. The pipeline should handle either case.
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_EMPTY_ASPECTS,
                aspect=GlobalTagsClass(tags=[]),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=DATASET_EMPTY_ASPECTS,
                aspect=OwnershipClass(owners=[]),
            ),
        ],
    )


def seed_soft_deleted(graph: DataHubGraph) -> None:
    """Create entities then soft-delete them, for deletion propagation tests.

    Only entity types with a registered ``status`` aspect can be soft-deleted
    via ``soft_delete_entity()``. In current DataHub head, ``domain`` does NOT
    have this aspect (returns 422). Tags and glossary terms do support it.

    Also soft-deletes TAG_ASSIGNED_THEN_DELETED which was created in
    seed_tags() and assigned to DATASET_WITH_DELETED_TAG in seed_enrichment().
    This tests that enrichment references to deleted governance entities are
    stripped during export.
    """
    logger.info("Seeding soft-deleted entities...")
    _emit(
        graph,
        [
            MetadataChangeProposalWrapper(
                entityUrn=TAG_DELETED,
                aspect=TagPropertiesClass(
                    name="Integration Deleted Tag",
                    description="This tag will be soft-deleted",
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=TERM_DELETED,
                aspect=GlossaryTermInfoClass(
                    definition="Term that will be soft-deleted",
                    name="Integration Deleted Term",
                    termSource="INTERNAL",
                    parentNode=NODE_ROOT,
                ),
            ),
        ],
    )
    # Soft-delete the entities
    graph.soft_delete_entity(TAG_DELETED)
    graph.soft_delete_entity(TERM_DELETED)
    # Soft-delete the tag that was already assigned to a dataset
    graph.soft_delete_entity(TAG_ASSIGNED_THEN_DELETED)
    logger.info("Soft-deleted test entities created.")


def seed_all(graph: DataHubGraph) -> None:
    """Run all seed functions in dependency order."""
    seed_tags(graph)
    seed_glossary(graph)
    seed_domains(graph)
    seed_data_assets(graph)
    seed_data_products(graph)
    seed_enrichment(graph)
    seed_soft_deleted(graph)
    logger.info("Seeding complete.")
