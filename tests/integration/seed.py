"""Seed a DataHub instance with all entity types and aspects for integration testing.

Creates a deterministic set of governance entities, data assets, and enrichment
assignments that cover every supported entity type and aspect.
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

# Glossary nodes (hierarchy: root -> child)
NODE_ROOT = "urn:li:glossaryNode:integration-root-node"
NODE_CHILD = "urn:li:glossaryNode:integration-child-node"

# Glossary terms (under nodes)
TERM_A = "urn:li:glossaryTerm:integration-term-a"
TERM_B = "urn:li:glossaryTerm:integration-term-b"

# Domains (hierarchy: root -> child)
DOMAIN_ROOT = "urn:li:domain:integration-root-domain"
DOMAIN_CHILD = "urn:li:domain:integration-child-domain"

# Data products
DATA_PRODUCT = "urn:li:dataProduct:integration-product"

# Datasets
DATASET_1 = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.users,PROD)"
DATASET_2 = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.orders,PROD)"
DATASET_NO_ENRICHMENT = "urn:li:dataset:(urn:li:dataPlatform:postgres,integration_db.public.logs,PROD)"

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
        ],
    )


def seed_all(graph: DataHubGraph) -> None:
    """Run all seed functions in dependency order."""
    seed_tags(graph)
    seed_glossary(graph)
    seed_domains(graph)
    seed_data_assets(graph)
    seed_data_products(graph)
    seed_enrichment(graph)
    logger.info("Seeding complete.")
