import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SystemMetadataClass

from src.interfaces import SyncResult, WriteStrategy

logger = logging.getLogger(__name__)

# Default batch size for bulk MCP emission.
# Each MCP is one HTTP request in non-batch mode; batch mode sends
# multiple MCPs in a single request via graph.emit_mcps().
DEFAULT_BATCH_SIZE = 100

# System metadata tag applied to all MCPs emitted by the CI/CD pipeline.
# Enables provenance filtering to distinguish pipeline writes from human writes.
CICD_SYSTEM_METADATA = SystemMetadataClass(
    properties={"appSource": "cicd-pipeline"}
)


class OverwriteStrategy(WriteStrategy):
    """Full UPSERT -- replaces existing aspect entirely.

    Uses per-MCP emission with per-MCP error tracking so that a single
    failure does not abort the entire batch.

    Performance: For high-throughput scenarios, override emit_batch()
    to use graph.emit_mcps() which sends multiple MCPs in fewer HTTP
    round-trips (but loses per-MCP error granularity on failure).
    """

    def emit(
        self, graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]
    ) -> list[SyncResult]:
        results = []
        for mcp in mcps:
            try:
                mcp.systemMetadata = CICD_SYSTEM_METADATA
                graph.emit_mcp(mcp)
                results.append(
                    SyncResult(
                        entity_type=mcp.entityType,
                        urn=mcp.entityUrn,
                        status="success",
                    )
                )
            except Exception as e:
                logger.error(
                    f"Failed to emit {mcp.entityType} {mcp.entityUrn} "
                    f"aspect={mcp.aspectName}: {e}"
                )
                results.append(
                    SyncResult(
                        entity_type=mcp.entityType,
                        urn=mcp.entityUrn,
                        status="failed",
                        error=str(e),
                    )
                )
        return results


class DryRunStrategy(WriteStrategy):
    """Logs MCPs without emitting. For validation and preview."""

    def emit(
        self, graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]
    ) -> list[SyncResult]:
        results = []
        for mcp in mcps:
            logger.info(
                f"[DRY RUN] Would emit: {mcp.entityType} "
                f"{mcp.entityUrn} {mcp.aspectName}"
            )
            results.append(
                SyncResult(
                    entity_type=mcp.entityType,
                    urn=mcp.entityUrn,
                    status="skipped",
                )
            )
        return results


# Future extension points:
# class MergeStrategy(WriteStrategy):  -- Cluster 5: read-merge-write
# class PatchStrategy(WriteStrategy):  -- Cluster 5: PATCH-based additive writes
