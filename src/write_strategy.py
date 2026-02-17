import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph

from src.interfaces import SyncResult, WriteStrategy

logger = logging.getLogger(__name__)


class OverwriteStrategy(WriteStrategy):
    """Full UPSERT -- replaces existing aspect entirely."""

    def emit(
        self, graph: DataHubGraph, mcps: list[MetadataChangeProposalWrapper]
    ) -> list[SyncResult]:
        results = []
        for mcp in mcps:
            try:
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
