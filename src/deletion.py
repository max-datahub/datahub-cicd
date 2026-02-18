"""Deletion propagation: detect soft-deleted entities in dev, apply to prod.

DataHub soft-deletes set `status.removed=true`. Tombstones remain queryable
for `retention_days` (default 10) before GC hard-deletes them. The SDK
provides `RemovedStatusFilter.ONLY_SOFT_DELETED` to query tombstones.

Usage:
    deletions = detect_soft_deleted(graph)
    results = apply_deletions(prod_graph, deletions, dry_run=True)
"""

import logging

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter

from src.interfaces import SyncResult

logger = logging.getLogger(__name__)

DEFAULT_ENTITY_TYPES = [
    "tag",
    "glossaryNode",
    "glossaryTerm",
    "domain",
    "dataProduct",
]


def detect_soft_deleted(
    graph: DataHubGraph,
    entity_types: list[str] | None = None,
) -> list[dict]:
    """Detect soft-deleted governance entities in a DataHub instance.

    Args:
        graph: DataHubGraph client connected to the source instance.
        entity_types: Entity types to scan. Defaults to all governance types.

    Returns:
        List of {"urn": ..., "entity_type": ...} dicts for soft-deleted entities.
    """
    if entity_types is None:
        entity_types = DEFAULT_ENTITY_TYPES

    deletions: list[dict] = []
    for et in entity_types:
        logger.info(f"Scanning for soft-deleted {et} entities...")
        try:
            urns = list(
                graph.get_urns_by_filter(
                    entity_types=[et],
                    status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                )
            )
            for urn in urns:
                deletions.append({"urn": urn, "entity_type": et})
            if urns:
                logger.info(f"Found {len(urns)} soft-deleted {et} entities")
        except Exception as e:
            logger.warning(f"Failed to scan soft-deleted {et}: {e}")

    logger.info(f"Total soft-deleted entities detected: {len(deletions)}")
    return deletions


def apply_deletions(
    graph: DataHubGraph,
    deletions: list[dict],
    dry_run: bool = False,
) -> list[SyncResult]:
    """Apply soft-deletions to a target DataHub instance.

    Args:
        graph: DataHubGraph client connected to the target instance.
        deletions: List of {"urn": ..., "entity_type": ...} dicts.
        dry_run: If True, log deletions without applying them.

    Returns:
        List of SyncResult tracking per-entity success/failure/skipped.
    """
    results: list[SyncResult] = []

    if not deletions:
        logger.info("No deletions to apply")
        return results

    for entry in deletions:
        urn = entry["urn"]
        entity_type = entry["entity_type"]

        if dry_run:
            logger.info(f"[DRY RUN] Would soft-delete: {entity_type} {urn}")
            results.append(
                SyncResult(
                    entity_type=entity_type,
                    urn=urn,
                    status="skipped",
                )
            )
            continue

        try:
            graph.soft_delete_entity(urn)
            logger.info(f"Soft-deleted: {entity_type} {urn}")
            results.append(
                SyncResult(
                    entity_type=entity_type,
                    urn=urn,
                    status="success",
                )
            )
        except Exception as e:
            logger.error(f"Failed to soft-delete {entity_type} {urn}: {e}")
            results.append(
                SyncResult(
                    entity_type=entity_type,
                    urn=urn,
                    status="failed",
                    error=str(e),
                )
            )

    succeeded = sum(1 for r in results if r.status == "success")
    failed = sum(1 for r in results if r.status == "failed")
    skipped = sum(1 for r in results if r.status == "skipped")
    logger.info(
        f"Deletion results: {succeeded} succeeded, "
        f"{failed} failed, {skipped} skipped"
    )
    return results
