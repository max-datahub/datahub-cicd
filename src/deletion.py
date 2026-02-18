"""Deletion propagation: detect soft-deleted entities in dev, apply to prod.

DataHub soft-deletes set `status.removed=true`. Tombstones remain queryable
for `retention_days` (default 10) before GC hard-deletes them. The SDK
provides `RemovedStatusFilter.ONLY_SOFT_DELETED` to query tombstones.

IMPORTANT: Not all entity types support the ``status`` aspect required for
soft-deletion. Only entity types with ``status`` registered in the DataHub
entity registry can be soft-deleted via ``soft_delete_entity()``. Notably,
``domain`` does NOT have a ``status`` aspect in DataHub (as of v1.x head)
and will return a 422 error. Domain deletion requires the GraphQL
``deleteDomain`` mutation instead.

Usage:
    deletions = detect_soft_deleted(graph)
    results = apply_deletions(prod_graph, deletions, dry_run=True)
"""

import logging
import traceback

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter

from src.error_classification import classify_error
from src.interfaces import SKIP_DRY_RUN, SyncResult
from src.retry import retry_transient

logger = logging.getLogger(__name__)

# Entity types that support the `status` aspect for soft-deletion.
# `domain` is intentionally excluded — it does not have a `status` aspect
# in the DataHub entity registry. Soft-deleting a domain returns 422.
DEFAULT_ENTITY_TYPES = [
    "tag",
    "glossaryNode",
    "glossaryTerm",
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
            logger.debug(
                f"Failed to scan soft-deleted {et}",
                exc_info=True,
            )
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
                    skip_reason=SKIP_DRY_RUN,
                )
            )
            continue

        try:

            @retry_transient(max_retries=3, base_delay=1.0)
            def _delete():
                graph.soft_delete_entity(urn)

            _delete()
            logger.info(f"Soft-deleted: {entity_type} {urn}")
            results.append(
                SyncResult(
                    entity_type=entity_type,
                    urn=urn,
                    status="success",
                )
            )
        except Exception as e:
            logger.debug(
                f"Failed to soft-delete {entity_type} {urn}",
                exc_info=True,
            )
            category, suggestion = classify_error(e)
            logger.error(f"Failed to soft-delete {entity_type} {urn}: {e}")
            results.append(
                SyncResult(
                    entity_type=entity_type,
                    urn=urn,
                    status="failed",
                    error=str(e),
                    traceback=traceback.format_exc(),
                    error_category=category,
                    error_suggestion=suggestion,
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
