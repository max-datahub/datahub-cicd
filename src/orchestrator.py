import logging
import os
import time

from datahub.ingestion.graph.client import DataHubGraph

from src.interfaces import SyncResult, UrnMapper, WriteStrategy
from src.registry import HandlerRegistry
from src.utils import write_json

logger = logging.getLogger(__name__)

# Progress is logged every N entities during sync.
_PROGRESS_INTERVAL = 50


class SyncOrchestrator:
    """Runs export and sync in dependency-resolved phase order.
    Tracks per-entity results. Reports summary.

    Performance architecture:
    - Handlers run sequentially in dependency order (required for correctness)
    - Within a handler, entities are processed sequentially but MCPs are
      emitted via WriteStrategy which can batch internally
    - Future: handlers with no inter-dependencies could run concurrently
      (e.g., tag and domain handlers have no dependency on each other)
    """

    def __init__(
        self,
        registry: HandlerRegistry,
        urn_mapper: UrnMapper,
        write_strategy: WriteStrategy,
    ) -> None:
        self.registry = registry
        self.urn_mapper = urn_mapper
        self.write_strategy = write_strategy
        self.results: list[SyncResult] = []

    def export_all(
        self, graph: DataHubGraph, output_dir: str
    ) -> dict[str, list[dict]]:
        """Export all entity types in dependency order. Write JSON files."""
        exports: dict[str, list[dict]] = {}
        for handler in self.registry.get_sync_order():
            logger.info(f"Exporting {handler.entity_type}...")
            t0 = time.monotonic()
            entities = handler.export(graph)
            elapsed = time.monotonic() - t0
            exports[handler.entity_type] = entities
            output_path = os.path.join(output_dir, f"{handler.entity_type}.json")
            write_json(entities, output_path)
            logger.info(
                f"Exported {len(entities)} {handler.entity_type} "
                f"entities in {elapsed:.1f}s"
            )
        return exports

    def export_single(
        self,
        handler: "EntityHandler",
        entities: list[dict],
        output_dir: str,
    ) -> None:
        """Write a single handler's entities to a JSON file."""
        output_path = os.path.join(output_dir, f"{handler.entity_type}.json")
        write_json(entities, output_path)

    def sync_all(
        self, graph: DataHubGraph, exports: dict[str, list[dict]]
    ) -> list[SyncResult]:
        """Sync all entity types to target in dependency order."""
        for handler in self.registry.get_sync_order():
            entities = exports.get(handler.entity_type, [])
            if not entities:
                logger.info(f"No entities for {handler.entity_type}, skipping")
                continue

            # Optional validation
            errors = handler.validate(entities)
            if errors:
                for err in errors:
                    self.results.append(
                        SyncResult(handler.entity_type, "", "failed", err)
                    )
                logger.error(
                    f"Validation failed for {handler.entity_type}: {errors}"
                )
                continue

            logger.info(
                f"Syncing {len(entities)} {handler.entity_type} entities..."
            )
            t0 = time.monotonic()
            for i, entity in enumerate(entities):
                urn = entity.get("urn") or entity.get("dataset_urn", "unknown")
                try:
                    mcps = handler.build_mcps(entity, self.urn_mapper)
                    phase_results = self.write_strategy.emit(graph, mcps)
                    self.results.extend(phase_results)
                except Exception as e:
                    logger.error(
                        f"Failed to build MCPs for {handler.entity_type} "
                        f"{urn}: {e}"
                    )
                    self.results.append(
                        SyncResult(
                            handler.entity_type, urn, "failed", str(e)
                        )
                    )

                if (i + 1) % _PROGRESS_INTERVAL == 0:
                    logger.info(
                        f"  Progress: {i + 1}/{len(entities)} "
                        f"{handler.entity_type} entities"
                    )

            elapsed = time.monotonic() - t0
            logger.info(
                f"Synced {len(entities)} {handler.entity_type} "
                f"entities in {elapsed:.1f}s"
            )

        return self.results

    def print_summary(self) -> None:
        """Print human-readable summary of sync results."""
        succeeded = sum(1 for r in self.results if r.status == "success")
        failed = sum(1 for r in self.results if r.status == "failed")
        skipped = sum(1 for r in self.results if r.status == "skipped")
        print(
            f"\nSync complete: {succeeded} succeeded, "
            f"{failed} failed, {skipped} skipped"
        )
        if failed:
            print("\nFailed entities:")
            for r in self.results:
                if r.status == "failed":
                    print(f"  [{r.entity_type}] {r.urn}: {r.error}")

    def has_failures(self) -> bool:
        return any(r.status == "failed" for r in self.results)
