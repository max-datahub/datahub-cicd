import logging
import os
import time
import traceback

from datahub.ingestion.graph.client import DataHubGraph

from src.error_classification import classify_error
from src.interfaces import SyncResult, UrnMapper, WriteStrategy
from src.registry import HandlerRegistry
from src.reporting import write_run_state
from src.utils import write_json

logger = logging.getLogger(__name__)


def _progress_interval(total: int) -> int:
    """Adaptive progress reporting interval based on entity count (Amendment 9)."""
    if total > 1000:
        return 100
    if total < 100:
        return 25
    return 50


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
        run_id: str | None = None,
        output_dir: str | None = None,
    ) -> None:
        self.registry = registry
        self.urn_mapper = urn_mapper
        self.write_strategy = write_strategy
        self.results: list[SyncResult] = []
        self._run_id = run_id or ""
        self._output_dir = output_dir
        self._started_at = ""
        self._completed_phases: list[dict] = []

    def _write_incremental_state(
        self, command: str, status: str = "in_progress"
    ) -> None:
        """Write incremental run state for crash resilience (Amendment 5)."""
        if not self._output_dir:
            return
        errors = [
            {
                "urn": r.urn,
                "entity_type": r.entity_type,
                "category": r.error_category or "unknown",
                "message": r.error or "",
            }
            for r in self.results
            if r.status == "failed"
        ]
        write_run_state(
            output_dir=self._output_dir,
            run_id=self._run_id,
            command=command,
            started_at=self._started_at,
            status=status,
            completed_phases=self._completed_phases,
            results=self.results,
            errors=errors,
        )

    def export_all(
        self, graph: DataHubGraph, output_dir: str
    ) -> dict[str, list[dict]]:
        """Export all entity types in dependency order. Write JSON files."""
        self._output_dir = output_dir
        self._started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
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
            self._completed_phases.append(
                {
                    "entity_type": handler.entity_type,
                    "phase": "export",
                    "duration_seconds": round(elapsed, 3),
                    "entity_count": len(entities),
                }
            )
            self._write_incremental_state("export")
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
        if not self._started_at:
            self._started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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

            total = len(entities)
            interval = _progress_interval(total)
            logger.info(
                f"Syncing {total} {handler.entity_type} entities..."
            )
            t0 = time.monotonic()
            last_progress_time = t0
            for i, entity in enumerate(entities):
                urn = entity.get("urn") or entity.get("dataset_urn", "unknown")
                try:
                    mcps = handler.build_mcps(entity, self.urn_mapper)
                    phase_results = self.write_strategy.emit(graph, mcps)
                    self.results.extend(phase_results)
                except Exception as e:
                    logger.debug(
                        f"Failed to build MCPs for {handler.entity_type} "
                        f"{urn}",
                        exc_info=True,
                    )
                    category, suggestion = classify_error(e)
                    logger.error(
                        f"Failed to build MCPs for {handler.entity_type} "
                        f"{urn}: {e}"
                    )
                    self.results.append(
                        SyncResult(
                            entity_type=handler.entity_type,
                            urn=urn,
                            status="failed",
                            error=str(e),
                            traceback=traceback.format_exc(),
                            error_category=category,
                            error_suggestion=suggestion,
                        )
                    )

                now = time.monotonic()
                if (i + 1) % interval == 0 or (
                    total > 1000 and now - last_progress_time >= 30
                ):
                    elapsed_so_far = now - t0
                    pct = (i + 1) / total * 100
                    eta = (
                        elapsed_so_far / (i + 1) * (total - i - 1)
                        if i > 0
                        else 0
                    )
                    logger.info(
                        f"  Progress: {i + 1}/{total} "
                        f"{handler.entity_type} entities "
                        f"({pct:.1f}%, ~{eta:.0f}s remaining)"
                    )
                    last_progress_time = now

            elapsed = time.monotonic() - t0
            logger.info(
                f"Synced {total} {handler.entity_type} "
                f"entities in {elapsed:.1f}s"
            )
            self._completed_phases.append(
                {
                    "entity_type": handler.entity_type,
                    "phase": "sync",
                    "duration_seconds": round(elapsed, 3),
                    "entity_count": total,
                }
            )
            self._write_incremental_state("sync")

        return self.results

    def print_summary(self) -> None:
        """Log human-readable summary of sync results."""
        succeeded = sum(1 for r in self.results if r.status == "success")
        failed = sum(1 for r in self.results if r.status == "failed")
        skipped = sum(1 for r in self.results if r.status == "skipped")
        logger.info(
            f"Sync complete: {succeeded} succeeded, "
            f"{failed} failed, {skipped} skipped"
        )
        if failed:
            logger.info("Failed entities:")
            for r in self.results:
                if r.status == "failed":
                    logger.info(f"  [{r.entity_type}] {r.urn}: {r.error}")

    def has_failures(self) -> bool:
        return any(r.status == "failed" for r in self.results)
