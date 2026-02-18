"""CLI: Sync governance + enrichment to prod DataHub.

Usage:
    python -m src.cli.sync_cmd --metadata-dir metadata/ [--dry-run]
    python -m src.cli.sync_cmd --metadata-dir metadata/ --live-enrichment [--dry-run]
    python -m src.cli.sync_cmd --metadata-dir metadata/ --apply-deletions [--dry-run]
"""

import argparse
import logging
import os
import sys

from src.client import get_dev_graph, get_prod_graph
from src.deletion import apply_deletions
from src.handlers import create_default_registry
from src.handlers.enrichment import (
    ENRICHABLE_ENTITY_TYPES,
    DatasetEnrichmentHandler,
    GenericEnrichmentHandler,
)
from src.logging_config import configure_logging
from src.orchestrator import SyncOrchestrator
from src.reporting import RunReport
from src.run_context import RunContext, TrackedGraph
from src.scope import ScopeConfig
from src.urn_mapper import PassthroughMapper
from src.utils import collect_governance_urns, read_json
from src.write_strategy import DryRunStrategy, OverwriteStrategy

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync governance and enrichment to prod DataHub"
    )
    parser.add_argument(
        "--metadata-dir",
        required=True,
        help="Directory containing exported JSON files (governance + enrichment)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview MCPs without writing to prod",
    )
    parser.add_argument(
        "--live-enrichment",
        action="store_true",
        help=(
            "Export enrichment live from dev instead of reading from files. "
            "Requires DATAHUB_DEV_URL and DATAHUB_DEV_TOKEN."
        ),
    )
    parser.add_argument(
        "--apply-deletions",
        action="store_true",
        help=(
            "Apply soft-deletions from deletions.json to prod. "
            "Must be paired with --include-deletions during export."
        ),
    )
    parser.add_argument(
        "--domain",
        action="append",
        dest="domains",
        help="Filter enrichment to entities in this domain (repeatable, --live-enrichment only)",
    )
    parser.add_argument(
        "--platform",
        action="append",
        dest="platforms",
        help="Filter enrichment to entities on this platform (repeatable, --live-enrichment only)",
    )
    parser.add_argument(
        "--env",
        help="Filter enrichment to entities in this environment (--live-enrichment only)",
    )
    parser.add_argument(
        "--scope-config",
        help="Path to YAML scope configuration file (--live-enrichment only)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Set up run context and structured logging
    ctx = RunContext(command="sync")
    configure_logging(
        output_dir=args.metadata_dir,
        log_level=args.log_level,
        run_id=ctx.run_id,
    )

    # Load governance entities from JSON files
    metadata_dir = args.metadata_dir
    exports: dict[str, list[dict]] = {}

    registry = create_default_registry()
    for handler in registry.get_all_handlers():
        filepath = os.path.join(metadata_dir, f"{handler.entity_type}.json")
        entities = read_json(filepath)
        exports[handler.entity_type] = entities

    governance_urns = collect_governance_urns(exports)
    logger.info(
        f"Loaded {sum(len(v) for v in exports.values())} governance entities "
        f"({len(governance_urns)} URNs)"
    )

    # Load or export enrichment
    dev_graph = None
    scope = ScopeConfig.from_cli_args(args) if args.live_enrichment else ScopeConfig()
    if args.live_enrichment:
        logger.info("Connecting to dev DataHub for live enrichment export...")
        dev_graph = get_dev_graph()
        if scope.is_scoped:
            logger.info(f"Enrichment scope: {scope}")

    # Dataset enrichment
    ds_handler = DatasetEnrichmentHandler(
        governance_urns=governance_urns, scope=scope
    )
    if args.live_enrichment:
        exports[ds_handler.entity_type] = ds_handler.export(dev_graph)
    else:
        path = os.path.join(metadata_dir, f"{ds_handler.entity_type}.json")
        exports[ds_handler.entity_type] = read_json(path)
    registry.register(ds_handler)

    # Other entity type enrichment
    for et in ENRICHABLE_ENTITY_TYPES:
        if et == "dataset":
            continue
        handler = GenericEnrichmentHandler(et, governance_urns, scope=scope)
        if args.live_enrichment:
            exports[handler.entity_type] = handler.export(dev_graph)
        else:
            path = os.path.join(metadata_dir, f"{handler.entity_type}.json")
            exports[handler.entity_type] = read_json(path)
        registry.register(handler)

    # Set up write strategy
    if args.dry_run:
        write_strategy = DryRunStrategy()
        logger.info("DRY RUN mode -- no changes will be written to prod")
    else:
        write_strategy = OverwriteStrategy()

    # Connect to prod and sync
    logger.info("Connecting to prod DataHub...")
    prod_graph_raw = get_prod_graph()
    prod_graph = TrackedGraph(prod_graph_raw)

    # Apply deletions before syncing (so deleted entities don't get re-created)
    if args.apply_deletions:
        deletions_path = os.path.join(metadata_dir, "deletions.json")
        deletions = read_json(deletions_path)
        if deletions:
            logger.info(
                f"Applying {len(deletions)} deletions to prod "
                f"({'DRY RUN' if args.dry_run else 'LIVE'})..."
            )
            deletion_results = apply_deletions(
                prod_graph, deletions, dry_run=args.dry_run
            )
        else:
            logger.info("No deletions to apply (deletions.json is empty)")
            deletion_results = []
    else:
        deletion_results = []

    orchestrator = SyncOrchestrator(
        registry=registry,
        urn_mapper=PassthroughMapper(),
        write_strategy=write_strategy,
        run_id=ctx.run_id,
        output_dir=metadata_dir,
    )
    orchestrator.sync_all(prod_graph, exports)

    # Append deletion results for unified summary
    orchestrator.results.extend(deletion_results)
    orchestrator.print_summary()

    # Generate run report
    report = RunReport.from_results(
        run_id=ctx.run_id,
        command="sync",
        results=orchestrator.results,
        duration_seconds=ctx.duration_seconds,
        timing=ctx.timing_summary(),
        api_stats=prod_graph.get_stats(),
    )
    report.write(metadata_dir)

    if orchestrator.has_failures():
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)
