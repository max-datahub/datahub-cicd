"""CLI: Export governance entities and enrichment from dev DataHub.

Usage:
    python -m src.cli.export_cmd --output-dir metadata/
    python -m src.cli.export_cmd --output-dir metadata/ --skip-enrichment
    python -m src.cli.export_cmd --output-dir metadata/ --include-deletions
    python -m src.cli.export_cmd --output-dir metadata/ --filter-by-source ui
"""

import argparse
import logging
import os
import sys
import time

from src.client import get_dev_graph
from src.deletion import detect_soft_deleted
from src.handlers import create_default_registry
from src.handlers.enrichment import (
    ENRICHABLE_ENTITY_TYPES,
    DatasetEnrichmentHandler,
    GenericEnrichmentHandler,
)
from src.logging_config import configure_logging
from src.orchestrator import SyncOrchestrator
from src.provenance import ProvenanceSource, filter_entities_by_provenance
from src.reporting import RunReport
from src.run_context import RunContext, TrackedGraph
from src.scope import ScopeConfig
from src.urn_mapper import PassthroughMapper
from src.utils import collect_governance_urns, write_json
from src.write_strategy import DryRunStrategy

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export governance entities and enrichment from dev DataHub"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write exported JSON files",
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Only export governance definitions, skip enrichment",
    )
    parser.add_argument(
        "--include-deletions",
        action="store_true",
        help="Detect soft-deleted governance entities and write deletions.json",
    )
    parser.add_argument(
        "--filter-by-source",
        choices=["ui", "all"],
        default="all",
        help=(
            "Filter governance entities by provenance source. "
            "'ui' keeps UI-authored and CI/CD entities (excludes ingestion). "
            "Default: 'all' (no filtering)."
        ),
    )
    parser.add_argument(
        "--domain",
        action="append",
        dest="domains",
        help="Filter enrichment to entities in this domain (repeatable)",
    )
    parser.add_argument(
        "--platform",
        action="append",
        dest="platforms",
        help="Filter enrichment to entities on this platform (repeatable)",
    )
    parser.add_argument(
        "--env",
        help="Filter enrichment to entities in this environment (e.g., PROD)",
    )
    parser.add_argument(
        "--scope-config",
        help="Path to YAML scope configuration file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Set up run context and structured logging
    ctx = RunContext(command="export")
    configure_logging(
        output_dir=args.output_dir,
        log_level=args.log_level,
        run_id=ctx.run_id,
    )

    logger.info("Connecting to dev DataHub...")
    dev_graph_raw = get_dev_graph()
    dev_graph = TrackedGraph(dev_graph_raw)

    registry = create_default_registry()
    orchestrator = SyncOrchestrator(
        registry=registry,
        urn_mapper=PassthroughMapper(),
        write_strategy=DryRunStrategy(),
        run_id=ctx.run_id,
        output_dir=args.output_dir,
    )

    logger.info(f"Exporting governance entities to {args.output_dir}/")
    exports = orchestrator.export_all(dev_graph, args.output_dir)

    # Detect soft-deleted governance entities
    if args.include_deletions:
        logger.info("Detecting soft-deleted governance entities...")
        deletions = detect_soft_deleted(dev_graph)
        deletions_path = os.path.join(args.output_dir, "deletions.json")
        write_json(deletions, deletions_path)
        logger.info(f"Wrote {len(deletions)} deletions to {deletions_path}")

    # Filter governance entities by provenance source
    if args.filter_by_source != "all":
        allowed = {ProvenanceSource.UI, ProvenanceSource.CICD, ProvenanceSource.UNKNOWN}
        logger.info(
            f"Filtering governance entities by provenance "
            f"(allowed: {[s.value for s in allowed]})..."
        )
        governance_types = {"tag", "glossaryNode", "glossaryTerm", "domain", "dataProduct"}
        for entity_type in list(exports.keys()):
            if entity_type in governance_types:
                original_count = len(exports[entity_type])
                kept, _filtered_out = filter_entities_by_provenance(
                    dev_graph, exports[entity_type], entity_type, allowed
                )
                exports[entity_type] = kept
                # Re-write filtered JSON
                output_path = os.path.join(
                    args.output_dir, f"{entity_type}.json"
                )
                write_json(exports[entity_type], output_path)
                logger.info(
                    f"Provenance filter {entity_type}: "
                    f"{original_count} -> {len(exports[entity_type])}"
                )

    if not args.skip_enrichment:
        governance_urns = collect_governance_urns(exports)
        logger.info(
            f"Exporting enrichment (filtering by {len(governance_urns)} "
            f"governance URNs)..."
        )

        scope = ScopeConfig.from_cli_args(args)
        if scope.is_scoped:
            logger.info(f"Enrichment scope: {scope}")

        # Dataset enrichment (includes editableSchemaMetadata)
        ds_handler = DatasetEnrichmentHandler(
            governance_urns=governance_urns, scope=scope
        )
        registry.register(ds_handler)
        ds_entities = ds_handler.export(dev_graph)
        exports[ds_handler.entity_type] = ds_entities
        orchestrator.export_single(ds_handler, ds_entities, args.output_dir)

        # Enrichment for other entity types (tags, terms, domains, ownership)
        for et in ENRICHABLE_ENTITY_TYPES:
            if et == "dataset":
                continue
            handler = GenericEnrichmentHandler(et, governance_urns, scope=scope)
            registry.register(handler)
            entities = handler.export(dev_graph)
            exports[handler.entity_type] = entities
            orchestrator.export_single(handler, entities, args.output_dir)

    total = sum(len(entities) for entities in exports.values())
    logger.info(
        f"Export complete: {total} entities across {len(exports)} types"
    )
    for entity_type, entities in exports.items():
        logger.info(f"  {entity_type}: {len(entities)} entities")

    # Generate run report
    report = RunReport.from_results(
        run_id=ctx.run_id,
        command="export",
        results=orchestrator.results,
        duration_seconds=ctx.duration_seconds,
        timing=ctx.timing_summary(),
        api_stats=dev_graph.get_stats(),
    )
    report.write(args.output_dir)


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)
