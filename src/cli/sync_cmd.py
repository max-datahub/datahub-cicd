"""CLI: Sync governance + enrichment to prod DataHub.

Usage:
    python -m src.cli.sync_cmd --governance-dir metadata/ [--dry-run]
"""

import argparse
import logging
import os
import sys

from src.client import get_dev_graph, get_prod_graph
from src.handlers import create_default_registry
from src.handlers.enrichment import DatasetEnrichmentHandler
from src.orchestrator import SyncOrchestrator
from src.urn_mapper import PassthroughMapper
from src.utils import read_json
from src.write_strategy import DryRunStrategy, OverwriteStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync governance and enrichment to prod DataHub"
    )
    parser.add_argument(
        "--governance-dir",
        required=True,
        help="Directory containing exported governance JSON files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview MCPs without writing to prod",
    )
    args = parser.parse_args()

    # Load governance entities from JSON files
    governance_dir = args.governance_dir
    exports: dict[str, list[dict]] = {}
    governance_urns: set[str] = set()

    registry = create_default_registry()
    for handler in registry.get_all_handlers():
        filepath = os.path.join(governance_dir, f"{handler.entity_type}.json")
        entities = read_json(filepath)
        exports[handler.entity_type] = entities
        for entity in entities:
            urn = entity.get("urn")
            if urn:
                governance_urns.add(urn)

    logger.info(
        f"Loaded {sum(len(v) for v in exports.values())} governance entities "
        f"({len(governance_urns)} URNs)"
    )

    # Export enrichment live from dev
    logger.info("Connecting to dev DataHub for enrichment export...")
    dev_graph = get_dev_graph()
    enrichment_handler = DatasetEnrichmentHandler(
        governance_urns=governance_urns
    )
    enrichment_entities = enrichment_handler.export(dev_graph)
    exports["enrichment"] = enrichment_entities

    # Register enrichment handler
    registry.register(enrichment_handler)

    # Set up write strategy
    if args.dry_run:
        write_strategy = DryRunStrategy()
        logger.info("DRY RUN mode -- no changes will be written to prod")
    else:
        write_strategy = OverwriteStrategy()

    # Connect to prod and sync
    logger.info("Connecting to prod DataHub...")
    prod_graph = get_prod_graph()

    orchestrator = SyncOrchestrator(
        registry=registry,
        urn_mapper=PassthroughMapper(),
        write_strategy=write_strategy,
    )
    orchestrator.sync_all(prod_graph, exports)
    orchestrator.print_summary()

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
