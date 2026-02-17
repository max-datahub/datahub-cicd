"""CLI: Export governance entities and enrichment from dev DataHub.

Usage:
    python -m src.cli.export_cmd --output-dir metadata/
    python -m src.cli.export_cmd --output-dir metadata/ --skip-enrichment
"""

import argparse
import logging
import sys

from src.client import get_dev_graph
from src.handlers import create_default_registry
from src.handlers.enrichment import DatasetEnrichmentHandler
from src.orchestrator import SyncOrchestrator
from src.urn_mapper import PassthroughMapper
from src.utils import collect_governance_urns
from src.write_strategy import DryRunStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
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
        help="Only export governance definitions, skip dataset enrichment",
    )
    args = parser.parse_args()

    logger.info("Connecting to dev DataHub...")
    dev_graph = get_dev_graph()

    registry = create_default_registry()
    # DryRunStrategy is unused for export, but required by orchestrator
    orchestrator = SyncOrchestrator(
        registry=registry,
        urn_mapper=PassthroughMapper(),
        write_strategy=DryRunStrategy(),
    )

    logger.info(f"Exporting governance entities to {args.output_dir}/")
    exports = orchestrator.export_all(dev_graph, args.output_dir)

    # Export enrichment (tag/term/domain assignments on datasets)
    if not args.skip_enrichment:
        governance_urns = collect_governance_urns(exports)
        logger.info(
            f"Exporting enrichment (filtering by {len(governance_urns)} "
            f"governance URNs)..."
        )
        enrichment_handler = DatasetEnrichmentHandler(
            governance_urns=governance_urns
        )
        registry.register(enrichment_handler)
        enrichment_entities = enrichment_handler.export(dev_graph)
        exports["enrichment"] = enrichment_entities
        orchestrator.export_single(
            enrichment_handler, enrichment_entities, args.output_dir
        )

    total = sum(len(entities) for entities in exports.values())
    print(f"\nExport complete: {total} entities across {len(exports)} types")
    for entity_type, entities in exports.items():
        print(f"  {entity_type}: {len(entities)} entities")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)
