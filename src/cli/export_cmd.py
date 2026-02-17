"""CLI: Export governance entities from dev DataHub.

Usage:
    python -m src.cli.export_cmd --output-dir metadata/
"""

import argparse
import logging
import sys

from src.client import get_dev_graph
from src.handlers import create_default_registry
from src.orchestrator import SyncOrchestrator
from src.urn_mapper import PassthroughMapper
from src.write_strategy import DryRunStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export governance entities from dev DataHub"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write exported JSON files",
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
