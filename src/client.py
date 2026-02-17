import os
import logging

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)


def get_graph(server_url: str, token: str) -> DataHubGraph:
    """Create a DataHubGraph client."""
    config = DatahubClientConfig(server=server_url, token=token)
    graph = DataHubGraph(config)
    logger.info(f"Connected to DataHub at {server_url}")
    return graph


def get_dev_graph() -> DataHubGraph:
    """Create client from DATAHUB_DEV_URL / DATAHUB_DEV_TOKEN env vars."""
    url = os.environ["DATAHUB_DEV_URL"]
    token = os.environ["DATAHUB_DEV_TOKEN"]
    return get_graph(url, token)


def get_prod_graph() -> DataHubGraph:
    """Create client from DATAHUB_PROD_URL / DATAHUB_PROD_TOKEN env vars."""
    url = os.environ["DATAHUB_PROD_URL"]
    token = os.environ["DATAHUB_PROD_TOKEN"]
    return get_graph(url, token)
