"""Integration test fixtures.

Two modes of operation:

1. **Against existing instance** (fast, no Docker required):
   DATAHUB_TEST_GMS_URL=http://localhost:8080 pytest -m integration tests/integration/ -v

2. **With Docker Quickstart** (full lifecycle, CI/CD):
   pytest -m integration tests/integration/ -v
   This downloads the official quickstart, starts services on non-standard
   ports (18080 scheme), seeds, tests, and tears down.

Run with:  pytest -m integration tests/integration/ -v -s
Skip with: pytest -m "not integration" tests/
"""

import logging
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import requests

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

from tests.integration.seed import seed_all

logger = logging.getLogger(__name__)

# ── Port scheme: standard + 10000 offset ──────────────────────────────────

PORTS = {
    "DATAHUB_MAPPED_GMS_PORT": "18080",
    "DATAHUB_MAPPED_FRONTEND_PORT": "19002",
    "DATAHUB_MAPPED_MYSQL_PORT": "13306",
    "DATAHUB_MAPPED_ELASTIC_PORT": "19200",
    "DATAHUB_MAPPED_KAFKA_BROKER_PORT": "19092",
    "DATAHUB_MAPPED_SCHEMA_REGISTRY_PORT": "18081",
    "DATAHUB_MAPPED_ZK_PORT": "12181",
}

# If set, skip Docker lifecycle and use this URL directly
EXTERNAL_GMS_URL = os.environ.get("DATAHUB_TEST_GMS_URL")

GMS_PORT = PORTS["DATAHUB_MAPPED_GMS_PORT"]
DOCKER_GMS_URL = f"http://localhost:{GMS_PORT}"
GMS_URL = EXTERNAL_GMS_URL or DOCKER_GMS_URL

# Docker Compose project name (separate from any user quickstart)
PROJECT_NAME = "datahub-cicd-integration"

# Official quickstart compose file (no Neo4j variant)
COMPOSE_URL = (
    "https://raw.githubusercontent.com/datahub-project/datahub/"
    "master/docker/quickstart/docker-compose-without-neo4j.quickstart.yml"
)

# Timeouts
PULL_TIMEOUT_SECONDS = int(os.environ.get("INTEGRATION_PULL_TIMEOUT", "600"))
UP_TIMEOUT_SECONDS = int(os.environ.get("INTEGRATION_UP_TIMEOUT", "120"))
GMS_TIMEOUT_SECONDS = int(os.environ.get("INTEGRATION_GMS_TIMEOUT", "180"))
GMS_POLL_INTERVAL = 5


def _compose_env() -> dict:
    return {**os.environ, **PORTS}


def _compose_cmd(compose_file: str) -> list[str]:
    return ["docker", "compose", "-f", compose_file, "-p", PROJECT_NAME]


def _download_compose_file(target_dir: str) -> str:
    compose_path = os.path.join(target_dir, "docker-compose.yml")
    logger.info(f"Downloading quickstart compose file to {compose_path}")
    resp = requests.get(COMPOSE_URL, timeout=30)
    resp.raise_for_status()
    with open(compose_path, "w") as f:
        f.write(resp.text)
    return compose_path


def _run_compose(
    compose_file: str, *args: str, timeout: int = 120
) -> subprocess.CompletedProcess:
    cmd = _compose_cmd(compose_file) + list(args)
    logger.info(f"Running: {' '.join(cmd)} (timeout={timeout}s)")
    result = subprocess.run(
        cmd,
        env=_compose_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        logger.error(f"stderr: {result.stderr[-1000:]}")
        raise RuntimeError(
            f"docker compose {args[0]} failed (rc={result.returncode})"
        )
    return result


def _run_compose_streaming(
    compose_file: str, *args: str, timeout: int = 600
) -> None:
    """Run a docker compose command with streaming output (for pull/up)."""
    cmd = _compose_cmd(compose_file) + list(args)
    logger.info(f"Running (streaming): {' '.join(cmd)} (timeout={timeout}s)")
    proc = subprocess.Popen(
        cmd,
        env=_compose_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + timeout
    while proc.poll() is None:
        if time.monotonic() > deadline:
            proc.kill()
            raise TimeoutError(
                f"docker compose {args[0]} timed out after {timeout}s"
            )
        line = proc.stdout.readline()
        if line:
            logger.info(f"  [compose] {line.rstrip()}")
    for line in proc.stdout:
        logger.info(f"  [compose] {line.rstrip()}")
    if proc.returncode != 0:
        raise RuntimeError(
            f"docker compose {args[0]} failed (rc={proc.returncode})"
        )


def _wait_for_gms(gms_url: str) -> None:
    health_url = f"{gms_url}/health"
    deadline = time.monotonic() + GMS_TIMEOUT_SECONDS
    logger.info(
        f"Waiting for GMS at {health_url} (timeout {GMS_TIMEOUT_SECONDS}s)..."
    )
    while time.monotonic() < deadline:
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code == 200:
                logger.info("GMS is healthy.")
                return
        except requests.ConnectionError:
            pass
        time.sleep(GMS_POLL_INTERVAL)
    raise TimeoutError(
        f"GMS did not become healthy at {health_url} "
        f"within {GMS_TIMEOUT_SECONDS}s"
    )


def _get_graph(gms_url: str) -> DataHubGraph:
    config = DatahubClientConfig(server=gms_url, token=None)
    return DataHubGraph(config)


# ── Session-scoped fixtures ───────────────────────────────────────────────


@pytest.fixture(scope="session")
def compose_dir():
    if EXTERNAL_GMS_URL:
        yield None
        return
    tmpdir = tempfile.mkdtemp(prefix="datahub-cicd-integration-")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def datahub_up(compose_dir):
    """Start DataHub or verify external instance is reachable."""
    if EXTERNAL_GMS_URL:
        logger.info(
            f"Using external DataHub instance at {EXTERNAL_GMS_URL}"
        )
        _wait_for_gms(EXTERNAL_GMS_URL)
        yield None
        return

    compose_file = _download_compose_file(compose_dir)

    # Pull images (streaming, generous timeout for first-time pulls)
    _run_compose_streaming(
        compose_file, "pull", timeout=PULL_TIMEOUT_SECONDS
    )

    # Start services
    _run_compose(compose_file, "up", "-d", timeout=UP_TIMEOUT_SECONDS)

    # Wait for GMS API
    _wait_for_gms(DOCKER_GMS_URL)

    yield compose_file

    # Teardown
    logger.info("Tearing down DataHub integration test environment...")
    try:
        _run_compose(compose_file, "down", "-v", timeout=120)
    except Exception as e:
        logger.warning(f"Teardown failed (non-fatal): {e}")


@pytest.fixture(scope="session")
def integration_graph(datahub_up) -> DataHubGraph:
    return _get_graph(GMS_URL)


@pytest.fixture(scope="session")
def seeded_graph(integration_graph) -> DataHubGraph:
    seed_all(integration_graph)
    time.sleep(5)
    return integration_graph


@pytest.fixture(scope="session")
def export_dir(seeded_graph):
    tmpdir = tempfile.mkdtemp(prefix="datahub-cicd-export-")
    env = {
        **os.environ,
        "DATAHUB_DEV_URL": GMS_URL,
        "DATAHUB_DEV_TOKEN": "",
    }
    result = subprocess.run(
        ["python", "-m", "src.cli.export_cmd", "--output-dir", tmpdir],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error(
            f"Export failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(f"Export CLI failed: {result.stderr}")
    logger.info(f"Export output:\n{result.stdout}")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)
