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


def _wait_for_elasticsearch_sync(graph: DataHubGraph, timeout: int = 60) -> None:
    """Wait for Elasticsearch to index all seeded entities.

    After writing MCPs, ES needs time to index them before
    get_urns_by_filter() can find them. We poll until all expected
    entity types have at least one result AND soft-deleted entities
    are no longer returned by the active filter.
    """
    from datahub.ingestion.graph.filters import RemovedStatusFilter

    from tests.integration.seed import TAG_ASSIGNED_THEN_DELETED, TAG_PII

    expected_types = ["tag", "glossaryTerm", "domain", "dataset", "chart",
                      "dashboard", "container", "dataFlow", "dataProduct"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        all_found = True
        for et in expected_types:
            urns = list(graph.get_urns_by_filter(entity_types=[et]))
            if not urns:
                all_found = False
                break
        # Also verify soft-deletes are indexed: TAG_ASSIGNED_THEN_DELETED
        # must NOT appear in the active (NOT_SOFT_DELETED) tag list.
        active_tags = list(graph.get_urns_by_filter(entity_types=["tag"]))
        soft_deletes_indexed = TAG_ASSIGNED_THEN_DELETED not in active_tags
        if all_found and TAG_PII in active_tags and soft_deletes_indexed:
            logger.info("Elasticsearch sync complete — all entity types discoverable, soft-deletes indexed.")
            return
        time.sleep(2)
    logger.warning(f"ES sync timed out after {timeout}s — some entities may not be indexed yet.")


@pytest.fixture(scope="session")
def seeded_graph(integration_graph) -> DataHubGraph:
    seed_all(integration_graph)
    _wait_for_elasticsearch_sync(integration_graph)
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


@pytest.fixture(scope="session")
def export_dir_with_deletions(seeded_graph):
    """Export with --include-deletions flag for deletion propagation tests."""
    tmpdir = tempfile.mkdtemp(prefix="datahub-cicd-export-deletions-")
    env = {
        **os.environ,
        "DATAHUB_DEV_URL": GMS_URL,
        "DATAHUB_DEV_TOKEN": "",
    }
    result = subprocess.run(
        [
            "python", "-m", "src.cli.export_cmd",
            "--output-dir", tmpdir,
            "--include-deletions",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error(
            f"Export with deletions failed:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(f"Export CLI (deletions) failed: {result.stderr}")
    logger.info(f"Export with deletions output:\n{result.stdout}")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def _run_scoped_export(
    seeded_graph: DataHubGraph,
    extra_args: list[str],
    prefix: str = "datahub-cicd-scoped-",
) -> str:
    """Helper: run the export CLI with extra arguments and return the output dir."""
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    env = {
        **os.environ,
        "DATAHUB_DEV_URL": GMS_URL,
        "DATAHUB_DEV_TOKEN": "",
    }
    cmd = ["python", "-m", "src.cli.export_cmd", "--output-dir", tmpdir] + extra_args
    result = subprocess.run(
        cmd, env=env, capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        logger.error(
            f"Scoped export failed ({extra_args}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(f"Scoped export CLI failed: {result.stderr}")
    logger.info(f"Scoped export output ({extra_args}):\n{result.stdout}")
    return tmpdir


@pytest.fixture(scope="session")
def export_dir_domain_scoped(seeded_graph):
    """Export with --domain filter (root domain only)."""
    from tests.integration.seed import DOMAIN_ROOT

    tmpdir = _run_scoped_export(
        seeded_graph, ["--domain", DOMAIN_ROOT], prefix="datahub-cicd-domain-"
    )
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def export_dir_platform_scoped(seeded_graph):
    """Export with --platform filter (postgres only)."""
    tmpdir = _run_scoped_export(
        seeded_graph, ["--platform", "postgres"], prefix="datahub-cicd-platform-"
    )
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def export_dir_combined_scoped(seeded_graph):
    """Export with --domain + --platform + --env combined scope."""
    from tests.integration.seed import DOMAIN_ROOT

    tmpdir = _run_scoped_export(
        seeded_graph,
        ["--domain", DOMAIN_ROOT, "--platform", "postgres", "--env", "PROD"],
        prefix="datahub-cicd-combined-",
    )
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def export_dir_yaml_scoped(seeded_graph):
    """Export with --scope-config YAML file."""
    from tests.integration.seed import DOMAIN_ROOT

    # Write a temporary scope config
    scope_dir = tempfile.mkdtemp(prefix="datahub-cicd-yaml-scope-cfg-")
    scope_file = os.path.join(scope_dir, "scope.yaml")
    with open(scope_file, "w") as f:
        f.write(f"scope:\n  domains:\n    - {DOMAIN_ROOT}\n  platforms:\n    - postgres\n")

    tmpdir = _run_scoped_export(
        seeded_graph,
        ["--scope-config", scope_file],
        prefix="datahub-cicd-yaml-",
    )
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)
    shutil.rmtree(scope_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def export_dir_empty_scope(seeded_graph):
    """Export with --domain pointing to a nonexistent domain.

    Should succeed (exit 0) with empty enrichment files.
    """
    tmpdir = _run_scoped_export(
        seeded_graph,
        ["--domain", "urn:li:domain:nonexistent-domain-12345"],
        prefix="datahub-cicd-empty-scope-",
    )
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def sync_round_trip_dir(seeded_graph, export_dir):
    """Sync exported JSON back to the same DataHub instance.

    To make this more than an idempotent no-op, we mutate a tag's
    description before syncing. The sync should overwrite the mutation
    back to the exported value, proving that UPSERT actually writes.

    Validates the full export -> sync pipeline end-to-end.

    Copies exported JSON to a separate directory so sync's observability
    outputs (run-report.json, .run-state.json, etc.) don't overwrite
    the export's observability files.
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import TagPropertiesClass

    from tests.integration.seed import TAG_PII

    # Mutate TAG_PII's description to something different.
    # After sync, it should be restored to the exported value.
    seeded_graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TAG_PII,
            aspect=TagPropertiesClass(
                name="Integration PII",
                description="MUTATED -- should be overwritten by sync",
                colorHex="#FF0000",
            ),
        )
    )
    # Verify the mutation took effect
    mutated = seeded_graph.get_aspect(TAG_PII, TagPropertiesClass)
    assert mutated.description == "MUTATED -- should be overwritten by sync"

    # Copy exported JSON files to a separate directory for sync
    sync_dir = tempfile.mkdtemp(prefix="datahub-cicd-sync-")
    for f in os.listdir(export_dir):
        if f.endswith(".json"):
            shutil.copy2(os.path.join(export_dir, f), sync_dir)

    env = {
        **os.environ,
        "DATAHUB_PROD_URL": GMS_URL,
        "DATAHUB_PROD_TOKEN": "",
    }
    result = subprocess.run(
        [
            "python", "-m", "src.cli.sync_cmd",
            "--metadata-dir", sync_dir,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        logger.error(
            f"Sync round-trip failed:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise RuntimeError(f"Sync CLI failed: {result.stderr}")
    logger.info(f"Sync round-trip output:\n{result.stdout}")
    yield {
        "export_dir": sync_dir,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    shutil.rmtree(sync_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def sync_round_trip_graph(sync_round_trip_dir, integration_graph):
    """DataHubGraph after sync round-trip, for verifying synced entities."""
    return integration_graph
