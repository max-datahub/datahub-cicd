"""Integration: logical model definitions export to a folder tree and push to a fresh target."""

import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInfoClass,
    GlobalTagsClass,
    LogicalParentClass,
    SchemaMetadataClass,
)

from tests.integration import seed
from tests.integration.conftest import EXTERNAL_GMS_URL, GMS_TOKEN, GMS_URL

pytestmark = pytest.mark.integration

SCOPE = ["--platform", "cicd_it_logical", "--platform", "postgres"]
TREE = Path("logicalModels/cicd_it_logical")


def _cli(module: str, *args: str) -> None:
    env = {**os.environ, "DATAHUB_DEV_URL": GMS_URL, "DATAHUB_DEV_TOKEN": GMS_TOKEN,
           "DATAHUB_PROD_URL": GMS_URL, "DATAHUB_PROD_TOKEN": GMS_TOKEN}
    result = subprocess.run(["python", "-m", module, *args], env=env, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, f"{module} failed:\n{result.stdout[-3000:]}\n{result.stderr[-3000:]}"


def _wait_indexed(graph, timeout: int = 90) -> None:
    """Search + graph indices must see the models and the child link before export."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        models = set(graph.get_urns_by_filter(entity_types=["dataset"], platform=seed.LM_PLATFORM))
        children = {r.urn for r in graph.get_related_entities(seed.LM_INVOICE, ["PhysicalInstanceOf"], RelationshipDirection.INCOMING)}
        if {seed.LM_INVOICE, seed.LM_CUSTOMER} <= models and seed.LM_CHILD in children:
            return
        time.sleep(2)
    raise TimeoutError("logical models not indexed")


def _tree_bytes(export_dir: Path) -> dict[str, bytes]:
    root = export_dir / "logicalModels"
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*.json"))}


@pytest.fixture(scope="module")
def lm_graph(seeded_graph):
    info = seeded_graph.get_aspect("urn:li:dataPlatform:logical", DataPlatformInfoClass)
    if not (info and info.logical):
        message = "target DataHub predates DataPlatformInfo.logical (needs OSS v1.7+ / Cloud v2.1+)"
        if EXTERNAL_GMS_URL:
            pytest.skip(message)
        pytest.fail(message)
    seed.seed_logical_models(seeded_graph)
    _wait_indexed(seeded_graph)
    return seeded_graph


@pytest.fixture(scope="module")
def first_export(lm_graph, tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("lm-export-1")
    _cli("src.cli.export_cmd", "--output-dir", str(out), *SCOPE)
    return out


@pytest.fixture(scope="module")
def pushed_to_fresh_target(lm_graph, first_export) -> Path:
    for urn in (seed.LM_INVOICE, seed.LM_CUSTOMER, seed.LM_SUB, seed.LM_ROOT, seed.LM_PLATFORM, seed.LM_CHILD):
        lm_graph.hard_delete_entity(urn)
    # schemaField URNs are distinct GMS entities; hard_delete_entity(LM_CHILD) does not
    # cascade to them, and seed_logical_models wrote LogicalParentClass directly onto
    # them, so they must be deleted explicitly to make the fresh-target precondition real.
    for c in seed.LM_COLUMNS:
        lm_graph.hard_delete_entity(make_schema_field_urn(seed.LM_CHILD, c))
    lm_graph.emit_mcp(seed.logical_child_schema_mcp())  # physical child re-ingested on the "new" instance, unlinked
    assert not lm_graph.exists(seed.LM_INVOICE)
    assert lm_graph.get_aspect(seed.LM_CHILD, LogicalParentClass) is None
    for c in seed.LM_COLUMNS:
        assert lm_graph.get_aspect(make_schema_field_urn(seed.LM_CHILD, c), LogicalParentClass) is None
    _cli("src.cli.sync_cmd", "--metadata-dir", str(first_export))
    return first_export


class TestExport:
    def test_folder_tree(self, first_export):
        root = first_export / TREE
        assert (root / "platform.json").exists()
        assert (root / "CICD_IT_Root/container.json").exists()
        assert (root / "CICD_IT_Root/Billing/container.json").exists()
        assert (root / "CICD_IT_Root/Billing/invoice.PROD.json").exists()
        assert (root / "CICD_IT_Root/customer.PROD.json").exists()

    def test_non_logical_platform_has_no_definitions(self, first_export):
        assert not (first_export / "logicalModels/postgres").exists()

    def test_physical_children_exported(self, first_export):
        body = json.loads((first_export / TREE / "CICD_IT_Root/Billing/invoice.PROD.json").read_text())
        (child,) = body["physicalChildren"]
        assert child["urn"] == seed.LM_CHILD
        assert {f["urn"] for f in child["fields"]} == {make_schema_field_urn(seed.LM_CHILD, c) for c in seed.LM_COLUMNS}


class TestFreshTargetPush:
    def test_models_restored(self, lm_graph, pushed_to_fresh_target):
        assert lm_graph.get_aspect(seed.LM_PLATFORM, DataPlatformInfoClass).logical is True
        schema = lm_graph.get_aspect(seed.LM_INVOICE, SchemaMetadataClass)
        assert [f.fieldPath for f in schema.fields] == list(seed.LM_COLUMNS)
        assert lm_graph.get_aspect(seed.LM_INVOICE, ContainerClass).container == seed.LM_SUB
        assert lm_graph.get_aspect(seed.LM_SUB, ContainerClass).container == seed.LM_ROOT
        tags = lm_graph.get_aspect(seed.LM_INVOICE, GlobalTagsClass)
        assert [str(t.tag) for t in tags.tags] == [seed.TAG_PII]

    def test_child_links_restored(self, lm_graph, pushed_to_fresh_target):
        assert lm_graph.get_aspect(seed.LM_CHILD, LogicalParentClass).parent.destinationUrn == seed.LM_INVOICE
        for c in seed.LM_COLUMNS:
            link = lm_graph.get_aspect(make_schema_field_urn(seed.LM_CHILD, c), LogicalParentClass)
            assert link.parent.destinationUrn == make_schema_field_urn(seed.LM_INVOICE, c)


class TestIdempotency:
    def test_reexport_is_byte_identical(self, lm_graph, pushed_to_fresh_target, tmp_path):
        _wait_indexed(lm_graph)
        _cli("src.cli.sync_cmd", "--metadata-dir", str(pushed_to_fresh_target))  # second push: no-op
        _wait_indexed(lm_graph)
        _cli("src.cli.export_cmd", "--output-dir", str(tmp_path), *SCOPE)
        assert _tree_bytes(tmp_path) == _tree_bytes(pushed_to_fresh_target)
