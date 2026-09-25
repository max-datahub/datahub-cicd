"""Regression: enrichment sync must not create stub entities on the target."""

import json
import os
import subprocess

import pytest

from tests.integration import seed
from tests.integration.conftest import GMS_TOKEN, GMS_URL

pytestmark = pytest.mark.integration

MISSING = "urn:li:dataset:(urn:li:dataPlatform:postgres,cicd_it.never_created,PROD)"


def test_enrichment_for_missing_entity_is_skipped_not_stubbed(seeded_graph, tmp_path):
    (tmp_path / "enrichment.json").write_text(
        json.dumps([{"dataset_urn": MISSING, "globalTags": [{"tag": seed.TAG_PII}]}])
    )
    env = {**os.environ, "DATAHUB_PROD_URL": GMS_URL, "DATAHUB_PROD_TOKEN": GMS_TOKEN}
    result = subprocess.run(
        ["python", "-m", "src.cli.sync_cmd", "--metadata-dir", str(tmp_path)],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert not seeded_graph.exists(MISSING)
    report = json.loads((tmp_path / "run-report.json").read_text())
    assert {"urn": MISSING, "entity_type": "enrichment", "reason": "target_missing"} in report["skips"]
