"""Integration tests for observability features.

Validates that the export and sync CLIs produce the expected observability
outputs when run against a real DataHub instance:
- JSONL structured log file
- JSON run report with API stats and entity summary
- Markdown run report
- Incremental .run-state.json
- Skip reason tracking (dry-run)

Run: pytest -m integration tests/integration/test_observability.py -v
"""

import glob
import json
import os

import pytest

from tests.integration import seed

pytestmark = pytest.mark.integration


def _load_json(path: str) -> dict | list:
    with open(path) as f:
        return json.load(f)


def _find_jsonl(directory: str) -> str | None:
    """Find the JSONL log file in a directory (run-*.jsonl)."""
    matches = glob.glob(os.path.join(directory, "run-*.jsonl"))
    return matches[0] if matches else None


# ── Export observability ──────────────────────────────────────────────────


class TestExportJsonlLog:
    """Verify the JSONL structured log file from export."""

    def test_jsonl_file_created(self, export_dir):
        jsonl = _find_jsonl(export_dir)
        assert jsonl is not None, "JSONL log file should be created"

    def test_jsonl_entries_are_valid_json(self, export_dir):
        jsonl = _find_jsonl(export_dir)
        with open(jsonl) as f:
            for i, line in enumerate(f):
                entry = json.loads(line)  # should not raise
                assert "timestamp" in entry
                assert "level" in entry
                assert "logger" in entry
                assert "message" in entry

    def test_jsonl_has_multiple_levels(self, export_dir):
        """JSONL should capture at least INFO messages (DEBUG if --log-level=DEBUG)."""
        jsonl = _find_jsonl(export_dir)
        levels = set()
        with open(jsonl) as f:
            for line in f:
                levels.add(json.loads(line)["level"])
        assert "INFO" in levels, "JSONL should contain INFO entries"

    def test_jsonl_captures_key_events(self, export_dir):
        """JSONL should contain logs from the export pipeline."""
        jsonl = _find_jsonl(export_dir)
        messages = []
        with open(jsonl) as f:
            for line in f:
                messages.append(json.loads(line)["message"])
        all_text = " ".join(messages)
        assert "Exporting" in all_text or "Exported" in all_text
        assert "enrichment" in all_text.lower()


class TestExportRunReport:
    """Verify the JSON run report from export."""

    def test_json_report_exists(self, export_dir):
        path = os.path.join(export_dir, "run-report.json")
        assert os.path.exists(path)

    def test_json_report_schema(self, export_dir):
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        assert "run_id" in report
        assert "command" in report
        assert report["command"] == "export"
        assert "timestamp" in report
        assert "duration_seconds" in report
        assert "exit_status" in report
        assert report["exit_status"] == "success"
        assert "entity_summary" in report
        assert "errors" in report
        assert "skips" in report
        assert "warnings" in report

    def test_json_report_has_api_stats(self, export_dir):
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        assert "api_stats" in report
        stats = report["api_stats"]
        assert "total_calls" in stats
        assert stats["total_calls"] > 0, "Export should make API calls"
        assert "total_time_seconds" in stats
        assert "by_method" in stats

    def test_api_stats_track_expected_methods(self, export_dir):
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        methods = report["api_stats"]["by_method"]
        # Export should call get_urns_by_filter and enrichment methods
        assert "get_urns_by_filter" in methods
        # Should have enrichment-related API calls
        enrichment_methods = {"get_tags", "get_glossary_terms", "get_domain", "get_ownership"}
        found = enrichment_methods & set(methods.keys())
        assert len(found) > 0, (
            f"Expected at least one enrichment method in API stats, got: {list(methods.keys())}"
        )

    def test_api_stats_method_details(self, export_dir):
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        for method, data in report["api_stats"]["by_method"].items():
            assert "calls" in data, f"Method {method} missing 'calls'"
            assert "time_seconds" in data, f"Method {method} missing 'time_seconds'"
            assert data["calls"] > 0, f"Method {method} should have >0 calls"

    def test_json_report_no_errors(self, export_dir):
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        assert len(report["errors"]) == 0, (
            f"Export should have no errors, got: {report['errors']}"
        )


class TestExportMarkdownReport:
    """Verify the Markdown run report from export."""

    def test_markdown_report_exists(self, export_dir):
        path = os.path.join(export_dir, "run-report.md")
        assert os.path.exists(path)

    def test_markdown_report_has_header(self, export_dir):
        with open(os.path.join(export_dir, "run-report.md")) as f:
            content = f.read()
        assert "# Run Report: export" in content
        assert "Run ID" in content
        assert "Duration" in content
        assert "success" in content


class TestExportIncrementalState:
    """Verify the incremental .run-state.json from export."""

    def test_run_state_exists(self, export_dir):
        path = os.path.join(export_dir, ".run-state.json")
        assert os.path.exists(path), ".run-state.json should be created"

    def test_run_state_schema(self, export_dir):
        state = _load_json(os.path.join(export_dir, ".run-state.json"))
        assert "run_id" in state
        assert "command" in state
        assert state["command"] == "export"
        assert "started_at" in state
        assert "last_updated_at" in state
        assert "status" in state
        assert "completed_phases" in state
        assert "results_so_far" in state

    def test_run_state_has_completed_phases(self, export_dir):
        state = _load_json(os.path.join(export_dir, ".run-state.json"))
        phases = state["completed_phases"]
        assert len(phases) > 0, "Should have at least one completed phase"
        for phase in phases:
            assert "entity_type" in phase
            assert "phase" in phase
            assert phase["phase"] == "export"
            assert "duration_seconds" in phase
            assert "entity_count" in phase

    def test_run_state_includes_governance_phases(self, export_dir):
        state = _load_json(os.path.join(export_dir, ".run-state.json"))
        entity_types = {p["entity_type"] for p in state["completed_phases"]}
        assert "tag" in entity_types
        assert "domain" in entity_types

    def test_run_id_consistent_across_outputs(self, export_dir):
        """JSONL filename, JSON report, and run state should share the same run ID."""
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        state = _load_json(os.path.join(export_dir, ".run-state.json"))
        run_id = report["run_id"]

        assert state["run_id"] == run_id, "Run state and report should share run_id"

        jsonl = _find_jsonl(export_dir)
        assert run_id in os.path.basename(jsonl), (
            f"JSONL filename should contain run_id={run_id}, got: {os.path.basename(jsonl)}"
        )


# ── Sync observability ────────────────────────────────────────────────────


class TestSyncRoundTripObservability:
    """Verify observability outputs from the sync round-trip."""

    def test_sync_summary_in_stderr(self, sync_round_trip_dir):
        """Sync summary is logged via logger (stderr), not print (stdout)."""
        stderr = sync_round_trip_dir["stderr"]
        assert "0 failed" in stderr, (
            f"Expected '0 failed' in sync stderr, got:\n{stderr}"
        )

    def test_sync_json_report_exists(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        path = os.path.join(export_dir, "run-report.json")
        assert os.path.exists(path)

    def test_sync_json_report_schema(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        assert report["command"] == "sync"
        assert report["exit_status"] == "success"
        assert "entity_summary" in report

    def test_sync_json_report_counts(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        # At least some entities should have been synced
        total_synced = sum(
            counts.get("synced", 0)
            for counts in report["entity_summary"].values()
        )
        assert total_synced > 0, "Sync should have synced at least one entity"

    def test_sync_api_stats_emit_mcp(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        report = _load_json(os.path.join(export_dir, "run-report.json"))
        stats = report.get("api_stats", {})
        methods = stats.get("by_method", {})
        assert "emit_mcp" in methods, "Sync should track emit_mcp calls"
        assert methods["emit_mcp"]["calls"] > 0

    def test_sync_incremental_state(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        state_path = os.path.join(export_dir, ".run-state.json")
        assert os.path.exists(state_path)
        state = _load_json(state_path)
        assert state["command"] == "sync"
        phases = state["completed_phases"]
        sync_phases = [p for p in phases if p["phase"] == "sync"]
        assert len(sync_phases) > 0

    def test_sync_markdown_report(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        md_path = os.path.join(export_dir, "run-report.md")
        assert os.path.exists(md_path)
        with open(md_path) as f:
            content = f.read()
        assert "# Run Report: sync" in content
        assert "Entity Summary" in content

    def test_sync_jsonl_log(self, sync_round_trip_dir):
        export_dir = sync_round_trip_dir["export_dir"]
        jsonl = _find_jsonl(export_dir)
        assert jsonl is not None, "Sync should produce a JSONL log"
        with open(jsonl) as f:
            lines = f.readlines()
        assert len(lines) > 0


# ── Dry-run skip tracking ─────────────────────────────────────────────────


class TestDryRunSkipTracking:
    """Verify skip reasons are tracked in dry-run mode."""

    @pytest.fixture(scope="class")
    def dry_run_dir(self, seeded_graph, export_dir):
        """Run a dry-run sync and return the metadata dir."""
        import shutil
        import subprocess
        import tempfile

        from tests.integration.conftest import GMS_URL

        tmpdir = tempfile.mkdtemp(prefix="datahub-cicd-dryrun-")
        # Copy exported JSON to the temp dir
        for f in os.listdir(export_dir):
            if f.endswith(".json"):
                shutil.copy2(os.path.join(export_dir, f), tmpdir)

        env = {
            **os.environ,
            "DATAHUB_PROD_URL": GMS_URL,
            "DATAHUB_PROD_TOKEN": "",
        }
        result = subprocess.run(
            [
                "python", "-m", "src.cli.sync_cmd",
                "--metadata-dir", tmpdir,
                "--dry-run",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"Dry-run failed: {result.stderr}"
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_dry_run_report_has_skips(self, dry_run_dir):
        report = _load_json(os.path.join(dry_run_dir, "run-report.json"))
        assert len(report["skips"]) > 0, "Dry run should have skip entries"

    def test_dry_run_all_skips_are_dry_run_reason(self, dry_run_dir):
        report = _load_json(os.path.join(dry_run_dir, "run-report.json"))
        for skip in report["skips"]:
            assert skip["reason"] == "dry_run", (
                f"All skips in dry-run should have reason=dry_run, got: {skip['reason']}"
            )
            assert "urn" in skip
            assert "entity_type" in skip

    def test_dry_run_entity_summary_shows_skipped(self, dry_run_dir):
        report = _load_json(os.path.join(dry_run_dir, "run-report.json"))
        total_skipped = sum(
            counts.get("skipped", 0)
            for counts in report["entity_summary"].values()
        )
        assert total_skipped > 0, "Dry run should show skipped entities"
        total_synced = sum(
            counts.get("synced", 0)
            for counts in report["entity_summary"].values()
        )
        assert total_synced == 0, "Dry run should have 0 synced"

    def test_dry_run_markdown_has_skip_table(self, dry_run_dir):
        with open(os.path.join(dry_run_dir, "run-report.md")) as f:
            content = f.read()
        assert "Skipped Entities" in content
        assert "dry_run" in content

    def test_dry_run_zero_api_calls(self, dry_run_dir):
        report = _load_json(os.path.join(dry_run_dir, "run-report.json"))
        stats = report.get("api_stats", {})
        assert stats.get("total_calls", 0) == 0, (
            "Dry run should make 0 API calls to prod"
        )
