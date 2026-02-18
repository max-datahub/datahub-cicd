"""Unit tests for src/reporting.py -- JSON + Markdown report generation."""

import json
import os
import tempfile

import pytest

from src.interfaces import SKIP_DRY_RUN, SKIP_NO_ENRICHMENT, SKIP_SYSTEM_ENTITY, SyncResult
from src.reporting import RunReport, write_run_state


class TestRunReport:
    def test_from_results_success(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
            SyncResult("tag", "urn:li:tag:b", "success"),
        ]
        report = RunReport.from_results(
            run_id="abc123", command="export", results=results
        )
        assert report.exit_status == "success"
        assert report.entity_summary["tag"]["exported"] == 2
        assert report.entity_summary["tag"]["failed"] == 0

    def test_from_results_with_failures(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
            SyncResult(
                "tag", "urn:li:tag:b", "failed",
                error="API error",
                error_category="server_error",
                error_suggestion="Retry later",
            ),
        ]
        report = RunReport.from_results(
            run_id="abc123", command="sync", results=results
        )
        assert report.exit_status == "failure"
        assert report.entity_summary["tag"]["synced"] == 1
        assert report.entity_summary["tag"]["failed"] == 1
        assert len(report.errors) == 1
        assert report.errors[0]["category"] == "server_error"
        assert report.errors[0]["suggestion"] == "Retry later"

    def test_from_results_with_skips(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "skipped", skip_reason=SKIP_DRY_RUN),
            SyncResult("tag", "urn:li:tag:b", "skipped", skip_reason=SKIP_DRY_RUN),
            SyncResult("domain", "urn:li:domain:c", "skipped", skip_reason=SKIP_SYSTEM_ENTITY),
        ]
        report = RunReport.from_results(
            run_id="abc123", command="sync", results=results
        )
        assert report.exit_status == "success"
        assert len(report.skips) == 3
        # Check skip reasons
        dry_run_skips = [s for s in report.skips if s["reason"] == SKIP_DRY_RUN]
        assert len(dry_run_skips) == 2

    def test_to_dict_schema(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
        ]
        report = RunReport.from_results(
            run_id="test123",
            command="export",
            results=results,
            duration_seconds=5.5,
        )
        d = report.to_dict()

        # Verify required schema fields
        assert "run_id" in d
        assert "command" in d
        assert "timestamp" in d
        assert "duration_seconds" in d
        assert "exit_status" in d
        assert "entity_summary" in d
        assert "timing" in d
        assert "errors" in d
        assert "skips" in d
        assert "warnings" in d

        assert d["run_id"] == "test123"
        assert d["command"] == "export"
        assert d["duration_seconds"] == 5.5

    def test_to_markdown(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
            SyncResult("tag", "urn:li:tag:b", "failed", error="bad"),
            SyncResult("domain", "urn:li:domain:c", "skipped", skip_reason=SKIP_DRY_RUN),
        ]
        report = RunReport.from_results(
            run_id="test123", command="sync", results=results
        )
        md = report.to_markdown()

        assert "# Run Report: sync" in md
        assert "test123" in md
        assert "Entity Summary" in md
        assert "tag" in md
        assert "Errors" in md
        assert "urn:li:tag:b" in md
        assert "Skipped Entities" in md

    def test_write_creates_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            results = [SyncResult("tag", "urn:li:tag:a", "success")]
            report = RunReport.from_results(
                run_id="test", command="export", results=results
            )
            json_path, md_path = report.write(tmpdir)

            assert os.path.exists(json_path)
            assert os.path.exists(md_path)

            with open(json_path) as f:
                data = json.load(f)
            assert data["run_id"] == "test"

            with open(md_path) as f:
                content = f.read()
            assert "# Run Report" in content

    def test_from_results_sync_counts_synced(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
        ]
        report = RunReport.from_results(
            run_id="abc", command="sync", results=results
        )
        assert report.entity_summary["tag"]["synced"] == 1
        assert report.entity_summary["tag"]["exported"] == 0

    def test_from_results_export_counts_exported(self):
        results = [
            SyncResult("tag", "urn:li:tag:a", "success"),
        ]
        report = RunReport.from_results(
            run_id="abc", command="export", results=results
        )
        assert report.entity_summary["tag"]["exported"] == 1
        assert report.entity_summary["tag"]["synced"] == 0


class TestWriteRunState:
    def test_writes_state_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            results = [
                SyncResult("tag", "urn:li:tag:a", "success"),
                SyncResult("tag", "urn:li:tag:b", "failed", error="err"),
            ]
            write_run_state(
                output_dir=tmpdir,
                run_id="test123",
                command="export",
                started_at="2024-01-01T00:00:00Z",
                status="in_progress",
                completed_phases=[
                    {"entity_type": "tag", "phase": "export", "duration_seconds": 1.5, "entity_count": 5}
                ],
                results=results,
                errors=[{"urn": "urn:li:tag:b", "entity_type": "tag", "category": "unknown", "message": "err"}],
            )

            state_path = os.path.join(tmpdir, ".run-state.json")
            assert os.path.exists(state_path)

            with open(state_path) as f:
                state = json.load(f)

            assert state["run_id"] == "test123"
            assert state["status"] == "in_progress"
            assert state["results_so_far"]["success"] == 1
            assert state["results_so_far"]["failed"] == 1
            assert len(state["completed_phases"]) == 1
