"""Unit tests for src/run_context.py -- TrackedGraph and RunContext."""

from unittest.mock import MagicMock

import pytest

from src.run_context import RunContext, TrackedGraph


class TestTrackedGraph:
    def test_tracked_method_counts_calls(self):
        mock_graph = MagicMock()
        mock_graph.get_tags.return_value = None
        tracked = TrackedGraph(mock_graph)

        tracked.get_tags("urn:li:dataset:test")
        tracked.get_tags("urn:li:dataset:test2")

        assert tracked.call_counts["get_tags"] == 2
        assert tracked.call_times["get_tags"] > 0

    def test_untracked_method_passes_through(self):
        mock_graph = MagicMock()
        mock_graph.some_other_method.return_value = "result"
        tracked = TrackedGraph(mock_graph)

        result = tracked.some_other_method()

        assert result == "result"
        assert "some_other_method" not in tracked.call_counts

    def test_attribute_passthrough(self):
        mock_graph = MagicMock()
        mock_graph.server_config = {"key": "value"}
        tracked = TrackedGraph(mock_graph)

        assert tracked.server_config == {"key": "value"}

    def test_emit_mcp_tracked(self):
        mock_graph = MagicMock()
        mock_graph.emit_mcp.return_value = None
        tracked = TrackedGraph(mock_graph)

        tracked.emit_mcp("some_mcp")

        assert tracked.call_counts["emit_mcp"] == 1
        mock_graph.emit_mcp.assert_called_once_with("some_mcp")

    def test_get_stats(self):
        mock_graph = MagicMock()
        mock_graph.get_tags.return_value = None
        mock_graph.emit_mcp.return_value = None
        tracked = TrackedGraph(mock_graph)

        tracked.get_tags("urn:li:dataset:test")
        tracked.emit_mcp("mcp1")
        tracked.emit_mcp("mcp2")

        stats = tracked.get_stats()
        assert stats["total_calls"] == 3
        assert stats["by_method"]["get_tags"]["calls"] == 1
        assert stats["by_method"]["emit_mcp"]["calls"] == 2

    def test_soft_delete_entity_tracked(self):
        mock_graph = MagicMock()
        mock_graph.soft_delete_entity.return_value = None
        tracked = TrackedGraph(mock_graph)

        tracked.soft_delete_entity("urn:li:tag:test")

        assert tracked.call_counts["soft_delete_entity"] == 1


class TestRunContext:
    def test_run_id_generated(self):
        ctx = RunContext(command="export")
        assert len(ctx.run_id) == 12

    def test_phase_timing(self):
        ctx = RunContext(command="export")
        timer = ctx.start_phase("export_tag")
        assert timer.phase == "export_tag"
        ctx.stop_phase("export_tag")
        assert timer.duration > 0

    def test_timing_summary(self):
        ctx = RunContext(command="sync")
        timer = ctx.start_phase("sync_tag")
        ctx.stop_phase("sync_tag")

        summary = ctx.timing_summary()
        assert "sync_tag" in summary
        assert "duration_seconds" in summary["sync_tag"]
        assert "api_calls" in summary["sync_tag"]

    def test_duration_seconds(self):
        ctx = RunContext(command="export")
        assert ctx.duration_seconds >= 0
