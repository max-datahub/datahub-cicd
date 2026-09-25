from unittest.mock import MagicMock, patch

import pytest

from src.interfaces import EntityHandler, SyncResult, UrnMapper, SKIP_TARGET_MISSING
from src.orchestrator import SyncOrchestrator, _progress_interval
from src.registry import HandlerRegistry
from src.urn_mapper import PassthroughMapper
from src.write_strategy import DryRunStrategy, OverwriteStrategy


class StubHandler(EntityHandler):
    def __init__(self, entity_type_name: str, deps: list[str] | None = None):
        self._entity_type = entity_type_name
        self._deps = deps or []

    @property
    def entity_type(self) -> str:
        return self._entity_type

    @property
    def dependencies(self) -> list[str]:
        return self._deps

    def export(self, graph) -> list[dict]:
        return []

    def build_mcps(self, entity, urn_mapper):
        return []


class TestProgressInterval:
    def test_small_runs(self):
        assert _progress_interval(50) == 25

    def test_medium_runs(self):
        assert _progress_interval(500) == 50

    def test_large_runs(self):
        assert _progress_interval(5000) == 100


class TestSyncOrchestrator:
    def test_sync_empty_exports(self, mock_graph):
        registry = HandlerRegistry()
        registry.register(StubHandler("tag"))
        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        results = orchestrator.sync_all(mock_graph, {})
        assert results == []

    def test_sync_tracks_results(self, mock_graph):
        registry = HandlerRegistry()

        class MCPHandler(StubHandler):
            def build_mcps(self, entity, urn_mapper):
                mcp = MagicMock()
                mcp.entityType = "tag"
                mcp.entityUrn = entity["urn"]
                mcp.aspectName = "tagProperties"
                return [mcp]

        registry.register(MCPHandler("tag"))

        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        exports = {"tag": [{"urn": "urn:li:tag:test"}]}
        results = orchestrator.sync_all(mock_graph, exports)
        assert len(results) == 1
        assert results[0].status == "skipped"  # DryRun skips

    def test_sync_dependency_order(self, mock_graph):
        registry = HandlerRegistry()
        order: list[str] = []

        class TrackingHandler(StubHandler):
            def build_mcps(self, entity, urn_mapper):
                order.append(self.entity_type)
                return []

        registry.register(TrackingHandler("tag"))
        registry.register(TrackingHandler("glossaryNode"))
        registry.register(TrackingHandler("glossaryTerm", deps=["glossaryNode"]))
        registry.register(TrackingHandler("domain"))
        registry.register(
            TrackingHandler(
                "enrichment",
                deps=["tag", "glossaryNode", "glossaryTerm", "domain"],
            )
        )

        exports = {
            "tag": [{"urn": "a"}],
            "glossaryNode": [{"urn": "b"}],
            "glossaryTerm": [{"urn": "c"}],
            "domain": [{"urn": "d"}],
            "enrichment": [{"dataset_urn": "e"}],
        }

        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        orchestrator.sync_all(mock_graph, exports)

        # glossaryNode must come before glossaryTerm
        assert order.index("glossaryNode") < order.index("glossaryTerm")
        # All governance before enrichment
        assert order.index("tag") < order.index("enrichment")
        assert order.index("domain") < order.index("enrichment")

    def test_sync_failure_tracking(self, mock_graph):
        registry = HandlerRegistry()

        class FailingHandler(StubHandler):
            def build_mcps(self, entity, urn_mapper):
                raise ValueError("test error")

        registry.register(FailingHandler("tag"))

        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=OverwriteStrategy(),
        )
        exports = {"tag": [{"urn": "urn:li:tag:fail"}]}
        results = orchestrator.sync_all(mock_graph, exports)
        assert len(results) == 1
        assert results[0].status == "failed"
        assert "test error" in results[0].error
        assert results[0].error_category == "data_error"
        assert results[0].traceback is not None
        assert orchestrator.has_failures()

    def test_validation_failure_skips_sync(self, mock_graph):
        registry = HandlerRegistry()

        class ValidatingHandler(StubHandler):
            def validate(self, entities):
                return ["Missing required field 'name'"]

        registry.register(ValidatingHandler("tag"))

        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        exports = {"tag": [{"urn": "urn:li:tag:invalid"}]}
        results = orchestrator.sync_all(mock_graph, exports)
        assert len(results) == 1
        assert results[0].status == "failed"

    def test_has_failures_false_when_all_succeed(self, mock_graph):
        registry = HandlerRegistry()
        registry.register(StubHandler("tag"))
        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        orchestrator.sync_all(mock_graph, {})
        assert not orchestrator.has_failures()

    def test_print_summary_logs(self, mock_graph, caplog):
        registry = HandlerRegistry()
        registry.register(StubHandler("tag"))
        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        )
        orchestrator.results = [
            SyncResult("tag", "urn:a", "success"),
            SyncResult("tag", "urn:b", "failed", "some error"),
            SyncResult("tag", "urn:c", "skipped"),
        ]
        with caplog.at_level("INFO"):
            orchestrator.print_summary()
        assert "1 succeeded" in caplog.text
        assert "1 failed" in caplog.text
        assert "1 skipped" in caplog.text
        assert "some error" in caplog.text

    def test_incremental_state_written(self, mock_graph, tmp_path):
        import os

        registry = HandlerRegistry()
        registry.register(StubHandler("tag"))
        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
            run_id="test123",
            output_dir=str(tmp_path),
        )

        class ExportHandler(StubHandler):
            def export(self, graph):
                return [{"urn": "urn:li:tag:a"}]

        registry._handlers["tag"] = ExportHandler("tag")

        orchestrator.export_all(mock_graph, str(tmp_path))

        state_path = os.path.join(str(tmp_path), ".run-state.json")
        assert os.path.exists(state_path)

        import json
        with open(state_path) as f:
            state = json.load(f)
        assert state["run_id"] == "test123"
        assert state["status"] == "in_progress"
        assert len(state["completed_phases"]) >= 1


class RequiresHandler(StubHandler):
    def required_target_urn(self, entity, urn_mapper):
        return entity["urn"]

    def build_mcps(self, entity, urn_mapper):
        mcp = MagicMock()
        mcp.entityType = "dataset"
        mcp.entityUrn = entity["urn"]
        mcp.aspectName = "globalTags"
        return [mcp]


class TestTargetExistenceGuard:
    EXPORTS = {
        "enrichment": [
            {"urn": "urn:li:dataset:missing"},
            {"urn": "urn:li:dataset:present"},
        ]
    }

    def _sync(self, graph, strategy):
        registry = HandlerRegistry()
        registry.register(RequiresHandler("enrichment"))
        orchestrator = SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=strategy,
        )
        return orchestrator.sync_all(graph, self.EXPORTS)

    def test_missing_target_is_skipped_and_not_emitted(self, mock_graph):
        mock_graph.exists.side_effect = lambda urn: urn.endswith("present")
        results = self._sync(mock_graph, OverwriteStrategy())
        by_urn = {r.urn: r for r in results}
        assert by_urn["urn:li:dataset:missing"].status == "skipped"
        assert by_urn["urn:li:dataset:missing"].skip_reason == SKIP_TARGET_MISSING
        assert by_urn["urn:li:dataset:present"].status == "success"
        emitted = [c.args[0].entityUrn for c in mock_graph.emit_mcp.call_args_list]
        assert emitted == ["urn:li:dataset:present"]

    def test_dry_run_also_checks_existence(self, mock_graph):
        mock_graph.exists.return_value = False
        results = self._sync(mock_graph, DryRunStrategy())
        assert {r.skip_reason for r in results} == {SKIP_TARGET_MISSING}

    def test_handlers_without_requirement_never_call_exists(self, mock_graph):
        registry = HandlerRegistry()
        registry.register(StubHandler("tag"))
        SyncOrchestrator(
            registry=registry,
            urn_mapper=PassthroughMapper(),
            write_strategy=DryRunStrategy(),
        ).sync_all(mock_graph, {"tag": [{"urn": "urn:li:tag:a"}]})
        mock_graph.exists.assert_not_called()
