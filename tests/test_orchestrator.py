from unittest.mock import MagicMock, patch

import pytest

from src.interfaces import EntityHandler, SyncResult, UrnMapper
from src.orchestrator import SyncOrchestrator
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

    def test_print_summary(self, mock_graph, capsys):
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
        orchestrator.print_summary()
        captured = capsys.readouterr()
        assert "1 succeeded" in captured.out
        assert "1 failed" in captured.out
        assert "1 skipped" in captured.out
        assert "some error" in captured.out
