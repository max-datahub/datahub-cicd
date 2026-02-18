"""Unit tests for src/deletion.py -- deletion propagation."""

from unittest.mock import MagicMock, call

import pytest

from src.deletion import apply_deletions, detect_soft_deleted


class TestDetectSoftDeleted:
    def test_returns_empty_when_no_soft_deleted(self, mock_graph):
        mock_graph.get_urns_by_filter.return_value = []
        result = detect_soft_deleted(mock_graph)
        assert result == []

    def test_detects_soft_deleted_tags(self, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:tag:deleted-tag"
        ]
        result = detect_soft_deleted(mock_graph, entity_types=["tag"])
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:tag:deleted-tag"
        assert result[0]["entity_type"] == "tag"

    def test_detects_across_multiple_entity_types(self, mock_graph):
        def side_effect(entity_types, status):
            et = entity_types[0]
            if et == "tag":
                return ["urn:li:tag:deleted-tag"]
            elif et == "domain":
                return ["urn:li:domain:deleted-domain"]
            return []

        mock_graph.get_urns_by_filter.side_effect = side_effect
        result = detect_soft_deleted(mock_graph)

        urns = {r["urn"] for r in result}
        types = {r["entity_type"] for r in result}
        assert "urn:li:tag:deleted-tag" in urns
        assert "urn:li:domain:deleted-domain" in urns
        assert "tag" in types
        assert "domain" in types

    def test_custom_entity_types(self, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:glossaryTerm:deleted-term"
        ]
        result = detect_soft_deleted(
            mock_graph, entity_types=["glossaryTerm"]
        )
        assert len(result) == 1
        assert result[0]["entity_type"] == "glossaryTerm"
        # Should only scan the specified type
        mock_graph.get_urns_by_filter.assert_called_once()


class TestApplyDeletions:
    def test_apply_empty_list(self, mock_graph):
        result = apply_deletions(mock_graph, [])
        assert result == []
        mock_graph.soft_delete_entity.assert_not_called()

    def test_dry_run_does_not_delete(self, mock_graph):
        deletions = [
            {"urn": "urn:li:tag:to-delete", "entity_type": "tag"},
        ]
        result = apply_deletions(mock_graph, deletions, dry_run=True)
        assert len(result) == 1
        assert result[0].status == "skipped"
        assert result[0].urn == "urn:li:tag:to-delete"
        mock_graph.soft_delete_entity.assert_not_called()

    def test_apply_calls_soft_delete(self, mock_graph):
        deletions = [
            {"urn": "urn:li:tag:to-delete", "entity_type": "tag"},
            {"urn": "urn:li:domain:to-delete", "entity_type": "domain"},
        ]
        result = apply_deletions(mock_graph, deletions)
        assert len(result) == 2
        assert all(r.status == "success" for r in result)
        mock_graph.soft_delete_entity.assert_has_calls(
            [
                call("urn:li:tag:to-delete"),
                call("urn:li:domain:to-delete"),
            ]
        )

    def test_failure_tracked(self, mock_graph):
        mock_graph.soft_delete_entity.side_effect = Exception("API error")
        deletions = [
            {"urn": "urn:li:tag:fail", "entity_type": "tag"},
        ]
        result = apply_deletions(mock_graph, deletions)
        assert len(result) == 1
        assert result[0].status == "failed"
        assert result[0].error == "API error"

    def test_partial_failure_continues(self, mock_graph):
        mock_graph.soft_delete_entity.side_effect = [
            Exception("first fails"),
            None,  # second succeeds
        ]
        deletions = [
            {"urn": "urn:li:tag:first", "entity_type": "tag"},
            {"urn": "urn:li:tag:second", "entity_type": "tag"},
        ]
        result = apply_deletions(mock_graph, deletions)
        assert len(result) == 2
        assert result[0].status == "failed"
        assert result[1].status == "success"
