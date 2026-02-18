"""Unit tests for src/provenance.py -- provenance filtering."""

from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import SystemMetadataClass

from src.provenance import (
    ProvenanceSource,
    classify_provenance,
    filter_entities_by_provenance,
)


def _make_mcpw(aspect_name, system_metadata=None):
    """Create a mock MCPW with the given aspect name and system metadata."""
    mcpw = MagicMock(spec=MetadataChangeProposalWrapper)
    mcpw.aspectName = aspect_name
    mcpw.systemMetadata = system_metadata
    return mcpw


class TestClassifyProvenance:
    def test_ui_source(self, mock_graph):
        sys_meta = SystemMetadataClass(properties={"appSource": "ui"})
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.UI

    def test_ingestion_source(self, mock_graph):
        sys_meta = SystemMetadataClass(
            runId="my-run-123",
            properties={"pipelineName": "my-pipeline"},
        )
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.INGESTION

    def test_cicd_source(self, mock_graph):
        sys_meta = SystemMetadataClass(
            properties={"appSource": "cicd-pipeline"}
        )
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.CICD

    def test_unknown_when_no_system_metadata(self, mock_graph):
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", None)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.UNKNOWN

    def test_unknown_when_api_fails(self, mock_graph):
        mock_graph.get_entity_as_mcps.side_effect = Exception("API error")
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.UNKNOWN

    def test_no_run_id_provided_not_ingestion(self, mock_graph):
        sys_meta = SystemMetadataClass(
            runId="no-run-id-provided",
            properties={},
        )
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.UNKNOWN

    def test_unknown_when_no_matching_aspect(self, mock_graph):
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("otherAspect", None)
        ]
        result = classify_provenance(
            mock_graph, "urn:li:tag:test", "tagProperties"
        )
        assert result == ProvenanceSource.UNKNOWN


class TestFilterEntitiesByProvenance:
    def test_keeps_matching_entities(self, mock_graph):
        sys_meta = SystemMetadataClass(properties={"appSource": "ui"})
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        entities = [{"urn": "urn:li:tag:ui-tag", "name": "UI Tag"}]
        kept, filtered_out = filter_entities_by_provenance(
            mock_graph, entities, "tag", {ProvenanceSource.UI}
        )
        assert len(kept) == 1
        assert kept[0]["urn"] == "urn:li:tag:ui-tag"
        assert len(filtered_out) == 0

    def test_filters_non_matching_entities(self, mock_graph):
        sys_meta = SystemMetadataClass(
            runId="run-1",
            properties={"pipelineName": "my-pipeline"},
        )
        mock_graph.get_entity_as_mcps.return_value = [
            _make_mcpw("tagProperties", sys_meta)
        ]
        entities = [{"urn": "urn:li:tag:ingested-tag", "name": "Ingested"}]
        kept, filtered_out = filter_entities_by_provenance(
            mock_graph, entities, "tag", {ProvenanceSource.UI}
        )
        assert len(kept) == 0
        assert len(filtered_out) == 1
        assert filtered_out[0]["urn"] == "urn:li:tag:ingested-tag"

    def test_unknown_entity_type_returns_all(self, mock_graph):
        entities = [
            {"urn": "urn:li:something:a"},
            {"urn": "urn:li:something:b"},
        ]
        kept, filtered_out = filter_entities_by_provenance(
            mock_graph, entities, "unknownType", {ProvenanceSource.UI}
        )
        assert len(kept) == 2
        assert len(filtered_out) == 0
        # Should not call get_entity_as_mcps for unmapped types
        mock_graph.get_entity_as_mcps.assert_not_called()

    def test_mixed_provenance(self, mock_graph):
        """Entities with different sources; only matching ones kept."""
        ui_meta = SystemMetadataClass(properties={"appSource": "ui"})
        ing_meta = SystemMetadataClass(
            runId="run-1",
            properties={"pipelineName": "pipeline"},
        )

        def side_effect(urn, aspects):
            if urn == "urn:li:tag:ui-tag":
                return [_make_mcpw("tagProperties", ui_meta)]
            else:
                return [_make_mcpw("tagProperties", ing_meta)]

        mock_graph.get_entity_as_mcps.side_effect = side_effect

        entities = [
            {"urn": "urn:li:tag:ui-tag"},
            {"urn": "urn:li:tag:ingested-tag"},
        ]
        kept, filtered_out = filter_entities_by_provenance(
            mock_graph, entities, "tag", {ProvenanceSource.UI}
        )
        assert len(kept) == 1
        assert kept[0]["urn"] == "urn:li:tag:ui-tag"
        assert len(filtered_out) == 1
        assert filtered_out[0]["urn"] == "urn:li:tag:ingested-tag"
