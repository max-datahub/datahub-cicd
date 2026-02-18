"""Unit tests for src/write_strategy.py -- CI/CD system metadata tagging."""

from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import TagPropertiesClass

from src.write_strategy import CICD_SYSTEM_METADATA, DryRunStrategy, OverwriteStrategy


class TestOverwriteSystemMetadata:
    def test_overwrite_sets_cicd_system_metadata(self, mock_graph):
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:tag:test",
            aspect=TagPropertiesClass(name="Test"),
        )
        strategy = OverwriteStrategy()
        strategy.emit(mock_graph, [mcp])

        assert mcp.systemMetadata is CICD_SYSTEM_METADATA
        assert mcp.systemMetadata.properties["appSource"] == "cicd-pipeline"
        mock_graph.emit_mcp.assert_called_once_with(mcp)


class TestDryRunSystemMetadata:
    def test_dry_run_does_not_set_metadata(self, mock_graph):
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:tag:test",
            aspect=TagPropertiesClass(name="Test"),
        )
        original_metadata = mcp.systemMetadata
        strategy = DryRunStrategy()
        strategy.emit(mock_graph, [mcp])

        # DryRunStrategy should not modify systemMetadata
        assert mcp.systemMetadata is original_metadata
        mock_graph.emit_mcp.assert_not_called()
