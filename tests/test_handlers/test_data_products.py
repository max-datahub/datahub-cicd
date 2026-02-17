from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import (
    DataProductAssociationClass,
    DataProductPropertiesClass,
)

from src.handlers.data_products import DataProductHandler
from src.urn_mapper import PassthroughMapper


class TestDataProductHandler:
    @pytest.fixture
    def handler(self):
        return DataProductHandler()

    def test_entity_type(self, handler):
        assert handler.entity_type == "dataProduct"

    def test_depends_on_domain(self, handler):
        assert "domain" in handler.dependencies

    def test_export(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataProduct:fraud",
        ]

        assoc = MagicMock(spec=DataProductAssociationClass)
        assoc.destinationUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,banking.public.fraud,PROD)"

        props = MagicMock(spec=DataProductPropertiesClass)
        props.name = "Fraud"
        props.description = "Fraud detection"
        props.customProperties = {"team": "security"}
        props.assets = [assoc]

        mock_graph.get_aspect.return_value = props

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert entities[0]["name"] == "Fraud"
        assert len(entities[0]["assets"]) == 1
        assert "fraud" in entities[0]["assets"][0]["destinationUrn"]

    def test_export_no_assets(self, handler, mock_graph):
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataProduct:empty",
        ]

        props = MagicMock(spec=DataProductPropertiesClass)
        props.name = "Empty"
        props.description = None
        props.customProperties = {}
        props.assets = []

        mock_graph.get_aspect.return_value = props

        entities = handler.export(mock_graph)
        assert len(entities) == 1
        assert entities[0]["assets"] == []

    def test_build_mcps(self, handler):
        entity = {
            "urn": "urn:li:dataProduct:fraud",
            "name": "Fraud",
            "description": "Fraud detection product",
            "customProperties": {"team": "security"},
            "assets": [
                {
                    "destinationUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.fraud,PROD)",
                }
            ],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        mcp = mcps[0]
        assert mcp.entityUrn == "urn:li:dataProduct:fraud"
        assert isinstance(mcp.aspect, DataProductPropertiesClass)
        assert mcp.aspect.name == "Fraud"
        assert len(mcp.aspect.assets) == 1
        assert (
            mcp.aspect.assets[0].destinationUrn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.fraud,PROD)"
        )

    def test_build_mcps_empty_assets(self, handler):
        entity = {
            "urn": "urn:li:dataProduct:empty",
            "name": "Empty",
            "description": None,
            "customProperties": {},
            "assets": [],
        }
        mapper = PassthroughMapper()
        mcps = handler.build_mcps(entity, mapper)

        assert len(mcps) == 1
        assert mcps[0].aspect.assets == []

    def test_build_mcps_maps_asset_urns(self, handler):
        """Asset URNs should be mapped through UrnMapper."""
        entity = {
            "urn": "urn:li:dataProduct:test",
            "name": "Test",
            "assets": [
                {"destinationUrn": "urn:li:dataset:source-ds"},
            ],
        }
        # Use a custom mapper that transforms URNs
        mapper = MagicMock()
        mapper.map.side_effect = lambda u: u.replace("source", "target")

        mcps = handler.build_mcps(entity, mapper)
        assert mcps[0].aspect.assets[0].destinationUrn == "urn:li:dataset:target-ds"
