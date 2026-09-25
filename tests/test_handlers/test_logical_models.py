import pytest
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.openapi import RelatedEntity, RelationshipDirection
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerKeyClass,
    ContainerPropertiesClass,
    DataPlatformInfoClass,
    DatasetPropertiesClass,
    EdgeClass,
    LogicalParentClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

from src.handlers.logical_models import LogicalModelHandler

P = "urn:li:dataPlatform:logical"
ROOT = "urn:li:container:root"
SUB = "urn:li:container:sub"
MODEL = f"urn:li:dataset:({P},invoice,PROD)"
CHILD = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.invoices,PROD)"


def schema(platform: str, *cols: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="s", platform=platform, version=0, hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[SchemaFieldClass(fieldPath=c, type=SchemaFieldDataTypeClass(StringTypeClass()), nativeDataType="string") for c in cols],
    )


@pytest.fixture
def world():
    """Source-instance state the mock graph serves."""
    return {
        "logical": {P},
        "datasets": {P: [MODEL]},
        "aspects": {
            ROOT: {"containerProperties": ContainerPropertiesClass(name="root")},
            SUB: {"containerProperties": ContainerPropertiesClass(name="sub"), "container": ContainerClass(container=ROOT)},
            MODEL: {
                "datasetProperties": DatasetPropertiesClass(name="invoice"),
                "schemaMetadata": schema(P, "invoice_id", "amount"),
                "container": ContainerClass(container=SUB),
            },
            CHILD: {"schemaMetadata": schema("urn:li:dataPlatform:snowflake", "invoice_id", "amount")},
        },
        "related": {},
        "logical_parent": {},
    }


@pytest.fixture
def graph(mock_graph, world):
    def get_aspect(urn, cls):
        if cls is DataPlatformInfoClass:
            return DataPlatformInfoClass(name=urn.split(":")[-1], type="OTHERS", datasetNameDelimiter=".", logical=urn in world["logical"])
        if cls is LogicalParentClass:
            return world["logical_parent"].get(urn)
        return world["aspects"].get(urn, {}).get(cls.ASPECT_NAME)

    def get_entity_semityped(urn, aspects=None):
        bag = {"containerKey": ContainerKeyClass(guid="k")}  # key aspects always come back
        bag.update({k: v for k, v in world["aspects"].get(urn, {}).items() if aspects is None or k in aspects})
        return bag

    def get_urns_by_filter(entity_types=None, platform=None, extraFilters=None, **_):
        if entity_types == ["dataPlatform"]:
            return sorted(world["logical"])
        return world["datasets"].get(platform, [])

    mock_graph.get_aspect.side_effect = get_aspect
    mock_graph.get_entity_semityped.side_effect = get_entity_semityped
    mock_graph.get_urns_by_filter.side_effect = get_urns_by_filter
    mock_graph.get_related_entities.side_effect = lambda urn, relationship_types, direction: [
        RelatedEntity(urn=c, relationship_type="PhysicalInstanceOf") for c in world["related"].get(urn, [])
    ]
    return mock_graph


def by_urn(entities):
    return {e["urn"]: e for e in entities}


def test_exports_platform_ancestor_containers_and_datasets(graph):
    out = by_urn(LogicalModelHandler(platforms=["logical"]).export(graph))
    assert set(out) == {P, ROOT, SUB, MODEL}
    assert out[P]["aspects"]["dataPlatformInfo"]["logical"] is True
    assert out[SUB]["aspects"]["container"] == {"container": ROOT}
    assert [f["fieldPath"] for f in out[MODEL]["aspects"]["schemaMetadata"]["fields"]] == ["invoice_id", "amount"]
    assert "containerKey" not in out[ROOT]["aspects"]
    assert "physicalChildren" not in out[MODEL]


def test_platform_given_as_urn_or_name(graph):
    by_name = LogicalModelHandler(platforms=["logical"]).export(graph)
    by_full_urn = LogicalModelHandler(platforms=[P]).export(graph)
    assert by_urn(by_name) == by_urn(by_full_urn)


def test_non_logical_platform_exports_nothing(graph):
    assert LogicalModelHandler(platforms=["snowflake"]).export(graph) == []


def test_unscoped_discovers_logical_platforms(graph):
    out = LogicalModelHandler().export(graph)
    assert MODEL in by_urn(out)
    graph.get_urns_by_filter.assert_any_call(
        entity_types=["dataPlatform"],
        extraFilters=[{"field": "logical", "condition": "EQUAL", "values": ["true"]}],
    )


def test_dangling_container_not_exported(graph, world):
    world["aspects"][MODEL]["container"] = ContainerClass(container="urn:li:container:gone")
    out = by_urn(LogicalModelHandler(platforms=["logical"]).export(graph))
    assert "urn:li:container:gone" not in out
    assert out[MODEL]["aspects"]["container"] == {"container": "urn:li:container:gone"}


def test_physical_children_exported_with_matching_column_links(graph, world):
    world["related"][MODEL] = [CHILD]
    world["logical_parent"][CHILD] = LogicalParentClass(parent=EdgeClass(destinationUrn=MODEL))
    sf = make_schema_field_urn(CHILD, "invoice_id")
    world["logical_parent"][sf] = LogicalParentClass(parent=EdgeClass(destinationUrn=make_schema_field_urn(MODEL, "invoice_id")))
    other = make_schema_field_urn(CHILD, "amount")
    world["logical_parent"][other] = LogicalParentClass(parent=EdgeClass(destinationUrn=make_schema_field_urn("urn:li:dataset:(urn:li:dataPlatform:logical,other,PROD)", "amount")))

    out = by_urn(LogicalModelHandler(platforms=["logical"]).export(graph))

    children = out[MODEL]["physicalChildren"]
    assert [c["urn"] for c in children] == [CHILD]
    assert children[0]["logicalParent"]["parent"]["destinationUrn"] == MODEL
    assert [f["urn"] for f in children[0]["fields"]] == [sf]
    graph.get_related_entities.assert_any_call(MODEL, relationship_types=["PhysicalInstanceOf"], direction=RelationshipDirection.INCOMING)
