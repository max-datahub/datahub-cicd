import json


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

from src.handlers import create_default_registry
from src.handlers.enrichment import DatasetEnrichmentHandler, GenericEnrichmentHandler
from src.handlers.logical_models import LogicalModelHandler
from src.interfaces import SKIP_TARGET_MISSING
from src.logical_layout import write_tree
from src.orchestrator import SyncOrchestrator
from src.registry import HandlerRegistry
from src.urn_mapper import PassthroughMapper
from src.write_strategy import OverwriteStrategy

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


@pytest.fixture
def exported_dir(graph, world, tmp_path):
    world["related"][MODEL] = [CHILD]
    world["logical_parent"][CHILD] = LogicalParentClass(parent=EdgeClass(destinationUrn=MODEL))
    sf = make_schema_field_urn(CHILD, "invoice_id")
    world["logical_parent"][sf] = LogicalParentClass(parent=EdgeClass(destinationUrn=make_schema_field_urn(MODEL, "invoice_id")))
    handler = LogicalModelHandler(platforms=["logical"])
    handler.write_export(handler.export(graph), str(tmp_path))
    return tmp_path


def test_read_export_orders_for_sync(exported_dir):
    loaded = LogicalModelHandler().read_export(str(exported_dir))
    assert [e["urn"] for e in loaded] == [P, ROOT, SUB, MODEL, CHILD]
    assert loaded[-1]["entityType"] == "physicalChild"


def test_required_target_urn(exported_dir, passthrough_mapper):
    handler = LogicalModelHandler()
    loaded = by_urn(handler.read_export(str(exported_dir)))
    assert handler.required_target_urn(loaded[P], passthrough_mapper) is None
    assert handler.required_target_urn(loaded[MODEL], passthrough_mapper) is None  # SUB is in the files
    assert handler.required_target_urn(loaded[CHILD], passthrough_mapper) == CHILD


def test_model_whose_container_is_not_in_files_requires_it_on_target(tmp_path, passthrough_mapper):
    write_tree(
        [
            {"urn": P, "entityType": "dataPlatform", "aspects": {"dataPlatformInfo": {"name": "logical", "type": "OTHERS", "datasetNameDelimiter": ".", "logical": True}}},
            {"urn": MODEL, "entityType": "dataset", "aspects": {"container": {"container": SUB}}},
        ],
        str(tmp_path),
    )
    handler = LogicalModelHandler()
    loaded = by_urn(handler.read_export(str(tmp_path)))
    assert handler.required_target_urn(loaded[MODEL], passthrough_mapper) == SUB


def test_build_mcps_rebuilds_typed_aspects(exported_dir, passthrough_mapper):
    handler = LogicalModelHandler()
    loaded = by_urn(handler.read_export(str(exported_dir)))
    mcps = handler.build_mcps(loaded[MODEL], passthrough_mapper)
    assert {type(m.aspect) for m in mcps} == {DatasetPropertiesClass, SchemaMetadataClass, ContainerClass}
    assert {m.entityUrn for m in mcps} == {MODEL}
    child_mcps = handler.build_mcps(loaded[CHILD], passthrough_mapper)
    assert [m.entityUrn for m in child_mcps] == [CHILD, make_schema_field_urn(CHILD, "invoice_id")]
    assert all(isinstance(m.aspect, LogicalParentClass) for m in child_mcps)


def test_build_mcps_rejects_unknown_aspect(passthrough_mapper):
    entity = {"urn": MODEL, "entityType": "dataset", "aspects": {"globalTags": {"tags": []}}}
    with pytest.raises(ValueError, match="globalTags"):
        LogicalModelHandler().build_mcps(entity, passthrough_mapper)


def _sync(graph, metadata_dir):
    registry = HandlerRegistry()
    handler = LogicalModelHandler()
    registry.register(handler)
    orchestrator = SyncOrchestrator(registry=registry, urn_mapper=PassthroughMapper(), write_strategy=OverwriteStrategy())
    return orchestrator.sync_all(graph, {"logicalModel": handler.read_export(str(metadata_dir))})


def test_sync_emits_parents_first_and_skips_missing_children(mock_graph, exported_dir):
    mock_graph.exists.side_effect = lambda urn: urn != CHILD
    results = _sync(mock_graph, exported_dir)
    emitted = [c.args[0].entityUrn for c in mock_graph.emit_mcp.call_args_list]
    first = {u: emitted.index(u) for u in (P, ROOT, SUB, MODEL)}
    assert first[P] < first[ROOT] < first[SUB] < first[MODEL]
    assert CHILD not in emitted
    assert any(r.urn == CHILD and r.skip_reason == SKIP_TARGET_MISSING for r in results)


def test_invalid_file_fails_only_that_entity(mock_graph, exported_dir):
    (exported_dir / "logicalModels/logical/broken.PROD.json").write_text("{nope")
    results = _sync(mock_graph, exported_dir)
    failed = [r for r in results if r.status == "failed"]
    assert [r.urn for r in failed] == ["logical/broken.PROD.json"]
    assert any(r.urn == MODEL and r.status == "success" for r in results)


def test_default_registry_orders_logical_models_before_enrichment():
    registry = create_default_registry(logical_platforms=["logical"])
    registry.register(DatasetEnrichmentHandler())
    registry.register(GenericEnrichmentHandler("container"))
    order = [h.entity_type for h in registry.get_sync_order()]
    assert order.index("logicalModel") < order.index("enrichment")
    assert order.index("logicalModel") < order.index("containerEnrichment")
    assert registry.get_handler("logicalModel").platforms == ["logical"]


def test_scoped_export_of_non_logical_platform_keeps_logical_folder(graph, exported_dir):
    files_before = {p: p.read_text() for p in (exported_dir / "logicalModels").rglob("*.json")}
    handler = LogicalModelHandler(platforms=["snowflake"])
    handler.write_export(handler.export(graph), str(exported_dir))
    assert {p: p.read_text() for p in (exported_dir / "logicalModels").rglob("*.json")} == files_before


BAD = f"urn:li:dataset:({P},bad,PROD)"


@pytest.mark.parametrize(
    "shape",
    [
        {"physicalChildren": ["urn:li:dataset:x"]},
        {"physicalChildren": [{}]},
        {"aspects": {"container": {"container": {"container": ["x"]}}}},
    ],
    ids=["physical_child_string", "physical_child_without_urn", "nested_container_value"],
)
def test_malformed_file_fails_only_that_entity(mock_graph, exported_dir, shape):
    body = {"urn": BAD, "aspects": {}, **shape}
    (exported_dir / "logicalModels/logical/bad.PROD.json").write_text(json.dumps(body))
    results = _sync(mock_graph, exported_dir)
    assert [r.urn for r in results if r.status == "failed"] == ["logical/bad.PROD.json"]
    assert {P, ROOT, SUB, MODEL} <= {r.urn for r in results if r.status == "success"}


def test_container_cycle_fails_only_the_cycle(mock_graph, exported_dir, passthrough_mapper):
    loop_a, loop_b = "urn:li:container:loop_a", "urn:li:container:loop_b"
    under = f"urn:li:dataset:({P},under_loop,PROD)"
    base = exported_dir / "logicalModels/logical"
    for urn, parent, name in ((loop_a, loop_b, "a"), (loop_b, loop_a, "b")):
        (base / name).mkdir()
        body = {"urn": urn, "aspects": {"containerProperties": {"name": name}, "container": {"container": parent}}}
        (base / name / "container.json").write_text(json.dumps(body))
    (base / "under_loop.PROD.json").write_text(json.dumps({"urn": under, "aspects": {"container": {"container": loop_a}}}))

    handler = LogicalModelHandler()
    loaded = by_urn(handler.read_export(str(exported_dir)))
    assert "cycle" in loaded[loop_a]["_load_error"] and "cycle" in loaded[loop_b]["_load_error"]
    assert handler.required_target_urn(loaded[under], passthrough_mapper) == loop_a

    mock_graph.exists.side_effect = lambda urn: urn not in (loop_a, loop_b, CHILD)
    results = _sync(mock_graph, exported_dir)
    assert sorted(r.urn for r in results if r.status == "failed") == [loop_a, loop_b]
    assert any(r.urn == under and r.skip_reason == SKIP_TARGET_MISSING for r in results)
    assert {P, ROOT, SUB, MODEL} <= {r.urn for r in results if r.status == "success"}
