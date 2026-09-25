import json
import logging
from pathlib import Path

import pytest

from src.logical_layout import plan_layout, read_tree, write_tree

P = "urn:li:dataPlatform:logical"
ROOT = "urn:li:container:root"
SUB = "urn:li:container:sub"


def platform() -> dict:
    return {
        "urn": P,
        "entityType": "dataPlatform",
        "aspects": {"dataPlatformInfo": {"name": "logical", "type": "OTHERS", "datasetNameDelimiter": ".", "logical": True}},
    }


def container(urn: str, name: str, parent: str | None = None) -> dict:
    aspects: dict = {"containerProperties": {"name": name}}
    if parent:
        aspects["container"] = {"container": parent}
    return {"urn": urn, "entityType": "container", "aspects": aspects}


def dataset(name: str, parent: str | None = None, env: str = "PROD") -> dict:
    aspects: dict = {"datasetProperties": {"name": name}}
    if parent:
        aspects["container"] = {"container": parent}
    return {"urn": f"urn:li:dataset:({P},{name},{env})", "entityType": "dataset", "aspects": aspects}


def tree() -> list[dict]:
    return [
        platform(),
        container(ROOT, "Customer Domain"),
        container(SUB, "billing", ROOT),
        dataset("invoice", SUB),
        dataset("orphan"),
    ]


def test_plan_layout_mirrors_hierarchy():
    paths = plan_layout(tree())
    assert paths[P] == Path("logical/platform.json")
    assert paths[ROOT] == Path("logical/Customer_Domain/container.json")
    assert paths[SUB] == Path("logical/Customer_Domain/billing/container.json")
    assert paths[f"urn:li:dataset:({P},invoice,PROD)"] == Path("logical/Customer_Domain/billing/invoice.PROD.json")
    assert paths[f"urn:li:dataset:({P},orphan,PROD)"] == Path("logical/orphan.PROD.json")


def test_same_name_different_env_gets_distinct_files():
    paths = plan_layout([platform(), dataset("invoice"), dataset("invoice", env="DEV")])
    assert paths[f"urn:li:dataset:({P},invoice,PROD)"] != paths[f"urn:li:dataset:({P},invoice,DEV)"]


def test_sibling_container_name_collision_raises():
    entities = [
        platform(),
        container(ROOT, "root"),
        container("urn:li:container:b1", "Billing", ROOT),
        container("urn:li:container:b2", "Billing", ROOT),
        dataset("x", "urn:li:container:b1"),
        dataset("y", "urn:li:container:b2"),
    ]
    with pytest.raises(ValueError, match="urn:li:container:b1") as exc:
        plan_layout(entities)
    assert "urn:li:container:b2" in str(exc.value)


def test_sanitized_dataset_name_collision_raises():
    with pytest.raises(ValueError, match="collision"):
        plan_layout([platform(), dataset("a/b"), dataset("a_b")])


def test_sibling_container_name_case_only_collision_raises():
    # Exports run on macOS, whose default filesystem is case-insensitive: "Billing"
    # and "billing" would land on the same path on disk even though they differ
    # in a case-sensitive string comparison.
    entities = [
        platform(),
        container(ROOT, "root"),
        container("urn:li:container:b1", "Billing", ROOT),
        container("urn:li:container:b2", "billing", ROOT),
        dataset("x", "urn:li:container:b1"),
        dataset("y", "urn:li:container:b2"),
    ]
    with pytest.raises(ValueError, match="urn:li:container:b1") as exc:
        plan_layout(entities)
    assert "urn:li:container:b2" in str(exc.value)


def test_write_then_read_round_trips(tmp_path):
    write_tree(tree(), str(tmp_path))
    loaded = {e["urn"]: e for e in read_tree(str(tmp_path))}
    for e in tree():
        assert loaded[e["urn"]]["aspects"] == e["aspects"]
        assert loaded[e["urn"]]["entityType"] == e["entityType"]
    body = json.loads((tmp_path / "logicalModels/logical/platform.json").read_text())
    assert set(body) == {"urn", "aspects"}


def test_write_tree_removes_stale_files(tmp_path):
    write_tree(tree(), str(tmp_path))
    renamed = tree()
    renamed[1] = container(ROOT, "Renamed")
    write_tree(renamed, str(tmp_path))
    assert not (tmp_path / "logicalModels/logical/Customer_Domain").exists()
    assert (tmp_path / "logicalModels/logical/Renamed/billing/invoice.PROD.json").exists()


def test_read_tree_warns_when_folder_disagrees_with_container_aspect(tmp_path, caplog):
    write_tree(tree(), str(tmp_path))
    root = tmp_path / "logicalModels/logical"
    (root / "Customer_Domain/billing/invoice.PROD.json").rename(root / "invoice.PROD.json")
    with caplog.at_level(logging.WARNING):
        loaded = {e["urn"]: e for e in read_tree(str(tmp_path))}
    assert "container aspect wins" in caplog.text
    assert loaded[f"urn:li:dataset:({P},invoice,PROD)"]["aspects"]["container"] == {"container": SUB}


def test_read_tree_marks_invalid_files(tmp_path):
    write_tree(tree(), str(tmp_path))
    (tmp_path / "logicalModels/logical/broken.PROD.json").write_text("{not json")
    (tmp_path / "logicalModels/logical/list.PROD.json").write_text("[]")
    entities = read_tree(str(tmp_path))
    invalid = [e for e in entities if "_load_error" in e]
    assert {e["urn"] for e in invalid} == {"logical/broken.PROD.json", "logical/list.PROD.json"}
    assert len(entities) - len(invalid) == len(tree())


def test_read_tree_isolates_file_with_malformed_aspects(tmp_path):
    write_tree(tree(), str(tmp_path))
    bad = {"urn": f"urn:li:dataset:({P},bad,PROD)", "aspects": "oops"}
    (tmp_path / "logicalModels/logical/bad.PROD.json").write_text(json.dumps(bad))
    entities = read_tree(str(tmp_path))
    invalid = [e for e in entities if "_load_error" in e]
    assert {e["urn"] for e in invalid} == {"logical/bad.PROD.json"}
    assert len(entities) - len(invalid) == len(tree())


def test_read_tree_without_directory_returns_empty(tmp_path):
    assert read_tree(str(tmp_path)) == []


LP_B = "urn:li:dataPlatform:lp_b"


def lp_b_tree() -> list[dict]:
    info = {"name": "lp_b", "type": "OTHERS", "datasetNameDelimiter": ".", "logical": True}
    return [
        {"urn": LP_B, "entityType": "dataPlatform", "aspects": {"dataPlatformInfo": info}},
        {"urn": f"urn:li:dataset:({LP_B},other,PROD)", "entityType": "dataset", "aspects": {"datasetProperties": {"name": "other"}}},
    ]


def test_scoped_write_tree_leaves_other_platforms_untouched(tmp_path):
    write_tree(tree() + lp_b_tree(), str(tmp_path))
    lp_b_file = tmp_path / "logicalModels/lp_b/other.PROD.json"
    before = lp_b_file.read_text()
    renamed = tree()
    renamed[1] = container(ROOT, "Renamed")
    write_tree(renamed, str(tmp_path), platforms=[P])
    assert lp_b_file.read_text() == before
    assert (tmp_path / "logicalModels/lp_b/platform.json").exists()
    assert not (tmp_path / "logicalModels/logical/Customer_Domain").exists()
    assert (tmp_path / "logicalModels/logical/Renamed/billing/invoice.PROD.json").exists()


def test_scoped_write_tree_for_non_logical_platform_deletes_nothing(tmp_path):
    write_tree(tree() + lp_b_tree(), str(tmp_path))
    files_before = sorted(p for p in (tmp_path / "logicalModels").rglob("*.json"))
    write_tree([], str(tmp_path), platforms=["urn:li:dataPlatform:snowflake"])
    assert sorted(p for p in (tmp_path / "logicalModels").rglob("*.json")) == files_before


def test_scoped_write_tree_removes_in_scope_platform_with_no_entities(tmp_path):
    write_tree(tree() + lp_b_tree(), str(tmp_path))
    write_tree([], str(tmp_path), platforms=[LP_B])
    assert not (tmp_path / "logicalModels/lp_b").exists()
    assert (tmp_path / "logicalModels/logical/platform.json").exists()


MALFORMED = {
    "physical_child_string": {"physicalChildren": ["urn:li:dataset:x"]},
    "physical_child_without_urn": {"physicalChildren": [{}]},
    "physical_child_fields_without_urn": {"physicalChildren": [{"urn": "urn:li:dataset:x", "fields": [{}]}]},
    "nested_container_value": {"aspects": {"container": {"container": {"container": ["x"]}}}},
}


@pytest.mark.parametrize("shape", MALFORMED.values(), ids=MALFORMED.keys())
def test_read_tree_isolates_malformed_shapes(tmp_path, shape):
    write_tree(tree(), str(tmp_path))
    bad = {"urn": f"urn:li:dataset:({P},bad,PROD)", "aspects": {}, **shape}
    (tmp_path / "logicalModels/logical/bad.PROD.json").write_text(json.dumps(bad))
    entities = read_tree(str(tmp_path))
    invalid = [e for e in entities if "_load_error" in e]
    assert {e["urn"] for e in invalid} == {"logical/bad.PROD.json"}
    assert len(entities) - len(invalid) == len(tree())
