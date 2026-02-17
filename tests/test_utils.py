import json
import os
import tempfile

import pytest

from src.utils import (
    collect_governance_urns,
    name_from_urn,
    read_json,
    topological_sort,
    write_json,
)


class TestTopologicalSort:
    def test_empty_list(self):
        assert topological_sort([], "parentNode") == []

    def test_single_root(self):
        entities = [{"urn": "urn:a", "parentNode": None}]
        result = topological_sort(entities, "parentNode")
        assert result == entities

    def test_parent_before_child(self):
        entities = [
            {"urn": "urn:child", "parentNode": "urn:parent"},
            {"urn": "urn:parent", "parentNode": None},
        ]
        result = topological_sort(entities, "parentNode")
        urns = [e["urn"] for e in result]
        assert urns.index("urn:parent") < urns.index("urn:child")

    def test_deep_nesting(self):
        entities = [
            {"urn": "urn:c", "parentNode": "urn:b"},
            {"urn": "urn:a", "parentNode": None},
            {"urn": "urn:b", "parentNode": "urn:a"},
        ]
        result = topological_sort(entities, "parentNode")
        urns = [e["urn"] for e in result]
        assert urns == ["urn:a", "urn:b", "urn:c"]

    def test_multiple_roots(self):
        entities = [
            {"urn": "urn:r1", "parentNode": None},
            {"urn": "urn:r2", "parentNode": None},
            {"urn": "urn:c1", "parentNode": "urn:r1"},
            {"urn": "urn:c2", "parentNode": "urn:r2"},
        ]
        result = topological_sort(entities, "parentNode")
        urns = [e["urn"] for e in result]
        assert urns.index("urn:r1") < urns.index("urn:c1")
        assert urns.index("urn:r2") < urns.index("urn:c2")

    def test_cycle_raises(self):
        entities = [
            {"urn": "urn:a", "parentNode": "urn:b"},
            {"urn": "urn:b", "parentNode": "urn:a"},
        ]
        with pytest.raises(ValueError, match="Cycle detected"):
            topological_sort(entities, "parentNode")

    def test_external_parent_treated_as_root(self):
        """Parent URN not in export list -- entity treated as root."""
        entities = [
            {"urn": "urn:child", "parentNode": "urn:external_parent"},
        ]
        result = topological_sort(entities, "parentNode")
        assert len(result) == 1
        assert result[0]["urn"] == "urn:child"

    def test_different_parent_key(self):
        entities = [
            {"urn": "urn:child", "parentDomain": "urn:parent"},
            {"urn": "urn:parent", "parentDomain": None},
        ]
        result = topological_sort(entities, "parentDomain")
        urns = [e["urn"] for e in result]
        assert urns == ["urn:parent", "urn:child"]

    def test_siblings_order_stable(self):
        """Multiple children of the same parent maintain relative order."""
        entities = [
            {"urn": "urn:parent", "parentNode": None},
            {"urn": "urn:child1", "parentNode": "urn:parent"},
            {"urn": "urn:child2", "parentNode": "urn:parent"},
            {"urn": "urn:child3", "parentNode": "urn:parent"},
        ]
        result = topological_sort(entities, "parentNode")
        urns = [e["urn"] for e in result]
        assert urns[0] == "urn:parent"
        # All children come after parent
        for child in ["urn:child1", "urn:child2", "urn:child3"]:
            assert child in urns


class TestNameFromUrn:
    def test_tag_urn(self):
        assert name_from_urn("urn:li:tag:PII") == "PII"

    def test_glossary_term_urn(self):
        assert name_from_urn("urn:li:glossaryTerm:customer_id") == "customer_id"

    def test_domain_urn(self):
        assert name_from_urn("urn:li:domain:financial_securities") == "financial_securities"

    def test_glossary_node_urn(self):
        assert name_from_urn("urn:li:glossaryNode:market_analytics") == "market_analytics"

    def test_short_string(self):
        assert name_from_urn("nourn") == "nourn"


class TestCollectGovernanceUrns:
    def test_collects_from_all_governance_types(self):
        exports = {
            "tag": [{"urn": "urn:li:tag:PII"}],
            "glossaryNode": [{"urn": "urn:li:glossaryNode:customer"}],
            "glossaryTerm": [{"urn": "urn:li:glossaryTerm:customer_id"}],
            "domain": [{"urn": "urn:li:domain:marketing"}],
            "enrichment": [{"dataset_urn": "urn:li:dataset:ds1"}],
        }
        urns = collect_governance_urns(exports)
        assert urns == {
            "urn:li:tag:PII",
            "urn:li:glossaryNode:customer",
            "urn:li:glossaryTerm:customer_id",
            "urn:li:domain:marketing",
        }

    def test_excludes_enrichment(self):
        exports = {
            "enrichment": [
                {"dataset_urn": "urn:li:dataset:ds1", "urn": "should_not_be_included"},
            ],
        }
        urns = collect_governance_urns(exports)
        assert len(urns) == 0

    def test_empty_exports(self):
        assert collect_governance_urns({}) == set()


class TestJsonIO:
    def test_write_and_read(self):
        data = [{"urn": "urn:test", "name": "Test"}]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.json")
            write_json(data, path)
            result = read_json(path)
            assert result == data

    def test_read_missing_file(self):
        result = read_json("/nonexistent/file.json")
        assert result == []

    def test_write_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nested", "dir", "test.json")
            write_json([{"key": "value"}], path)
            assert os.path.exists(path)

    def test_write_empty_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.json")
            write_json([], path)
            with open(path) as f:
                assert json.load(f) == []
