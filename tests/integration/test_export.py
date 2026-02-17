"""Integration tests for the export pipeline.

These tests validate that the export CLI correctly captures all entity types,
hierarchy ordering, system entity filtering, and enrichment from a live
DataHub instance seeded with known entities.

Run: pytest -m integration tests/integration/
"""

import json
import os

import pytest

from tests.integration import seed

pytestmark = pytest.mark.integration


def _load(export_dir: str, filename: str) -> list[dict]:
    path = os.path.join(export_dir, filename)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


# ── Governance entity exports ──────────────────────────────────────────────


class TestTagExport:
    def test_custom_tags_exported(self, export_dir):
        tags = _load(export_dir, "tag.json")
        urns = {t["urn"] for t in tags}
        assert seed.TAG_PII in urns
        assert seed.TAG_FINANCIAL in urns

    def test_system_tags_filtered(self, export_dir):
        tags = _load(export_dir, "tag.json")
        urns = {t["urn"] for t in tags}
        assert seed.TAG_SYSTEM not in urns

    def test_tag_properties(self, export_dir):
        tags = _load(export_dir, "tag.json")
        pii = next(t for t in tags if t["urn"] == seed.TAG_PII)
        assert pii["name"] == "Integration PII"
        assert pii["description"] == "PII tag for integration test"
        assert pii["colorHex"] == "#FF0000"


class TestGlossaryNodeExport:
    def test_nodes_exported(self, export_dir):
        nodes = _load(export_dir, "glossaryNode.json")
        urns = [n["urn"] for n in nodes]
        assert seed.NODE_ROOT in urns
        assert seed.NODE_CHILD in urns

    def test_parent_before_child(self, export_dir):
        """Topological sort: root node must appear before child node."""
        nodes = _load(export_dir, "glossaryNode.json")
        urns = [n["urn"] for n in nodes]
        root_idx = urns.index(seed.NODE_ROOT)
        child_idx = urns.index(seed.NODE_CHILD)
        assert root_idx < child_idx

    def test_hierarchy_preserved(self, export_dir):
        nodes = _load(export_dir, "glossaryNode.json")
        child = next(n for n in nodes if n["urn"] == seed.NODE_CHILD)
        assert child["parentNode"] == seed.NODE_ROOT

        root = next(n for n in nodes if n["urn"] == seed.NODE_ROOT)
        assert root["parentNode"] is None


class TestGlossaryTermExport:
    def test_terms_exported(self, export_dir):
        terms = _load(export_dir, "glossaryTerm.json")
        urns = {t["urn"] for t in terms}
        assert seed.TERM_A in urns
        assert seed.TERM_B in urns

    def test_term_parent_references(self, export_dir):
        terms = _load(export_dir, "glossaryTerm.json")
        term_a = next(t for t in terms if t["urn"] == seed.TERM_A)
        assert term_a["parentNode"] == seed.NODE_ROOT

        term_b = next(t for t in terms if t["urn"] == seed.TERM_B)
        assert term_b["parentNode"] == seed.NODE_CHILD

    def test_term_properties(self, export_dir):
        terms = _load(export_dir, "glossaryTerm.json")
        term_a = next(t for t in terms if t["urn"] == seed.TERM_A)
        assert term_a["name"] == "Integration Term A"
        assert term_a["definition"] == "Term A: first integration term"


class TestDomainExport:
    def test_domains_exported(self, export_dir):
        domains = _load(export_dir, "domain.json")
        urns = [d["urn"] for d in domains]
        assert seed.DOMAIN_ROOT in urns
        assert seed.DOMAIN_CHILD in urns

    def test_parent_before_child(self, export_dir):
        domains = _load(export_dir, "domain.json")
        urns = [d["urn"] for d in domains]
        root_idx = urns.index(seed.DOMAIN_ROOT)
        child_idx = urns.index(seed.DOMAIN_CHILD)
        assert root_idx < child_idx

    def test_hierarchy_preserved(self, export_dir):
        domains = _load(export_dir, "domain.json")
        child = next(d for d in domains if d["urn"] == seed.DOMAIN_CHILD)
        assert child["parentDomain"] == seed.DOMAIN_ROOT


class TestDataProductExport:
    def test_products_exported(self, export_dir):
        products = _load(export_dir, "dataProduct.json")
        urns = {p["urn"] for p in products}
        assert seed.DATA_PRODUCT in urns

    def test_product_properties(self, export_dir):
        products = _load(export_dir, "dataProduct.json")
        product = next(p for p in products if p["urn"] == seed.DATA_PRODUCT)
        assert product["name"] == "Integration Product"
        assert len(product["assets"]) == 1
        assert product["assets"][0]["destinationUrn"] == seed.DATASET_1


# ── Dataset enrichment ─────────────────────────────────────────────────────


class TestDatasetEnrichment:
    def _get_dataset(self, export_dir, urn):
        enrichment = _load(export_dir, "enrichment.json")
        matches = [e for e in enrichment if e["dataset_urn"] == urn]
        return matches[0] if matches else None

    def test_dataset1_tags(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        assert ds is not None
        tag_urns = {t["tag"] for t in ds["globalTags"]}
        assert seed.TAG_PII in tag_urns
        assert seed.TAG_FINANCIAL in tag_urns

    def test_dataset1_glossary_terms(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        term_urns = {t["urn"] for t in ds["glossaryTerms"]}
        assert seed.TERM_A in term_urns

    def test_dataset1_domains(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        assert seed.DOMAIN_ROOT in ds["domains"]

    def test_dataset1_ownership(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        owners = {o["owner"] for o in ds["ownership"]}
        assert seed.USER_ALICE in owners
        assert seed.USER_BOB in owners
        # Check ownership types
        owner_types = {o["owner"]: o["type"] for o in ds["ownership"]}
        assert owner_types[seed.USER_ALICE] == "TECHNICAL_OWNER"
        assert owner_types[seed.USER_BOB] == "BUSINESS_OWNER"

    def test_dataset1_field_level_tags(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        assert "editableSchemaMetadata" in ds
        fields = {f["fieldPath"]: f for f in ds["editableSchemaMetadata"]}
        assert "email" in fields
        email_tags = {t["tag"] for t in fields["email"]["globalTags"]}
        assert seed.TAG_PII in email_tags

    def test_dataset1_field_level_terms(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_1)
        fields = {f["fieldPath"]: f for f in ds["editableSchemaMetadata"]}
        assert "name" in fields
        name_terms = {t["urn"] for t in fields["name"]["glossaryTerms"]}
        assert seed.TERM_A in name_terms

    def test_dataset2_partial_enrichment(self, export_dir):
        ds = self._get_dataset(export_dir, seed.DATASET_2)
        assert ds is not None
        assert seed.DOMAIN_CHILD in ds["domains"]
        assert "ownership" in ds
        assert "globalTags" not in ds  # No tags on dataset 2

    def test_unenriched_dataset_may_have_ownership_only(self, export_dir):
        """Dataset with no governance enrichment might still appear if it has ownership."""
        ds = self._get_dataset(export_dir, seed.DATASET_NO_ENRICHMENT)
        # No tags, terms, or domains -- may or may not appear
        if ds is not None:
            assert "globalTags" not in ds
            assert "glossaryTerms" not in ds


# ── Chart enrichment ───────────────────────────────────────────────────────


class TestChartEnrichment:
    def test_chart_exported(self, export_dir):
        charts = _load(export_dir, "chartEnrichment.json")
        urns = {c["entity_urn"] for c in charts}
        assert seed.CHART_1 in urns

    def test_chart_ownership(self, export_dir):
        charts = _load(export_dir, "chartEnrichment.json")
        chart = next(c for c in charts if c["entity_urn"] == seed.CHART_1)
        owners = {o["owner"] for o in chart["ownership"]}
        assert seed.USER_BOB in owners

    def test_chart_domain(self, export_dir):
        charts = _load(export_dir, "chartEnrichment.json")
        chart = next(c for c in charts if c["entity_urn"] == seed.CHART_1)
        assert seed.DOMAIN_ROOT in chart["domains"]


class TestDashboardEnrichment:
    def test_dashboard_exported(self, export_dir):
        dashboards = _load(export_dir, "dashboardEnrichment.json")
        urns = {d["entity_urn"] for d in dashboards}
        assert seed.DASHBOARD_1 in urns

    def test_dashboard_ownership(self, export_dir):
        dashboards = _load(export_dir, "dashboardEnrichment.json")
        dash = next(
            d for d in dashboards if d["entity_urn"] == seed.DASHBOARD_1
        )
        owners = {o["owner"] for o in dash["ownership"]}
        assert seed.USER_ALICE in owners


class TestDataFlowEnrichment:
    def test_dataflow_exported(self, export_dir):
        flows = _load(export_dir, "dataFlowEnrichment.json")
        urns = {f["entity_urn"] for f in flows}
        assert seed.DATAFLOW_1 in urns

    def test_dataflow_ownership(self, export_dir):
        flows = _load(export_dir, "dataFlowEnrichment.json")
        flow = next(f for f in flows if f["entity_urn"] == seed.DATAFLOW_1)
        owners = {o["owner"] for o in flow["ownership"]}
        assert seed.USER_ALICE in owners


class TestContainerEnrichment:
    def test_container_exported(self, export_dir):
        containers = _load(export_dir, "containerEnrichment.json")
        urns = {c["entity_urn"] for c in containers}
        assert seed.CONTAINER_1 in urns

    def test_container_domain(self, export_dir):
        containers = _load(export_dir, "containerEnrichment.json")
        container = next(
            c for c in containers if c["entity_urn"] == seed.CONTAINER_1
        )
        assert seed.DOMAIN_ROOT in container["domains"]

    def test_container_ownership(self, export_dir):
        containers = _load(export_dir, "containerEnrichment.json")
        container = next(
            c for c in containers if c["entity_urn"] == seed.CONTAINER_1
        )
        owners = {o["owner"] for o in container["ownership"]}
        assert seed.USER_ALICE in owners


class TestDataProductEnrichment:
    def test_product_enrichment_exported(self, export_dir):
        products = _load(export_dir, "dataProductEnrichment.json")
        urns = {p["entity_urn"] for p in products}
        assert seed.DATA_PRODUCT in urns

    def test_product_domain(self, export_dir):
        products = _load(export_dir, "dataProductEnrichment.json")
        product = next(
            p for p in products if p["entity_urn"] == seed.DATA_PRODUCT
        )
        assert seed.DOMAIN_CHILD in product["domains"]

    def test_product_ownership(self, export_dir):
        products = _load(export_dir, "dataProductEnrichment.json")
        product = next(
            p for p in products if p["entity_urn"] == seed.DATA_PRODUCT
        )
        owners = {o["owner"] for o in product["ownership"]}
        assert seed.USER_BOB in owners


# ── Cross-cutting export validations ──────────────────────────────────────


class TestExportCompleteness:
    def test_all_governance_files_exist(self, export_dir):
        for filename in [
            "tag.json",
            "glossaryNode.json",
            "glossaryTerm.json",
            "domain.json",
            "dataProduct.json",
        ]:
            path = os.path.join(export_dir, filename)
            assert os.path.exists(path), f"Missing governance file: {filename}"

    def test_all_enrichment_files_exist(self, export_dir):
        for filename in [
            "enrichment.json",
            "chartEnrichment.json",
            "dashboardEnrichment.json",
            "containerEnrichment.json",
            "dataFlowEnrichment.json",
            "dataProductEnrichment.json",
        ]:
            path = os.path.join(export_dir, filename)
            assert os.path.exists(path), f"Missing enrichment file: {filename}"

    def test_governance_urns_are_deterministic(self, export_dir):
        """All governance entity URNs should match what we seeded."""
        tags = _load(export_dir, "tag.json")
        # Our seeded tags should be present (may also contain pre-existing tags)
        tag_urns = {t["urn"] for t in tags}
        assert seed.TAG_PII in tag_urns
        assert seed.TAG_FINANCIAL in tag_urns

    def test_enrichment_only_references_exported_governance(self, export_dir):
        """Tag/term/domain refs in enrichment should only be governance URNs."""
        # Collect all exported governance URNs
        gov_urns = set()
        for fname in ["tag.json", "glossaryNode.json", "glossaryTerm.json",
                       "domain.json", "dataProduct.json"]:
            for e in _load(export_dir, fname):
                gov_urns.add(e["urn"])

        # Check dataset enrichment
        for ds in _load(export_dir, "enrichment.json"):
            for t in ds.get("globalTags", []):
                assert t["tag"] in gov_urns, (
                    f"Tag {t['tag']} on {ds['dataset_urn']} not in governance"
                )
            for t in ds.get("glossaryTerms", []):
                assert t["urn"] in gov_urns, (
                    f"Term {t['urn']} on {ds['dataset_urn']} not in governance"
                )
            for d in ds.get("domains", []):
                assert d in gov_urns, (
                    f"Domain {d} on {ds['dataset_urn']} not in governance"
                )
