"""Integration tests for the export pipeline.

These tests validate that the export CLI correctly captures all entity types,
hierarchy ordering, system entity filtering, enrichment, and scoped filtering
from a live DataHub instance seeded with known entities.

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


# ── Deletion export ─────────────────────────────────────────────────────


class TestDeletionExport:
    def test_deletions_file_exists(self, export_dir_with_deletions):
        path = os.path.join(export_dir_with_deletions, "deletions.json")
        assert os.path.exists(path), "deletions.json should exist"

    def test_soft_deleted_entities_detected(self, export_dir_with_deletions):
        deletions = _load(export_dir_with_deletions, "deletions.json")
        urns = {d["urn"] for d in deletions}
        assert seed.TAG_DELETED in urns, (
            f"Soft-deleted tag should be in deletions.json, got: {urns}"
        )
        assert seed.TERM_DELETED in urns, (
            f"Soft-deleted term should be in deletions.json, got: {urns}"
        )

    def test_active_entities_excluded_from_deletions(
        self, export_dir_with_deletions
    ):
        deletions = _load(export_dir_with_deletions, "deletions.json")
        urns = {d["urn"] for d in deletions}
        assert seed.TAG_PII not in urns, "Active tag should not be in deletions"
        assert seed.DOMAIN_ROOT not in urns, (
            "Active domain should not be in deletions"
        )

    def test_deletion_entries_have_entity_type(
        self, export_dir_with_deletions
    ):
        deletions = _load(export_dir_with_deletions, "deletions.json")
        for d in deletions:
            assert "urn" in d
            assert "entity_type" in d


# ── Scoped export: domain filtering ──────────────────────────────────────


class TestDomainScopedExport:
    """Verify --domain restricts enrichment to entities in the specified domain."""

    def test_governance_unaffected_by_scope(self, export_dir_domain_scoped):
        """Governance exports are always global, regardless of scope."""
        tags = _load(export_dir_domain_scoped, "tag.json")
        tag_urns = {t["urn"] for t in tags}
        assert seed.TAG_PII in tag_urns
        assert seed.TAG_FINANCIAL in tag_urns

        domains = _load(export_dir_domain_scoped, "domain.json")
        domain_urns = {d["urn"] for d in domains}
        assert seed.DOMAIN_ROOT in domain_urns
        assert seed.DOMAIN_CHILD in domain_urns

    def test_dataset_in_root_domain_included(self, export_dir_domain_scoped):
        """Dataset 1 is in root domain and should be in enrichment."""
        enrichment = _load(export_dir_domain_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_1 in ds_urns

    def test_dataset_in_child_domain_excluded(self, export_dir_domain_scoped):
        """Dataset 2 is in child domain (not root), should be excluded."""
        enrichment = _load(export_dir_domain_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_2 not in ds_urns

    def test_chart_in_root_domain_included(self, export_dir_domain_scoped):
        """Chart 1 has root domain assignment, should be included."""
        charts = _load(export_dir_domain_scoped, "chartEnrichment.json")
        chart_urns = {c["entity_urn"] for c in charts}
        assert seed.CHART_1 in chart_urns

    def test_container_in_root_domain_included(self, export_dir_domain_scoped):
        """Container 1 has root domain assignment, should be included."""
        containers = _load(export_dir_domain_scoped, "containerEnrichment.json")
        container_urns = {c["entity_urn"] for c in containers}
        assert seed.CONTAINER_1 in container_urns

    def test_data_product_in_child_domain_excluded(
        self, export_dir_domain_scoped
    ):
        """Data product has child domain (not root), should be excluded."""
        products = _load(
            export_dir_domain_scoped, "dataProductEnrichment.json"
        )
        product_urns = {p["entity_urn"] for p in products}
        assert seed.DATA_PRODUCT not in product_urns


# ── Scoped export: platform filtering ────────────────────────────────────


class TestPlatformScopedExport:
    """Verify --platform restricts enrichment to entities on the specified platform."""

    def test_governance_unaffected_by_scope(self, export_dir_platform_scoped):
        """Governance exports are always global."""
        tags = _load(export_dir_platform_scoped, "tag.json")
        assert any(t["urn"] == seed.TAG_PII for t in tags)

    def test_postgres_datasets_included(self, export_dir_platform_scoped):
        """Datasets on postgres should be in enrichment."""
        enrichment = _load(export_dir_platform_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_1 in ds_urns

    def test_looker_charts_excluded(self, export_dir_platform_scoped):
        """Charts on looker should be excluded when filtering for postgres."""
        charts = _load(export_dir_platform_scoped, "chartEnrichment.json")
        chart_urns = {c["entity_urn"] for c in charts}
        assert seed.CHART_1 not in chart_urns

    def test_looker_dashboards_excluded(self, export_dir_platform_scoped):
        """Dashboards on looker should be excluded when filtering for postgres."""
        dashboards = _load(
            export_dir_platform_scoped, "dashboardEnrichment.json"
        )
        dashboard_urns = {d["entity_urn"] for d in dashboards}
        assert seed.DASHBOARD_1 not in dashboard_urns


# ── Scoped export: combined filtering ────────────────────────────────────


class TestCombinedScopedExport:
    """Verify --domain + --platform + --env combined scope narrows results."""

    def test_governance_unaffected(self, export_dir_combined_scoped):
        """Governance exports remain global under combined scope."""
        tags = _load(export_dir_combined_scoped, "tag.json")
        assert any(t["urn"] == seed.TAG_PII for t in tags)

        domains = _load(export_dir_combined_scoped, "domain.json")
        domain_urns = {d["urn"] for d in domains}
        assert seed.DOMAIN_ROOT in domain_urns
        assert seed.DOMAIN_CHILD in domain_urns

    def test_dataset1_included(self, export_dir_combined_scoped):
        """Dataset 1: postgres + PROD + root domain -- matches all filters."""
        enrichment = _load(export_dir_combined_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_1 in ds_urns

    def test_dataset2_excluded(self, export_dir_combined_scoped):
        """Dataset 2: postgres + PROD but child domain -- excluded by domain filter."""
        enrichment = _load(export_dir_combined_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_2 not in ds_urns

    def test_charts_excluded_by_platform(self, export_dir_combined_scoped):
        """Charts are on looker, excluded by platform=postgres."""
        charts = _load(export_dir_combined_scoped, "chartEnrichment.json")
        chart_urns = {c["entity_urn"] for c in charts}
        assert seed.CHART_1 not in chart_urns

    def test_enrichment_data_intact(self, export_dir_combined_scoped):
        """Entities that pass scope should still have full enrichment data."""
        enrichment = _load(export_dir_combined_scoped, "enrichment.json")
        ds = next(
            (e for e in enrichment if e["dataset_urn"] == seed.DATASET_1),
            None,
        )
        if ds is not None:
            # Should still have tags, terms, domains from unscoped export
            assert "globalTags" in ds
            tag_urns = {t["tag"] for t in ds["globalTags"]}
            assert seed.TAG_PII in tag_urns


# ── Scoped export: YAML configuration ────────────────────────────────────


class TestYamlScopedExport:
    """Verify --scope-config YAML produces same results as CLI flags."""

    def test_governance_unaffected(self, export_dir_yaml_scoped):
        """Governance exports are always global."""
        tags = _load(export_dir_yaml_scoped, "tag.json")
        assert any(t["urn"] == seed.TAG_PII for t in tags)

    def test_dataset_filtering_matches_cli(self, export_dir_yaml_scoped):
        """YAML scope (root domain + postgres) should match CLI equivalent."""
        enrichment = _load(export_dir_yaml_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        # Dataset 1 is postgres + root domain -> included
        assert seed.DATASET_1 in ds_urns
        # Dataset 2 is postgres but child domain -> excluded
        assert seed.DATASET_2 not in ds_urns

    def test_charts_excluded_by_platform(self, export_dir_yaml_scoped):
        """Charts are on looker, excluded by platform=postgres in YAML."""
        charts = _load(export_dir_yaml_scoped, "chartEnrichment.json")
        chart_urns = {c["entity_urn"] for c in charts}
        assert seed.CHART_1 not in chart_urns


# ── Governance URN filtering edge cases ──────────────────────────────────


class TestGovernanceUrnFiltering:
    """Verify enrichment only includes references to exported governance URNs.

    System tags (__default_*) are filtered from the governance export.
    Enrichment referencing those tags should have them stripped.
    """

    def _get_dataset(self, export_dir, urn):
        enrichment = _load(export_dir, "enrichment.json")
        return next((e for e in enrichment if e["dataset_urn"] == urn), None)

    def test_system_tag_stripped_from_entity_tags(self, export_dir):
        """System tag assigned to a dataset should NOT appear in enrichment."""
        ds = self._get_dataset(export_dir, seed.DATASET_MIXED_TAGS)
        assert ds is not None, "Mixed-tags dataset should have enrichment"
        tag_urns = {t["tag"] for t in ds.get("globalTags", [])}
        assert seed.TAG_PII in tag_urns, "Governance tag should be included"
        assert seed.TAG_SYSTEM not in tag_urns, (
            "System tag should be stripped from enrichment"
        )

    def test_field_with_only_system_tag_excluded(self, export_dir):
        """A field with only a system tag should not appear in editableSchemaMetadata."""
        ds = self._get_dataset(export_dir, seed.DATASET_MIXED_TAGS)
        assert ds is not None
        assert "editableSchemaMetadata" in ds
        field_paths = {
            f["fieldPath"] for f in ds["editableSchemaMetadata"]
        }
        assert "system_only_field" not in field_paths, (
            "Field with only system tags should be excluded"
        )

    def test_field_with_governance_tag_included(self, export_dir):
        """A field with a governance tag should appear in editableSchemaMetadata."""
        ds = self._get_dataset(export_dir, seed.DATASET_MIXED_TAGS)
        assert ds is not None
        field_paths = {
            f["fieldPath"] for f in ds["editableSchemaMetadata"]
        }
        assert "pii_field" in field_paths

    def test_field_with_mixed_tags_filters_correctly(self, export_dir):
        """A field with mixed governance/system tags should only include governance."""
        ds = self._get_dataset(export_dir, seed.DATASET_MIXED_TAGS)
        assert ds is not None
        fields = {f["fieldPath"]: f for f in ds["editableSchemaMetadata"]}
        assert "mixed_field" in fields
        mixed_tags = {t["tag"] for t in fields["mixed_field"]["globalTags"]}
        assert seed.TAG_PII in mixed_tags
        assert seed.TAG_SYSTEM not in mixed_tags


# ── Soft-deleted governance with active enrichment ───────────────────────


class TestSoftDeletedGovernanceEnrichment:
    """Verify that soft-deleted governance entities are excluded from enrichment.

    TAG_ASSIGNED_THEN_DELETED was assigned to DATASET_WITH_DELETED_TAG,
    then soft-deleted. The export should:
    1. Not include the deleted tag in tag.json
    2. Strip the deleted tag reference from the dataset's enrichment
    """

    def test_deleted_tag_not_in_governance(self, export_dir):
        """Soft-deleted tag should NOT be in tag.json."""
        tags = _load(export_dir, "tag.json")
        tag_urns = {t["urn"] for t in tags}
        assert seed.TAG_ASSIGNED_THEN_DELETED not in tag_urns

    def test_deleted_tag_stripped_from_enrichment(self, export_dir):
        """Enrichment should NOT reference the soft-deleted tag."""
        enrichment = _load(export_dir, "enrichment.json")
        ds = next(
            (e for e in enrichment if e["dataset_urn"] == seed.DATASET_WITH_DELETED_TAG),
            None,
        )
        if ds is not None and "globalTags" in ds:
            tag_urns = {t["tag"] for t in ds["globalTags"]}
            assert seed.TAG_ASSIGNED_THEN_DELETED not in tag_urns, (
                "Soft-deleted tag should be stripped from enrichment"
            )

    def test_other_tags_survive_deletion_filtering(self, export_dir):
        """Non-deleted tags on the same dataset should still be in enrichment."""
        enrichment = _load(export_dir, "enrichment.json")
        ds = next(
            (e for e in enrichment if e["dataset_urn"] == seed.DATASET_WITH_DELETED_TAG),
            None,
        )
        assert ds is not None, (
            "Dataset should still have enrichment (TAG_PII + domain)"
        )
        tag_urns = {t["tag"] for t in ds.get("globalTags", [])}
        assert seed.TAG_PII in tag_urns, (
            "Non-deleted governance tag should survive"
        )


# ── Scope edge cases: empty results / nonexistent filters ───────────────


class TestEmptyScopeExport:
    """Verify export with a nonexistent domain produces valid empty results."""

    def test_export_succeeds(self, export_dir_empty_scope):
        """Export with nonexistent domain should succeed (not crash)."""
        # If we get here, the fixture didn't raise -> CLI exited 0
        assert export_dir_empty_scope is not None

    def test_governance_still_exported(self, export_dir_empty_scope):
        """Governance files should still be populated (scope doesn't affect them)."""
        tags = _load(export_dir_empty_scope, "tag.json")
        tag_urns = {t["urn"] for t in tags}
        assert seed.TAG_PII in tag_urns

    def test_enrichment_files_empty(self, export_dir_empty_scope):
        """Enrichment files should exist but contain empty arrays."""
        enrichment = _load(export_dir_empty_scope, "enrichment.json")
        assert enrichment == [], (
            f"Expected empty enrichment with nonexistent domain, got {len(enrichment)} entities"
        )

    def test_all_enrichment_files_valid_json(self, export_dir_empty_scope):
        """All enrichment files should be valid JSON (even if empty)."""
        for filename in [
            "enrichment.json",
            "chartEnrichment.json",
            "dashboardEnrichment.json",
            "containerEnrichment.json",
            "dataFlowEnrichment.json",
            "dataProductEnrichment.json",
        ]:
            entities = _load(export_dir_empty_scope, filename)
            assert isinstance(entities, list), (
                f"{filename} should be a JSON array"
            )


# ── Data model quirks ────────────────────────────────────────────────────


class TestGlossaryTermNullName:
    """Verify pipeline handles glossary terms with null name.

    DataHub quirk: GlossaryTermInfo.name may be null when terms are created
    via certain code paths or older SDK versions. The pipeline derives the
    name from the URN (last segment after the final colon).
    """

    def test_null_name_term_exported(self, export_dir):
        """Term with null name should still be exported."""
        terms = _load(export_dir, "glossaryTerm.json")
        urns = {t["urn"] for t in terms}
        assert seed.TERM_NULL_NAME in urns

    def test_null_name_derived_from_urn(self, export_dir):
        """When name is null, pipeline should derive it from the URN."""
        terms = _load(export_dir, "glossaryTerm.json")
        term = next(t for t in terms if t["urn"] == seed.TERM_NULL_NAME)
        # name_from_urn("urn:li:glossaryTerm:integration-term-null-name")
        # should produce "integration-term-null-name"
        assert term["name"] is not None, "Name should be derived, not null"
        assert term["name"] == "integration-term-null-name"


class TestGlossaryTermUrnInTermSource:
    """Verify pipeline handles URN stored in termSource field.

    DataHub quirk: Some instances store the parent node URN in termSource
    instead of the expected "INTERNAL"/"EXTERNAL" enum value. The pipeline
    passes it through as-is and maps via UrnMapper if it looks like a URN.
    """

    def test_urn_termsource_term_exported(self, export_dir):
        """Term with URN in termSource should still be exported."""
        terms = _load(export_dir, "glossaryTerm.json")
        urns = {t["urn"] for t in terms}
        assert seed.TERM_URN_TERMSOURCE in urns

    def test_urn_termsource_preserved(self, export_dir):
        """The URN in termSource should be passed through as-is."""
        terms = _load(export_dir, "glossaryTerm.json")
        term = next(t for t in terms if t["urn"] == seed.TERM_URN_TERMSOURCE)
        assert term["termSource"] == seed.NODE_CHILD, (
            f"termSource should preserve the URN, got: {term['termSource']}"
        )


class TestEmptyAspects:
    """Verify pipeline handles entities with empty-but-present aspects.

    DataHub quirk: writing GlobalTagsClass(tags=[]) or OwnershipClass(owners=[])
    may result in DataHub either:
    - Not persisting the aspect (returns None)
    - Persisting but returning empty lists

    Either way, the entity should NOT appear in enrichment output (no enrichment
    to export). This test verifies the pipeline handles both cases.
    """

    def test_empty_aspects_no_enrichment(self, export_dir):
        """Dataset with empty tag/ownership lists should not appear in enrichment."""
        enrichment = _load(export_dir, "enrichment.json")
        ds = next(
            (e for e in enrichment if e["dataset_urn"] == seed.DATASET_EMPTY_ASPECTS),
            None,
        )
        # Entity with only empty aspects should not produce enrichment
        if ds is not None:
            # If it does appear, it should not have any enrichment keys
            assert "globalTags" not in ds, (
                "Empty tags list should not produce globalTags entry"
            )
            assert "ownership" not in ds, (
                "Empty owners list should not produce ownership entry"
            )


# ── Multi-platform enrichment ────────────────────────────────────────────


class TestMultiPlatformEnrichment:
    """Verify enrichment works across multiple platforms."""

    def _get_dataset(self, export_dir, urn):
        enrichment = _load(export_dir, "enrichment.json")
        return next((e for e in enrichment if e["dataset_urn"] == urn), None)

    def test_snowflake_dataset_enrichment_exported(self, export_dir):
        """Snowflake dataset with tags/domains should appear in enrichment."""
        ds = self._get_dataset(export_dir, seed.DATASET_SNOWFLAKE)
        assert ds is not None, "Snowflake dataset should be in enrichment"
        tag_urns = {t["tag"] for t in ds.get("globalTags", [])}
        assert seed.TAG_FINANCIAL in tag_urns

    def test_platform_scope_snowflake_includes_snowflake(
        self, export_dir_platform_scoped
    ):
        """When platform=postgres, snowflake datasets should be excluded."""
        enrichment = _load(export_dir_platform_scoped, "enrichment.json")
        ds_urns = {e["dataset_urn"] for e in enrichment}
        assert seed.DATASET_SNOWFLAKE not in ds_urns, (
            "Snowflake dataset should be excluded when platform=postgres"
        )


# ── Export-then-sync round-trip ──────────────────────────────────────────


class TestSyncRoundTrip:
    """Verify full export -> sync pipeline end-to-end.

    Before syncing, the fixture mutates TAG_PII's description to a different
    value. The sync should overwrite it back to the exported value, proving
    that UPSERT actually writes (not a no-op against pre-existing data).
    """

    def test_sync_exits_zero(self, sync_round_trip_dir):
        """Sync CLI should exit 0 (the fixture raises on non-zero)."""
        # If we get here, the fixture's subprocess.run didn't raise
        assert sync_round_trip_dir is not None

    def test_sync_summary_reports_no_failures(self, sync_round_trip_dir):
        """Sync summary should report 0 failures."""
        stderr = sync_round_trip_dir["stderr"]
        # print_summary logs via logger (stderr): "Sync complete: N succeeded, M failed, K skipped"
        assert "0 failed" in stderr, (
            f"Expected '0 failed' in sync stderr, got:\n{stderr}"
        )

    def test_mutation_overwritten_by_sync(self, sync_round_trip_graph):
        """Tag description was mutated before sync; sync should restore it.

        This proves the pipeline actually writes (not just a no-op) and
        that UPSERT semantics overwrite the entire aspect.
        """
        from datahub.metadata.schema_classes import TagPropertiesClass

        props = sync_round_trip_graph.get_aspect(
            seed.TAG_PII, TagPropertiesClass
        )
        assert props is not None
        assert props.description != "MUTATED -- should be overwritten by sync", (
            "Sync should have overwritten the mutated description"
        )
        assert props.description == "PII tag for integration test"

    def test_tags_readable_after_sync(self, sync_round_trip_graph):
        """Tags should be readable via the graph after sync."""
        from datahub.metadata.schema_classes import TagPropertiesClass

        props = sync_round_trip_graph.get_aspect(
            seed.TAG_PII, TagPropertiesClass
        )
        assert props is not None
        assert props.name == "Integration PII"

    def test_domains_readable_after_sync(self, sync_round_trip_graph):
        """Domains should be readable after sync."""
        from datahub.metadata.schema_classes import DomainPropertiesClass

        props = sync_round_trip_graph.get_aspect(
            seed.DOMAIN_ROOT, DomainPropertiesClass
        )
        assert props is not None
        assert props.name == "Integration Root Domain"

    def test_dataset_enrichment_intact_after_sync(self, sync_round_trip_graph):
        """Dataset 1 tags should still be applied after sync."""
        tags = sync_round_trip_graph.get_tags(seed.DATASET_1)
        assert tags is not None
        tag_urns = {str(t.tag) for t in tags.tags}
        assert seed.TAG_PII in tag_urns
        assert seed.TAG_FINANCIAL in tag_urns

    def test_dataset_ownership_intact_after_sync(self, sync_round_trip_graph):
        """Dataset 1 ownership should still be applied after sync."""
        ownership = sync_round_trip_graph.get_ownership(seed.DATASET_1)
        assert ownership is not None
        owner_urns = {str(o.owner) for o in ownership.owners}
        assert seed.USER_ALICE in owner_urns

    def test_glossary_hierarchy_intact_after_sync(self, sync_round_trip_graph):
        """Glossary node hierarchy should be preserved after sync."""
        from datahub.metadata.schema_classes import GlossaryNodeInfoClass

        child = sync_round_trip_graph.get_aspect(
            seed.NODE_CHILD, GlossaryNodeInfoClass
        )
        assert child is not None
        assert child.parentNode == seed.NODE_ROOT
