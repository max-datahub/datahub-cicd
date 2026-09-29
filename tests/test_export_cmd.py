from src.cli.export_cmd import logical_enrichment_scope
from src.handlers import create_default_registry
from src.scope import ScopeConfig


def test_logical_models_only_registry_drops_data_products():
    full = {h.entity_type for h in create_default_registry().get_sync_order()}
    logical = {h.entity_type for h in create_default_registry(logical_models_only=True).get_sync_order()}
    assert full - logical == {"dataProduct"}
    assert {"tag", "glossaryNode", "glossaryTerm", "domain", "logicalModel"} <= logical


def test_logical_enrichment_scope_uses_exported_logical_platforms():
    exports = {
        "logicalModel": [
            {"urn": "urn:li:dataPlatform:logical", "entityType": "dataPlatform"},
            {"urn": "urn:li:dataset:(urn:li:dataPlatform:logical,m,PROD)", "entityType": "dataset"},
        ]
    }
    scope = logical_enrichment_scope(ScopeConfig(domains=["urn:li:domain:d"], env="PROD"), exports)
    assert scope == ScopeConfig(domains=["urn:li:domain:d"], platforms=["urn:li:dataPlatform:logical"], env="PROD")


def test_logical_enrichment_scope_none_without_logical_platforms():
    # An empty platform list would otherwise mean "unscoped" and scan every entity.
    assert logical_enrichment_scope(ScopeConfig(), {"logicalModel": []}) is None
    assert logical_enrichment_scope(ScopeConfig(), {}) is None
