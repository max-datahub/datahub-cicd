import argparse
from unittest.mock import patch

import pytest

from src.cli.export_cmd import _positive_int, logical_enrichment_scope
from src.client import get_graph
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


def test_logical_workers_reach_handler():
    registry = create_default_registry(logical_workers=3)
    handler = next(h for h in registry.get_sync_order() if h.entity_type == "logicalModel")
    assert handler.max_workers == 3


@pytest.mark.parametrize("value", ["0", "-1", "x"])
def test_positive_int_rejects_invalid_workers(value):
    with pytest.raises((argparse.ArgumentTypeError, ValueError)):
        _positive_int(value)


@pytest.mark.parametrize("env,expected", [(None, None), ("120", 120.0)])
def test_timeout_env_var_reaches_client_config(monkeypatch, env, expected):
    if env is None:
        monkeypatch.delenv("DATAHUB_TIMEOUT_SEC", raising=False)
    else:
        monkeypatch.setenv("DATAHUB_TIMEOUT_SEC", env)
    with patch("src.client.DataHubGraph") as graph_cls:
        get_graph("http://localhost:8080", "t")
    assert graph_cls.call_args.args[0].timeout_sec == expected
