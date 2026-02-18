import argparse
import os
import tempfile

import pytest

from src.scope import ScopeConfig


class TestScopeConfig:
    def test_is_scoped_no_filters(self):
        scope = ScopeConfig()
        assert scope.is_scoped is False

    def test_is_scoped_with_domains(self):
        scope = ScopeConfig(domains=["urn:li:domain:marketing"])
        assert scope.is_scoped is True

    def test_is_scoped_with_platforms(self):
        scope = ScopeConfig(platforms=["snowflake"])
        assert scope.is_scoped is True

    def test_is_scoped_with_env(self):
        scope = ScopeConfig(env="PROD")
        assert scope.is_scoped is True

    def test_build_extra_filters_no_domains(self):
        scope = ScopeConfig(platforms=["snowflake"], env="PROD")
        assert scope.build_extra_filters() is None

    def test_build_extra_filters_with_domains(self):
        scope = ScopeConfig(domains=["urn:li:domain:marketing", "urn:li:domain:finance"])
        filters = scope.build_extra_filters()
        assert filters is not None
        assert len(filters) == 1
        assert filters[0]["field"] == "domains"
        assert filters[0]["condition"] == "EQUAL"
        assert filters[0]["values"] == [
            "urn:li:domain:marketing",
            "urn:li:domain:finance",
        ]

    def test_build_extra_filters_no_negated_key(self):
        scope = ScopeConfig(domains=["urn:li:domain:marketing"])
        filters = scope.build_extra_filters()
        assert "negated" not in filters[0]

    def test_str_no_filters(self):
        scope = ScopeConfig()
        assert str(scope) == "ScopeConfig(none)"

    def test_str_with_filters(self):
        scope = ScopeConfig(
            domains=["urn:li:domain:marketing"],
            platforms=["snowflake"],
            env="PROD",
        )
        result = str(scope)
        assert "domains=" in result
        assert "platforms=" in result
        assert "env=" in result


class TestScopeConfigFromYaml:
    def test_from_yaml_full(self, tmp_path):
        yaml_content = """\
scope:
  domains:
    - urn:li:domain:marketing
    - urn:li:domain:finance
  platforms:
    - snowflake
  env: PROD
"""
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        scope = ScopeConfig.from_yaml(str(config_file))
        assert scope.domains == ["urn:li:domain:marketing", "urn:li:domain:finance"]
        assert scope.platforms == ["snowflake"]
        assert scope.env == "PROD"

    def test_from_yaml_partial(self, tmp_path):
        yaml_content = """\
scope:
  platforms:
    - bigquery
"""
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        scope = ScopeConfig.from_yaml(str(config_file))
        assert scope.domains is None
        assert scope.platforms == ["bigquery"]
        assert scope.env is None

    def test_from_yaml_empty_scope(self, tmp_path):
        yaml_content = "scope: {}\n"
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        scope = ScopeConfig.from_yaml(str(config_file))
        assert scope.is_scoped is False

    def test_from_yaml_missing_scope_key(self, tmp_path):
        yaml_content = "other_key: value\n"
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        scope = ScopeConfig.from_yaml(str(config_file))
        assert scope.is_scoped is False


class TestScopeConfigFromCliArgs:
    def test_from_cli_args_all_flags(self):
        args = argparse.Namespace(
            domains=["urn:li:domain:marketing"],
            platforms=["snowflake", "bigquery"],
            env="PROD",
            scope_config=None,
        )
        scope = ScopeConfig.from_cli_args(args)
        assert scope.domains == ["urn:li:domain:marketing"]
        assert scope.platforms == ["snowflake", "bigquery"]
        assert scope.env == "PROD"

    def test_from_cli_args_no_flags(self):
        args = argparse.Namespace(
            domains=None,
            platforms=None,
            env=None,
            scope_config=None,
        )
        scope = ScopeConfig.from_cli_args(args)
        assert scope.is_scoped is False

    def test_from_cli_args_partial(self):
        args = argparse.Namespace(
            domains=None,
            platforms=["snowflake"],
            env=None,
            scope_config=None,
        )
        scope = ScopeConfig.from_cli_args(args)
        assert scope.platforms == ["snowflake"]
        assert scope.domains is None
        assert scope.env is None

    def test_from_cli_args_with_yaml(self, tmp_path):
        yaml_content = """\
scope:
  domains:
    - urn:li:domain:marketing
  platforms:
    - bigquery
  env: DEV
"""
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        args = argparse.Namespace(
            domains=None,
            platforms=None,
            env=None,
            scope_config=str(config_file),
        )
        scope = ScopeConfig.from_cli_args(args)
        assert scope.domains == ["urn:li:domain:marketing"]
        assert scope.platforms == ["bigquery"]
        assert scope.env == "DEV"

    def test_cli_overrides_yaml(self, tmp_path):
        yaml_content = """\
scope:
  domains:
    - urn:li:domain:marketing
  platforms:
    - bigquery
  env: DEV
"""
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        args = argparse.Namespace(
            domains=["urn:li:domain:finance"],
            platforms=["snowflake"],
            env="PROD",
            scope_config=str(config_file),
        )
        scope = ScopeConfig.from_cli_args(args)
        # CLI flags override YAML
        assert scope.domains == ["urn:li:domain:finance"]
        assert scope.platforms == ["snowflake"]
        assert scope.env == "PROD"

    def test_cli_partial_override_yaml(self, tmp_path):
        yaml_content = """\
scope:
  domains:
    - urn:li:domain:marketing
  platforms:
    - bigquery
  env: DEV
"""
        config_file = tmp_path / "scope.yaml"
        config_file.write_text(yaml_content)

        # Only override env, keep domains and platforms from YAML
        args = argparse.Namespace(
            domains=None,
            platforms=None,
            env="PROD",
            scope_config=str(config_file),
        )
        scope = ScopeConfig.from_cli_args(args)
        assert scope.domains == ["urn:li:domain:marketing"]  # from YAML
        assert scope.platforms == ["bigquery"]  # from YAML
        assert scope.env == "PROD"  # overridden by CLI
