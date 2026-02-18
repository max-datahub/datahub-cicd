"""Pipeline scoping: filter enrichment exports by domain, platform, and environment."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field

import yaml
from datahub.ingestion.graph.filters import RawSearchFilterRule, SearchFilterRule

logger = logging.getLogger(__name__)

# Entity types where the `env` filter is meaningful.
# Charts, dashboards, dataFlows, and dataProducts lack an environment field —
# passing env to get_urns_by_filter() for those types excludes all results.
ENV_SUPPORTED_ENTITY_TYPES = {"dataset", "container"}


@dataclass
class ScopeConfig:
    """Filter configuration for scoping enrichment exports.

    Governance entities (tags, terms, domains, data products) are always
    exported globally. Scope filters only apply to enrichment targets
    (datasets, charts, dashboards, etc.).
    """

    domains: list[str] | None = None
    platforms: list[str] | None = None
    env: str | None = None

    @property
    def is_scoped(self) -> bool:
        """True if any filter is active."""
        return bool(self.domains or self.platforms or self.env)

    def build_extra_filters(self) -> list[RawSearchFilterRule] | None:
        """Build extraFilters for get_urns_by_filter().

        Domains use extraFilters (SearchFilterRule). Platform and env use
        direct parameters on get_urns_by_filter() instead.

        Returns None if no extra filters are needed.
        """
        if not self.domains:
            return None
        return [
            SearchFilterRule(
                field="domains",
                condition="EQUAL",
                values=self.domains,
            ).to_raw()
        ]

    def __str__(self) -> str:
        parts = []
        if self.domains:
            parts.append(f"domains={self.domains}")
        if self.platforms:
            parts.append(f"platforms={self.platforms}")
        if self.env:
            parts.append(f"env={self.env}")
        return f"ScopeConfig({', '.join(parts)})" if parts else "ScopeConfig(none)"

    @classmethod
    def from_yaml(cls, path: str) -> ScopeConfig:
        """Load scope configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        scope_data = data.get("scope", {})
        return cls(
            domains=scope_data.get("domains"),
            platforms=scope_data.get("platforms"),
            env=scope_data.get("env"),
        )

    @classmethod
    def from_cli_args(cls, args: argparse.Namespace) -> ScopeConfig:
        """Build from parsed CLI args. Merges with YAML if --scope-config provided.

        CLI flags override YAML values when both are present.
        """
        # Start from YAML if provided
        if hasattr(args, "scope_config") and args.scope_config:
            scope = cls.from_yaml(args.scope_config)
        else:
            scope = cls()

        # CLI flags override YAML
        if hasattr(args, "domains") and args.domains:
            scope.domains = args.domains
        if hasattr(args, "platforms") and args.platforms:
            scope.platforms = args.platforms
        if hasattr(args, "env") and args.env:
            scope.env = args.env

        return scope
