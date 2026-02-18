# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Related Repositories

| Repo | Local Path | Description |
|------|-----------|-------------|
| DataHub Cloud (fork) | `~/Documents/Repos/datahub-fork` | Acryl DataHub Cloud source code |
| DataHub OSS | `~/Documents/Repos/datahub` | Open-source DataHub upstream |
| DataHub Helm Charts (fork) | `~/Documents/Repos/datahub-helm-fork` | Helm charts for DataHub deployment |

These repos are useful for understanding the DataHub SDK, GraphQL API, entity/aspect models, and deployment configuration that this pipeline interacts with.

## Project Overview

datahub-cicd is a Python CI/CD pipeline that syncs DataHub governance metadata (tags, glossary, domains, data products) and enrichment (tag/term/domain/ownership assignments on datasets, charts, dashboards, etc.) from a dev DataHub instance to prod. It exports entities to JSON files, then syncs them in dependency-resolved order.

## Commands

```bash
# Install dependencies
pip install -e .

# Run unit tests (integration tests excluded by default via pytest.ini)
pytest tests/ -v

# Run a single test file
pytest tests/test_handlers/test_tags.py -v

# Run a single test
pytest tests/test_handlers/test_tags.py::TestTagHandler::test_export -v

# Run integration tests (requires Docker DataHub instance)
pytest -m integration tests/integration/ -v

# Export from dev DataHub
python -m src.cli.export_cmd --output-dir metadata/

# Sync to prod DataHub (dry-run)
python -m src.cli.sync_cmd --metadata-dir metadata/ --dry-run

# Sync to prod DataHub (live)
python -m src.cli.sync_cmd --metadata-dir metadata/
```

**Environment variables** (see `config/example.env`): `DATAHUB_DEV_URL`, `DATAHUB_DEV_TOKEN`, `DATAHUB_PROD_URL`, `DATAHUB_PROD_TOKEN`.

## Architecture

### Execution Flow

1. **Export**: Registry resolves handler dependency order → each handler calls `export(graph)` → results written to JSON in `metadata/` → governance URNs collected → enrichment handlers export tag/term/domain/ownership assignments filtered to those URNs.
2. **Sync**: JSON files loaded → handlers process entities in dependency order → `build_mcps()` creates MetadataChangeProposalWrappers → `WriteStrategy.emit()` sends to target DataHub.

### Key Abstractions (`src/interfaces.py`)

- **EntityHandler**: Abstract base for each entity type. Defines `export()`, `build_mcps()`, `dependencies`, `is_system_entity()`, `validate()`.
- **WriteStrategy**: Controls how MCPs are written. `OverwriteStrategy` does full UPSERT; `DryRunStrategy` logs without emitting.
- **UrnMapper**: Maps source URNs to target URNs. Currently `PassthroughMapper` (identity); designed for future cross-environment mapping.

### Handler Registry & Dependency Resolution (`src/registry.py`)

`HandlerRegistry` stores handlers by entity type and provides `get_sync_order()` which topologically sorts handlers by their declared `dependencies`. This ensures parents (e.g., glossary nodes) are synced before children (glossary terms).

### Handlers (`src/handlers/`)

| Handler | Entity Type | Dependencies | Key Behavior |
|---------|-------------|-------------|--------------|
| `TagHandler` | `tag` | none | Filters `__default_*` system tags |
| `GlossaryNodeHandler` | `glossaryNode` | none | Topological sort for hierarchy |
| `GlossaryTermHandler` | `glossaryTerm` | `glossaryNode` | Topological sort, parent node refs |
| `DomainHandler` | `domain` | none | Topological sort for nested domains |
| `DataProductHandler` | `dataProduct` | `domain` | Asset URN mapping |
| `DatasetEnrichmentHandler` | `enrichment` | none | Tags, terms, domains, ownership, field-level schema |
| `GenericEnrichmentHandler` | varies | none | Reusable for chart/dashboard/container/dataFlow/dataProduct enrichment |

### Orchestrator (`src/orchestrator.py`)

`SyncOrchestrator` coordinates export and sync across all handlers. Tracks per-entity `SyncResult` (success/failed/skipped) and reports failures. Progress logged every 50 entities.

### Utilities (`src/utils.py`)

- `topological_sort(entities, parent_key)`: Orders hierarchical entities so parents precede children.
- `collect_governance_urns(exports)`: Gathers all governance URNs for enrichment filtering (prevents dangling references).
- `write_json` / `read_json`: JSON I/O for metadata files.

## Key Design Decisions

- **UPSERT semantics**: DataHub replaces the entire aspect on write — no atomic add/remove of individual tags or terms.
- **URN passthrough**: Dev and prod URNs are assumed identical (same UUIDs, same ingestion topology). This is fundamental to the current sync model.
- **Per-entity error tracking**: A single entity failure doesn't abort the batch. Results accumulate and are summarized at the end.
- **Governance URN filtering**: Enrichment handlers only sync assignments that reference governance entities being managed (tags, terms, domains), preventing references to entities that don't exist in prod.

## Adding a New Entity Type

1. Subclass `EntityHandler` in `src/handlers/`.
2. Implement `export()`, `build_mcps()`, set `entity_type` and `dependencies`.
3. Register in `HandlerRegistry` (in the CLI commands).
4. Add unit tests in `tests/test_handlers/`.

## Testing Patterns

- Unit tests use a `mock_graph` fixture (from `tests/conftest.py`) that stubs `DataHubGraph` methods.
- Handler tests cover: export, build_mcps, system entity filtering, hierarchical ordering.
- Integration tests (`@pytest.mark.integration`) spin up a Docker DataHub instance with a 10000-port offset and seed test data via `tests/integration/seed.py`.
