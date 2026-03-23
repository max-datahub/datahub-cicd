# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Related Repositories

| Repo | URL | Description |
|------|-----|-------------|
| DataHub OSS | https://github.com/datahub-project/datahub | Open-source DataHub upstream |
| DataHub Helm Charts | https://github.com/acryldata/datahub-helm | Helm charts for DataHub deployment |

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

# Export with deletion detection
python -m src.cli.export_cmd --output-dir metadata/ --include-deletions

# Export with provenance filtering (GraphQL-authored only; see caveat in provenance.py)
python -m src.cli.export_cmd --output-dir metadata/ --filter-by-source ui

# Export with enrichment scoping (domain, platform, environment)
python -m src.cli.export_cmd --output-dir metadata/ --domain urn:li:domain:marketing
python -m src.cli.export_cmd --output-dir metadata/ --platform snowflake --env PROD
python -m src.cli.export_cmd --output-dir metadata/ --scope-config config/example-scope.yaml

# Export with DEBUG logging (shows full stack traces)
python -m src.cli.export_cmd --output-dir metadata/ --log-level DEBUG

# Sync to prod DataHub (dry-run)
python -m src.cli.sync_cmd --metadata-dir metadata/ --dry-run

# Sync to prod DataHub (live)
python -m src.cli.sync_cmd --metadata-dir metadata/

# Sync with deletion propagation
python -m src.cli.sync_cmd --metadata-dir metadata/ --apply-deletions
```

**Environment variables** (see `config/example.env`): `DATAHUB_DEV_URL`, `DATAHUB_DEV_TOKEN`, `DATAHUB_PROD_URL`, `DATAHUB_PROD_TOKEN`.

## Architecture

### Execution Flow

1. **Export**: Registry resolves handler dependency order → each handler calls `export(graph)` → results written to JSON in `metadata/` → governance URNs collected → enrichment handlers export tag/term/domain/ownership assignments filtered to those URNs.
2. **Sync**: JSON files loaded → handlers process entities in dependency order → `build_mcps()` creates MetadataChangeProposalWrappers → `WriteStrategy.emit()` sends to target DataHub.

### Key Abstractions (`src/interfaces.py`)

- **EntityHandler**: Abstract base for each entity type. Defines `export()`, `build_mcps()`, `dependencies`, `is_system_entity()`, `validate()`.
- **SyncResult**: Per-entity result with `status` (success/failed/skipped), `error`, `skip_reason`, `traceback`, `error_category`, and `error_suggestion` fields.
- **WriteStrategy**: Controls how MCPs are written. `OverwriteStrategy` does full UPSERT with retry; `DryRunStrategy` logs without emitting (sets `skip_reason=SKIP_DRY_RUN`).
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

### Enrichment Scoping (`src/scope.py`)

`ScopeConfig` restricts which entities are scanned during enrichment export. Governance entities (tags, terms, domains, data products) are always global. Scope filters apply only to enrichment targets (datasets, charts, dashboards, etc.).

- **domains**: Filter by domain URN via `extraFilters` (Elasticsearch query).
- **platforms**: Filter by platform name via `get_urns_by_filter(platform=...)`.
- **env**: Filter by environment via `get_urns_by_filter(env=...)`. Only applied to entity types that support it (datasets, containers). Omitted for charts, dashboards, dataFlows, and dataProducts to avoid excluding all results.

Configuration via CLI flags (`--domain`, `--platform`, `--env`), YAML file (`--scope-config`), or both (CLI overrides YAML).

### Orchestrator (`src/orchestrator.py`)

`SyncOrchestrator` coordinates export and sync across all handlers. Tracks per-entity `SyncResult` (success/failed/skipped) and reports failures. Uses adaptive progress intervals (every 25/50/100 entities depending on total count, plus time-based every 30s for large runs). Writes incremental `.run-state.json` after each handler phase for crash resilience.

### Deletion Propagation (`src/deletion.py`)

- `detect_soft_deleted(graph, entity_types)`: Queries DataHub for soft-deleted governance entities via `RemovedStatusFilter.ONLY_SOFT_DELETED`. Returns list of `{"urn": ..., "entity_type": ...}` dicts. Only scans entity types with a registered `status` aspect (tags, glossary nodes/terms, data products). **Domains are excluded** — they lack the `status` aspect and return 422 on soft-delete.
- `apply_deletions(graph, deletions, dry_run)`: Calls `graph.soft_delete_entity(urn)` for each deletion. Per-entity error tracking. Dry-run mode returns "skipped" without deleting.

### Provenance Filtering (`src/provenance.py`)

- `ProvenanceSource` enum: `UI`, `INGESTION`, `CICD`, `UNKNOWN`.
- `classify_provenance(graph, urn, aspect_name)`: Inspects `systemMetadata` on an entity's primary aspect to classify its creation source.
- `filter_entities_by_provenance(graph, entities, entity_type, allowed_sources)`: Returns `(kept, filtered_out)` tuple. Filters entities to only those matching allowed provenance sources while tracking filtered-out entities for skip reporting.
- CI/CD writes are tagged with `appSource: cicd-pipeline` via `CICD_SYSTEM_METADATA` in `OverwriteStrategy`.

### Observability (`src/logging_config.py`, `src/run_context.py`, `src/error_classification.py`, `src/reporting.py`, `src/retry.py`)

Each CLI run produces structured observability outputs in the output directory:

- **JSONL log** (`run-{run_id}.jsonl`): Machine-parseable structured log, one JSON object per line with timestamp, level, logger, message. Captures all log levels (DEBUG to ERROR) regardless of console log level.
- **JSON report** (`run-report.json`): Run summary with entity counts, API call stats, errors with classification/suggestions, skip reasons, and timing.
- **Markdown report** (`run-report.md`): Human-readable report with entity summary table, timing, skip table, and error details.
- **Incremental state** (`.run-state.json`): Updated after each handler phase. If the pipeline crashes, this file is the best available record of what completed.

Key modules:

- **`TrackedGraph`** (`src/run_context.py`): Wraps `DataHubGraph` with `__getattr__` delegation. Tracks call counts and timing for API methods (`get_tags`, `emit_mcp`, etc.) transparently. Untracked methods pass through with zero overhead.
- **`RunContext`** (`src/run_context.py`): Tracks run ID, command, phase timing, and duration.
- **Error classification** (`src/error_classification.py`): `classify_error(exc)` returns `(category, suggestion)` tuple. Handles DataHub SDK exceptions (HTTP status codes in attrs/messages), standard Python exceptions, and unknown errors.
- **Retry** (`src/retry.py`): `@retry_transient` decorator with exponential backoff. Retries `ConnectionError`, `TimeoutError`, HTTP 429/502/503/504. Does NOT retry auth/validation/client errors. Applied to `OverwriteStrategy.emit()`, enrichment API calls, and `apply_deletions()`.
- **Skip tracking**: `SyncResult.skip_reason` field with constants (`SKIP_DRY_RUN`, `SKIP_SYSTEM_ENTITY`, `SKIP_PROVENANCE_FILTER`, `SKIP_NO_ENRICHMENT`, etc.) in `src/interfaces.py`.
- **Stack traces**: Full tracebacks at `logger.debug(exc_info=True)` (visible with `--log-level DEBUG`), clean one-line messages at `logger.error()`. Tracebacks also stored in `SyncResult.traceback` for the run report.

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
- Observability unit tests cover: retry logic, error classification, TrackedGraph, run reports (JSON/Markdown), JSONL logging, incremental state.
- Integration tests (`@pytest.mark.integration`) spin up a Docker DataHub instance with a 10000-port offset and seed test data via `tests/integration/seed.py`.
- Integration observability tests (`tests/integration/test_observability.py`) validate that JSONL logs, JSON/Markdown reports, `.run-state.json`, API stats, and skip tracking are produced correctly during real export/sync runs.
