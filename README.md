# datahub-cicd

CI/CD pipeline for syncing DataHub governance metadata (tags, glossary, domains) and enrichment (tag/term/domain assignments on datasets) from dev to prod.

## Architecture

- **Unidirectional**: Dev DataHub is source of truth, prod is updated exclusively via CI/CD
- **UUID passthrough**: Governance entity URNs are identical in dev and prod
- **Full overwrite**: All writes use UPSERT (full replace). Dev wins.
- **Phased execution**: Entities synced in dependency order (tags -> glossary nodes -> terms -> domains -> enrichment)

## Usage

### Export governance entities from dev

```bash
export DATAHUB_DEV_URL=http://dev-datahub:8080
export DATAHUB_DEV_TOKEN=your-token

python -m src.cli.export_cmd --output-dir metadata/
```

### Sync to prod

```bash
export DATAHUB_DEV_URL=http://dev-datahub:8080
export DATAHUB_DEV_TOKEN=your-dev-token
export DATAHUB_PROD_URL=http://prod-datahub:8080
export DATAHUB_PROD_TOKEN=your-prod-token

# Dry run (preview without writing)
python -m src.cli.sync_cmd --governance-dir metadata/ --dry-run

# Live sync
python -m src.cli.sync_cmd --governance-dir metadata/
```

### GitHub Actions

Trigger the `Sync Metadata` workflow manually with `dry_run: true` for preview or `dry_run: false` for live sync.

## Adding new entity types

1. Subclass `EntityHandler` in `src/handlers/`
2. Implement `entity_type`, `export()`, `build_mcps()`
3. Register in `src/handlers/__init__.py`

No changes to orchestrator, CLI, or workflow needed.

## Tests

```bash
pytest tests/
```
