# datahub-cicd

CI/CD pipeline for syncing DataHub governance metadata (tags, glossary, domains) and enrichment (tag/term/domain assignments on datasets) from dev to prod.

## Architecture

- **Unidirectional**: Dev DataHub is source of truth, prod is updated exclusively via CI/CD
- **UUID passthrough**: Governance entity URNs are identical in dev and prod
- **Full overwrite**: All writes use UPSERT (full replace). Dev wins.
- **Phased execution**: Entities synced in dependency order (tags -> glossary nodes -> terms -> domains -> enrichment)

## Entity & Aspect Reference

### Status Legend

| Status | Meaning |
|---|---|
| **Supported** | Implemented in current PoC |
| **Planned** | Identified for future implementation with a clear path |
| **Via ingestion** | Re-created by running ingestion recipes in each environment separately; not a CI/CD concern |
| **Per-environment** | Must be configured independently in each environment; migration is incorrect or dangerous |
| **Not migrated** | System/internal entity that is identical across environments or auto-generated |

### Governance Entities (definitions synced dev -> prod)

| Entity Type | Status | Aspects Synced | Justification |
|---|---|---|---|
| `tag` | **Supported** | `tagProperties` (name, description, colorHex) | Flat entity, no hierarchy. System tags (`__default_*`) filtered. |
| `glossaryNode` | **Supported** | `glossaryNodeInfo` (name, definition, parentNode) | Topologically sorted so parents created before children. System nodes (`__system__*`) filtered. |
| `glossaryTerm` | **Supported** | `glossaryTermInfo` (name, definition, termSource, parentNode) | Depends on glossaryNode. Topologically sorted. Names derived from URN when null. |
| `domain` | **Supported** | `domainProperties` (name, description, parentDomain) | Topologically sorted for nested domains. System domains (`__system__*`) filtered. |
| `structuredProperty` | **Planned** | `propertyDefinition` (qualifiedName, valueType, allowedValues, entityTypes) | Best-behaved entity: URNs are deterministic via `qualifiedName`. No UUID problem. |
| `dataProduct` | **Planned** | `dataProductProperties` (name, description, assets) | UUID URNs by default. Asset lists contain dataset URNs that may differ cross-env. Requires UrnMapper. |
| `ownershipType` | **Planned** | `ownershipTypeInfo` (name, description) | Custom types only; system types (`__system__*` prefix, always UUID) are identical in all envs and skipped. |
| `form` | **Planned** | `formInfo` (name, description, type, prompts) | UUID URNs, not discoverable by connector. Skip `formAssignmentStatus` (runtime state). |
| `businessAttribute` | **Planned (Phase 6)** | `businessAttributeInfo` | Sparse entity (only ownership + institutionalMemory). Defines reusable column-level metadata templates. Referenced via `businessAttributes` aspect on `schemaField` only. Migrate if in use. |
| `application` | **Planned (Phase 6)** | `applicationProperties` | Full governance support (gT, glT, dom, own, sP, forms). Groups related data assets. `applications` aspect appears on datasets, charts, dashboards, etc. Migrate if in use. |

### Governance Aspects on Governance Entities (secondary metadata)

These aspects appear on the governance entities above. They are not synced in the PoC but are identified for future phases.

| Aspect | On Entity Types | Status | Justification |
|---|---|---|---|
| `ownership` | tag, glossaryNode, glossaryTerm, domain, dataProduct, form | **Planned** | Owner URNs (corpuser/corpGroup) are identity-based (same SSO = same URN). Custom ownershipType URNs may differ. |
| `glossaryRelatedTerms` | glossaryTerm | **Planned** | Cross-references between terms. All referenced term URNs must exist before writing. Requires two-pass sync. |
| `institutionalMemory` | glossaryNode, glossaryTerm, domain, dataProduct | **Planned** | Links to external documentation (URLs). No URN references inside -- straightforward to migrate. |
| `structuredProperties` | glossaryNode, glossaryTerm, domain, dataProduct, form | **Planned** | Property assignments. Property URNs are deterministic (`qualifiedName`-based). |
| `deprecation` | tag, glossaryTerm, dataset, chart, dashboard, container, dataJob, dataFlow, notebook, schemaField | **Planned (Phase 3)** | Contains deprecated flag, note, actor (corpuser URN), and replacement entity URN. Under PoC constraints, all URNs map via passthrough. If deprecated in dev, should be deprecated in prod. |
| `displayProperties` | glossaryNode, domain | **Planned** | Visual display settings. No URN references. |

### Enrichment Aspects on Data Assets (assignments synced dev -> prod)

| Aspect | On Entity Types | Status | Justification |
|---|---|---|---|
| `globalTags` (dataset-level) | dataset | **Supported** | Tag assignments on datasets. Filtered to governance URNs. |
| `glossaryTerms` (dataset-level) | dataset | **Supported** | Term assignments on datasets. Filtered to governance URNs. |
| `domains` (dataset-level) | dataset | **Supported** | Domain membership. Filtered to governance URNs. |
| `editableSchemaMetadata` (field-level tags/terms) | dataset | **Supported** | Per-column tag and term assignments. Filtered to governance URNs. |
| `globalTags` on other assets | chart, dashboard, dataJob, dataFlow, container, notebook, mlModel, mlFeature | **Planned (Phase 3)** | Same pattern as dataset enrichment. Requires one handler per entity type (or a generic enrichment handler). |
| `glossaryTerms` on other assets | chart, dashboard, dataJob, dataFlow, container, notebook, mlModel, mlFeature | **Planned (Phase 3)** | Same pattern as dataset enrichment. |
| `domains` on other assets | chart, dashboard, dataJob, dataFlow, container, notebook, mlModel, mlFeature | **Planned (Phase 3)** | Same pattern as dataset enrichment. |
| `editableDatasetProperties` | dataset | **Planned (Phase 3)** | User-authored description override. No URN references inside -- straightforward. |
| `editableContainerProperties` | container | **Planned (Phase 3)** | User-authored container description. Same pattern as dataset. Containers are ingestion-sourced but may have user edits. |
| `editableSchemaMetadata` (field descriptions) | dataset | **Planned (Phase 3)** | Per-column descriptions. Currently only tags/terms are synced; descriptions would come with these. Note: `editableSchemaMetadata` exists ONLY on `dataset` -- no other entity type has this aspect. |
| `editableChartProperties` | chart | **Planned (Phase 3)** | Same pattern as dataset. |
| `editableDashboardProperties` | dashboard | **Planned (Phase 3)** | Same pattern as dataset. |
| `editableDataJobProperties` | dataJob | **Planned (Phase 3)** | Same pattern as dataset. |
| `editableDataFlowProperties` | dataFlow | **Planned (Phase 3)** | Same pattern as dataset. |
| `editableNotebookProperties` | notebook | **Planned (Phase 3)** | Same pattern as dataset. |
| `editableMlModelProperties` (and variants) | mlModel, mlModelGroup, mlFeature, mlFeatureTable, mlPrimaryKey | **Planned (Phase 3)** | Same pattern as dataset. Full governance support on all ML entities. |
| `structuredProperties` (assignments) | dataset, chart, dashboard, container, dataJob, dataFlow, mlModel, etc. | **Planned (Phase 2)** | Property value assignments. Property URNs deterministic via `qualifiedName`. Dataset URNs require mapping in cross-env. |
| `documentation` (SaaS-only) | dataset, dashboard, chart, container, dataJob, dataFlow, notebook | **Planned (Phase 3)** | Rich README-style docs. SaaS/Acryl Cloud only. |
| `schemaField` enrichment (tags/terms) | schemaField | **Planned (Phase 3)** | Newer column-level entity model (vs. nested `editableSchemaMetadata`). Has globalTags, glossaryTerms, structuredProperties, forms, and uniquely `businessAttributes`. URN contains parent dataset URN (identical under PoC constraints). |

### Access Control & Identity

| Entity/Aspect | Status | Justification |
|---|---|---|
| `dataHubPolicy` (custom, `editable: true`) | **Per-environment** | Policies reference env-specific actor/resource URNs. Always UUID (forced). Incorrect policies can lock users out. Require admin review. |
| `dataHubPolicy` (system, `editable: false`) | **Not migrated** | Identical in all environments. Re-bootstrapped on startup. |
| `dataHubRole` (Admin, Editor, Reader) | **Not migrated** | Well-known built-in roles, identical in all environments. |
| `corpuser` | **Via ingestion** | Identity-based URNs (LDAP/SSO username). Populated by SSO/SCIM integration, not by CI/CD. |
| `corpGroup` | **Via ingestion** | Identity-based URNs (LDAP/SSO group name). Same as corpuser. |
| `roleMembership` | **Per-environment** | Role assignments are env-specific (different admins in dev vs prod). |
| `groupMembership` | **Per-environment** | Usually managed by SSO/LDAP. |
| `corpUserInfo` / `corpGroupInfo` | **Via ingestion** | User/group profiles populated by SSO. |

### Data Assets (datasets, charts, dashboards, etc.)

| Entity/Aspect | Status | Justification |
|---|---|---|
| `dataset` entity | **Via ingestion** | Data assets are created by running ingestion recipes in each environment. Not a CI/CD concern. |
| `chart` entity | **Via ingestion** | Same as dataset. |
| `dashboard` entity | **Via ingestion** | Same as dataset. |
| `dataFlow` / `dataJob` entity | **Via ingestion** | Same as dataset. |
| `container` (databases/schemas) | **Via ingestion** | Ingestion-sourced. User enrichment (tags, terms, domains, descriptions via `editableContainerProperties`) synced as enrichment in Phase 3. |
| `schemaField` entity | **Planned (Phase 3)** | Newer column-level entity model. Has globalTags, glossaryTerms, structuredProperties, forms, businessAttributes. URN contains parent dataset URN (identical under PoC constraints). |
| `notebook` entity | **Via ingestion** | Ingestion-sourced. Enrichment (tags, terms, domains, `editableNotebookProperties`) follows dataset pattern in Phase 3. Has gT, glT, dom, own but no structuredProperties or forms. |
| `mlModel` / `mlModelGroup` / `mlFeature` / `mlFeatureTable` | **Via ingestion** | Ingestion-sourced. Full governance support (gT, glT, dom, own, sP, forms). Enrichment follows dataset pattern in Phase 3. |
| `document` entity | **Via ingestion** | Newer entity. Has gT, glT, dom, own, sP. Enrichment follows dataset pattern in Phase 3. |
| `datasetProperties` | **Via ingestion** | Ingestion-sourced metadata (schema, platform, etc.). |
| `schemaMetadata` | **Via ingestion** | Schema information from source systems. |
| `upstreamLineage` | **Via ingestion** | Ingested lineage is re-created by running ingestion recipes. Manual lineage (user-drawn edges) could be synced in Phase 6 -- under PoC constraints dataset URNs are identical so edge references are valid. Requires distinguishing manual vs ingested lineage (no built-in flag). |
| `siblings` | **Not migrated** | Sibling relationships are system-generated by ingestion. |
| `erModelRelationship` | **Via ingestion** | ER model relationships. Has gT, glT, own. Ingestion-sourced. |

### Data Quality & Contracts

| Entity/Aspect | Status | Justification |
|---|---|---|
| `assertion` entity | **Per-environment** | Assertions define data quality checks referencing dataset URNs. Operationally env-specific (schedules, connections, runtime). Only governance aspect is `globalTags`. Create and manage independently per environment. |
| `dataContract` entity | **Per-environment** | Contracts reference datasets and assertion URNs. Assertions are per-environment (see above), making contract migration fragile. Only governance aspect is `structuredProperties`. Define as code and apply per environment. |
| `assertionRunEvent` | **Not migrated** | Assertion run history -- operational/ephemeral data. Regenerated by running assertions. |
| `testResults` | **Not migrated** | Test run history -- operational data. |

### System & Internal Entities (never migrated)

| Entity Type | Justification |
|---|---|
| `dataHubSecret` | Encrypted credentials with env-specific encryption keys. Security risk. |
| `dataHubAccessToken` | API tokens. Security risk. |
| `inviteToken` | Invitation tokens. Security risk. |
| `dataHubConnection` | External connections with env-specific credentials. |
| `dataHubIngestionSource` | Ingestion recipes with env-specific DB/API configs. |
| `dataHubExecutionRequest` | Execution history -- ephemeral operational data. |
| `dataHubRetention` | Retention policies -- configure per environment. |
| `dataHubUpgrade` | Version upgrade tracking -- internal. |
| `dataHubStepState` | Workflow step state -- internal. |
| `dataHubAction` | Action system entities -- env-specific. |
| `globalSettings` | Platform-wide settings -- configure per environment. |
| `telemetry` | Usage telemetry -- not needed. |
| `dataHubPersona` | User personas -- may conflict across envs. |
| `dataHubView` (saved views) | User-specific saved views. Per-user, per-env. |
| `subscription` | User subscriptions -- env-specific. |
| `recommendation` | Recommendation cache -- auto-regenerated. |
| `linkPreview` | Link preview cache -- auto-regenerated. |
| `dataHubOpenAPISchema` | OpenAPI schema storage -- internal. |
| `dataHubRemoteExecutor` / `*Pool` / `*GlobalConfig` | Remote execution infrastructure (SaaS-only). |
| `dataHubMetricCube` | Metrics/observability -- internal. |
| `dataHubAiConversation` | AI conversation history -- internal. |
| `dataHubPageTemplate` / `dataHubPageModule` | Page templates/modules -- internal. |
| `dataHubFile` | File storage -- internal. |
| `platformResource` | Platform resource tracking -- internal. |
| `actionRequest` / `actionWorkflow` | Workflow requests/definitions -- ephemeral. |
| `dataHubConnection` | External connections with env-specific credentials. |
| `monitor` / `monitorSuite` | Data monitoring -- per-environment operational. |
| `incident` | Incident tracking -- per-environment operational. Only has `globalTags`. |
| `versionSet` | Version tracking -- internal. |
| `post` | Announcements -- per-environment. |
| `query` | Query entities -- operational. |
| `test` | Test definitions -- operational. |
| `constraint` | Constraint definitions -- internal. |

### Aspects Always Skipped (even on migrated entities)

| Aspect | Justification |
|---|---|
| `datasetProfile` | Time-series profiling stats -- regenerated by re-ingestion. |
| `datasetUsageStatistics` | Time-series usage stats -- regenerated. |
| `dashboardUsageStatistics` / `chartUsageStatistics` | Time-series -- regenerated. |
| `operation` | Time-series operation records. |
| `browsePaths` / `browsePathsV2` | System-generated browse paths. |
| `dataPlatformInstance` | Platform instance config -- env-specific. |
| `status` | Soft-delete flag -- handle separately if needed. |
| `corpUserCredentials` | Hashed passwords -- security risk. |
| `corpUserSettings` / `corpGroupSettings` | User UI preferences -- per-user, per-env. |
| `embed` | Embed URLs -- may be env-specific. |
| `formAssignmentStatus` | Form completion runtime state -- env-specific. |
| `assertionRunEvent` | Assertion run history -- time-series operational data. |
| `testResults` | Test run history -- operational data. |
| `incidentsSummary` | Incident references -- operational. |
| `nativeGroupMembership` | Native group membership -- env-specific. |
| `subTypes` | Entity sub-type classification -- system-managed. |
| `access` | Access metadata -- env-specific. |
| `applications` (assignment) | Application group membership. Planned with `application` entity in Phase 6. |

---

## Supported Features

### Governance Entity Export & Sync

| Entity Type | Export | Sync | Hierarchy | System Filtering |
|---|---|---|---|---|
| Tags | Tag definitions (name, description, color) | Full UPSERT | Flat (no hierarchy) | Filters `__default_*` |
| Glossary Nodes | Node definitions (name, definition, parent) | Full UPSERT | Topological sort (parents before children) | Filters `__system__*` |
| Glossary Terms | Term definitions (name, definition, termSource, parent) | Full UPSERT | Topological sort by parentNode | Filters `__system__*` |
| Domains | Domain definitions (name, description, parent) | Full UPSERT | Topological sort by parentDomain | Filters `__system__*` |

### Enrichment Export & Sync

| Enrichment Type | Export | Sync | Notes |
|---|---|---|---|
| Dataset-level tags | Tag assignments on datasets | Full UPSERT of `globalTags` aspect | Filtered to governance URNs only |
| Dataset-level glossary terms | Term assignments on datasets | Full UPSERT of `glossaryTerms` aspect | Filtered to governance URNs only |
| Dataset-level domains | Domain membership | Full UPSERT of `domains` aspect | Filtered to governance URNs only |
| Field-level tags | Per-column tag assignments | Full UPSERT of `editableSchemaMetadata` | Via `editableSchemaFieldInfo` |
| Field-level glossary terms | Per-column term assignments | Full UPSERT of `editableSchemaMetadata` | Via `editableSchemaFieldInfo` |

### Enrichment Filtering

Enrichment is filtered to only include associations referencing governance entities that were exported. This means:
- If tag `urn:li:tag:PII` is in the governance export, dataset-level PII tag assignments are included
- Tags, terms, or domains not in the governance export are excluded from enrichment
- This prevents syncing references to entities that don't exist in prod

## Usage

### Export governance + enrichment from dev

```bash
export DATAHUB_DEV_URL=http://dev-datahub:8080
export DATAHUB_DEV_TOKEN=your-token

python -m src.cli.export_cmd --output-dir metadata/
```

This exports 5 JSON files to `metadata/`:
- `tag.json` -- tag definitions
- `glossaryNode.json` -- glossary node definitions
- `glossaryTerm.json` -- glossary term definitions
- `domain.json` -- domain definitions
- `enrichment.json` -- tag/term/domain assignments on datasets

Use `--skip-enrichment` to export only governance definitions.

### Sync to prod

```bash
export DATAHUB_PROD_URL=http://prod-datahub:8080
export DATAHUB_PROD_TOKEN=your-prod-token

# Dry run (preview without writing)
python -m src.cli.sync_cmd --metadata-dir metadata/ --dry-run

# Live sync
python -m src.cli.sync_cmd --metadata-dir metadata/
```

By default, sync reads enrichment from the exported `enrichment.json`. To export enrichment live from dev at sync time instead:

```bash
export DATAHUB_DEV_URL=http://dev-datahub:8080
export DATAHUB_DEV_TOKEN=your-dev-token

python -m src.cli.sync_cmd --metadata-dir metadata/ --live-enrichment
```

### GitHub Actions

Trigger the `Sync Metadata` workflow manually with `dry_run: true` for preview or `dry_run: false` for live sync.

## Performance Architecture

### Current Performance Profile

| Operation | Bottleneck | Current | Notes |
|---|---|---|---|
| Governance export | 1 HTTP call per entity (aspect read) | ~0.05s per entity | 33 entities = ~1.5s |
| Enrichment export | 4 HTTP calls per dataset (tags, terms, domain, ESM) | ~0.04s per dataset | 146 datasets = ~5.5s |
| Sync (write) | 1 HTTP call per MCP | ~0.02s per MCP | Sequential per-entity emission |

### Extension Points for Future Optimization

**Batch reads during export** (`EntityHandler.export()`):
- Current: `graph.get_aspect(urn, AspectClass)` per entity (1 HTTP call each)
- Future: `graph.get_entities(entity_name, urns, aspects)` fetches multiple entities' aspects in a single call
- Impact: Reduces enrichment export from 4N HTTP calls to ~4 batch calls

**Batch writes during sync** (`WriteStrategy.emit_batch()`):
- Current: `graph.emit_mcp(mcp)` per MCP (1 HTTP call each)
- Future: `graph.emit_mcps(mcps)` sends multiple MCPs in fewer HTTP round-trips
- The `WriteStrategy` interface provides `emit_batch()` which accepts all MCPs for a handler phase
- Trade-off: batch emission loses per-MCP error granularity (one failure aborts the batch)

**Concurrent handler execution** (`SyncOrchestrator`):
- Current: all handlers run sequentially in dependency order
- Future: handlers with no dependency relationship can run concurrently (e.g., tag and domain handlers are independent)
- The `HandlerRegistry.get_sync_order()` topological sort already identifies which handlers are independent

**Progress tracking**:
- Export and sync operations log progress every 50 entities
- Enrichment scan logs progress showing datasets scanned vs enriched

## Assumptions

1. **Prod is greenfield** (or willing to accept dev URNs overwriting existing entities)
2. **All data sources are ingested into all DataHub environments** (identical dataset URNs). A dataset `urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.users,PROD)` must exist in both dev and prod DataHub.
3. **No manual edits in prod** (will be overwritten on next sync)
4. **Dev and prod DataHub APIs are accessible** from the CI/CD runner (network + auth token)
5. **Entity counts are manageable** for in-memory processing (pagination handles scale, but no streaming for very large deployments)
6. **Governance entity URNs are identical** in dev and prod (UUID passthrough). This requires entities to have been created via API/SDK with explicit IDs, or both environments to share the same creation history. If entities were created via the DataHub UI (which generates random UUIDs), URNs will differ and a `UrnMapper` implementation beyond `PassthroughMapper` is required.

## Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| No merge/conflict resolution | Prod enrichment overwritten on every sync | Don't edit prod directly; dev is authoritative |
| No deletion sync | Entities deleted in dev remain in prod | Manually delete orphans in prod |
| No drift detection | No comparison between export state and live DataHub | Re-run export to get latest state |
| No rollback | No undo for a bad sync | Re-run sync from dev (dev is authoritative) |
| No staging environment | No pre-production validation | Use `--dry-run` mode |
| Glossary term names may be null | DataHub doesn't always populate `GlossaryTermInfo.name` | Names derived from URN when null |
| `termSource` field may contain URNs | Some DataHub instances store parent node URN instead of INTERNAL/EXTERNAL | Passed through as-is to preserve fidelity |
| Enrichment not version-controlled | `enrichment.json` is a snapshot, not a diff | Re-export before each sync |
| Only dataset enrichment | Tags/terms on other entity types (charts, dashboards) not exported | Add handlers for other entity types |
| UPSERT replaces entire aspect | Writing 1 tag to a dataset replaces ALL existing tags, not just adding one | By design (dev is authoritative). Future: MergeStrategy or PatchStrategy. |

## Roadmap

### Phase 1 (Current): PoC -- Tags, Glossary, Domains + Enrichment
- [x] Tag, glossary, domain export and sync
- [x] Dataset enrichment (dataset-level + field-level tags/terms/domains)
- [x] Hierarchical ordering (topological sort)
- [x] System entity filtering
- [x] Per-entity result tracking with progress logging
- [x] Dry-run mode
- [x] GitHub Actions workflow

### Phase 2: Extended Governance Entity Types
- [ ] `StructuredPropertyHandler` -- deterministic URNs via qualifiedName; easiest to add
- [ ] `OwnershipTypeHandler` -- custom ownership types (skip `__system__*`)
- [ ] `DataProductHandler` -- requires UrnMapper for asset list dataset URNs
- [ ] `FormHandler` -- UUID URNs, not connector-discoverable; skip formAssignmentStatus

### Phase 3: Extended Enrichment & Aspects
- [ ] `ownership` aspect on governance entities and data assets
- [ ] `editableDatasetProperties` (user-authored dataset descriptions)
- [ ] `editableContainerProperties`, `editableChartProperties`, `editableDashboardProperties`, etc.
- [ ] `institutionalMemory` (documentation links)
- [ ] `glossaryRelatedTerms` (cross-references between terms; requires two-pass)
- [ ] `deprecation` aspect (deprecated flag, note, replacement URN)
- [ ] Chart, dashboard, dataJob, dataFlow, container, notebook enrichment (tags, terms, domains)
- [ ] ML entity enrichment (mlModel, mlModelGroup, mlFeature, etc.)
- [ ] `schemaField` entity enrichment (newer column-level model)

### Phase 4: Advanced Write Strategies
- [ ] `MergeStrategy` -- read-merge-write for non-destructive updates
- [ ] `PatchStrategy` -- PATCH-based additive writes via SDK patch builders
- [ ] Write strategy selection via CLI (`--write-strategy merge`)

### Phase 5: Advanced URN Mapping
- [ ] `NameBasedMapper` -- match entities by display name between environments (for UUID-based envs)
- [ ] `PatternMapper` -- regex-based URN transform for cross-environment URN divergence
- [ ] `MappingFileMapper` -- pre-computed mapping file for human review

### Phase 6: Operational Maturity & Conditional Entity Types
- [ ] Deletion sync (track removed entities)
- [ ] Drift detection (compare export vs live state)
- [ ] Schema validation (`EntityHandler.validate()` with JSON Schema)
- [ ] Checkpointing and resume on failure
- [ ] Batch reads via `graph.get_entities()` for enrichment export
- [ ] Concurrent handler execution for independent phases
- [ ] Manual lineage sync (distinguishing user-created vs ingested)
- [ ] `BusinessAttributeHandler` -- if business attributes are in use
- [ ] `ApplicationHandler` -- if application grouping is in use

## Adding New Entity Types

1. Subclass `EntityHandler` in `src/handlers/`
2. Implement `entity_type`, `export()`, `build_mcps()`
3. Optionally override `dependencies`, `is_system_entity()`, `validate()`
4. Register in `src/handlers/__init__.py`

No changes to orchestrator, CLI, or workflow needed.

## Extension Points

| Interface | Purpose | PoC Default |
|---|---|---|
| `EntityHandler` | Add new entity types | Tag, Glossary, Domain, Enrichment handlers |
| `UrnMapper` | Transform URNs between environments | `PassthroughMapper` (identity) |
| `WriteStrategy` | Control how MCPs are written | `OverwriteStrategy` (full UPSERT) |
| `WriteStrategy.emit_batch()` | Batch-optimized writes across entities | Delegates to per-entity `emit()` |
| `HandlerRegistry` | Discover and order handlers | Topological sort on dependencies |

## Tests

```bash
pytest tests/ -v
```
