# datahub-cicd

CI/CD pipeline for syncing DataHub governance metadata (tags, glossary, domains, data products) and enrichment (tag/term/domain/ownership assignments on datasets, charts, dashboards, containers, and more) from dev to prod.

## Architecture

- **Unidirectional**: Dev DataHub is source of truth, prod is updated exclusively via CI/CD
- **UUID passthrough**: Governance entity URNs are identical in dev and prod
- **Full overwrite**: All writes use UPSERT (full replace). Dev wins.
- **Phased execution**: Entities synced in dependency order (tags -> glossary nodes -> terms -> domains -> data products -> enrichment)

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
| `dataProduct` | **Supported** | `dataProductProperties` (name, description, customProperties, assets) | Asset URNs mapped through `UrnMapper`. Depends on domain handler. 10 products validated against local cluster. |
| `ownershipType` | **Planned** | `ownershipTypeInfo` (name, description) | Custom types only; system types (`__system__*` prefix, always UUID) are identical in all envs and skipped. |
| `form` | **Planned** | `formInfo` (name, description, type, prompts) | UUID URNs, not discoverable by connector. Skip `formAssignmentStatus` (runtime state). |
| `businessAttribute` | **Planned (Phase 6)** | `businessAttributeInfo` | Sparse entity (only ownership + institutionalMemory). Defines reusable column-level metadata templates. Referenced via `businessAttributes` aspect on `schemaField` only. Migrate if in use. |
| `application` | **Planned (Phase 6)** | `applicationProperties` | Full governance support (gT, glT, dom, own, sP, forms). Groups related data assets. `applications` aspect appears on datasets, charts, dashboards, etc. Migrate if in use. |

### Governance Aspects on Governance Entities (secondary metadata)

These aspects appear on the governance entities above. They are not synced in the PoC but are identified for future phases.

| Aspect | On Entity Types | Status | Justification |
|---|---|---|---|
| `ownership` | tag, glossaryNode, glossaryTerm, domain, dataProduct, form | **Planned** (on governance entities themselves) | Owner URNs (corpuser/corpGroup) are identity-based (same SSO = same URN). Ownership on data assets (datasets, charts, etc.) is **Supported** via enrichment. |
| `glossaryRelatedTerms` | glossaryTerm | **Planned** | Cross-references between terms. All referenced term URNs must exist before writing. Requires two-pass sync. |
| `institutionalMemory` | glossaryNode, glossaryTerm, domain, dataProduct | **Planned** | Links to external documentation (URLs). No URN references inside -- straightforward to migrate. |
| `structuredProperties` | glossaryNode, glossaryTerm, domain, dataProduct, form | **Planned** | Property assignments. Property URNs are deterministic (`qualifiedName`-based). |
| `deprecation` | tag, glossaryTerm, dataset, chart, dashboard, container, dataJob, dataFlow, notebook, schemaField | **Planned (Phase 3)** | Contains deprecated flag, note, actor (corpuser URN), and replacement entity URN. Under PoC constraints, all URNs map via passthrough. If deprecated in dev, should be deprecated in prod. |
| `displayProperties` | glossaryNode, domain | **Planned** | Visual display settings. No URN references. |

### Enrichment Aspects on Data Assets (assignments synced dev -> prod)

| Aspect | On Entity Types | Status | Justification |
|---|---|---|---|
| `globalTags` | dataset, chart, dashboard, container, dataFlow, dataProduct | **Supported** | Tag assignments. Filtered to governance URNs. Generic handler covers all entity types. |
| `glossaryTerms` | dataset, chart, dashboard, container, dataFlow, dataProduct | **Supported** | Term assignments. Filtered to governance URNs. |
| `domains` | dataset, chart, dashboard, container, dataFlow, dataProduct | **Supported** | Domain membership. Filtered to governance URNs. |
| `ownership` | dataset, chart, dashboard, container, dataFlow, dataProduct | **Supported** | Owner assignments (corpuser/corpGroup URNs). Not filtered by governance URNs -- owner URNs are identity-based. |
| `editableSchemaMetadata` (field-level tags/terms) | dataset | **Supported** | Per-column tag and term assignments. Filtered to governance URNs. Only exists on `dataset`. |
| `globalTags` / `glossaryTerms` / `domains` / `ownership` on remaining assets | notebook, mlModel, mlFeature, mlModelGroup, mlFeatureTable | **Planned (Phase 3)** | Same pattern -- add the entity type to `ENRICHABLE_ENTITY_TYPES` list. |
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
| Data Products | Product definitions (name, description, assets) | Full UPSERT | Flat (depends on domain) | N/A |

### Enrichment Export & Sync

Enrichment covers tag, term, domain, and ownership assignments on data assets. Each entity type is exported into its own JSON file.

| Entity Type | Aspects Exported | Notes |
|---|---|---|
| dataset | `globalTags`, `glossaryTerms`, `domains`, `ownership`, `editableSchemaMetadata` | Field-level tags/terms via editableSchemaMetadata. Primary enrichment target. |
| chart | `globalTags`, `glossaryTerms`, `domains`, `ownership` | |
| dashboard | `globalTags`, `glossaryTerms`, `domains`, `ownership` | |
| container | `globalTags`, `glossaryTerms`, `domains`, `ownership` | Databases/schemas. Enrichment on ingestion-sourced entities. |
| dataFlow | `globalTags`, `glossaryTerms`, `domains`, `ownership` | Airflow DAGs, etc. |
| dataProduct | `globalTags`, `glossaryTerms`, `domains`, `ownership` | Enrichment on data products (in addition to definition sync). |

Additional entity types (dataJob, notebook, ML entities) can be added to the `ENRICHABLE_ENTITY_TYPES` list in `src/handlers/enrichment.py`. dataJob is excluded by default due to high entity count with typically no user-authored enrichment.

### Enrichment Filtering

- Tags, terms, and domains are filtered to only include references to governance entities that were exported. This prevents syncing references to entities that don't exist in prod.
- Ownership is **not** filtered -- owner URNs (corpuser/corpGroup) are identity-based and the same across environments (same SSO = same URN).

## Usage

### Export governance + enrichment from dev

```bash
export DATAHUB_DEV_URL=http://dev-datahub:8080
export DATAHUB_DEV_TOKEN=your-token

python -m src.cli.export_cmd --output-dir metadata/
```

This exports governance definitions and enrichment JSON files to `metadata/`:

**Governance definitions:**
- `tag.json`, `glossaryNode.json`, `glossaryTerm.json`, `domain.json`, `dataProduct.json`

**Enrichment (tags, terms, domains, ownership on data assets):**
- `enrichment.json` -- dataset enrichment (includes field-level via editableSchemaMetadata)
- `chartEnrichment.json`, `dashboardEnrichment.json`, `containerEnrichment.json`
- `dataFlowEnrichment.json`, `dataProductEnrichment.json`

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
| Governance export | 1 HTTP call per entity (aspect read) | ~0.05s per entity | 43 entities = ~0.6s |
| Dataset enrichment | 5 HTTP calls per dataset (tags, terms, domain, ownership, ESM) | ~0.05s per dataset | 146 datasets = ~7s |
| Non-dataset enrichment | 4 HTTP calls per entity (tags, terms, domain, ownership) | ~0.04s per entity | 43 entities (chart+dashboard+container+dataFlow+dataProduct) = ~1.7s |
| Sync (write) | 1 HTTP call per MCP | ~0.02s per MCP | Sequential per-entity emission |
| **Full export** | | **~8s total** | 226 entities across 11 types |

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

1. **Governance entity URNs are identical in dev and prod** (UUID passthrough). DataHub generates random UUIDs when entities are created via the UI. This pipeline assumes governance entities were created via API/SDK with explicit IDs, or both environments share the same creation history. If URNs diverge, a `UrnMapper` implementation beyond `PassthroughMapper` is required (see Phase 5). There is no name-based deduplication -- creating a tag named "PII" in both environments independently produces two different URNs and the sync will create a duplicate.
2. **Dataset URNs are identical across environments** (many-to-many ingestion topology). Both dev and prod DataHub ingest from the same data sources, producing identical dataset URNs. If environments have different platform instances, database names, or env suffixes, dataset URNs will diverge and enrichment references will break silently (DataHub does not validate URN references at write time).
3. **Prod is greenfield or dev-authoritative**. Prod receives governance metadata exclusively via this pipeline. Any enrichment applied directly in prod (tags, terms, domain assignments, ownership) will be overwritten on the next sync because DataHub's UPSERT replaces the entire aspect, not individual items within it.
4. **No manual edits in prod**. DataHub's UPSERT write semantics mean every sync fully replaces each aspect. If a prod admin adds a third owner to a dataset, the next sync overwrites the ownership aspect with dev's version, silently removing the prod-only owner.
5. **Dev and prod DataHub APIs are accessible** from the CI/CD runner (network connectivity + auth token).
6. **Entity counts are manageable** for in-memory processing. The SDK's scroll-based pagination handles arbitrarily large entity sets, but all exported data is held in memory. For very large deployments (100k+ datasets), streaming or chunked processing may be needed.

## Limitations

### DataHub Data Model Constraints

These are inherent properties of DataHub's data model that affect any cross-environment sync tool, not limitations specific to this implementation.

| Constraint | Impact | Current Mitigation |
|---|---|---|
| **UPSERT = full aspect replace** | Writing any aspect (tags, terms, ownership, etc.) replaces the entire previous value. There is no atomic add/remove of individual items within a list-valued aspect. | By design: dev is authoritative. Future: `MergeStrategy` (read-merge-write) or `PatchStrategy` (SDK patch builders). |
| **No write-time reference validation** | DataHub silently accepts aspects referencing non-existent entities (e.g., a tag assignment referencing a tag URN that was never created). Only `structuredProperties` validates at write time. For all other aspects (globalTags, glossaryTerms, domains, ownership, parentNode), dangling references are stored without error. | Dependency-ordered execution ensures definitions exist before assignments. However, if a definition sync fails, subsequent enrichment proceeds with dangling references. No pre-flight existence check. |
| **No transaction atomicity** | Each MCP is an independent write. A sync of 100 entities can leave 50 applied and 50 unapplied if the process fails midway. There is no multi-MCP atomic commit or server-side rollback. | Per-entity result tracking with error reporting. Re-running is safe (idempotent under full overwrite). No resume-from-checkpoint yet. |
| **No deletion propagation** | DataHub's ingestion model is additive (UPSERT creates/updates, never deletes). Entities deleted in dev remain in prod. Enrichment removed in dev may persist in prod if the dataset loses all enrichment (the handler exports nothing, leaving stale aspects). | Manual cleanup in prod. If a dataset retains _some_ enrichment, full overwrite correctly removes deleted items from the aspect. |
| **UUID URNs by default** | Most governance entity types (tags, glossary, domains, data products, forms, policies, ownership types) generate random UUID URNs when created via the DataHub UI. The same logical entity created independently in two environments will have different URNs, causing duplicates on sync. | UUID passthrough assumption (entities created only in dev). Future: `NameBasedMapper` for environments with independent entity creation. |
| **Dataset URNs encode environment specifics** | Dataset URNs include platform instance, database name, and environment/fabric type. The same physical table in dev and prod typically has different URNs unless ingestion is configured identically. | Many-to-many ingestion assumption (identical URNs). Future: `PatternMapper` or `MappingFileMapper` for divergent URNs. |
| **No provenance on aspects** | DataHub does not track whether an aspect was applied by a human, an ingestion pipeline, or a CI/CD tool. Enrichment authored in dev is indistinguishable from enrichment applied by automated processes. | Governance URN filtering: only enrichment referencing exported governance entities is synced. Ownership is synced unfiltered (identity-based URNs). |
| **System entity discrimination is ad-hoc** | There is no universal `isSystemEntity` flag. System tags use `__default_*` prefix, system ownership types use `__system__*` prefix, system policies have `editable: false`. Each entity type requires its own filtering rule, discovered empirically. | Per-handler `is_system_entity()` with type-specific rules. |
| **Entity discoverability gaps** | Three entity types (`dataHubPolicy`, `form`, `ownershipType`) lack `searchGroup` annotations in DataHub's entity registry, making them invisible to the standard `get_urns_by_filter()` API. They require entity-specific GraphQL queries. | Not yet relevant (these are planned entity types). Handlers will need custom discovery when implemented. |
| **Glossary term names may be null** | DataHub does not always populate `GlossaryTermInfo.name`. Terms created via certain code paths or older SDK versions may have null names. | Names derived from URN when null. |
| **`termSource` field may store URNs** | Some DataHub instances store the parent node URN in `GlossaryTermInfo.termSource` instead of the expected enum value (`INTERNAL`/`EXTERNAL`). | Passed through as-is. Mapped through `UrnMapper` if it looks like a URN. |

### Pipeline Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| No merge/conflict resolution | Prod enrichment overwritten on every sync | Don't edit prod directly; dev is authoritative |
| No deletion sync | Entities deleted in dev remain in prod indefinitely | Manually delete orphans in prod |
| No drift detection | No comparison between export state and live DataHub state | Re-run export to get latest state |
| No rollback | No undo for a bad sync | Re-run sync from dev (idempotent under full overwrite) |
| No resume on failure | Partial failures require re-running entire sync | Per-entity error tracking identifies failures; re-run is safe |
| No pre-flight validation | Referenced URNs are not checked for existence in prod before writing | Dependency ordering prevents most issues; edge cases require manual verification |
| No staging environment | No pre-production validation environment | Use `--dry-run` mode to preview MCPs |
| Enrichment not version-controlled | Enrichment JSON files are point-in-time snapshots, not diffs | Re-export before each sync to capture latest state |

## Roadmap

### Phase 1 (Complete): PoC -- Tags, Glossary, Domains + Dataset Enrichment
- [x] Tag, glossary, domain export and sync
- [x] Dataset enrichment (dataset-level + field-level tags/terms/domains)
- [x] Hierarchical ordering (topological sort)
- [x] System entity filtering
- [x] Per-entity result tracking with progress logging
- [x] Dry-run mode
- [x] GitHub Actions workflow

### Phase 2 (Complete): Data Products + Ownership + Multi-Entity Enrichment
- [x] `DataProductHandler` -- export/sync data product definitions with asset URN mapping
- [x] `ownership` aspect on datasets, charts, dashboards, containers, dataFlows, data products
- [x] `GenericEnrichmentHandler` -- reusable handler for tags/terms/domains/ownership on any entity type
- [x] Chart enrichment (11 charts validated)
- [x] Dashboard enrichment (5 dashboards validated)
- [x] Container enrichment (13 containers validated, 2 with domain assignments)
- [x] DataFlow enrichment (4 dataFlows)
- [x] DataProduct enrichment (10 products, all with domains + ownership)
- [ ] `StructuredPropertyHandler` -- deterministic URNs via qualifiedName; easiest to add
- [ ] `OwnershipTypeHandler` -- custom ownership types (skip `__system__*`)
- [ ] `FormHandler` -- UUID URNs, not connector-discoverable; skip formAssignmentStatus

### Phase 3: Extended Aspects
- [ ] `editableDatasetProperties` (user-authored dataset descriptions)
- [ ] `editableContainerProperties`, `editableChartProperties`, `editableDashboardProperties`, etc.
- [ ] `institutionalMemory` (documentation links)
- [ ] `glossaryRelatedTerms` (cross-references between terms; requires two-pass)
- [ ] `deprecation` aspect (deprecated flag, note, replacement URN)
- [ ] Notebook, ML entity enrichment (add to `ENRICHABLE_ENTITY_TYPES`)
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
5. **Required**: Add unit tests in `tests/test_handlers/`
6. **Required (if entity exists in OSS)**: Add to integration test seed (`tests/integration/seed.py`) and assertions (`tests/integration/test_export.py`)

No changes to orchestrator, CLI, or workflow needed.

**Test coverage policy**: Every `EntityHandler` must have unit tests covering `export()`, `build_mcps()`, system entity filtering, and hierarchy ordering (if applicable). For entity types available in DataHub OSS, integration tests must also seed the entity with all supported aspects and validate the export output. This ensures that changes to the SDK, DataHub API, or handler logic are caught before deployment.

## Extension Points

| Interface | Purpose | PoC Default |
|---|---|---|
| `EntityHandler` | Add new entity types | Tag, Glossary, Domain, DataProduct, Dataset/Generic Enrichment handlers |
| `UrnMapper` | Transform URNs between environments | `PassthroughMapper` (identity) |
| `WriteStrategy` | Control how MCPs are written | `OverwriteStrategy` (full UPSERT) |
| `WriteStrategy.emit_batch()` | Batch-optimized writes across entities | Delegates to per-entity `emit()` |
| `HandlerRegistry` | Discover and order handlers | Topological sort on dependencies |

## Tests

### Unit tests

```bash
pytest tests/ -v
```

### Integration tests

Integration tests require Docker and start a full DataHub OSS instance via the official Docker Quickstart. All services use non-standard ports (10000 offset) to avoid collisions with local DataHub instances.

| Service | Standard Port | Integration Port |
|---|---|---|
| GMS | 8080 | 18080 |
| Frontend | 9002 | 19002 |
| MySQL | 3306 | 13306 |
| Elasticsearch | 9200 | 19200 |
| Kafka | 9092 | 19092 |
| Schema Registry | 8081 | 18081 |
| Zookeeper | 2181 | 12181 |

```bash
# Run integration tests (starts Docker, seeds entities, exports, validates)
pytest -m integration tests/integration/ -v

# With custom GMS timeout (default 180s)
INTEGRATION_GMS_TIMEOUT=300 pytest -m integration tests/integration/ -v
```

The integration test suite:
1. Downloads the official DataHub quickstart `docker-compose.yml`
2. Starts all services with a dedicated project name (`datahub-cicd-integration`)
3. Seeds deterministic test entities covering all supported types:
   - Governance: tags (incl. system tag for filtering), glossary nodes (nested), glossary terms (with parents), domains (nested), data products (with assets)
   - Data assets: datasets, charts, dashboards, containers, dataflows
   - Enrichment: tags, terms, domains, ownership on all asset types + field-level tags/terms on datasets
4. Runs the export CLI and validates JSON output against expected entities
5. Tears down all containers on completion
