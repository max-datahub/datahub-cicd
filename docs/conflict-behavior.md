# URN Conflicts, Duplicates, and Write Behavior in DataHub

This document describes how DataHub handles entity creation across its two write paths (GraphQL mutations used by the UI, and MCP/SDK ingestion used by the CI/CD pipeline), which scenarios produce duplicates or errors, and the implications for `datahub-cicd`.

## Two Write Paths, Different Rules

DataHub exposes two ways to create and update entities. They share the same storage layer but apply **different validation rules** at the API boundary.

### 1. GraphQL Mutations (UI / Frontend)

When a user creates a tag, domain, glossary term, etc. through the DataHub UI, the frontend calls a GraphQL mutation (e.g., `createTag`, `createDomain`). These mutations are handled by resolver classes in `datahub-graphql-core` that apply validation **before** writing:

- **URN existence check**: Every resolver calls `entityClient.exists(urn)` before creating. If the URN already exists, it throws `"This Tag already exists!"` (or equivalent). This prevents re-creating an entity with the same URN.
- **Name uniqueness check** (some types only): Glossary terms and domains additionally search for siblings with the same display name under the same parent. If found, creation is rejected.
- **UUID generation**: If the caller does not provide an explicit `id`, a `UUID.randomUUID()` is generated. This means two entities created via the UI with the same name will get different URNs (different UUIDs).

### 2. MCP / SDK Ingestion (CI/CD Pipeline, Ingestion)

When the CI/CD pipeline (or any ingestion source) writes an entity, it emits a `MetadataChangeProposal` (MCP) with `changeType=UPSERT`. This goes through `EntityServiceImpl.ingestProposal()`:

- **No existence check**: The MCP path does not check whether the URN already exists. It always upserts.
- **No name uniqueness check**: There is no validation that the display name is unique among siblings or globally.
- **Caller controls the URN**: The MCP includes the full `entityUrn`. The caller decides the URN -- it can be a UUID, a deterministic ID, or anything else.

This is the path used by `datahub-cicd`'s `OverwriteStrategy`, which calls `graph.emit_mcp()` for each aspect.

## URN Generation by Entity Type

| Entity Type | Key Field | URN Format | UI Default | Example |
|---|---|---|---|---|
| tag | `TagKey.name` | `urn:li:tag:{name}` | `UUID.randomUUID()` | `urn:li:tag:5f6a8b2c-...` |
| glossaryNode | `GlossaryNodeKey.name` | `urn:li:glossaryNode:{name}` | `UUID.randomUUID()` | `urn:li:glossaryNode:a1b2c3d4-...` |
| glossaryTerm | `GlossaryTermKey.name` | `urn:li:glossaryTerm:{name}` | `UUID.randomUUID()` | `urn:li:glossaryTerm:e5f6a7b8-...` |
| domain | `DomainKey.id` | `urn:li:domain:{id}` | `UUID.randomUUID()` | `urn:li:domain:9c0d1e2f-...` |
| dataProduct | `DataProductKey.id` | `urn:li:dataProduct:{id}` | `UUID.randomUUID()` | `urn:li:dataProduct:3a4b5c6d-...` |

All governance entity types use random UUIDs when created through the UI. The URN is derived from this UUID, **not** from the display name. Two tags both named "PII" created via the UI will have completely different URNs.

When created via the SDK/MCP path, the caller provides the full URN, so it can be deterministic (e.g., `urn:li:tag:pii`) or a UUID -- the choice is the caller's.

## Validation Matrix

This table shows what validation each write path performs for each entity type:

| Entity Type | GraphQL: URN Exists | GraphQL: Name Unique | MCP: URN Exists | MCP: Name Unique |
|---|---|---|---|---|
| **tag** | Rejects | No check | No check (upserts) | No check |
| **glossaryNode** | Rejects | No check | No check (upserts) | No check |
| **glossaryTerm** | Rejects | Rejects (within parent) | No check (upserts) | No check |
| **domain** | Rejects | Rejects (within parent) | No check (upserts) | No check |
| **dataProduct** | Rejects | No check | No check (upserts) | No check |

Source code references:
- `CreateTagResolver.java:61-64` -- URN existence check only
- `CreateGlossaryTermResolver.java:67,163-178` -- URN existence + `validateGlossaryTermName()` searches siblings by parent
- `CreateGlossaryNodeResolver.java:57-61` -- URN existence check only
- `CreateDomainResolver.java:72-90` -- URN existence + `DomainUtils.hasNameConflict()` searches by name and parent
- `DataProductService.java:77-80` -- URN existence check only
- `EntityServiceImpl.ingestProposal()` -- always upserts, no pre-checks

## Conflict Scenarios

### Scenario 1: Same URN, Same Properties (Idempotent Re-sync)

**What happens**: The pipeline exports an entity from dev and syncs it to prod. Then syncs again without changes.

| Path | Outcome |
|---|---|
| MCP (pipeline) | Silent overwrite. The aspect is replaced with identical content. No error. This is the normal steady-state for `datahub-cicd`. |
| GraphQL (UI) | Error: `"This Tag already exists!"`. The UI prevents re-creating an entity with the same URN. |

**Risk**: None. This is the designed behavior.

### Scenario 2: Same URN, Different Properties (Update)

**What happens**: A tag's description is changed in dev, then synced to prod.

| Path | Outcome |
|---|---|
| MCP (pipeline) | Silent overwrite. The aspect is replaced with the new content. The old description is gone. |
| GraphQL (UI) | Error: `"This Tag already exists!"`. The UI's `createTag` mutation cannot update -- it only creates. (Updates go through separate `updateTag` mutations.) |

**Risk**: None for the pipeline. This is the intended UPSERT behavior.

### Scenario 3: Different URN, Same Name (Duplicate)

**What happens**: A tag named "PII" exists in prod (created via UI with URN `urn:li:tag:uuid-aaa`). The pipeline syncs a different tag also named "PII" from dev (URN `urn:li:tag:uuid-bbb`).

| Path | Outcome |
|---|---|
| MCP (pipeline) | **Silent duplicate created.** Prod now has two tags named "PII" with different URNs. No error. DataHub does not enforce name uniqueness at the storage layer. |
| GraphQL (UI) | **Depends on entity type.** For tags, glossary nodes, and data products: the UI would generate a new UUID, so the URNs wouldn't collide and it would succeed (creating a duplicate). For glossary terms and domains: the UI rejects creation with `"Glossary Term with this name already exists at this level"` or `"already exists in this domain"`. |

**Risk**: **High for the pipeline.** This is the primary duplicate scenario. It occurs when:
- Governance entities were created independently in both dev and prod (each got a different UUID)
- The pipeline syncs dev's version to prod, creating a second entity with the same display name but a different URN
- Users see two "PII" tags in the UI with no way to distinguish them

### Scenario 4: Cross-Environment URN Divergence (Enrichment Breaks)

**What happens**: Dev and prod have the same logical tag "PII" but with different URNs because they were created independently. A dataset in prod has the prod-URN tag applied. The pipeline syncs dev's enrichment, which references the dev-URN tag.

| Path | Outcome |
|---|---|
| MCP (pipeline) | The enrichment aspect (e.g., `globalTags`) is overwritten with dev's version. The prod-only tag reference is removed. The dev-URN tag reference may point to a tag that doesn't exist in prod (if it wasn't synced), creating a **dangling reference**. DataHub does not validate URN references at write time (except for `structuredProperties`). |

**Risk**: **High.** The UI will show a broken tag reference (or silently hide it). The original prod tag assignment is lost.

### Scenario 5: Entity Deleted in Dev, Still Exists in Prod (Orphan)

**What happens**: A tag is deleted in dev. The pipeline exports (the tag is absent from the export). Sync runs.

| Path | Outcome |
|---|---|
| MCP (pipeline, default) | **No deletion occurs.** The pipeline only writes entities present in the export. DataHub's ingestion model is additive (UPSERT never deletes). The orphaned tag remains in prod indefinitely. |
| MCP (pipeline, `--apply-deletions`) | **Soft-delete propagated.** The export detects the soft-deleted tag via `RemovedStatusFilter.ONLY_SOFT_DELETED` and writes it to `deletions.json`. Sync applies `soft_delete_entity()` to prod. Enrichment reference cleanup happens naturally via UPSERT sync (full aspect replace removes stale tag references from datasets). |

**Risk**: Low with `--apply-deletions` enabled. Without it, stale governance entities accumulate in prod. Note: soft-deleted entities are only detectable within the GC retention window (default 10 days). After GC hard-deletes them, the pipeline cannot detect or propagate the deletion.

### Scenario 6: Concurrent Edits in Prod (Overwrite)

**What happens**: A prod admin adds a third owner to a dataset. The pipeline syncs the ownership aspect from dev, which only has two owners.

| Path | Outcome |
|---|---|
| MCP (pipeline) | The entire `ownership` aspect is replaced with dev's version. The prod-only owner is **silently removed**. DataHub UPSERT replaces the full aspect -- there is no merge of individual items within a list. |

**Risk**: **High if prod edits are expected.** This is inherent to the UPSERT-only write model. Mitigation requires a `MergeStrategy` (read-merge-write) or `PatchStrategy` (SDK patch builders), neither of which are implemented yet.

### Scenario 7: Parent Created After Child (Ordering Violation)

**What happens**: A glossary term references a parent node that hasn't been synced yet.

| Path | Outcome |
|---|---|
| MCP (pipeline) | **Silent success with dangling reference.** DataHub does not validate that `parentNode` URN exists. The term is created with a `parentNode` pointing to a non-existent node. The UI may display the term at the root level or show a broken hierarchy. |
| GraphQL (UI) | The `CreateGlossaryTermResolver` checks `entityClient.exists(parentNode)` before creation and would reject if the parent doesn't exist. |

**Risk**: Low for `datahub-cicd` because the pipeline uses topological sort (`HandlerRegistry.get_sync_order()`) to ensure parents are synced before children. However, if a parent entity fails to sync, the child proceeds with a dangling `parentNode` reference.

## Summary: Which Entity Types Allow Duplicates?

| Entity Type | Duplicate via MCP? | Duplicate via UI? | Name Dedup in UI? |
|---|---|---|---|
| **tag** | Yes (always) | Yes (different UUIDs) | No |
| **glossaryNode** | Yes (always) | Yes (different UUIDs) | No |
| **glossaryTerm** | Yes (always) | No (rejected within same parent) | Yes (within parent) |
| **domain** | Yes (always) | No (rejected within same parent) | Yes (within parent) |
| **dataProduct** | Yes (always) | Yes (different UUIDs) | No |

Key insight: **The MCP path (used by the pipeline) never prevents duplicates.** It relies entirely on the caller providing the correct URN. If dev and prod have different URNs for the same logical entity, the pipeline will create duplicates.

## Implications for datahub-cicd

### Current Safety Model

The pipeline's safety relies on two assumptions documented in `README.md`:

1. **URN passthrough**: Governance entities in dev and prod have identical URNs (same UUIDs). This is true if entities are created only in dev and synced to prod, or if both environments share the same creation history.
2. **Dev-authoritative**: Prod receives governance metadata exclusively via the pipeline. No one creates or edits governance entities directly in prod.

If either assumption is violated, duplicates and data loss can occur.

### When Things Go Wrong

| Violation | Consequence | Detection | Mitigation |
|---|---|---|---|
| Entity created independently in prod | Duplicate entity on next sync (same name, different URN) | Manual inspection: search for entities by name, look for multiple results | Delete the prod-created entity; or implement `NameBasedMapper` (Phase 5) |
| Entity created independently in both envs | Duplicate entity; enrichment references diverge | Compare exported JSON with prod state | Adopt deterministic URN creation via SDK/API with explicit IDs |
| Prod admin adds enrichment | Overwritten on next sync | Diff prod state before/after sync | Implement `MergeStrategy` (Phase 4); or prohibit direct prod edits |
| Entity deleted in dev | Orphan remains in prod (without `--apply-deletions`) | Compare export with prod entity list | Use `--include-deletions` / `--apply-deletions` to propagate soft-deletes; must export within GC retention window (default 10 days) |
| Parent sync fails, child succeeds | Dangling `parentNode` reference | Check sync results for parent failures | Re-run sync (idempotent); or add pre-flight validation |

### Recommended Safeguards

1. **Pre-flight duplicate detection**: Before syncing, query prod for entities with the same display name as entities being synced. Flag any name collisions where the URN differs.
2. **Deterministic URN creation policy**: Always create governance entities via the SDK/API with explicit, deterministic IDs (e.g., slugified names) rather than through the UI. This prevents UUID divergence.
3. **Prod edit lockdown**: Use DataHub policies to restrict direct governance entity creation/modification in prod to CI/CD service accounts only.
4. **Post-sync drift report**: After syncing, compare the set of governance URNs in prod against the export. Flag any URNs in prod that weren't in the export (potential orphans or independently-created entities).

## Appendix: Source Code References

### GraphQL Resolvers (validation layer)

| File | Entity | Validations |
|---|---|---|
| `datahub-graphql-core/.../resolvers/tag/CreateTagResolver.java` | tag | URN existence |
| `datahub-graphql-core/.../resolvers/glossary/CreateGlossaryNodeResolver.java` | glossaryNode | URN existence |
| `datahub-graphql-core/.../resolvers/glossary/CreateGlossaryTermResolver.java` | glossaryTerm | URN existence + name uniqueness within parent |
| `datahub-graphql-core/.../resolvers/domain/CreateDomainResolver.java` | domain | URN existence + name uniqueness within parent |
| `metadata-service/.../service/DataProductService.java` | dataProduct | URN existence |

### MCP Ingestion Path (no validation)

| File | Behavior |
|---|---|
| `metadata-io/.../entity/EntityServiceImpl.java` | `ingestProposal()` always upserts. `ChangeType.UPSERT` replaces existing aspects or creates new entities. No existence or name checks. |

### datahub-cicd Write Path

| File | Behavior |
|---|---|
| `src/write_strategy.py` (`OverwriteStrategy`) | Calls `graph.emit_mcp()` per MCP. Each MCP is an independent UPSERT. No pre-flight checks. |
| `src/urn_mapper.py` (`PassthroughMapper`) | Identity mapping. Assumes dev URN = prod URN. |
| `src/registry.py` | Topological sort ensures parents sync before children. |
