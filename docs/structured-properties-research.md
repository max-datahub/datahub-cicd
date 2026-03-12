# Structured Properties in DataHub: CI/CD Design Research

> Research conducted 2026-02-18 against DataHub OSS and DataHub Cloud source code.
> Target: Design CI/CD pipeline support for structured property governance in `datahub-cicd`.

---

## Table of Contents

1. [Entity Model & Schema](#1-entity-model--schema)
2. [Write Semantics](#2-write-semantics)
3. [Server-Side Validation](#3-server-side-validation)
4. [Version-Bump Escape Hatch](#4-version-bump-escape-hatch)
5. [Deletion Behavior](#5-deletion-behavior)
6. [Cloud-Specific: StructuredPropertySettings](#6-cloud-specific-structuredpropertysettings)
7. [Python SDK Surface](#7-python-sdk-surface)
8. [CI/CD Handler Design](#8-cicd-handler-design)

---

## 1. Entity Model & Schema

### PDL Schema Files

Located in `metadata-models/src/main/pegasus/com/linkedin/structured/`:

### StructuredPropertyDefinition (Entity Aspect)

- **Aspect Name**: `propertyDefinition`
- **Key Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `qualifiedName` | String | Fully qualified name (e.g., `io.acryl.datahub.myProperty`), searchable KEYWORD |
| `displayName` | Optional String | UI-friendly name |
| `valueType` | Urn | Data type URN (e.g., `urn:li:dataType:datahub.string`, `datahub.number`, `datahub.urn`, `datahub.rich_text`, `datahub.date`) |
| `typeQualifier` | Optional Map[String, Array[String]] | Specializes value type (e.g., `{"allowedTypes": ["urn:li:entityType:datahub.corpuser"]}`) |
| `allowedValues` | Optional Array[PropertyValue] | Restricts values to specific list |
| `cardinality` | Enum | `SINGLE` (default) or `MULTIPLE` |
| `entityTypes` | Array[Urn] | Which entity types this property can apply to |
| `immutable` | Boolean (default=false) | Once set, cannot be changed |
| `version` | Optional String | Allows breaking schema changes when monotonically incremented (14-digit timestamp) |
| `description` | Optional String | |
| `searchConfiguration` | Optional DataHubSearchConfig | Custom Elasticsearch indexing |
| `created` / `lastModified` | Optional AuditStamp | Audit trail |

### StructuredProperties (Value Aspect)

- **Aspect Name**: `structuredProperties`
- **Purpose**: Holds actual structured property values on entities (datasets, charts, dashboards, etc.)
- **Structure**: Array of `StructuredPropertyValueAssignment` objects

### StructuredPropertyValueAssignment

| Field | Type | Description |
|-------|------|-------------|
| `propertyUrn` | Urn | References the property definition URN |
| `values` | Array[PrimitivePropertyValue] | Array of values (supports SINGLE or MULTIPLE cardinality) |
| `created` / `lastModified` | Optional AuditStamp | When this assignment was created/modified |
| `attribution` | Optional MetadataAttribution | Who, why, and how this value was applied (provenance) |

### PrimitivePropertyValue

```
typeref PrimitivePropertyValue = union [string, double]
```

Values can be strings or doubles (numbers).

### PropertyValue (Allowed Values Definition)

| Field | Type | Description |
|-------|------|-------------|
| `value` | PrimitivePropertyValue | String or number |
| `description` | Optional String | Description of this allowed value |

### URN Format

```
urn:li:structuredProperty:{namespace}.{property_name}

Examples:
- urn:li:structuredProperty:io.acryl.privacy.retention
- urn:li:structuredProperty:io.datahubproject.test.deprecationDate
```

### Storage on Entities

Every entity supporting structured properties has a `structuredProperties` aspect:

```
dataset-urn:
  structuredProperties:
    properties:
      - propertyUrn: "urn:li:structuredProperty:io.acryl.datahub.dataQuality"
        values: ["high", "compliant"]
        created: {time: 1234567890, actor: "urn:li:corpuser:admin"}
      - propertyUrn: "urn:li:structuredProperty:io.acryl.datahub.sla"
        values: [99.9]
```

---

## 2. Write Semantics

### Definition Writes

**UPSERT**: Emitting a `MetadataChangeProposalWrapper` with `StructuredPropertyDefinitionClass` as the aspect replaces the entire definition aspect. The server-side `PropertyDefinitionValidator` runs pre-commit validation.

### Value Writes (Full Replacement)

**Key finding**: The `structuredProperties` aspect uses **full replacement** semantics, not true append-only.

In `UpsertStructuredPropertiesResolver.java`:
1. Fetches current `structuredProperties` aspect from the entity
2. Creates a new array of `StructuredPropertyValueAssignment`
3. For **existing** properties: finds them by propertyUrn, **replaces their values** entirely, updates `lastModified`
4. For **new** properties: appends them to the array with new `created` and `lastModified`
5. Emits a **single MetadataChangeProposal** with the full updated aspect

```java
// Existing property update:
propAssignment.setValues(new PrimitivePropertyValueArray(...));
propAssignment.setLastModified(auditStamp);

// New property addition:
properties.add(valueAssignment);
```

### Value Removal

Uses `StructuredPropertiesPatchBuilder.removeProperty(propertyUrn)` which generates a JSON patch `REMOVE` operation to remove the entire assignment from the array.

### Immutability

If a property is marked `immutable: true`, once a value is set on an entity, it cannot be changed. Enforced via validation logic. This is a one-way flag.

### GraphQL Mutation API

| Mutation | Description |
|----------|-------------|
| `createStructuredProperty` | Create new property definition |
| `updateStructuredProperty` | Update definition (append-only by default) |
| `deleteStructuredProperty` | Soft-delete property definition |
| `upsertStructuredProperties` | Add/update values on an entity |
| `removeStructuredProperties` | Remove properties from entity |

**GraphQL `updateStructuredProperty` is append-only**:
- `newAllowedValues`: Only adds to allowed values list
- `newEntityTypes`: Only adds entity types
- `setCardinalityAsMultiple`: Can change SINGLE→MULTIPLE but NOT MULTIPLE→SINGLE

### MCP-Based Writes are Full Replacement

When using `MetadataChangeProposalWrapper` to emit a `StructuredPropertyDefinitionClass`, the **entire aspect** is replaced. This means MCP-based writes CAN make breaking changes — **but the server-side `PropertyDefinitionValidator` blocks them unless the version-bump escape hatch is used**.

---

## 3. Server-Side Validation

### PropertyDefinitionValidator

**Source**: `metadata-io/src/main/java/com/linkedin/metadata/structuredproperties/validation/PropertyDefinitionValidator.java`

Extends `AspectPayloadValidator` and runs at `validatePreCommitAspects` time (after the MCP is proposed but before it's committed).

#### Validation checks performed:

1. **Soft-delete check**: Cannot mutate a soft-deleted property definition
2. **Version format check**: If `version` is set, must match `[0-9]{14}` (14-digit pattern)
3. **URN ID check**: Cannot contain spaces
4. **Qualified name check**: Cannot contain spaces
5. **Allowed types check**: `typeQualifier.allowedTypes` must be valid entity type URNs that exist
6. **Value type check**: Must be a valid `urn:li:dataType:*` or `urn:li:logicalType:*` URN that exists

#### Backwards-compatibility checks (only on updates, when `previousSystemAspect != null`):

| Check | Error Message | Bypass via Version? |
|-------|--------------|---------------------|
| Value type changed | "Value type cannot be changed as this is a backwards incompatible change" | Yes |
| Cardinality MULTIPLE→SINGLE | "Property definition cardinality cannot be changed from MULTI to SINGLE" | Yes |
| Qualified name changed | "Cannot change the fully qualified name of a Structured Property" | **No** |
| Allowed values removed | "Cannot restrict values that were previously allowed" | Yes |
| Allowed values added to unrestricted | "Cannot restrict values that were previously allowed" | Yes |

**Critical**: Changing `qualifiedName` is ALWAYS blocked, even with a version bump.

#### Allowed values removal logic (detailed):

```java
if (newDefinition.getAllowedValues() != null) {
    if ((!previousDefinition.hasAllowedValues() || previousDefinition.getAllowedValues() == null)
        && !allowBreakingWithVersion(previousDefinition, newDefinition, item, exceptions)) {
        // Adding allowed values to previously unrestricted property → blocked without version bump
        exceptions.addException(item, "Cannot restrict values that were previously allowed");
    } else if (!allowBreakingWithVersion(previousDefinition, newDefinition, item, exceptions)) {
        // Check each previous allowed value is still present
        Set<PrimitivePropertyValue> newAllowedValues = newDefinition.getAllowedValues().stream()
            .map(PropertyValue::getValue).collect(Collectors.toSet());
        for (PropertyValue value : previousDefinition.getAllowedValues()) {
            if (!newAllowedValues.contains(value.getValue())) {
                exceptions.addException(item, "Cannot restrict values that were previously allowed");
            }
        }
    }
}
```

### StructuredPropertiesValidator

Validates value assignments on entities (the `structuredProperties` aspect). Key validation:

- Values must match the property's declared `valueType`
- If `allowedValues` is set, values must be in the allowed set
- If `cardinality` is `SINGLE`, only one value allowed
- If `cardinality` is `MULTIPLE`, array is allowed
- Property definition must exist and not be soft-deleted

---

## 4. Version-Bump Escape Hatch

### `allowBreakingWithVersion()` Method

```java
private static boolean allowBreakingWithVersion(
    StructuredPropertyDefinition oldDefinition,
    StructuredPropertyDefinition newDefinition,
    ChangeMCP item,
    ValidationExceptionCollection exceptions) {

    final String oldVersion = oldDefinition.getVersion(GetMode.NULL);
    final String newVersion = newDefinition.getVersion(GetMode.NULL);

    // Version cannot contain "."
    if (newVersion != null && newVersion.contains(".")) {
        exceptions.addException(item,
            String.format("Invalid version `%s` cannot contain the `.` character.", newVersion));
    }

    // Case 1: No old version, new version set → allow breaking change
    if (oldVersion == null && newVersion != null) {
        return true;
    }
    // Case 2: Both have versions → new must be greater (case-insensitive)
    else if (newVersion != null) {
        return newVersion.compareToIgnoreCase(oldVersion) > 0;
    }
    // Case 3: No new version → don't allow breaking change
    return false;
}
```

### Key behaviors:

1. **Version format**: Must be exactly 14 digits matching `[0-9]{14}`. Recommended: timestamps like `20260218120000`.
2. **Monotonically increasing**: `newVersion.compareToIgnoreCase(oldVersion) > 0` — string comparison, not numeric.
3. **Cannot contain `.`**: Explicitly blocked.
4. **First version**: Going from `null` to any valid version always allows breaking change.
5. **Breaking changes allowed with version bump**:
   - Remove allowed values
   - Add allowed values to previously unrestricted property
   - Change value type
   - Change cardinality MULTIPLE→SINGLE
6. **NOT bypassed by version bump**:
   - Changing `qualifiedName` (always blocked)

### What the version bump does NOT do:

- **No migration**: Existing value assignments on entities are NOT automatically updated/validated when the definition changes
- **No cleanup**: Old values that no longer conform to the new definition remain on entities
- **No write protection**: After a breaking change, the `StructuredPropertiesValidator` may reject new writes that reference removed allowed values

### Open questions for empirical validation:

1. After removing an allowed value, do existing assignments with that value still read back?
2. Can new assignments be written with a removed allowed value?
3. What happens to MULTIPLE-cardinality values when cardinality is changed to SINGLE?
4. Does the StructuredPropertiesValidator enforce against the NEW definition immediately?

---

## 5. Deletion Behavior

### Soft Delete

Structured properties support soft-delete via the `status` aspect:

```java
// Via GraphQL:
deleteStructuredProperty(input: DeleteStructuredPropertyInput!) → Boolean

// Via Python SDK:
graph.soft_delete_entity(urn)
```

### PropertyDefinitionDeleteSideEffect

**Source**: `metadata-io/src/main/java/com/linkedin/metadata/structuredproperties/hooks/PropertyDefinitionDeleteSideEffect.java`

When a structured property is soft-deleted, a **post-commit side effect** runs:

1. Triggered by changes to `propertyDefinition` or `structuredPropertyKey` aspects
2. Uses `EntityWithPropertyIterator` to scroll through ALL entities that have this property assigned
3. For each entity, generates a JSON PATCH to REMOVE the property from the `structuredProperties` aspect:

```java
GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
patchOp.setOp(PatchOperationType.REMOVE.getValue());
patchOp.setPath(String.format("/properties/%s", propertyUrn.toString()));
```

**Key behavior**:
- Cleanup is **asynchronous** — the delete call returns immediately, cleanup happens in background
- Uses scroll-based iteration with `SEARCH_SCROLL_SIZE = 1000`
- Only processes entities that have the property assigned (found via search)
- After cleanup, the property definition is marked as deleted but remains in the system

### Validation Protection After Soft-Delete

`PropertyDefinitionValidator.softDeleteCheck()` prevents any mutation to a soft-deleted property:

```java
if (aspect != null && new Status(aspect.data()).isRemoved()) {
    return Optional.of(AspectValidationException.forItem(item,
        "Cannot mutate a soft deleted Structured Property Definition"));
}
```

---

## 6. Cloud-Specific: StructuredPropertySettings

### StructuredPropertySettings Aspect

**Aspect Name**: `structuredPropertySettings`
**Only available in DataHub Cloud**

| Field | Type | Description |
|-------|------|-------------|
| `isHidden` | Boolean (default=false) | Hide property from UI |
| `showInAssetSummary` | Boolean (default=false) | Show in asset summary sidebar |
| `showAsAssetBadge` | Boolean (default=false) | Show as badge on assets |
| `showInSearchFilters` | Boolean (default=false) | Enable as search filter |
| `showInColumnsTable` | Boolean (default=false) | Show in columns table |
| `hideInAssetSummaryWhenEmpty` | Boolean (default=false) | Only show in summary when value is set. Requires `showInAssetSummary=true` |

### GraphQL API (Cloud only)

```graphql
type StructuredPropertyEntity {
    definition: StructuredPropertyDefinition!
    settings: StructuredPropertySettings  # Cloud only
}

input UpdateStructuredPropertyInput {
    # ... standard fields ...
    settings: StructuredPropertySettingsInput  # Cloud only
}
```

### CI/CD Implications

- Settings are a separate aspect from the definition — can be synced independently
- Settings validation: `hideInAssetSummaryWhenEmpty` requires `showInAssetSummary=true`
- OSS DataHub will ignore/drop the settings aspect (no entity spec for it)

---

## 7. Python SDK Surface

### Core Classes & Imports

```python
# Schema classes (for MCP construction)
from datahub.metadata.schema_classes import (
    StructuredPropertyDefinitionClass,       # Define property type/constraints
    StructuredPropertiesClass,               # Container for assigned values
    StructuredPropertyValueAssignmentClass,  # Individual value assignment
    PropertyValueClass,                      # Allowed values in enum properties
)

# MCP wrapper
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# Graph client
from datahub.ingestion.graph.client import DataHubGraph

# URN utilities
from datahub.utilities.urns.urn import Urn
```

### Creating a Property Definition

```python
property_def = StructuredPropertyDefinitionClass(
    qualifiedName="io.acryl.privacy.retention",
    valueType=Urn.make_data_type_urn("string"),
    description="Data retention policy",
    entityTypes=[Urn.make_entity_type_urn("dataset")],
    cardinality="SINGLE",  # or "MULTIPLE"
    allowedValues=[
        PropertyValueClass(value="30d", description="30 days"),
        PropertyValueClass(value="90d", description="90 days"),
    ],
    version=None,  # Set to 14-digit string to allow breaking changes
)

mcp = MetadataChangeProposalWrapper(
    entityUrn="urn:li:structuredProperty:io.acryl.privacy.retention",
    aspect=property_def,
)

graph.emit_mcp(mcp)
```

### URN Helper Methods

```python
Urn.make_data_type_urn("string")      # → urn:li:dataType:datahub.string
Urn.make_data_type_urn("number")      # → urn:li:dataType:datahub.number
Urn.make_data_type_urn("date")        # → urn:li:dataType:datahub.date
Urn.make_data_type_urn("rich_text")   # → urn:li:dataType:datahub.rich_text
Urn.make_data_type_urn("urn")         # → urn:li:dataType:datahub.urn

Urn.make_entity_type_urn("dataset")   # → urn:li:entityType:datahub.dataset
Urn.make_entity_type_urn("chart")     # → urn:li:entityType:datahub.chart

Urn.make_structured_property_urn("io.acryl.test")
# → urn:li:structuredProperty:io.acryl.test (idempotent)
```

### Assigning Values to Entities

```python
mcp = MetadataChangeProposalWrapper(
    entityUrn="urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.mytable,PROD)",
    aspect=StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.privacy.retention",
                values=["30d"],  # Always a list, even for SINGLE cardinality
            ),
        ]
    ),
)

graph.emit_mcp(mcp)
```

### Reading Values Back

```python
structured_props = graph.get_aspect(
    entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.mytable,PROD)",
    aspect_type=StructuredPropertiesClass,
)

if structured_props:
    for prop in structured_props.properties:
        print(f"Property: {prop.propertyUrn}")
        print(f"Values: {prop.values}")
```

### Reading a Property Definition

```python
prop_def = graph.get_aspect(
    entity_urn="urn:li:structuredProperty:io.acryl.privacy.retention",
    aspect_type=StructuredPropertyDefinitionClass,
)

if prop_def:
    print(f"Name: {prop_def.qualifiedName}")
    print(f"Value type: {prop_def.valueType}")
    print(f"Cardinality: {prop_def.cardinality}")
    print(f"Version: {prop_def.version}")
    if prop_def.allowedValues:
        for av in prop_def.allowedValues:
            print(f"  Allowed: {av.value}")
```

### Listing All Structured Properties

```python
for urn in graph.get_urns_by_filter(entity_types=["structuredProperty"]):
    print(urn)
```

### High-Level API (Alternative)

```python
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)

# Create from YAML
StructuredProperties.create(file="properties.yaml", graph=graph)

# Fetch one
prop = StructuredProperties.from_datahub(
    graph=graph,
    urn="urn:li:structuredProperty:io.acryl.privacy.retention"
)

# List all
for prop in StructuredProperties.list(graph):
    print(f"{prop.id}: {prop.type}")
```

### Patch Builders

```python
from datahub.specific.dataset import DatasetPatchBuilder

patcher = DatasetPatchBuilder(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
)

# Upsert a property value
patcher.set_structured_property(
    key="io.acryl.privacy.retention",
    value="30d"
)

# Remove a property
patcher.remove_structured_property(key="io.acryl.privacy.retention")

# Emit patches
for mcp in patcher.build():
    graph.emit(mcp)
```

---

## 8. CI/CD Handler Design

### Governance Handler: StructuredPropertyHandler

A new handler following the existing `EntityHandler` pattern:

```python
class StructuredPropertyHandler(EntityHandler):
    entity_type = "structuredProperty"
    dependencies = []  # No dependencies (top-level governance entity)

    def export(self, graph):
        # List all structured properties
        # Fetch StructuredPropertyDefinitionClass for each
        # Serialize to dict (qualifiedName, valueType, cardinality, allowedValues, version, etc.)

    def build_mcps(self, entity, urn_mapper):
        # Reconstruct StructuredPropertyDefinitionClass from dict
        # Emit MCP with the definition aspect
```

### Enrichment Extension

Structured property value assignments are part of the `structuredProperties` aspect on entities. The existing enrichment handlers (`DatasetEnrichmentHandler`, `GenericEnrichmentHandler`) would need to be extended to:

1. Read `StructuredPropertiesClass` from each entity
2. Filter to only managed structured property URNs (governance URN filtering)
3. Serialize value assignments to the enrichment JSON
4. On sync, rebuild the `StructuredPropertiesClass` and emit

### Breaking Change Strategy (Version-Bump)

For the CI/CD pipeline to support breaking changes:

1. **Export**: Read current definition including `version` field
2. **Detect breaking change**: Compare dev definition with prod definition
   - Are allowed values being removed?
   - Is cardinality changing MULTIPLE→SINGLE?
   - Is value type changing?
3. **Version management**: If breaking change detected, auto-increment the `version` field
   - Use 14-digit timestamp format: `YYYYMMDDHHmmss`
   - Must be strictly greater than the current version (string comparison)
4. **Emit**: Include the new `version` in the definition MCP

### Mutation Strategy Matrix

| Change Type | Default Behavior | With Version Bump | CI/CD Strategy |
|-------------|-----------------|-------------------|----------------|
| Add allowed values | Allowed | N/A | Direct emit |
| Remove allowed values | **Blocked** | Allowed | Auto-version-bump |
| Add entity types | Allowed | N/A | Direct emit |
| Change SINGLE→MULTIPLE | Allowed | N/A | Direct emit |
| Change MULTIPLE→SINGLE | **Blocked** | Allowed | Auto-version-bump |
| Change value type | **Blocked** | Allowed | Auto-version-bump |
| Change qualified name | **Always blocked** | **Always blocked** | Error: not supported |
| Change display name | Allowed | N/A | Direct emit |
| Change description | Allowed | N/A | Direct emit |

### Risks and Open Questions

1. **Orphaned values**: After removing an allowed value, existing entities may have that value. What happens when those entities are read? Are they filtered? Shown as-is?
2. **Value migration**: After changing cardinality MULTIPLE→SINGLE, entities with multiple values may be in an inconsistent state. Does the server pick the first value? Error?
3. **Cross-environment consistency**: If dev and prod have different versions, the pipeline needs to track version progression carefully
4. **Deletion propagation**: Soft-deleting a structured property in dev should trigger soft-delete in prod. The async reference cleanup means there could be a window where prod entities still reference the deleted property.
5. **Settings sync**: StructuredPropertySettings is Cloud-only. The pipeline should handle this conditionally.

### Registration in HandlerRegistry

```python
def create_default_registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register(TagHandler())
    registry.register(GlossaryNodeHandler())
    registry.register(GlossaryTermHandler())
    registry.register(DomainHandler())
    registry.register(DataProductHandler())
    registry.register(StructuredPropertyHandler())  # New
    return registry
```

### Dependency Ordering

Structured properties have **no dependencies** on other governance entities (tags, terms, domains). However, enrichment handlers that sync structured property values DO depend on structured properties being synced first:

```
StructuredPropertyHandler → (enrichment handlers that sync SP values)
```

---

## Appendix: Key Source Files

### Server-Side (Java)

| File | Description |
|------|-------------|
| `metadata-models/.../structured/StructuredPropertyDefinition.pdl` | PDL schema for property definitions |
| `metadata-models/.../structured/StructuredProperties.pdl` | PDL schema for value assignments |
| `metadata-io/.../validation/PropertyDefinitionValidator.java` | Server-side validation for definition changes |
| `metadata-io/.../validation/StructuredPropertiesValidator.java` | Server-side validation for value assignments |
| `metadata-io/.../hooks/PropertyDefinitionDeleteSideEffect.java` | Async cleanup on property deletion |
| `datahub-graphql-core/.../resolvers/structuredproperties/` | GraphQL resolvers |
| `datahub-graphql-core/.../resources/properties.graphql` | GraphQL schema |

### Python SDK

| File | Description |
|------|-------------|
| `metadata-ingestion/.../schema_classes.py` | Generated classes (StructuredPropertyDefinitionClass, etc.) |
| `metadata-ingestion/.../structuredproperties/structuredproperties.py` | High-level API |
| `metadata-ingestion/.../aspect_helpers/structured_properties.py` | Patch builder mixin |
| `metadata-ingestion/.../specific/structured_property.py` | StructuredPropertyPatchBuilder |
| `metadata-ingestion/.../cli/specific/structuredproperties_cli.py` | CLI commands |
| `metadata-ingestion/.../urns/_urn_base.py` | URN helpers (make_data_type_urn, etc.) |
