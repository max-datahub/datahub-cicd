"""Provenance filtering: classify entities by their creation source.

DataHub tracks provenance via `systemMetadata.properties`:
- GraphQL mutations set `appSource = "ui"` (see caveat below)
- CI/CD pipeline writes set `appSource = "cicd-pipeline"` (tagged by OverwriteStrategy)
- Ingestion sets `runId` (non-default) + `pipelineName`

Caveat: DataHub labels ALL GraphQL mutations as `appSource = "ui"`, not just
those from the browser UI. Any script or SDK call that uses the /api/graphql
endpoint (including DataHubGraph.execute_graphql()) gets the same label.
There is no server-side differentiation between browser sessions and
programmatic GraphQL clients. The "ui" filter therefore means "via GraphQL"
in practice, excluding only REST/OpenAPI ingest-path writes.

Usage:
    source = classify_provenance(graph, urn, "tagProperties")
    kept, filtered_out = filter_entities_by_provenance(
        graph, entities, "tag", {ProvenanceSource.UI}
    )
"""

import logging
from enum import Enum

from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class ProvenanceSource(Enum):
    """Source that last wrote an entity's primary aspect."""

    UI = "ui"
    INGESTION = "ingestion"
    CICD = "cicd"
    UNKNOWN = "unknown"


# Maps entity types to their primary defining aspect.
ENTITY_TYPE_TO_ASPECT = {
    "tag": "tagProperties",
    "glossaryNode": "glossaryNodeInfo",
    "glossaryTerm": "glossaryTermInfo",
    "domain": "domainProperties",
    "dataProduct": "dataProductProperties",
}


def classify_provenance(
    graph: DataHubGraph,
    urn: str,
    aspect_name: str,
) -> ProvenanceSource:
    """Classify the provenance source of a specific aspect on an entity.

    Calls `graph.get_entity_as_mcps()` to retrieve systemMetadata for
    the target aspect.

    Args:
        graph: DataHubGraph client.
        urn: Entity URN.
        aspect_name: Aspect to inspect (e.g., "tagProperties").

    Returns:
        ProvenanceSource classification. Returns UNKNOWN on API errors.
    """
    try:
        mcpws = graph.get_entity_as_mcps(urn, aspects=[aspect_name])
    except Exception as e:
        logger.debug(
            f"Failed to get systemMetadata for {urn}",
            exc_info=True,
        )
        logger.warning(f"Failed to get systemMetadata for {urn}: {e}")
        return ProvenanceSource.UNKNOWN

    # Find the MCPW matching the target aspect
    target_mcpw = None
    for mcpw in mcpws:
        if mcpw.aspectName == aspect_name:
            target_mcpw = mcpw
            break

    if target_mcpw is None:
        return ProvenanceSource.UNKNOWN

    sys_meta = target_mcpw.systemMetadata
    if sys_meta is None:
        return ProvenanceSource.UNKNOWN

    props = getattr(sys_meta, "properties", None) or {}

    # Check appSource first (most specific)
    app_source = props.get("appSource")
    if app_source == "ui":
        return ProvenanceSource.UI
    if app_source == "cicd-pipeline":
        return ProvenanceSource.CICD

    # Check for ingestion markers
    run_id = getattr(sys_meta, "runId", None)
    pipeline_name = props.get("pipelineName")
    if run_id and run_id != "no-run-id-provided" and pipeline_name:
        return ProvenanceSource.INGESTION

    return ProvenanceSource.UNKNOWN


def filter_entities_by_provenance(
    graph: DataHubGraph,
    entities: list[dict],
    entity_type: str,
    allowed_sources: set[ProvenanceSource],
) -> tuple[list[dict], list[dict]]:
    """Filter entities to only those matching allowed provenance sources.

    Args:
        graph: DataHubGraph client.
        entities: List of entity dicts (each must have "urn" key).
        entity_type: Entity type string (e.g., "tag").
        allowed_sources: Set of ProvenanceSource values to keep.

    Returns:
        Tuple of (kept_entities, filtered_out_entities).

    # TODO: Performance optimization -- replace per-entity classify_provenance()
    # with batch implementation using graph.get_entities(entity_name, urns,
    # aspects=[aspect_name], with_system_metadata=True) to reduce N API calls
    # to 1 batch call per entity type.
    """
    aspect_name = ENTITY_TYPE_TO_ASPECT.get(entity_type)
    if aspect_name is None:
        logger.warning(
            f"No aspect mapping for entity type '{entity_type}', "
            f"passing through unfiltered"
        )
        return entities, []

    kept = []
    filtered_out = []
    for i, entity in enumerate(entities):
        urn = entity.get("urn", "unknown")
        source = classify_provenance(graph, urn, aspect_name)
        if source in allowed_sources:
            kept.append(entity)
        else:
            filtered_out.append(entity)
            logger.debug(
                f"Filtered out {entity_type} {urn} "
                f"(source={source.value}, allowed={[s.value for s in allowed_sources]})"
            )

        if (i + 1) % 50 == 0:
            logger.info(
                f"Provenance filter progress: {i + 1}/{len(entities)} "
                f"{entity_type} entities checked"
            )

    logger.info(
        f"Provenance filter for {entity_type}: "
        f"{len(kept)} kept, {len(filtered_out)} filtered out "
        f"(of {len(entities)} total)"
    )
    return kept, filtered_out
