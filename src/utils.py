import json
import logging
from pathlib import Path

from src.retry import retry_transient

logger = logging.getLogger(__name__)


def topological_sort(entities: list[dict], parent_key: str) -> list[dict]:
    """Sort entities so parents appear before children.

    Args:
        entities: List of entity dicts, each with a "urn" key.
        parent_key: Field name containing parent URN (e.g., 'parentNode', 'parentDomain').

    Returns:
        Sorted list with parents before children.

    Raises:
        ValueError: If a cycle is detected.
    """
    if not entities:
        return []

    urn_to_entity = {e["urn"]: e for e in entities}
    known_urns = set(urn_to_entity.keys())

    # Build adjacency: parent -> children
    children_of: dict[str, list[str]] = {urn: [] for urn in known_urns}
    roots = []

    for entity in entities:
        parent = entity.get(parent_key)
        if parent and parent in known_urns:
            children_of[parent].append(entity["urn"])
        else:
            # Root entity (no parent, or parent not in this export)
            roots.append(entity["urn"])

    # BFS from roots
    sorted_urns: list[str] = []
    visited: set[str] = set()

    queue = list(roots)
    while queue:
        urn = queue.pop(0)
        if urn in visited:
            continue
        visited.add(urn)
        sorted_urns.append(urn)
        for child_urn in children_of.get(urn, []):
            if child_urn not in visited:
                queue.append(child_urn)

    # Check for cycles (unvisited nodes)
    if len(visited) != len(known_urns):
        unvisited = known_urns - visited
        raise ValueError(
            f"Cycle detected or orphaned entities: {unvisited}"
        )

    return [urn_to_entity[urn] for urn in sorted_urns]


def batch_get_aspect(graph, entity_type: str, urns: list[str], aspect_cls: type, batch_size: int = 100) -> dict:
    """Fetch one aspect for many URNs via OpenAPI v3 batchGet, one request per batch_size URNs.

    Returns {urn: aspect}; URNs without the aspect are omitted.
    """
    found = batch_get_aspects(graph, entity_type, urns, (aspect_cls,), batch_size)
    return {urn: bag[aspect_cls.ASPECT_NAME] for urn, bag in found.items()}


def batch_get_aspects(graph, entity_type: str, urns: list[str], aspect_classes: tuple[type, ...], batch_size: int = 100) -> dict:
    """Like batch_get_aspect, but several aspects per request.

    Returns {urn: {aspect_name: aspect}}; URNs with none of the aspects are omitted.
    """
    names = [cls.ASPECT_NAME for cls in aspect_classes]
    found = {}
    for i in range(0, len(urns), batch_size):
        batch = urns[i : i + batch_size]

        @retry_transient(max_retries=3, base_delay=1.0)
        def _get():
            return graph.get_entities(entity_type, batch, aspects=names)

        for urn, aspects in _get().items():
            bag = {name: aspects[name][0] for name in names if name in aspects}
            if bag:
                found[urn] = bag
    return found


def name_from_urn(urn: str) -> str:
    """Derive a display name from a URN.

    Examples:
        urn:li:tag:PII -> PII
        urn:li:glossaryTerm:customer_id -> customer_id
        urn:li:domain:financial_securities -> financial_securities
    """
    # URN format: urn:li:<entity_type>:<id>
    parts = urn.split(":")
    if len(parts) >= 4:
        return parts[-1]
    return urn


def collect_governance_urns(exports: dict[str, list[dict]]) -> set[str]:
    """Collect all governance entity URNs from export data.

    Scans tag, glossaryNode, glossaryTerm, and domain exports
    to build the set of URNs used to filter enrichment.
    """
    governance_urns: set[str] = set()
    governance_types = {"tag", "glossaryNode", "glossaryTerm", "domain", "dataProduct"}
    for entity_type, entities in exports.items():
        if entity_type in governance_types:
            for entity in entities:
                urn = entity.get("urn")
                if urn:
                    governance_urns.add(urn)
    return governance_urns


def write_json(data: list[dict], path: str) -> None:
    """Write a list of dicts to a JSON file."""
    filepath = Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote {len(data)} entities to {path}")


def read_json(path: str) -> list[dict]:
    """Read a list of dicts from a JSON file."""
    filepath = Path(path)
    if not filepath.exists():
        logger.warning(f"File not found: {path}, returning empty list")
        return []
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)
