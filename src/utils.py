import json
import logging
from pathlib import Path

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


def write_json(data: list[dict], path: str) -> None:
    """Write a list of dicts to a JSON file."""
    filepath = Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote {len(data)} entities to {path}")


def read_json(path: str) -> list[dict]:
    """Read a list of dicts from a JSON file."""
    filepath = Path(path)
    if not filepath.exists():
        logger.warning(f"File not found: {path}, returning empty list")
        return []
    with open(filepath) as f:
        return json.load(f)
