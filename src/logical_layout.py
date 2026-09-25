"""Folder-tree layout for logical model definitions.

    logicalModels/<platform>/platform.json
    logicalModels/<platform>/<container>/[<sub-container>/...]container.json
    logicalModels/<platform>/<container>/.../<dataset-name>.<ENV>.json

The JSON is authoritative: a dataset's parent is its `container` aspect, not
the folder it sits in. Folders are regenerated from those aspects on every
export and exist only for human navigation and readable git diffs.
"""

import json
import logging
import re
import shutil
from pathlib import Path

from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)

LOGICAL_MODELS_DIR = "logicalModels"
PLATFORM_FILE = "platform.json"
CONTAINER_FILE = "container.json"
_FILE_KEYS = ("urn", "aspects", "physicalChildren")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def sanitize(name: str) -> str:
    return _UNSAFE.sub("_", name)


def _urn_id(urn: str) -> str:
    """urn:li:dataPlatform:logical -> logical; urn:li:container:abc -> abc."""
    return urn.split(":", 3)[3]


def parent_container(entity: dict) -> str | None:
    return entity["aspects"].get("container", {}).get("container")


def plan_layout(entities: list[dict]) -> dict[str, Path]:
    """Map each entity URN to its path relative to logicalModels/.

    Raises ValueError when two entities would share a path. Display names are
    not unique, so colliding siblings fail loudly instead of overwriting.
    """
    by_urn = {e["urn"]: e for e in entities}
    # A container's platform folder comes from the datasets inside it.
    container_platform: dict[str, str] = {}
    for e in sorted(entities, key=lambda e: e["urn"]):
        if e["entityType"] != "dataset":
            continue
        platform = str(DatasetUrn.from_string(e["urn"]).platform)
        c = parent_container(e)
        while c in by_urn and c not in container_platform:
            container_platform[c] = platform
            c = parent_container(by_urn[c])

    def container_dir(urn: str, seen: tuple[str, ...] = ()) -> Path:
        if urn in seen:
            raise ValueError(f"Container cycle involving {urn}")
        c = by_urn[urn]
        name = c["aspects"].get("containerProperties", {}).get("name") or _urn_id(urn)
        parent = parent_container(c)
        if parent in by_urn:
            base = container_dir(parent, seen + (urn,))
        else:
            base = Path(sanitize(_urn_id(container_platform[urn])))
        return base / sanitize(name)

    paths: dict[str, Path] = {}
    # Keyed by casefold(str(path)): exports land on macOS, whose default filesystem
    # is case-insensitive, so "Billing" and "billing" would otherwise silently
    # overwrite each other on disk even though this check passed.
    owner: dict[str, str] = {}
    for e in entities:
        urn, kind = e["urn"], e["entityType"]
        if kind == "dataPlatform":
            path = Path(sanitize(_urn_id(urn))) / PLATFORM_FILE
        elif kind == "container":
            if urn not in container_platform:
                continue  # ponytail: unreachable from any dataset → no stable folder; mismatch check skips it
            path = container_dir(urn) / CONTAINER_FILE
        elif kind == "dataset":
            key = DatasetUrn.from_string(urn)
            parent = parent_container(e)
            base = container_dir(parent) if parent in by_urn else Path(sanitize(_urn_id(str(key.platform))))
            path = base / f"{sanitize(key.name)}.{key.env}.json"
        else:
            continue
        collision_key = str(path).casefold()
        if collision_key in owner:
            raise ValueError(f"Layout collision at {path}: {owner[collision_key]} and {urn}; rename one of them")
        owner[collision_key] = urn
        paths[urn] = path
    return paths


def write_tree(entities: list[dict], output_dir: str, platforms: list[str] | None = None) -> None:
    """Rewrite the tree from scratch so renamed/deleted entities leave nothing stale.

    `platforms` (platform URNs) scopes the rewrite to those platforms' folders,
    so a scoped export never deletes other platforms' definitions. None rewrites
    the whole logicalModels/ tree.
    """
    root = Path(output_dir) / LOGICAL_MODELS_DIR
    paths = plan_layout(entities)
    unplaced = [e["urn"] for e in entities if e["urn"] not in paths]
    if unplaced:
        raise ValueError(f"No layout path for: {unplaced}")
    owned = [root] if platforms is None else [root / sanitize(_urn_id(p)) for p in platforms]
    for folder in owned:
        if folder.exists():
            shutil.rmtree(folder)
    for e in entities:
        target = root / paths[e["urn"]]
        target.parent.mkdir(parents=True, exist_ok=True)
        body = {k: e[k] for k in _FILE_KEYS if k in e}
        target.write_text(json.dumps(body, indent=2, default=str) + "\n")
    logger.info(f"Wrote {len(entities)} logical model entities to {root}")


def _check_has_urn(item: object, what: str) -> None:
    if not isinstance(item, dict) or not isinstance(item.get("urn"), str):
        raise ValueError(f"{what} must be an object with a string urn, got {item!r}")


def read_tree(metadata_dir: str) -> list[dict]:
    """Load every *.json under logicalModels/. Broken files become `_load_error` entries."""
    root = Path(metadata_dir) / LOGICAL_MODELS_DIR
    if not root.is_dir():
        return []
    entities: list[dict] = []
    actual: dict[str, Path] = {}
    for f in sorted(root.rglob("*.json")):
        rel = f.relative_to(root)
        try:
            body = json.loads(f.read_text())
            urn = body["urn"]
            aspects = body.get("aspects", {})
            if not isinstance(aspects, dict) or not all(isinstance(v, dict) for v in aspects.values()):
                # A malformed aspect (e.g. "container" as a string instead of a dict)
                # would otherwise blow up parent_container() deep inside plan_layout,
                # crashing the whole read_tree() call instead of isolating this file.
                raise ValueError(f"aspects must be a dict of dicts, got {aspects!r}")
            entity = {"urn": urn, "entityType": urn.split(":")[2], "aspects": aspects}
            if "physicalChildren" in body:
                physical_children = body["physicalChildren"]
                if not isinstance(physical_children, list):
                    raise ValueError(f"physicalChildren must be a list, got {physical_children!r}")
                for child in physical_children:
                    _check_has_urn(child, "physicalChildren item")
                    fields = child.get("fields", [])
                    if not isinstance(fields, list):
                        raise ValueError(f"fields must be a list, got {fields!r}")
                    for field in fields:
                        _check_has_urn(field, "fields item")
                entity["physicalChildren"] = physical_children
            if "container" in aspects and not isinstance(aspects["container"].get("container"), str):
                raise ValueError(f"container aspect needs a string container URN, got {aspects['container']!r}")
        except (ValueError, KeyError, IndexError, TypeError, AttributeError) as e:
            entities.append({"urn": str(rel), "entityType": "invalid", "_load_error": f"{rel}: {e}"})
            continue
        entities.append(entity)
        actual[urn] = rel
    try:
        expected = plan_layout([e for e in entities if "_load_error" not in e])
    except ValueError as e:
        logger.warning(f"Could not verify logicalModels/ folder layout: {e}")
        expected = {}
    for urn, rel in actual.items():
        if urn in expected and expected[urn] != rel:
            logger.warning(
                f"{rel} does not match its container aspect (expected {expected[urn]}); "
                f"the container aspect wins"
            )
    return entities
