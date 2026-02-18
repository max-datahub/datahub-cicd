"""Run context: tracks run metadata, API call profiling, and timing.

TrackedGraph wraps DataHubGraph with transparent API call counting and timing
using __getattr__ delegation — untracked methods pass through without overhead.

Usage:
    from src.run_context import RunContext, TrackedGraph

    ctx = RunContext(command="export")
    tracked = TrackedGraph(graph)
    # Use tracked exactly like graph — all methods work transparently
    tracked.get_tags(urn)
    # Check stats
    print(tracked.call_counts)  # {"get_tags": 1}
    print(tracked.call_times)   # {"get_tags": 0.05}
"""

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps

from datahub.ingestion.graph.client import DataHubGraph


@dataclass
class PhaseTimer:
    """Tracks timing for a single phase (e.g., 'export_tag', 'sync_domain')."""

    phase: str
    start_time: float = 0.0
    end_time: float = 0.0
    api_calls: int = 0
    api_time: float = 0.0

    @property
    def duration(self) -> float:
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.monotonic() - self.start_time

    def start(self) -> None:
        self.start_time = time.monotonic()

    def stop(self) -> None:
        self.end_time = time.monotonic()

    def to_dict(self) -> dict:
        return {
            "duration_seconds": round(self.duration, 3),
            "api_calls": self.api_calls,
            "api_time_seconds": round(self.api_time, 3),
        }


@dataclass
class RunContext:
    """Top-level run context. Tracks run ID, command, timing, and phase data."""

    command: str  # "export" or "sync"
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    _start_mono: float = field(default_factory=time.monotonic, repr=False)
    phases: dict[str, PhaseTimer] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        return time.monotonic() - self._start_mono

    def start_phase(self, phase: str) -> PhaseTimer:
        timer = PhaseTimer(phase=phase)
        timer.start()
        self.phases[phase] = timer
        return timer

    def stop_phase(self, phase: str) -> None:
        if phase in self.phases:
            self.phases[phase].stop()

    def timing_summary(self) -> dict[str, dict]:
        return {name: timer.to_dict() for name, timer in self.phases.items()}


class TrackedGraph:
    """Transparent wrapper around DataHubGraph that tracks API call counts and timing.

    Uses __getattr__ delegation: only methods listed in TRACKED_METHODS get
    timing/counting overhead. All other attributes pass through transparently.
    New SDK methods work without changes.
    """

    TRACKED_METHODS = {
        "get_urns_by_filter",
        "get_tags",
        "get_glossary_terms",
        "get_domain",
        "get_ownership",
        "get_aspect",
        "emit_mcp",
        "emit_mcps",
        "get_entity_as_mcps",
        "soft_delete_entity",
    }

    def __init__(self, graph: DataHubGraph) -> None:
        # Use object.__setattr__ to avoid triggering __getattr__
        object.__setattr__(self, "_graph", graph)
        object.__setattr__(self, "call_counts", defaultdict(int))
        object.__setattr__(self, "call_times", defaultdict(float))

    def __getattr__(self, name: str):
        attr = getattr(self._graph, name)
        if name in self.TRACKED_METHODS and callable(attr):

            @wraps(attr)
            def tracked(*args, **kwargs):
                t0 = time.monotonic()
                result = attr(*args, **kwargs)
                elapsed = time.monotonic() - t0
                self.call_counts[name] += 1
                self.call_times[name] += elapsed
                return result

            return tracked
        return attr

    def get_stats(self) -> dict:
        """Return summary of API call statistics."""
        return {
            "total_calls": sum(self.call_counts.values()),
            "total_time_seconds": round(sum(self.call_times.values()), 3),
            "by_method": {
                method: {
                    "calls": self.call_counts[method],
                    "time_seconds": round(self.call_times[method], 3),
                }
                for method in sorted(self.call_counts.keys())
            },
        }
