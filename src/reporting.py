"""Run report generation: JSON + Markdown dual-format output.

Single source of truth: builds a RunReport data structure, then renders
to both JSON and Markdown. Pattern borrowed from datahub-skills
(https://github.com/datahub-project/datahub-skills) StatusFormatter approach.

Usage:
    from src.reporting import RunReport
    report = RunReport.from_results(ctx, results, command="export")
    report.write(output_dir)
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.interfaces import SyncResult

logger = logging.getLogger(__name__)

# Skip reason constants — shared with interfaces.py
SKIP_SYSTEM_ENTITY = "system_entity"
SKIP_PROVENANCE_FILTER = "provenance_filter"
SKIP_SCOPE_FILTER = "scope_filter"
SKIP_NO_ENRICHMENT = "no_enrichment"
SKIP_GOVERNANCE_URN_FILTER = "governance_urn_filter"
SKIP_DRY_RUN = "dry_run"


@dataclass
class RunReport:
    """Structured run report that renders to JSON and Markdown."""

    run_id: str
    command: str  # "export" | "sync"
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    duration_seconds: float = 0.0
    exit_status: str = "success"  # "success" | "failure"
    entity_summary: dict[str, dict[str, int]] = field(default_factory=dict)
    timing: dict[str, dict] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    skips: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    api_stats: dict | None = None

    @classmethod
    def from_results(
        cls,
        run_id: str,
        command: str,
        results: list[SyncResult],
        duration_seconds: float = 0.0,
        timing: dict[str, dict] | None = None,
        api_stats: dict | None = None,
    ) -> "RunReport":
        """Build a report from a list of SyncResults."""
        report = cls(
            run_id=run_id,
            command=command,
            duration_seconds=round(duration_seconds, 3),
            timing=timing or {},
            api_stats=api_stats,
        )

        # Build entity summary
        entity_counts: dict[str, dict[str, int]] = {}
        for r in results:
            if r.entity_type not in entity_counts:
                entity_counts[r.entity_type] = {
                    "exported": 0,
                    "synced": 0,
                    "failed": 0,
                    "skipped": 0,
                }
            counts = entity_counts[r.entity_type]
            if r.status == "success":
                key = "exported" if command == "export" else "synced"
                counts[key] += 1
            elif r.status == "failed":
                counts["failed"] += 1
            elif r.status == "skipped":
                counts["skipped"] += 1
        report.entity_summary = entity_counts

        # Collect errors
        for r in results:
            if r.status == "failed" and r.error:
                error_entry: dict = {
                    "urn": r.urn,
                    "entity_type": r.entity_type,
                    "phase": command,
                    "message": r.error,
                }
                if r.error_category:
                    error_entry["category"] = r.error_category
                if r.error_suggestion:
                    error_entry["suggestion"] = r.error_suggestion
                report.errors.append(error_entry)

        # Collect skips
        for r in results:
            if r.status == "skipped" and r.skip_reason:
                report.skips.append(
                    {
                        "urn": r.urn,
                        "entity_type": r.entity_type,
                        "reason": r.skip_reason,
                    }
                )

        # Determine exit status
        if any(r.status == "failed" for r in results):
            report.exit_status = "failure"

        return report

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict."""
        d = {
            "run_id": self.run_id,
            "command": self.command,
            "timestamp": self.timestamp,
            "duration_seconds": self.duration_seconds,
            "exit_status": self.exit_status,
            "entity_summary": self.entity_summary,
            "timing": self.timing,
            "errors": self.errors,
            "skips": self.skips,
            "warnings": self.warnings,
        }
        if self.api_stats:
            d["api_stats"] = self.api_stats
        return d

    def to_markdown(self) -> str:
        """Render the report as Markdown."""
        lines: list[str] = []
        lines.append(f"# Run Report: {self.command}")
        lines.append("")
        lines.append(f"- **Run ID**: {self.run_id}")
        lines.append(f"- **Timestamp**: {self.timestamp}")
        lines.append(f"- **Duration**: {self.duration_seconds:.1f}s")
        lines.append(f"- **Status**: {self.exit_status}")
        lines.append("")

        # Entity summary table
        if self.entity_summary:
            lines.append("## Entity Summary")
            lines.append("")
            lines.append("| Entity Type | Exported | Synced | Failed | Skipped |")
            lines.append("|---|---|---|---|---|")
            for et, counts in sorted(self.entity_summary.items()):
                lines.append(
                    f"| {et} | {counts.get('exported', 0)} | "
                    f"{counts.get('synced', 0)} | {counts.get('failed', 0)} | "
                    f"{counts.get('skipped', 0)} |"
                )
            lines.append("")

        # Timing
        if self.timing:
            lines.append("## Phase Timing")
            lines.append("")
            lines.append("| Phase | Duration | API Calls | API Time |")
            lines.append("|---|---|---|---|")
            for phase, data in self.timing.items():
                lines.append(
                    f"| {phase} | {data.get('duration_seconds', 0):.1f}s | "
                    f"{data.get('api_calls', 0)} | "
                    f"{data.get('api_time_seconds', 0):.1f}s |"
                )
            lines.append("")

        # Skip summary
        if self.skips:
            lines.append("## Skipped Entities")
            lines.append("")
            # Group by reason
            by_reason: dict[str, list[dict]] = {}
            for skip in self.skips:
                reason = skip["reason"]
                by_reason.setdefault(reason, []).append(skip)

            lines.append("| Reason | Count | Sample URNs |")
            lines.append("|---|---|---|")
            for reason, entries in sorted(by_reason.items()):
                samples = [e["urn"] for e in entries[:3]]
                sample_str = ", ".join(samples)
                if len(entries) > 3:
                    sample_str += f" (+{len(entries) - 3} more)"
                lines.append(f"| {reason} | {len(entries)} | {sample_str} |")
            lines.append("")

        # Errors
        if self.errors:
            lines.append("## Errors")
            lines.append("")
            for err in self.errors:
                lines.append(
                    f"- **[{err['entity_type']}]** `{err['urn']}`: {err['message']}"
                )
                if "suggestion" in err:
                    lines.append(f"  - Suggestion: {err['suggestion']}")
            lines.append("")

        # Warnings
        if self.warnings:
            lines.append("## Warnings")
            lines.append("")
            for w in self.warnings:
                lines.append(f"- {w}")
            lines.append("")

        return "\n".join(lines)

    def write(self, output_dir: str) -> tuple[str, str]:
        """Write both JSON and Markdown reports. Returns (json_path, md_path)."""
        os.makedirs(output_dir, exist_ok=True)

        json_path = os.path.join(output_dir, "run-report.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        logger.info(f"Wrote JSON report to {json_path}")

        md_path = os.path.join(output_dir, "run-report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self.to_markdown())
        logger.info(f"Wrote Markdown report to {md_path}")

        return json_path, md_path


def write_run_state(
    output_dir: str,
    run_id: str,
    command: str,
    started_at: str,
    status: str,
    completed_phases: list[dict],
    results: list[SyncResult],
    errors: list[dict],
) -> None:
    """Write incremental run state for crash resilience (Amendment 5).

    Written after each handler phase completes. On crash, this file
    is the best available record of what happened.
    """
    succeeded = sum(1 for r in results if r.status == "success")
    failed = sum(1 for r in results if r.status == "failed")
    skipped = sum(1 for r in results if r.status == "skipped")

    state = {
        "run_id": run_id,
        "command": command,
        "started_at": started_at,
        "last_updated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "completed_phases": completed_phases,
        "results_so_far": {
            "success": succeeded,
            "failed": failed,
            "skipped": skipped,
        },
        "errors_so_far": errors[:50],  # cap to avoid huge state files
    }

    os.makedirs(output_dir, exist_ok=True)
    state_path = os.path.join(output_dir, ".run-state.json")
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
