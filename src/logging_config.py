"""Structured logging configuration for the CI/CD pipeline.

Sets up dual output:
- Console: human-readable formatted messages
- JSONL file: machine-parseable structured log lines (one JSON object per line)

Usage:
    from src.logging_config import configure_logging
    configure_logging(output_dir="metadata/", log_level="INFO")
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone


class JsonlFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects for JSONL output."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        # Include extra fields added via logger.info("msg", extra={...})
        for key in ("entity_type", "urn", "phase", "run_id"):
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val
        return json.dumps(entry, default=str)


def configure_logging(
    output_dir: str | None = None,
    log_level: str = "INFO",
    run_id: str | None = None,
) -> None:
    """Configure root logger with console and optional JSONL file handlers.

    Args:
        output_dir: Directory for the JSONL log file. If None, file logging is skipped.
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR).
        run_id: Run identifier to include in the JSONL filename.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    # Clear existing handlers to avoid duplicates on re-configuration
    root.handlers.clear()

    # Console handler — human-readable
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level)
    console.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root.addHandler(console)

    # JSONL file handler — machine-parseable
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filename = f"run-{run_id}.jsonl" if run_id else "run.jsonl"
        filepath = os.path.join(output_dir, filename)
        file_handler = logging.FileHandler(filepath, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # capture everything to file
        file_handler.setFormatter(JsonlFormatter())
        root.addHandler(file_handler)
