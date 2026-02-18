"""Unit tests for src/logging_config.py -- structured logging setup."""

import json
import logging
import os
import tempfile

from src.logging_config import JsonlFormatter, configure_logging


class TestJsonlFormatter:
    def test_formats_as_json(self):
        formatter = JsonlFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert data["message"] == "test message"
        assert "timestamp" in data

    def test_includes_exception(self):
        formatter = JsonlFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="error occurred",
            args=(),
            exc_info=exc_info,
        )
        output = formatter.format(record)
        data = json.loads(output)

        assert "exception" in data
        assert "ValueError" in data["exception"]


class TestConfigureLogging:
    def test_configures_console_handler(self):
        configure_logging(log_level="INFO")
        root = logging.getLogger()
        assert len(root.handlers) >= 1
        assert root.level == logging.INFO

    def test_configures_file_handler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            configure_logging(output_dir=tmpdir, run_id="test123")
            root = logging.getLogger()

            # Should have both console and file handlers
            assert len(root.handlers) == 2

            filepath = os.path.join(tmpdir, "run-test123.jsonl")
            assert os.path.exists(filepath)

            # Clean up
            configure_logging()

    def test_no_file_handler_without_output_dir(self):
        configure_logging()
        root = logging.getLogger()
        # Only console handler
        assert len(root.handlers) == 1
