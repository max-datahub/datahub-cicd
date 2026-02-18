"""Unit tests for src/error_classification.py -- error categorization."""

import pytest

from src.error_classification import classify_error


class TestClassifyError:
    def test_connection_error(self):
        cat, sug = classify_error(ConnectionError("reset"))
        assert cat == "connection"
        assert "network" in sug.lower()

    def test_timeout_error(self):
        cat, sug = classify_error(TimeoutError("timed out"))
        assert cat == "timeout"

    def test_value_error(self):
        cat, sug = classify_error(ValueError("bad"))
        assert cat == "data_error"

    def test_key_error(self):
        cat, sug = classify_error(KeyError("missing"))
        assert cat == "data_error"

    def test_permission_error(self):
        cat, sug = classify_error(PermissionError("denied"))
        assert cat == "auth"

    def test_file_not_found(self):
        cat, sug = classify_error(FileNotFoundError("gone"))
        assert cat == "not_found"

    def test_unknown_exception(self):
        cat, sug = classify_error(RuntimeError("something"))
        assert cat == "unknown"
        assert "RuntimeError" in sug

    def test_sdk_status_code_401(self):
        exc = Exception("unauthorized")
        exc.status_code = 401
        cat, sug = classify_error(exc)
        assert cat == "auth"
        assert "token" in sug.lower()

    def test_sdk_status_code_403(self):
        exc = Exception("forbidden")
        exc.status_code = 403
        cat, sug = classify_error(exc)
        assert cat == "auth"
        assert "permission" in sug.lower()

    def test_sdk_status_code_404(self):
        exc = Exception("not found")
        exc.status_code = 404
        cat, sug = classify_error(exc)
        assert cat == "not_found"

    def test_sdk_status_code_422(self):
        exc = Exception("unprocessable")
        exc.status_code = 422
        cat, sug = classify_error(exc)
        assert cat == "validation"

    def test_sdk_status_code_429(self):
        exc = Exception("too many requests")
        exc.status_code = 429
        cat, sug = classify_error(exc)
        assert cat == "rate_limit"

    def test_sdk_status_code_500(self):
        exc = Exception("internal error")
        exc.status_code = 500
        cat, sug = classify_error(exc)
        assert cat == "server_error"

    def test_status_in_message(self):
        exc = Exception("Request failed with Status: 401 Unauthorized")
        cat, sug = classify_error(exc)
        assert cat == "auth"

    def test_code_in_message(self):
        exc = Exception("Error code: 503 Service Unavailable")
        cat, sug = classify_error(exc)
        assert cat == "server_error"
