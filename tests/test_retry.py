"""Unit tests for src/retry.py -- retry with exponential backoff."""

from unittest.mock import MagicMock, patch

import pytest

from src.retry import retry_transient, _is_transient


class TestIsTransient:
    def test_connection_error_is_transient(self):
        assert _is_transient(ConnectionError("reset")) is True

    def test_timeout_error_is_transient(self):
        assert _is_transient(TimeoutError("timed out")) is True

    def test_connection_reset_error_is_transient(self):
        assert _is_transient(ConnectionResetError("reset")) is True

    def test_value_error_not_transient(self):
        assert _is_transient(ValueError("bad value")) is False

    def test_key_error_not_transient(self):
        assert _is_transient(KeyError("missing")) is False

    def test_http_429_is_transient(self):
        exc = Exception("rate limited")
        exc.status_code = 429
        assert _is_transient(exc) is True

    def test_http_502_is_transient(self):
        exc = Exception("bad gateway")
        exc.status_code = 502
        assert _is_transient(exc) is True

    def test_http_503_is_transient(self):
        exc = Exception("service unavailable")
        exc.status_code = 503
        assert _is_transient(exc) is True

    def test_http_504_is_transient(self):
        exc = Exception("gateway timeout")
        exc.status_code = 504
        assert _is_transient(exc) is True

    def test_http_401_not_transient(self):
        exc = Exception("unauthorized")
        exc.status_code = 401
        assert _is_transient(exc) is False

    def test_http_404_not_transient(self):
        exc = Exception("not found")
        exc.status_code = 404
        assert _is_transient(exc) is False


class TestRetryTransient:
    @patch("src.retry.time.sleep")
    def test_succeeds_first_try(self, mock_sleep):
        mock_fn = MagicMock(return_value="ok")

        @retry_transient(max_retries=3, base_delay=1.0)
        def call():
            return mock_fn()

        result = call()
        assert result == "ok"
        assert mock_fn.call_count == 1
        mock_sleep.assert_not_called()

    @patch("src.retry.time.sleep")
    def test_retries_on_connection_error(self, mock_sleep):
        mock_fn = MagicMock(
            side_effect=[ConnectionError("fail"), ConnectionError("fail"), "ok"]
        )

        @retry_transient(max_retries=3, base_delay=1.0, backoff_factor=2.0)
        def call():
            return mock_fn()

        result = call()
        assert result == "ok"
        assert mock_fn.call_count == 3
        assert mock_sleep.call_count == 2
        # Check backoff delays: 1.0, 2.0
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)

    @patch("src.retry.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep):
        mock_fn = MagicMock(side_effect=ConnectionError("always fails"))

        @retry_transient(max_retries=2, base_delay=0.1)
        def call():
            return mock_fn()

        with pytest.raises(ConnectionError, match="always fails"):
            call()
        assert mock_fn.call_count == 3  # initial + 2 retries

    @patch("src.retry.time.sleep")
    def test_does_not_retry_non_transient(self, mock_sleep):
        mock_fn = MagicMock(side_effect=ValueError("bad value"))

        @retry_transient(max_retries=3, base_delay=1.0)
        def call():
            return mock_fn()

        with pytest.raises(ValueError, match="bad value"):
            call()
        assert mock_fn.call_count == 1
        mock_sleep.assert_not_called()

    @patch("src.retry.time.sleep")
    def test_retries_on_http_503(self, mock_sleep):
        exc = Exception("service unavailable")
        exc.status_code = 503
        mock_fn = MagicMock(side_effect=[exc, "ok"])

        @retry_transient(max_retries=3, base_delay=1.0)
        def call():
            return mock_fn()

        result = call()
        assert result == "ok"
        assert mock_fn.call_count == 2
