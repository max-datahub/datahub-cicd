"""Retry with exponential backoff for transient DataHub API failures.

Retries on transient network/server errors. Does NOT retry on client errors
(auth, validation, not found) which require human intervention.

Usage:
    from src.retry import retry_transient

    @retry_transient(max_retries=3, base_delay=1.0)
    def call_api():
        graph.emit_mcp(mcp)

    # Or as a direct wrapper:
    retry_transient(max_retries=3)(lambda: graph.emit_mcp(mcp))()
"""

import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)

# Exception types that indicate transient failures worth retrying
TRANSIENT_EXCEPTIONS = (ConnectionError, ConnectionResetError, TimeoutError)

# HTTP status codes that indicate transient server issues
TRANSIENT_HTTP_CODES = {429, 502, 503, 504}


def _is_transient(exc: Exception) -> bool:
    """Check if an exception represents a transient failure."""
    if isinstance(exc, TRANSIENT_EXCEPTIONS):
        return True

    # Check for HTTP status code on SDK exceptions
    status_code = getattr(exc, "status_code", None)
    if status_code and status_code in TRANSIENT_HTTP_CODES:
        return True

    return False


def retry_transient(
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
):
    """Decorator that retries on transient failures with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (total attempts = max_retries + 1).
        base_delay: Initial delay in seconds before first retry.
        backoff_factor: Multiplier for delay between retries (1s, 2s, 4s with factor=2).

    Retries on:
        ConnectionError, TimeoutError, HTTP 429/502/503/504

    Does NOT retry on:
        HTTP 401/403/404/409, ValueError, KeyError, or other non-transient errors.

    After final failure, raises the original exception for the caller's
    existing try/except to handle.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries and _is_transient(e):
                        delay = base_delay * (backoff_factor ** attempt)
                        logger.warning(
                            f"Transient failure (attempt {attempt + 1}/{max_retries + 1}), "
                            f"retrying in {delay:.1f}s: {e}"
                        )
                        time.sleep(delay)
                    else:
                        raise
            raise last_exc  # should not reach here, but safety net

        return wrapper

    return decorator
