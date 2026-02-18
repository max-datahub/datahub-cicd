"""Error classification: categorize exceptions into actionable error types.

Maps exceptions to (category, suggestion) tuples for structured error reporting.
Handles both standard Python exceptions and DataHub SDK-specific patterns
(Amendment 8: SDK exception patterns).

Usage:
    category, suggestion = classify_error(exc)
"""

import re


def _classify_http_status(status_code: int) -> tuple[str, str]:
    """Classify an HTTP status code into a category and suggestion."""
    if status_code == 401:
        return ("auth", "Check DATAHUB_*_TOKEN environment variables")
    if status_code == 403:
        return ("auth", "Token lacks required permissions for this operation")
    if status_code == 404:
        return ("not_found", "Entity or aspect does not exist on target instance")
    if status_code == 409:
        return ("conflict", "Concurrent write conflict — retry may resolve")
    if status_code == 422:
        return (
            "validation",
            "Entity type may not support this operation (e.g., domain soft-delete)",
        )
    if status_code == 429:
        return ("rate_limit", "API rate limit exceeded — reduce concurrency or add delay")
    if 400 <= status_code < 500:
        return ("client_error", f"HTTP {status_code} client error — check request payload")
    if 500 <= status_code < 600:
        return ("server_error", f"HTTP {status_code} server error — DataHub instance issue")
    return ("unknown", f"Unexpected HTTP status {status_code}")


def classify_error(exc: Exception) -> tuple[str, str]:
    """Classify an exception into (category, suggestion).

    Checks in order:
    1. DataHub SDK types with status_code attribute
    2. HTTP status codes in exception message (SDK pattern)
    3. Standard Python exception type mapping

    Returns:
        Tuple of (category: str, suggestion: str).
    """
    # Check DataHub SDK types with status_code attribute
    if hasattr(exc, "status_code"):
        return _classify_http_status(exc.status_code)

    # Check for HTTP status in exception message (SDK pattern: "... Status: 401 ...")
    status_match = re.search(
        r"(?:status|code)[:\s]*(\d{3})", str(exc), re.IGNORECASE
    )
    if status_match:
        return _classify_http_status(int(status_match.group(1)))

    # Standard Python exception type mapping
    if isinstance(exc, (ConnectionError, ConnectionResetError)):
        return ("connection", "Check network connectivity to DataHub instance")
    if isinstance(exc, TimeoutError):
        return ("timeout", "DataHub instance may be overloaded — increase timeout or retry")
    if isinstance(exc, (ValueError, KeyError)):
        return ("data_error", "Invalid entity data — check export JSON for malformed entries")
    if isinstance(exc, TypeError):
        return ("data_error", "Type mismatch in entity data — check SDK compatibility")
    if isinstance(exc, PermissionError):
        return ("auth", "File system permission error — check output directory permissions")
    if isinstance(exc, FileNotFoundError):
        return ("not_found", "Expected file not found — verify metadata directory path")
    if isinstance(exc, OSError):
        return ("io_error", "File system I/O error — check disk space and permissions")

    return ("unknown", f"Unexpected error: {type(exc).__name__}")
