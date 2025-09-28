"""Connection URI parsing, validation, and security utilities.

This module provides utilities for parsing connection URIs, validating connection
parameters, and sanitizing sensitive information for safe logging.
"""

import os
from typing import Any
from urllib.parse import urlparse

from loguru import logger


def parse_connection_uri(uri: str | None) -> dict[str, Any]:
    """Parse a connection URI into connection parameters.

    Supports various URI formats:
    - bolt://[username:password@]host[:port]
    - postgres://[username:password@]host[:port][/database]
    - postgresql://[username:password@]host[:port][/database]
    - key=value format: "host=localhost port=7687 username=user"

    Args:
        uri: Connection URI string to parse

    Returns:
        Dictionary containing parsed connection parameters including:
        - protocol: The connection protocol (bolt, postgres, postgresql, etc.)
        - host: Database host
        - port: Database port (if specified)
        - username: Username (if specified)
        - password: Password (if specified)
        - database: Database name (if specified)

    Raises:
        ValueError: If the URI format is invalid or contains invalid port numbers

    Examples:
        >>> parse_connection_uri("bolt://user:pass@localhost:7687")
        {'protocol': 'bolt', 'host': 'localhost', 'port': 7687, 'username': 'user', 'password': 'pass'}

        >>> parse_connection_uri("postgres://admin:secret@db.example.com:5432/mydb")
        {
            'protocol': 'postgres', 'host': 'db.example.com', 'port': 5432,
            'username': 'admin', 'password': 'secret', 'database': 'mydb'
        }

        >>> parse_connection_uri("host=localhost port=7687 username=user")
        {'host': 'localhost', 'port': 7687, 'username': 'user'}

        >>> parse_connection_uri("")
        {}
    """
    if not uri:
        return {}

    # Check if it's a URI format (contains ://)
    if "://" in uri:
        return _parse_uri_format(uri)
    else:
        return _parse_key_value_format(uri)


def _parse_uri_format(uri: str) -> dict[str, Any]:
    """Parse URI format connection strings like bolt://user:pass@host:port/db.

    Args:
        uri: URI format connection string.

    Returns:
        Dictionary of parsed connection parameters.

    Raises:
        ValueError: If URI format is invalid.
    """
    try:
        parsed = urlparse(uri)
    except Exception as exc:
        raise ValueError(f"Invalid URI format: {uri}") from exc

    params = {}

    # Extract protocol
    if parsed.scheme:
        params["protocol"] = parsed.scheme

    # Extract host
    if parsed.hostname:
        params["host"] = parsed.hostname

    # Extract port
    if parsed.port:
        params["port"] = parsed.port

    # Extract username
    if parsed.username:
        params["username"] = parsed.username

    # Extract password
    if parsed.password:
        params["password"] = parsed.password

    # Extract database name (path without leading slash)
    if parsed.path and parsed.path != "/":
        params["database"] = parsed.path.lstrip("/")

    return params


def _parse_key_value_format(cinfo: str) -> dict[str, Any]:
    """Parse key=value format connection strings like 'host=localhost port=7687'.

    Args:
        cinfo: Key=value format connection string.

    Returns:
        Dictionary of parsed connection parameters.

    Raises:
        ValueError: If port value is invalid.
    """
    params = {}

    for pair in cinfo.split():
        if "=" in pair:
            key, value = pair.split("=", 1)
            key = key.strip()
            value = value.strip()

            # Convert port to int
            if key == "port":
                try:
                    params[key] = int(value)
                except ValueError as exc:
                    raise ValueError(f"Invalid port value: {value}. Port must be a valid integer.") from exc
            else:
                params[key] = value

    return params


def validate_protocol(params: dict[str, Any], expected_protocols: list[str]) -> None:
    """Validate that the protocol matches expected protocols.

    Args:
        params: Parsed connection parameters
        expected_protocols: List of acceptable protocols (e.g., ['bolt'])

    Raises:
        ValueError: If protocol is specified but not in expected protocols

    Examples:
        >>> validate_protocol({'protocol': 'bolt', 'host': 'localhost'}, ['bolt', 'postgres'])
        # No exception raised

        >>> validate_protocol({'protocol': 'mysql', 'host': 'localhost'}, ['bolt'])
        Traceback (most recent call last):
        ...
        ValueError: Unsupported protocol 'mysql'. Expected one of: ['bolt']

        >>> validate_protocol({'host': 'localhost'}, ['bolt'])  # No protocol specified
        # No exception raised
    """
    if "protocol" in params and params["protocol"] not in expected_protocols:
        raise ValueError(f"Unsupported protocol '{params['protocol']}'. Expected one of: {expected_protocols}")


def sanitize_connection_params_for_logging(params: dict[str, Any]) -> dict[str, Any]:
    """Sanitize connection parameters for safe logging by masking values.

    Masks sensitive fields like passwords, secrets, tokens, etc. to prevent
    accidental logging of credentials.

    Args:
        params: Connection parameters dictionary

    Returns:
        Sanitized parameters dictionary safe for logging

    Examples:
        >>> params = {'host': 'localhost', 'username': 'user', 'password': 'secret123'}
        >>> sanitize_connection_params_for_logging(params)
        {'host': 'localhost', 'username': 'user', 'password': '***MASKED***'}

        >>> sanitize_connection_params_for_logging({'host': 'localhost', 'port': 5432})
        {'host': 'localhost', 'port': 5432}

        >>> sanitize_connection_params_for_logging({})
        {}
    """
    if not params:
        return params

    sanitized = params.copy()

    # Mask sensitive fields
    sensitive_fields = {"password", "pass", "pwd", "secret", "token", "key"}

    for field in sensitive_fields:
        if field in sanitized and sanitized[field]:
            sanitized[field] = "***MASKED***"

    return sanitized


def sanitize_connection_string_for_logging(connection_string: str) -> str:
    """Sanitize connection string for safe logging by masking passwords.

    Handles both URI format and key=value format connection strings,
    masking sensitive information while preserving structure for debugging.

    Args:
        connection_string: Connection string that may contain passwords

    Returns:
        Sanitized connection string safe for logging

    Examples:
        >>> sanitize_connection_string_for_logging("postgres://user:secret@host:5432/db")
        'postgres://user:***MASKED***@host:5432/db'

        >>> sanitize_connection_string_for_logging("host=localhost password=secret")
        'host=localhost password=***MASKED***'

        >>> sanitize_connection_string_for_logging("host=localhost port=5432")
        'host=localhost port=5432'

        >>> sanitize_connection_string_for_logging("unparseable_string")
        '***CONNECTION_STRING_MASKED***'
    """
    if not connection_string:
        return connection_string

    # Handle URI format (e.g., postgres://user:pass@host:port/db)
    if "://" in connection_string:
        try:
            parsed = urlparse(connection_string)
            if parsed.password:
                # Replace password with mask
                masked_netloc = parsed.netloc.replace(f":{parsed.password}@", ":***MASKED***@")
                return connection_string.replace(parsed.netloc, masked_netloc)
            else:
                # No password in URI, return as-is
                return connection_string
        except (ValueError, AttributeError):
            # If parsing fails, fall through to key=value handling
            pass

    # Handle key=value format (e.g., "host=localhost password=secret")
    if "=" in connection_string:
        parts = []
        for part in connection_string.split():
            if "=" in part:
                key, _ = part.split("=", 1)
                key = key.strip().lower()
                if key in {"password", "pass", "pwd", "secret", "token"}:
                    parts.append(f"{key}=***MASKED***")
                else:
                    parts.append(part)
            else:
                parts.append(part)
        return " ".join(parts)

    # If we can't parse it safely, mask the entire string to be safe
    return "***CONNECTION_STRING_MASKED***"


def log_env(name: str, level: str | None = None) -> None:
    """Log the given environment variable with sensitive data masked.

    Safely logs environment variables by automatically detecting and masking
    sensitive values like passwords, connection strings, and tokens.

    Args:
        name: Name of the environment variable
        level: The log level (defaults to DEBUG)

    Examples:
        >>> import os
        >>> os.environ['TEST_HOST'] = 'localhost'
        >>> log_env('TEST_HOST')
        # Logs: TEST_HOST=localhost

        >>> os.environ['DB_PASSWORD'] = 'secret123'
        >>> log_env('DB_PASSWORD')
        # Logs: DB_PASSWORD=***MASKED***

        >>> os.environ['DB_CONNECTION'] = 'postgres://user:pass@host/db'
        >>> log_env('DB_CONNECTION')
        # Logs: DB_CONNECTION=postgres://user:***MASKED***@host/db
    """
    if not level:
        level = "DEBUG"

    value = os.getenv(name)

    # Check if this is a sensitive environment variable
    sensitive_env_vars = {
        "CGDB_CINFO",  # Connection info that may contain passwords
        "PASSWORD",
        "PASS",
        "PWD",
        "SECRET",
        "TOKEN",
        "KEY",
    }

    is_sensitive = any(sensitive_var.lower() in name.lower() for sensitive_var in sensitive_env_vars)

    if is_sensitive and value:
        # Sanitize the value if it looks like a connection string
        if "://" in value or "=" in value:
            sanitized_value = sanitize_connection_string_for_logging(value)
        else:
            sanitized_value = "***MASKED***"
        logger.log(level, f"{name}={sanitized_value}")
    else:
        logger.log(level, f"{name}={value}")
