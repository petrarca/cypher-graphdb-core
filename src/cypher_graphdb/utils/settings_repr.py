"""Utility functions for safe settings representation with sanitized fields."""

from collections.abc import Callable
from typing import Any


def safe_settings_repr(
    instance: Any,
    field_sanitizers: dict[str, Callable[[Any], Any]] | None = None,
    class_name_override: str | None = None,
) -> str:
    """Generate safe string representation with sanitized sensitive fields.

    Args:
        instance: The settings instance to represent
        field_sanitizers: Dictionary mapping field names to sanitizer functions
        class_name_override: Optional class name override for representation

    Returns:
        String representation with sensitive fields sanitized

    Example:
        >>> def sanitize_password(value):
        ...     return "***MASKED***" if value else None
        >>> safe_settings_repr(
        ...     settings,
        ...     field_sanitizers={"password": sanitize_password}
        ... )
        'Settings(host="localhost", password="***MASKED***")'
    """
    if field_sanitizers is None:
        field_sanitizers = {}

    fields = []
    # Use class-level model_fields to avoid Pydantic deprecation warning
    model_fields = getattr(instance.__class__, "model_fields", {})

    for field_name, _field_info in model_fields.items():
        value = getattr(instance, field_name, None)

        # Apply sanitizer if available for this field
        if field_name in field_sanitizers:
            value = field_sanitizers[field_name](value)

        # Include all fields (matching Pydantic's default behavior)
        fields.append(f"{field_name}={value!r}")

    class_name = class_name_override or instance.__class__.__name__
    return f"{class_name}({', '.join(fields)})"


def safe_settings_str(
    instance: Any,
    field_sanitizers: dict[str, Callable[[Any], Any]] | None = None,
) -> str:
    """Generate safe string representation of settings for informal display.

    Args:
        instance: The settings instance to represent
        field_sanitizers: Dictionary mapping field names to sanitizer functions

    Returns:
        Space-separated field=value pairs with sensitive fields sanitized

    Example:
        >>> safe_settings_str(settings, {"password": lambda x: "***MASKED***"})
        'host="localhost" password="***MASKED***"'
    """
    if field_sanitizers is None:
        field_sanitizers = {}

    fields = []
    # Use class-level model_fields to avoid Pydantic deprecation warning
    model_fields = getattr(instance.__class__, "model_fields", {})

    for field_name, _field_info in model_fields.items():
        value = getattr(instance, field_name, None)

        # Apply sanitizer if available for this field
        if field_name in field_sanitizers:
            value = field_sanitizers[field_name](value)

        # Include all fields (matching Pydantic's default behavior)
        fields.append(f"{field_name}={value!r}")

    return " ".join(fields)
