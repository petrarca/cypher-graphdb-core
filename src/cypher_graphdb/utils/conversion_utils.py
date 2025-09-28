"""Type conversion and object serialization utilities.

This module provides utilities for converting between different Python types,
serializing objects to collections, and handling type-related operations.
"""

import ast
from collections.abc import Callable
from enum import Enum
from typing import Any

import numpy as np
from pydantic import BaseModel


def type_to_default_value(t: type) -> Any:
    """Return the default value for a given type.

    Provides sensible default values for common Python types.

    Args:
        t: Python type to get default value for.

    Returns:
        Default value for the type (empty string for str, False for bool, etc.).

    Examples:
        >>> type_to_default_value(str)
        ''

        >>> type_to_default_value(int)
        0

        >>> type_to_default_value(bool)
        False

        >>> type_to_default_value(list)
        []

        >>> type_to_default_value(dict)
        {}

        >>> type_to_default_value(set)
        set()
    """
    if t is str:
        return ""
    if t is bool:
        return False
    if t is int:
        return 0
    if t is dict:
        return {}
    if t is list:
        return []
    if t is set:
        return set()
    if t is tuple:
        return ()

    return None


def resolve_properties(properties: dict) -> dict:
    """Process and normalize property values in a dictionary.

    Handles various property value formats including NaN values, None strings,
    and JSON-like string representations.

    Args:
        properties: Dictionary of properties to normalize.

    Returns:
        Dictionary with normalized property values (NaN to None, string parsing, etc.).

    Examples:
        >>> props = {"p1": "None", "p2": "['a', 1]", "p3": "{'key': 'value'}"}
        >>> resolve_properties(props)
        {'p1': None, 'p2': ['a', 1], 'p3': {'key': 'value'}}

        >>> resolve_properties({"normal": "value", "null_str": "null"})
        {'normal': 'value', 'null_str': None}

        >>> resolve_properties({"number": 42, "keep": "as-is"})
        {'number': 42, 'keep': 'as-is'}
    """
    for key, value in properties.items():
        # convert NAN (null values) to None
        if isnan(value):
            properties[key] = None
            continue

        if not isinstance(value, str):
            continue

        if value.strip().lower() in ("nil", "null", "none"):
            properties[key] = None
            continue

        if value.startswith(("[", "{", '"', "'")):
            # handle quoted str, dict, list - simplified detection for now
            try:
                val = ast.literal_eval(value)
                properties[key] = val
            except (ValueError, SyntaxError):
                # If parsing fails, keep original value
                pass

    return properties


def isnan(value) -> bool:
    """Check if value is a float NaN.

    Uses numpy to check for NaN values, handling both float and non-float types safely.

    Args:
        value: Value to check.

    Returns:
        True if value is a float NaN, False otherwise.

    Examples:
        >>> import numpy as np
        >>> isnan(np.nan)
        True

        >>> isnan(42.0)
        False

        >>> isnan("not a number")
        False

        >>> isnan(None)
        False
    """
    return isinstance(value, float) and np.isnan(value)


def _convert_rich_repr(items) -> dict:
    """Convert rich representation items to a dictionary.

    Internal helper for handling objects with __rich_repr__ method.

    Args:
        items: Rich representation items from __rich_repr__.

    Returns:
        Dictionary representation of rich repr items.
    """
    return {v[0]: v[1] for v in items}


def _handle_special_types(value, use_enum_name: bool, encode_base_models: bool):
    """Handle special Python types like Enum, type objects, and BaseModel instances.

    Args:
        value: The value to handle.
        use_enum_name: Whether to use enum names instead of values.
        encode_base_models: Whether to encode Pydantic BaseModel instances.

    Returns:
        Processed value or None if not a special type.
    """
    # Handle type objects
    if isinstance(value, type):
        return f"{value.__module__}.{value.__name__}"

    # Handle Enum values
    if use_enum_name and isinstance(value, Enum):
        return value.name

    # Handle Pydantic models
    if encode_base_models and isinstance(value, BaseModel):
        return value.model_dump()

    # Handle objects with rich representation
    if hasattr(value, "__rich_repr__"):
        return _convert_rich_repr(value.__rich_repr__())

    return None


def _process_collection(obj, convert_func):
    """Process collection objects (dict, list, tuple, set).

    Args:
        obj: Collection object to process.
        convert_func: Function to apply to each element.

    Returns:
        Processed collection or None if not a collection.
    """
    if isinstance(obj, dict):
        return {key: convert_func(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return type(obj)([convert_func(value) for value in obj])
    if hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [convert_func(value) for value in obj]
    return None


def _handle_recursive_conversion(value, params):
    """Handle recursive conversion of values with the same parameters.

    Args:
        value: Value to convert recursively.
        params: Conversion parameters dictionary.

    Returns:
        Recursively converted value.
    """
    return to_collection(
        value,
        expand_objects=params["expand_objects"],
        encode_base_models=params["encode_base_models"],
        encoder=params["encoder"],
        use_enum_name=params["use_enum_name"],
        context=params["context"],
    )


def _try_custom_encoder(value, encoder, context):
    """Try using a custom encoder if provided.

    Args:
        value: Value to encode.
        encoder: Custom encoder function.
        context: Context to pass to encoder.

    Returns:
        Encoded value or None if no encoder or encoding failed.
    """
    if not encoder:
        return None
    try:
        return encoder(value, context)
    except Exception:
        return None


def _handle_object_dict(value, params):
    """Handle objects with __dict__ attribute.

    Args:
        value: Object to handle.
        params: Conversion parameters.

    Returns:
        Converted object dict or None.
    """
    if not params["expand_objects"] or not hasattr(value, "__dict__"):
        return None
    return _handle_recursive_conversion(vars(value), params)


def _prepare_object(obj):
    """Prepare object for conversion, handling None and __dict__ cases.

    Args:
        obj: Object to prepare.

    Returns:
        Tuple of (prepared_obj, is_done).
    """
    if obj is None:
        return None, True

    if hasattr(obj, "__dict__") and not isinstance(obj, type):
        return obj.__dict__, False

    return obj, False


def _handle_scalar(obj):
    """Handle scalar type objects.

    Args:
        obj: Object to check for scalar type.

    Returns:
        Tuple of (wrapped_scalar, is_done).
    """
    # Define scalar types inline to avoid circular imports
    scalar_types = (int, float, str, bool)

    if isinstance(obj, scalar_types):
        return {"val": obj}, True
    return None, False


def to_collection(
    obj: object,
    expand_objects: bool = False,
    encode_base_models: bool = True,
    encoder: Callable | None = None,
    use_enum_name: bool = True,
    context: Any = None,
) -> list[Any] | dict[str, Any] | tuple[Any, ...] | set[Any] | None:
    """Convert Python objects to JSON-serializable collections.

    Recursively converts complex Python objects into JSON-serializable
    collections (dicts, lists, etc.), with various customization options.

    Args:
        obj: Object to convert
        expand_objects: Whether to expand object attributes using __dict__
        encode_base_models: Whether to encode Pydantic BaseModel instances
        encoder: Custom encoder function for special handling
        use_enum_name: Whether to use enum names instead of values
        context: Context to pass to the encoder function

    Returns:
        JSON-serializable collection representation of the object

    Examples:
        >>> to_collection({"a": 1, "b": [2, 3]})
        {'a': 1, 'b': [2, 3]}

        >>> from enum import Enum
        >>> class Color(Enum):
        ...     RED = 1
        ...     BLUE = 2
        >>> to_collection([Color.RED, Color.BLUE])
        ['RED', 'BLUE']

        >>> class Person:
        ...     def __init__(self, name, age):
        ...         self.name = name
        ...         self.age = age
        >>> to_collection(Person("John", 30), expand_objects=True)
        {'name': 'John', 'age': 30}
    """
    # Define scalar types inline to avoid circular imports
    scalar_types = (int, float, str, bool)

    # Store parameters for recursive calls
    params = {
        "expand_objects": expand_objects,
        "encode_base_models": encode_base_models,
        "encoder": encoder,
        "use_enum_name": use_enum_name,
        "context": context,
    }

    def convert_value(value):
        # Handle None and scalar types directly
        if value is None or isinstance(value, scalar_types):
            return value

        # Handle collections recursively
        if isinstance(value, (dict, tuple, list, set)):
            return _handle_recursive_conversion(value, params)

        # Try custom encoder if provided
        encoder_result = _try_custom_encoder(value, encoder, context)
        if encoder_result is not None:
            return encoder_result

        # Handle special types
        special_result = _handle_special_types(value, use_enum_name, encode_base_models)
        if special_result is not None:
            return special_result

        # Handle objects with __dict__ attribute
        dict_result = _handle_object_dict(value, params)
        if dict_result is not None:
            return dict_result

        # Default: convert to string
        return str(value)

    # Prepare the object (handle None and __dict__ cases)
    prepared_obj, is_done = _prepare_object(obj)
    if is_done:
        return prepared_obj

    # Handle scalar types
    scalar_result, is_done = _handle_scalar(prepared_obj)
    if is_done:
        return scalar_result

    # Process collections
    collection_result = _process_collection(prepared_obj, convert_value)
    if collection_result is not None:
        return collection_result

    # Default case: convert the value
    return convert_value(prepared_obj)
