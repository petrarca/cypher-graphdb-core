"""Contains various cross concern utility functions."""

import ast
import os
import secrets
import string
import uuid
from collections.abc import Callable, Container, Generator
from enum import Enum
from typing import Any, Final

import numpy as np
from loguru import logger
from pydantic import BaseModel

SCALAR_TYPES: Final[tuple[type, ...]] = (int, float, str, bool)


def is_scalar_type(value: object) -> bool:
    """Returns if an object is a scalar type.

    Args:
        value (object): the object to be checked

    Returns:
        bool: True if the object is a scalar type, False otherwise

    """
    return isinstance(value, SCALAR_TYPES)


def remove_from_dict(d: dict, keys: set[Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding the specified keys.

    Args:
        d: Original dictionary to filter.
        keys: Set of keys to remove from the dictionary.

    Returns:
        A dictionary containing only items whose keys are not in `keys`.

    """
    return {k: v for k, v in d.items() if k not in keys}


def remove_values_from_dict(d: dict, values: set[Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding entries with specified values.

    Args:
        d: Original dictionary to filter.
        values: Set of values to remove from the dictionary.

    Returns:
        A dictionary containing only items whose values are not in `values`.

    """
    return {k: v for k, v in d.items() if v not in values}


def remove_default_from_dict(d: dict[Any, Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding default empty values (None, False, empty containers).

    Args:
        d: Original dictionary to filter.

    Returns:
        A dictionary with no entries whose values are None, False, empty list, empty dict, or empty tuple.

    """
    return remove_values_from_dict(d, (None, False, [], {}, ()))


def concat_dict(*d: dict[Any, Any]) -> dict[Any, Any]:
    """Concatenate multiple dictionaries into one, with later values overriding earlier ones.

    Args:
        *d: One or more dictionaries to merge.

    Returns:
        A single dictionary containing all key-value pairs from the inputs.

    """
    result = {}

    for v in d:
        result.update(v)

    return result


def order_dict(d: dict, keys: set[Any]) -> dict[Any, Any]:
    """Reorder dictionary with specified keys first, followed by remaining keys.

    Args:
        d: Dictionary to reorder.
        keys: Set of keys to place at the beginning.

    Returns:
        A new dictionary with the specified keys first, then remaining keys.

    """
    return concat_dict({key: d[key] for key in set(keys)}, {key: d[key] for key in (set(d.keys()) - set(keys))})


def convert_to_str(value) -> str:
    """Convert various Python types to their string representation.

    Args:
        value: Value to convert (string, dict, list, None, or other).

    Returns:
        String representation of the value.

    """
    if isinstance(value, str):
        return f'"{value}"'
    elif isinstance(value, dict):
        return dict_to_non_quoted_json(value)
    elif isinstance(value, list):
        return "[" + ",".join(convert_to_str(item) for item in value) + "]"
    elif value is None:
        return "NULL"
    else:
        return str(value)


def type_to_default_value(t: type) -> Any:
    """Return the default value for a given type.

    Args:
        t: Python type to get default value for.

    Returns:
        Default value for the type (empty string for str, False for bool, etc.).

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


def dict_to_non_quoted_json(values: dict[str, Any]):
    """Convert dictionary to JSON-like string without quotes around keys.

    Args:
        values: Dictionary to convert.

    Returns:
        String representation in {key: value} format.

    """
    items = [f"{key}: {convert_to_str(value)}" for key, value in values.items()]
    return "{" + ",".join(items) + "}"


def dict_to_value_pairs(dict_values, prefix="", assignment_op="=", separator=","):
    """Convert dictionary to string of key-value pairs.

    Args:
        dict_values: Dictionary to convert.
        prefix: String to prefix each key with.
        assignment_op: Operator to use between key and value.
        separator: String to separate pairs.

    Returns:
        String representation of key-value pairs.

    """
    items = [f"{prefix}{key}{assignment_op}{convert_to_str(value)}" for key, value in dict_values.items()]
    return separator.join(items)


def dict_from_value_pairs(value_pairs: str | dict, assignment_op="=", separator=","):
    """Parse string of key-value pairs into a dictionary.

    Args:
        value_pairs: String to parse or existing dictionary.
        assignment_op: Operator used between key and value.
        separator: String that separates pairs.

    Returns:
        Dictionary parsed from the string.

    """
    if isinstance(value_pairs, dict):
        return value_pairs

    result = {}

    for pair in value_pairs.split(separator):
        key, value = pair.split(assignment_op)
        if value.startswith('"'):
            value = value.replace('"', "")
        elif value.startswith("'"):
            value = value.replace("'", "")
        elif value.isdigit():
            value = int(value)
        elif value.lower() in ("true", "false"):
            value = bool(value)
        elif value.lower() in ("null", "nil", "none"):
            value = None

        result[key.strip()] = value

    return result


def resolve_properties(properties):
    """Process and normalize property values in a dictionary.

    Args:
        properties: Dictionary of properties to normalize.

    Returns:
        Dictionary with normalized property values (NaN to None, string parsing, etc.).

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
            val = ast.literal_eval(value)
            properties[key] = val

    return properties


def generate_unique_string_id(length: int = 16):
    """Generate a unique string identifier combining UUID and random characters.

    Args:
        length: Total length of the generated ID.

    Returns:
        Unique string identifier.

    """
    part1 = length // 2
    part2 = length - part1

    uuid_str = str(uuid.uuid4())[:part1]

    random_string = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(part2))

    # Combine the UUIDv3 and the random string to create a unique string ID
    unique_id = uuid_str + random_string

    return unique_id


def resolve_template(template: str, **kwargs) -> str:
    """Resolve a string template with named arguments.

    Supports Python format strings with curly braces: "labels({node})[0]"

    Args:
        template: Template string with named placeholders in {name} format.
        **kwargs: Named arguments to substitute in the template.

    Returns:
        Resolved string with placeholders replaced by values.

    Raises:
        KeyError: If a placeholder in the template is not provided in kwargs.

    Examples:
        >>> resolve_template("labels({node})[0]", node="n")
        'labels(n)[0]'

        >>> resolve_template("MATCH ({node}:{label}) RETURN {node}",
        ...                   node="n", label="Person")
        'MATCH (n:Person) RETURN n'
    """
    return template.format(**kwargs)


class StringMatch:
    """Utility class for type checking string representations of collections."""

    @classmethod
    def is_list(cls, s: str) -> bool:
        """Check if string represents a list type.

        Args:
            s: String to check.

        Returns:
            True if string represents a list.

        """
        return issubclass(resolve_to_type(s), list)

    @classmethod
    def is_tuple(cls, s: str) -> bool:
        """Check if string represents a tuple type.

        Args:
            s: String to check.

        Returns:
            True if string represents a tuple.

        """
        return issubclass(resolve_to_type(s), tuple)

    @classmethod
    def is_set(cls, s: str) -> bool:
        """Check if string represents a set type.

        Args:
            s: String to check.

        Returns:
            True if string represents a set.

        """
        return issubclass(resolve_to_type(s), set)

    @classmethod
    def is_dict(cls, s: str):
        """Check if string represents a dictionary type.

        Args:
            s: String to check.

        Returns:
            True if string represents a dictionary.

        """
        return issubclass(resolve_to_type(s), dict)


def resolve_to_type(s: str) -> type:
    """Resolve string representation to its Python type.

    Args:
        s: String to evaluate.

    Returns:
        Type of the evaluated string, or NoneType if evaluation fails.

    """
    try:
        return type(ast.literal_eval(s))
    except (ValueError, SyntaxError):
        return type(None)


def isnan(value):
    """Check if value is a float NaN.

    Args:
        value: Value to check.

    Returns:
        True if value is a float NaN, False otherwise.

    """
    return isinstance(value, float) and np.isnan(value)


def split_str(s: str, sep: str) -> list[str]:
    """Split string by separator, respecting quoted sections.

    Args:
        s: String to split.
        sep: Separator to split on.

    Returns:
        List of string parts, with quoted sections preserved.

    """
    parts = []
    current = []
    quote_char = None
    escaped = False
    sep_len = len(sep)

    i = 0
    while i < len(s):
        char = s[i]

        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char in ('"', "'"):
            if quote_char == char:
                quote_char = None  # End of the quoted string
            elif quote_char is None:
                quote_char = char  # Start of a quoted string
            current.append(char)
        elif s[i : i + sep_len] == sep and quote_char is None:
            parts.append("".join(current))
            current = []
            i += sep_len - 1  # Skip the separator length
        else:
            current.append(char)
        i += 1

    parts.append("".join(current))  # Add the last part
    return parts


def partition_str(s: str, sep: str) -> tuple[str, str, str]:
    """Partition string into three parts: before separator, separator, after separator.

    Args:
        s: String to partition.
        sep: Separator to partition on.

    Returns:
        Tuple of (before, separator, after) strings.

    """
    parts = split_str(s, sep)

    if len(parts) > 1:
        return parts[0], sep, sep.join(parts[1:])
    else:
        return parts[0], "", ""


def _process_quoted_char(char: str, current: str, quote_char: str | None) -> tuple[str, str | None]:
    """Process a character that might be inside quotes.

    Args:
        char: The current character being processed
        current: The current accumulated string
        quote_char: The current quote character (None if not in quotes)

    Returns:
        Tuple of (updated_current, updated_quote_char)
    """
    if quote_char and char == quote_char:
        # End of quoted section
        return current + char, None
    elif not quote_char and char in ('"', "'"):
        # Start of quoted section
        return current + char, char
    else:
        # Regular character (inside or outside quotes)
        return current + char, quote_char


def _process_bracket(char: str, in_brackets: int) -> int:
    """Update bracket counter based on opening/closing brackets.

    Args:
        char: The current character being processed
        in_brackets: Current bracket nesting level

    Returns:
        Updated bracket count
    """
    if char in ("[", "{", "("):
        return in_brackets + 1
    elif char in ("]", "}", ")"):
        return in_brackets - 1
    return in_brackets


def _add_current_arg(current: str, is_kwarg: bool, key: str, args: list, kwargs: dict) -> tuple[str, bool]:
    """Add the current argument to either args or kwargs.

    Args:
        current: The current argument value
        is_kwarg: Whether this is a keyword argument
        key: The key name if this is a keyword argument
        args: List of positional arguments
        kwargs: Dictionary of keyword arguments

    Returns:
        Tuple of (empty_string, is_kwarg_reset)
    """
    current = current.strip()
    if is_kwarg:
        try:
            kwargs[key] = ast.literal_eval(current)
        except (ValueError, SyntaxError):
            kwargs[key] = current
        return "", False
    else:
        args.append(current)
        return "", False


def split_into_args_kwargs(s: str) -> tuple[tuple[str, ...], dict[str, str]]:
    """Parse string into positional and keyword arguments.

    Args:
        s: String containing comma-separated arguments.

    Returns:
        Tuple of (positional_args, keyword_args).
    """
    args = []
    kwargs = {}
    current = ""
    in_brackets = 0
    quote_char = None
    escaped = False
    is_kwarg = False
    key = ""

    for char in s:
        # Handle escaped characters
        if escaped:
            current += char
            escaped = False
            continue

        # Handle characters inside or outside quotes
        if quote_char is not None:
            current, quote_char = _process_quoted_char(char, current, quote_char)
        else:
            # Handle escape character
            if char == "\\":
                escaped = True
            # Handle quote characters
            elif char in ('"', "'"):
                current, quote_char = _process_quoted_char(char, current, quote_char)
            # Handle argument separator (comma)
            elif char == "," and in_brackets == 0:
                current, is_kwarg = _add_current_arg(current, is_kwarg, key, args, kwargs)
            # Handle all other characters
            else:
                # Update bracket counter
                new_bracket_count = _process_bracket(char, in_brackets)
                if new_bracket_count != in_brackets:
                    in_brackets = new_bracket_count
                # Handle key-value separator for kwargs
                elif char == "=" and in_brackets == 0 and not is_kwarg:
                    is_kwarg = True
                    key = current.strip()
                    current = ""
                    continue
                # Add character to current argument
                current += char

    # Handle the last argument if there is one
    if current:
        _add_current_arg(current, is_kwarg, key, args, kwargs)

    return tuple(args), kwargs


def args_to_dict(s: str | list[str]) -> dict[str, Any]:
    """Convert argument strings to a dictionary.

    Args:
        s: Argument string or list of argument strings.

    Returns:
        Dictionary with argument names as keys and values.

    """
    if not isinstance(s, list):
        s = [s]

    result = {}
    for val in s:
        args, kwargs = split_into_args_kwargs(val)

        result.update(dict.fromkeys(args, True))
        result.update(kwargs)

    return result


def try_literal_eval(literal) -> tuple[Any, bool]:
    """Attempt to evaluate a string as a Python literal.

    Args:
        literal: String to evaluate.

    Returns:
        Tuple of (evaluated_value, is_literal_string).

    """
    try:
        value = ast.literal_eval(literal)
        is_literal = False
    except (ValueError, SyntaxError):
        value = literal
        is_literal = True

    return value, is_literal


def startswith(s, tokens):
    """Check if string starts with any of the given tokens.

    Args:
        s: String to check.
        tokens: Collection of tokens to check against.

    Returns:
        Length of matched token, or -1 if no match found.

    """
    for value in tokens:
        if value is None:
            continue

        if s.startswith(value):
            return len(value)

    return -1


def resolve_fileformat(file_or_dirname):
    """Resolve file format from file extension.

    Args:
        file_or_dirname: File path or directory name.

    Returns:
        File format string (excel, csv, json, xml, yaml) or None.

    """
    ext_to_format = {
        ".xlsx": "excel",
        ".xls": "excel",
        ".csv": "csv",
        ".json": "json",
        ".xml": "xml",
        ".yaml": "yaml",
    }

    if not file_or_dirname:
        return None

    # Handle glob patterns like "*.csv" or "./out/*.csv"
    if "*" in file_or_dirname:
        # Extract extension from glob pattern
        # Examples: "*.csv" -> ".csv", "./out/*.xlsx" -> ".xlsx"
        import re

        pattern_match = re.search(r"\*\.([a-zA-Z0-9]+)$", file_or_dirname)
        if pattern_match:
            ext = "." + pattern_match.group(1)
            return ext_to_format.get(ext)
        return None

    # Handle direct extension input like ".csv" (not paths like "./file.csv")
    if file_or_dirname.startswith(".") and "/" not in file_or_dirname:
        return ext_to_format.get(file_or_dirname)

    # Handle full file path - extract extension
    _, ext = os.path.splitext(file_or_dirname)
    return ext_to_format.get(ext)


def split_path(path):
    """Split file path into directory, basename, and extension.

    Args:
        path: File path to split.

    Returns:
        Tuple of (directory, basename, extension).

    """
    assert path

    dirfile, ext = os.path.splitext(path)
    parts = dirfile.split("/")

    basename = parts[-1]
    dirname = "/".join(parts[:-1])

    return dirname, basename, ext


def resolve_nested_lengths(value):
    """Get the nested structure lengths of a collection.

    Args:
        value: Collection to analyze.

    Returns:
        Tuple of nested lengths.

    """
    result = []
    if isinstance(value, list | tuple | dict):
        n = len(value)
        if n > 0:
            result.append(n)
            j = resolve_nested_lengths(value[0])
            for k in j:
                result.append(k)

    return tuple(result)


def expected_nested_lengths(value, sizes) -> bool:
    """Check if value has expected nested structure lengths.

    Args:
        value: Collection to check.
        sizes: Expected nested sizes.

    Returns:
        True if structure matches expected sizes.

    """
    return resolve_nested_lengths(value) == sizes


def chunk_list(items: list[Any], batch_size: int) -> Generator[Any, Any, Any]:
    """Split list into chunks of specified size.

    Args:
        items: List to chunk.
        batch_size: Size of each chunk.

    Yields:
        List chunks of specified size.

    """
    assert isinstance(items, list)

    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def replace_empty_dicts(data: dict[Any, Any]) -> dict[Any, Any]:
    """Recursively replace in a hierarchical dictionary all empty values ({}) with None

    Args:
        data (Dict[Any, Any]): the dictionary to process

    Returns:
        Dict[Any, Any]: The processed dictionary. Same as the original dictionary.

    """
    for key, value in data.items():
        if isinstance(value, dict):
            # if the dictionry is empty {}
            if not value:
                data[key] = None
            else:
                replace_empty_dicts(value)
    return data


def sort_nested_dict(data: dict[Any, Any]) -> dict[Any, Any]:
    """Recursively sort a hierarchical dictionary by keys.

    Args:
        data (Dict[Any, Any]): The dictionary to process. Key needs to be sortable.

    Returns:
        Dict[Any, Any]: The new, sorted dictionary

    """
    if not isinstance(data, dict):
        return data
    sorted_dict = {}
    for key in sorted(data):
        sorted_dict[key] = sort_nested_dict(data[key])

    return sorted_dict


def extract_from_nested_dict(data: dict[Any, Any], path: str) -> dict[Any, Any]:
    """Extract values from nested dictionary using path specification.

    Args:
        data: Nested dictionary to extract from.
        path: Path specification for extraction.

    Returns:
        Dictionary with extracted values.

    """
    result = {}
    for key in path:
        if isinstance(key, tuple):
            sub_key = key[0]
            if sub_key in data:
                result[sub_key] = extract_from_nested_dict(data[sub_key], key[1:])
        else:
            if key in data:
                result[key] = data[key]
    return result


def log_env(name: str, level: str = None) -> None:
    """Log the given environment variable.

    Args:
        name (str): name of the environment variable
        level (str, optional): The log level. Defaults to DEBUG.

    """
    if not level:
        level = "DEBUG"

    logger.log(level, f"{name}={os.getenv(name)}")


def _convert_rich_repr(items) -> dict:
    """Convert rich representation items to a dictionary."""
    return {v[0]: v[1] for v in items}


def _handle_special_types(value, use_enum_name: bool, encode_base_models: bool):
    """Handle special Python types like Enum, type objects, and BaseModel instances."""
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
    """Process collection objects (dict, list, tuple, set)."""
    if isinstance(obj, dict):
        return {key: convert_func(value) for key, value in obj.items()}
    if isinstance(obj, list | tuple | set):
        return type(obj)([convert_func(value) for value in obj])
    if hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [convert_func(value) for value in obj]
    return None


def _handle_recursive_conversion(value, params):
    """Handle recursive conversion of values with the same parameters."""
    return to_collection(
        value,
        expand_objects=params["expand_objects"],
        encode_base_models=params["encode_base_models"],
        encoder=params["encoder"],
        use_enum_name=params["use_enum_name"],
        context=params["context"],
    )


def _try_custom_encoder(value, encoder, context):
    """Try using a custom encoder if provided."""
    if not encoder:
        return None
    return encoder(value, context)


def _handle_object_dict(value, params):
    """Handle objects with __dict__ attribute."""
    if not params["expand_objects"] or not hasattr(value, "__dict__"):
        return None
    return _handle_recursive_conversion(vars(value), params)


def _prepare_object(obj):
    """Prepare object for conversion, handling None and __dict__ cases."""
    if obj is None:
        return None, True

    if hasattr(obj, "__dict__") and not isinstance(obj, type):
        return obj.__dict__, False

    return obj, False


def _handle_scalar(obj):
    """Handle scalar type objects."""
    if is_scalar_type(obj):
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

    Args:
        obj: Object to convert
        expand_objects: Whether to expand object attributes
        encode_base_models: Whether to encode Pydantic BaseModel instances
        encoder: Custom encoder function
        use_enum_name: Whether to use enum names instead of values
        context: Context to pass to the encoder

    Returns:
        JSON-serializable collection representation of the object
    """
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
        if value is None or is_scalar_type(value):
            return value

        # Handle collections recursively
        if isinstance(value, dict | tuple | list | set):
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


def str_to_collection(str_val: str) -> str | None | Container[str]:
    """Parse string representation of a collection into the actual collection.

    Args:
        str_val: String representation of list, set, tuple, or plain string.

    Returns:
        Parsed collection or original string if not a collection format.

    """
    _map = (
        ("[", "]", list),
        ("{", "}", set),
        ("(", ")", tuple),
    )

    if str_val is None:
        return None

    str_val = str_val.strip()

    for m in _map:
        if str_val.startswith(m[0]) and str_val.endswith(m[1]):
            return m[2]([v.strip() for v in str_val.strip(f"{m[0]}{m[1]}").split(",")])

    return str_val


def slice_model_properties(cls: type[BaseModel], properties: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split properties into model fields and extra properties.

    Args:
        cls: Pydantic model class.
        properties: Dictionary of properties to split.

    Returns:
        Tuple of (model_properties, extra_properties).

    """
    fields = cls.model_fields
    # Contains properties which are in the pydantic model
    t1 = {}
    # Contains properties which are not in the pydantic model
    t2 = {}

    for k, v in properties.items():
        if k not in fields:
            t2[k] = v
        else:
            t1[k] = v

    return t1, t2


def unnest_result(result, unnest_mode: str | bool | None):
    """Unnest result collections based on specified mode.

    Args:
        result: Result to unnest.
        unnest_mode: Mode for unnesting ('r', 'c', 'rc', 'cr', True, False, or None).

    Returns:
        Unnested result based on the mode.

    """
    if not unnest_mode or result is None:
        return result

    assert isinstance(unnest_mode, str | bool), "unnest_result must be of type str|bool|None!"

    if isinstance(unnest_mode, bool):
        unnest_mode = "rc" if unnest_mode else ""

    if unnest_mode in ("r", "rc", "cr") and isinstance(result, list):
        # handle Dict|ScalarValue] result sets
        result = result[0] if len(result) == 1 else None if len(result) == 0 else result

    if result and unnest_mode in ("c", "rc", "cr"):
        if isinstance(result, list) and len(result) > 0:
            # handle not empty List[Tuple, ScalarValue]
            if isinstance(result[0], tuple) and len(result[0]) < 2:
                for i, value in enumerate(result):
                    result[i] = value[0] if len(value) == 1 else None
        elif isinstance(result, tuple):
            # hanlde Tuple[Any, ...]
            result = result[0] if len(result) == 1 else None if len(result) == 0 else result

    return result
