"""String processing and parsing utilities.

This module provides utility functions for string manipulation, parsing,
template resolution, and string-based type conversions.
"""

import ast
from typing import Any


def _escape_cypher_string(value: str) -> str:
    """Escape a string for safe use in double-quoted Cypher literals.

    Handles backslashes, double quotes, newlines, carriage returns, and tabs.

    Args:
        value: Raw string to escape.

    Returns:
        Escaped string (without surrounding quotes).
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def convert_to_str(value: Any) -> str:
    """Convert various Python types to their Cypher literal representation.

    Produces valid Cypher syntax: lowercase null/true/false, escaped strings,
    unquoted keys in maps.

    Args:
        value: Value to convert (string, dict, list, None, bool, or other).

    Returns:
        Cypher literal string representation of the value.

    Examples:
        >>> convert_to_str("hello")
        '"hello"'

        >>> convert_to_str({"a": 1, "b": 2})
        '{a: 1,b: 2}'

        >>> convert_to_str([1, 2, 3])
        '[1,2,3]'

        >>> convert_to_str(None)
        'null'

        >>> convert_to_str(42)
        '42'

        >>> convert_to_str(True)
        'true'

        >>> convert_to_str('say "hi"')
        '"say \\\\"hi\\\\""'
    """
    # bool must be checked before int (bool is a subclass of int in Python)
    if isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, str):
        return f'"{_escape_cypher_string(value)}"'
    elif isinstance(value, dict):
        return dict_to_non_quoted_json(value)
    elif isinstance(value, list):
        return "[" + ",".join(convert_to_str(item) for item in value) + "]"
    elif value is None:
        return "null"
    else:
        return str(value)


def dict_to_non_quoted_json(values: dict[str, Any]) -> str:
    """Convert dictionary to JSON-like string without quotes around keys.

    Creates a JSON-like representation where keys are not quoted,
    useful for certain serialization formats.

    Args:
        values: Dictionary to convert.

    Returns:
        String representation in {key: value} format.

    Examples:
        >>> dict_to_non_quoted_json({"name": "John", "age": 30})
        '{name: "John",age: 30}'

        >>> dict_to_non_quoted_json({"items": [1, 2, 3]})
        '{items: [1,2,3]}'

        >>> dict_to_non_quoted_json({})
        '{}'
    """
    items = [f"{key}: {convert_to_str(value)}" for key, value in values.items()]
    return "{" + ",".join(items) + "}"


def dict_to_value_pairs(dict_values: dict[str, Any], prefix: str = "", assignment_op: str = "=", separator: str = ",") -> str:
    """Convert dictionary to string of key-value pairs.

    Transforms a dictionary into a delimited string of key-value pairs,
    with configurable prefix, assignment operator, and separator.

    Args:
        dict_values: Dictionary to convert.
        prefix: String to prefix each key with.
        assignment_op: Operator to use between key and value.
        separator: String to separate pairs.

    Returns:
        String representation of key-value pairs.

    Examples:
        >>> dict_to_value_pairs({"a": 1, "b": "hello"})
        'a=1,b="hello"'

        >>> dict_to_value_pairs({"x": 10, "y": 20}, prefix="var_", assignment_op=":", separator="; ")
        'var_x:10; var_y:20'

        >>> dict_to_value_pairs({})
        ''
    """
    items = [f"{prefix}{key}{assignment_op}{convert_to_str(value)}" for key, value in dict_values.items()]
    return separator.join(items)


def dict_from_value_pairs(value_pairs: str | dict, assignment_op: str = "=", separator: str = ",") -> dict:
    """Parse string of key-value pairs into a dictionary.

    Parses a delimited string of key-value pairs back into a dictionary,
    with automatic type conversion for common types.

    Args:
        value_pairs: String to parse or existing dictionary.
        assignment_op: Operator used between key and value.
        separator: String that separates pairs.

    Returns:
        Dictionary parsed from the string.

    Examples:
        >>> dict_from_value_pairs('a=1,b="hello",c=true')
        {'a': 1, 'b': 'hello', 'c': True}

        >>> dict_from_value_pairs('x:10;y:20', assignment_op=":", separator=";")
        {'x': 10, 'y': 20}

        >>> dict_from_value_pairs({'existing': 'dict'})
        {'existing': 'dict'}
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


def resolve_template(template: str, **kwargs: Any) -> str:
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

        >>> resolve_template("Hello {name}!", name="World")
        'Hello World!'

        >>> resolve_template("No placeholders here")
        'No placeholders here'
    """
    return template.format(**kwargs)


class StringMatch:
    """Utility class for type checking string representations of collections.

    Provides static methods to determine if a string represents various
    Python collection types (list, tuple, set, dict).
    """

    @classmethod
    def is_list(cls, s: str) -> bool:
        """Check if string represents a list type.

        Args:
            s: String to check.

        Returns:
            True if string represents a list.

        Examples:
            >>> StringMatch.is_list("[1,2,3]")
            True

            >>> StringMatch.is_list("[]")
            True

            >>> StringMatch.is_list("not a list")
            False
        """
        return issubclass(resolve_to_type(s), list)

    @classmethod
    def is_tuple(cls, s: str) -> bool:
        """Check if string represents a tuple type.

        Args:
            s: String to check.

        Returns:
            True if string represents a tuple.

        Examples:
            >>> StringMatch.is_tuple("(1,2,3)")
            True

            >>> StringMatch.is_tuple("()")
            True

            >>> StringMatch.is_tuple("not a tuple")
            False
        """
        return issubclass(resolve_to_type(s), tuple)

    @classmethod
    def is_set(cls, s: str) -> bool:
        """Check if string represents a set type.

        Args:
            s: String to check.

        Returns:
            True if string represents a set.

        Examples:
            >>> StringMatch.is_set("{1,2,3}")
            True

            >>> StringMatch.is_set("set()")
            False  # This would be a function call, not a literal

            >>> StringMatch.is_set("not a set")
            False
        """
        return issubclass(resolve_to_type(s), set)

    @classmethod
    def is_dict(cls, s: str) -> bool:
        """Check if string represents a dictionary type.

        Args:
            s: String to check.

        Returns:
            True if string represents a dictionary.

        Examples:
            >>> StringMatch.is_dict("{'a': 1, 'b': 2}")
            True

            >>> StringMatch.is_dict("{}")
            True

            >>> StringMatch.is_dict("not a dict")
            False
        """
        return issubclass(resolve_to_type(s), dict)


def resolve_to_type(s: str) -> type:
    """Resolve string representation to its Python type.

    Attempts to evaluate a string as a Python literal and returns the type
    of the resulting object.

    Args:
        s: String to evaluate.

    Returns:
        Type of the evaluated string, or NoneType if evaluation fails.

    Examples:
        >>> resolve_to_type("[1,2,3]")
        <class 'list'>

        >>> resolve_to_type("{'a': 1}")
        <class 'dict'>

        >>> resolve_to_type("invalid syntax")
        <class 'NoneType'>
    """
    try:
        return type(ast.literal_eval(s))
    except ValueError, SyntaxError:
        return type(None)


def try_literal_eval(literal: str) -> tuple[Any, bool]:
    """Attempt to evaluate a string as a Python literal.

    Tries to parse a string as a Python literal expression. If successful,
    returns the parsed value; otherwise, returns the original string.

    Args:
        literal: String to evaluate.

    Returns:
        Tuple of (evaluated_value, is_literal_string).
        is_literal_string is True if evaluation failed (meaning it's a literal string).

    Examples:
        >>> try_literal_eval("42")
        (42, False)

        >>> try_literal_eval("'hello'")
        ('hello', False)

        >>> try_literal_eval("[1,2,3]")
        ([1, 2, 3], False)

        >>> try_literal_eval("variable_name")
        ('variable_name', True)
    """
    try:
        value = ast.literal_eval(literal)
        is_literal = False
    except ValueError, SyntaxError:
        value = literal
        is_literal = True

    return value, is_literal


def starts_with(s: str, tokens: list[str] | tuple[str, ...]) -> int:
    """Check if string starts with any of the given tokens.

    Checks if a string starts with any token from a collection and returns
    the length of the matched token.

    Args:
        s: String to check.
        tokens: Collection of tokens to check against.

    Returns:
        Length of matched token, or -1 if no match found.

    Examples:
        >>> starts_with("hello world", ["hello", "hi"])
        5

        >>> starts_with("testing", ["test", "temp"])
        4

        >>> startswith("nomatch", ["yes", "no"])
        -1

        >>> startswith("example", [None, "ex"])
        2
    """
    for value in tokens:
        if value is None:
            continue

        if s.startswith(value):
            return len(value)

    return -1


def str_to_collection(str_val: str) -> str | None | list | set | tuple:
    """Parse string representation of a collection into the actual collection.

    Parses string representations of Python collections (list, set, tuple)
    into their actual collection types.

    Args:
        str_val: String representation of list, set, tuple, or plain string.

    Returns:
        Parsed collection or original string if not a collection format.

    Examples:
        >>> str_to_collection("[1,2,3]")
        ['1', '2', '3']

        >>> str_to_collection("{a,b,c}")
        {'a', 'b', 'c'}

        >>> str_to_collection("(x,y)")
        ('x', 'y')

        >>> str_to_collection("plain text")
        'plain text'

        >>> str_to_collection(None)
        None
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


def resolve_env_var(value: str, default: str | None = None) -> str:
    """Resolve environment variable in a string using shell-style syntax.

    Supports:
    - $VAR          - Simple variable name (terminated by non-alphanumeric)
    - ${VAR}        - Braced variable name
    - ${VAR:default} - Inline default value (takes precedence over parameter default)

    Args:
        value: The string that may contain env var references
        default: Default value if env var is not set (used when no inline default)

    Returns:
        The resolved string with env vars replaced by their values

    Raises:
        ValueError: If an environment variable is not set and no default provided

    Examples:
        >>> import os
        >>> os.environ["HOST"] = "localhost"
        >>> resolve_env_var("$HOST")
        'localhost'
        >>> resolve_env_var("${PORT:8080}")
        '8080'
        >>> resolve_env_var("$MISSING", default="fallback")
        'fallback'
        >>> resolve_env_var("no_vars_here")
        'no_vars_here'
    """
    import os
    import re

    if not value:
        return value

    def replace_var(match: re.Match) -> str:
        var_name = match.group(1)
        inline_default = match.group(2)

        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        if inline_default is not None:
            return inline_default
        if default is not None:
            return default
        raise ValueError(f"Environment variable '{var_name}' not set and no default provided")

    pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"
    result = re.sub(pattern, replace_var, value)

    simple_pattern = r"\$([A-Za-z_][A-Za-z0-9_]*)"
    result = re.sub(simple_pattern, lambda m: os.environ.get(m.group(1), default or m.group(0)), result)

    return result


def resolve_env_var_or_none(value: str | None, default: str | None = None) -> str | None:
    """Resolve environment variable or return None if value is None.

    Args:
        value: The value to resolve, or None
        default: Default value if env var is not set

    Returns:
        The resolved string, or None if input was None
    """
    if value is None:
        return None
    return resolve_env_var(value, default=default)
