"""Collection manipulation utilities for dictionaries, lists, tuples, and sets.

This module provides utility functions for working with Python collections,
including dictionary operations, list processing, and nested data structure
manipulation.
"""

from collections.abc import Generator
from typing import Any

from pydantic import BaseModel


def remove_from_dict(d: dict, keys: set[Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding the specified keys.

    Creates a shallow copy of the dictionary with certain keys removed.
    The original dictionary remains unchanged.

    Args:
        d: Original dictionary to filter.
        keys: Set of keys to remove from the dictionary.

    Returns:
        A dictionary containing only items whose keys are not in `keys`.

    Examples:
        >>> data = {"a": 1, "b": 2, "c": 3}
        >>> remove_from_dict(data, {"b", "c"})
        {"a": 1}

        >>> remove_from_dict({"x": 10, "y": 20}, {"z"})
        {"x": 10, "y": 20}

        >>> remove_from_dict({}, {"any_key"})
        {}
    """
    return {k: v for k, v in d.items() if k not in keys}


def remove_values_from_dict(d: dict, values: set[Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding entries with specified values.

    Creates a shallow copy of the dictionary with certain values removed.
    The original dictionary remains unchanged.

    Args:
        d: Original dictionary to filter.
        values: Set of values to remove from the dictionary.

    Returns:
        A dictionary containing only items whose values are not in `values`.

    Examples:
        >>> data = {"a": 1, "b": None, "c": 3, "d": None}
        >>> remove_values_from_dict(data, {None})
        {"a": 1, "c": 3}

        >>> remove_values_from_dict({"x": "keep", "y": "remove"}, {"remove"})
        {"x": "keep"}

        >>> remove_values_from_dict({"a": 1, "b": 2}, {3, 4})
        {"a": 1, "b": 2}
    """
    return {k: v for k, v in d.items() if v not in values}


def remove_default_from_dict(d: dict[Any, Any]) -> dict[Any, Any]:
    """Return a new dictionary excluding default empty values.

    Removes entries with values that are considered "empty" or default:
    None, False, empty list [], empty dict {}, and empty tuple ().

    Args:
        d: Original dictionary to filter.

    Returns:
        A dictionary with no entries whose values are None, False,
        empty list, empty dict, or empty tuple.

    Examples:
        >>> data = {
        ...     "a": 10, "b": True, "c": False, "d": None,
        ...     "e": [], "f": {}, "g": ()
        ... }
        >>> remove_default_from_dict(data)
        {"a": 10, "b": True}

        >>> remove_default_from_dict({"valid": "data", "empty": None})
        {"valid": "data"}

        >>> remove_default_from_dict({"all_valid": 1, "also_valid": "text"})
        {"all_valid": 1, "also_valid": "text"}
    """
    # Use individual checks since empty containers are not hashable
    return {k: v for k, v in d.items() if v is not None and v is not False and v != [] and v != {} and v != ()}


def concat_dict(*d: dict[Any, Any]) -> dict[Any, Any]:
    """Concatenate multiple dictionaries into one.

    Later values override earlier ones for the same keys.

    Args:
        *d: One or more dictionaries to merge.

    Returns:
        A single dictionary containing all key-value pairs from the inputs.

    Examples:
        >>> dict1 = {"a": 1, "b": 2}
        >>> dict2 = {"b": 3, "c": 4}
        >>> dict3 = {"c": 5, "d": 6}
        >>> concat_dict(dict1, dict2, dict3)
        {"a": 1, "b": 3, "c": 5, "d": 6}

        >>> concat_dict({"x": 10}, {"y": 20})
        {"x": 10, "y": 20}

        >>> concat_dict({})
        {}
    """
    result = {}

    for v in d:
        result.update(v)

    return result


def order_dict(d: dict, keys: set[Any] | tuple[Any, ...] | list[Any]) -> dict[Any, Any]:
    """Reorder dictionary with specified keys first.

    Remaining keys follow in their original order.

    Creates a new dictionary where the specified keys appear first
    (in the order provided if using tuple/list, or in arbitrary order if using set),
    followed by the remaining keys from the original dictionary.

    Args:
        d: Dictionary to reorder.
        keys: Collection of keys to place at the beginning. Use tuple or list to preserve order.

    Returns:
        A new dictionary with the specified keys first, then remaining keys.

    Examples:
        >>> data = {"c": 3, "a": 1, "b": 2, "d": 4}
        >>> order_dict(data, ("a", "b"))  # Order preserved with tuple
        {"a": 1, "b": 2, "c": 3, "d": 4}

        >>> order_dict({"x": 1, "y": 2}, ("y",))
        {"y": 2, "x": 1}

        >>> order_dict({"a": 1, "b": 2}, {"nonexistent"})
        {"a": 1, "b": 2}

        >>> order_dict({"z": 1, "a": 2}, ["a"])  # Order preserved with list
        {"a": 2, "z": 1}
    """
    # Preserve order by iterating keys in the order provided (not as set)
    ordered_keys = {key: d[key] for key in keys if key in d}
    keys_set = set(keys)
    remaining_keys = {key: d[key] for key in d if key not in keys_set}
    return concat_dict(ordered_keys, remaining_keys)


def replace_empty_dicts(data: dict[Any, Any]) -> dict[Any, Any]:
    """Recursively replace empty values ({}) with None.

    Traverses a nested dictionary structure and replaces any empty dictionary
    values with None. The operation is performed in-place on the original
    dictionary.

    Args:
        data: The dictionary to process.

    Returns:
        The processed dictionary (same as the original dictionary,
        modified in-place).

    Examples:
        >>> nested_data = {
        ...     "a": {"nested": "value"},
        ...     "b": {},
        ...     "c": {"deeper": {"empty": {}}}
        ... }
        >>> replace_empty_dicts(nested_data)
        {"a": {"nested": "value"}, "b": None, "c": {"deeper": {"empty": None}}}

        >>> simple_data = {"empty": {}, "filled": {"key": "value"}}
        >>> replace_empty_dicts(simple_data)
        {"empty": None, "filled": {"key": "value"}}
    """
    for key, value in data.items():
        if isinstance(value, dict):
            # if the dictionary is empty {}
            if not value:
                data[key] = None
            else:
                replace_empty_dicts(value)
    return data


def sort_nested_dict(data: dict[Any, Any]) -> dict[Any, Any]:
    """Recursively sort a hierarchical dictionary by keys.

    Creates a new dictionary with all keys sorted at every level of nesting.
    Keys must be sortable types (strings, numbers, etc.).

    Args:
        data: The dictionary to process. Keys need to be sortable.

    Returns:
        The new, sorted dictionary.

    Examples:
        >>> unsorted = {"c": 3, "a": 1, "b": {"z": 26, "x": 24}}
        >>> sort_nested_dict(unsorted)
        {"a": 1, "b": {"x": 24, "z": 26}, "c": 3}

        >>> sort_nested_dict({"z": {"c": 1, "a": 2}, "a": "value"})
        {"a": "value", "z": {"a": 2, "c": 1}}

        >>> sort_nested_dict({})
        {}
    """
    if not isinstance(data, dict):
        return data
    sorted_dict = {}
    for key in sorted(data):
        sorted_dict[key] = sort_nested_dict(data[key])

    return sorted_dict


def extract_from_nested_dict(data: dict[Any, Any], path: tuple) -> dict[Any, Any]:
    """Extract values from nested dictionary using path specification.

    Extracts specific keys and nested paths from a hierarchical dictionary.
    Supports both simple keys and nested tuple paths for deep extraction.

    Args:
        data: Nested dictionary to extract from.
        path: Tuple specification for extraction. Can contain:
            - Simple keys for top-level extraction
            - Tuples for nested extraction: (parent_key, child_key, ...)

    Returns:
        Dictionary with extracted values.

    Examples:
        >>> data = {
        ...     "a": 1,
        ...     "b": 2,
        ...     "c": {"c1": 31, "c2": 32},
        ...     "d": {},
        ...     "e": 5
        ... }
        >>> extract_from_nested_dict(data, ("a", "b"))
        {"a": 1, "b": 2}

        >>> extract_from_nested_dict(data, (("c", "c1"),))
        {"c": {"c1": 31}}

        >>> extract_from_nested_dict(data, ("a", ("c", "c2")))
        {"a": 1, "c": {"c2": 32}}
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


def resolve_nested_lengths(value: Any) -> tuple:
    """Get the nested structure lengths of a collection.

    Analyzes the nested structure of lists, tuples, or dictionaries to determine
    the lengths at each level of nesting.

    Args:
        value: Collection to analyze (list, tuple, dict, or other).

    Returns:
        Tuple of nested lengths. Empty tuple if not a collection or empty.

    Examples:
        >>> resolve_nested_lengths([1, 2, 3])
        (3,)

        >>> resolve_nested_lengths([[1, 1, 1], [2, 2, 2]])
        (2, 3)

        >>> resolve_nested_lengths([[[1], [2]], [[3], [4]]])
        (2, 2, 1)

        >>> resolve_nested_lengths("not a collection")
        ()

        >>> resolve_nested_lengths([])
        ()
    """
    result = []
    if isinstance(value, (list, tuple, dict)):
        n = len(value)
        if n > 0:
            result.append(n)
            if isinstance(value, dict):
                first_value = next(iter(value.values()))
            else:
                first_value = value[0]
            nested_lengths = resolve_nested_lengths(first_value)
            result.extend(nested_lengths)

    return tuple(result)


def expected_nested_lengths(value: Any, sizes: tuple) -> bool:
    """Check if value has expected nested structure lengths.

    Validates that a collection has the expected nested structure by comparing
    its actual nested lengths with the expected sizes.

    Args:
        value: Collection to check.
        sizes: Expected nested sizes as a tuple.

    Returns:
        True if structure matches expected sizes, False otherwise.

    Examples:
        >>> data = [[1, 2, 3], [4, 5, 6]]
        >>> expected_nested_lengths(data, (2, 3))
        True

        >>> expected_nested_lengths([1, 2, 3], (3,))
        True

        >>> expected_nested_lengths([[1, 2], [3, 4, 5]], (2, 2))
        False  # Second level has different lengths
    """
    return resolve_nested_lengths(value) == sizes


def chunk_list(items: list[Any], batch_size: int) -> Generator[list[Any], Any, Any]:
    """Split list into chunks of specified size.

    Divides a list into smaller sublists of the specified size.
    The last chunk may be smaller than batch_size if the list length
    is not evenly divisible by batch_size.

    Args:
        items: List to chunk.
        batch_size: Size of each chunk.

    Yields:
        List chunks of specified size.

    Examples:
        >>> list(chunk_list([1, 2, 3, 4, 5, 6, 7], 3))
        [[1, 2, 3], [4, 5, 6], [7]]

        >>> list(chunk_list(['a', 'b', 'c', 'd'], 2))
        [['a', 'b'], ['c', 'd']]

        >>> list(chunk_list([1, 2], 5))
        [[1, 2]]

        >>> list(chunk_list([], 3))
        []
    """
    assert isinstance(items, list)

    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def slice_model_properties(cls: type[BaseModel], properties: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split properties into model fields and extra properties.

    Separates a properties dictionary into two parts: properties that correspond
    to Pydantic model fields and properties that don't.

    Args:
        cls: Pydantic model class.
        properties: Dictionary of properties to split.

    Returns:
        Tuple of (model_properties, extra_properties).

    Examples:
        >>> from pydantic import BaseModel
        >>> class Person(BaseModel):
        ...     name: str
        ...     age: int
        >>>
        >>> props = {"name": "John", "age": 30, "city": "NYC", "country": "USA"}
        >>> model_props, extra_props = slice_model_properties(Person, props)
        >>> model_props
        {"name": "John", "age": 30}
        >>> extra_props
        {"city": "NYC", "country": "USA"}
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


def unnest_result(result: Any, unnest_mode: str | bool | None) -> Any:
    """Unnest result collections based on specified mode.

    Simplifies nested result structures based on the unnest mode.
    Useful for flattening query results or collection structures.

    Args:
        result: Result to unnest (list, tuple, dict, or scalar).
        unnest_mode: Mode for unnesting:
            - 'r' or 'rc' or 'cr': unnest rows (first level)
            - 'c' or 'rc' or 'cr': unnest columns (extract single values from tuples)
            - True: equivalent to 'rc'
            - False or None: no unnesting

    Returns:
        Unnested result based on the mode.

    Examples:
        >>> # Row unnesting - extract single item from list
        >>> unnest_result([{"name": "John"}], 'r')
        {"name": "John"}

        >>> # Column unnesting - extract values from tuples
        >>> unnest_result([("John",), ("Jane",)], 'c')
        ["John", "Jane"]

        >>> # Both row and column unnesting
        >>> unnest_result([("result",)], 'rc')
        "result"

        >>> # No unnesting
        >>> unnest_result([1, 2, 3], False)
        [1, 2, 3]

        >>> # Empty result handling
        >>> unnest_result([], 'r')
        None
    """
    if not unnest_mode or result is None:
        return result

    assert isinstance(unnest_mode, (str, bool)), "unnest_result must be of type str|bool|None!"

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
            # handle Tuple[Any, ...]
            result = result[0] if len(result) == 1 else None if len(result) == 0 else result

    return result
