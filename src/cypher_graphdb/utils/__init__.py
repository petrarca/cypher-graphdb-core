"""Utilities package for cypher-graphdb.

This package provides various utility functions organized into functional modules.
All functions are re-exported from this __init__.py to maintain backward compatibility.
"""

# Import all functions from the modular utils files
from .collection_utils import (
    chunk_list,
    concat_dict,
    expected_nested_lengths,
    extract_from_nested_dict,
    order_dict,
    remove_default_from_dict,
    remove_from_dict,
    remove_values_from_dict,
    replace_empty_dicts,
    resolve_nested_lengths,
    slice_model_properties,
    sort_nested_dict,
    unnest_result,
)
from .column_utils import resolve_column_names
from .connection_utils import (
    log_env,
    parse_connection_uri,
    sanitize_connection_params_for_logging,
    sanitize_connection_string_for_logging,
    validate_protocol,
)
from .conversion_utils import isnan, resolve_properties, to_collection, type_to_default_value
from .core_utils import SCALAR_TYPES, generate_unique_string_id, is_scalar_type, resolve_fileformat, split_path
from .string_utils import (
    StringMatch,
    args_to_dict,
    convert_to_str,
    dict_from_value_pairs,
    dict_to_non_quoted_json,
    dict_to_value_pairs,
    partition_str,
    resolve_template,
    resolve_to_type,
    split_into_args_kwargs,
    split_str,
    starts_with,
    str_to_collection,
    try_literal_eval,
)

# Re-export all functions at package level for backward compatibility
__all__ = [
    # Core utilities
    "SCALAR_TYPES",
    "is_scalar_type",
    "generate_unique_string_id",
    "resolve_fileformat",
    "split_path",
    # Collection utilities
    "remove_from_dict",
    "remove_values_from_dict",
    "remove_default_from_dict",
    "concat_dict",
    "order_dict",
    "replace_empty_dicts",
    "sort_nested_dict",
    "extract_from_nested_dict",
    "resolve_nested_lengths",
    "expected_nested_lengths",
    "chunk_list",
    "slice_model_properties",
    "unnest_result",
    # Column utilities
    "resolve_column_names",
    # String utilities
    "convert_to_str",
    "dict_to_non_quoted_json",
    "dict_to_value_pairs",
    "dict_from_value_pairs",
    "resolve_template",
    "StringMatch",
    "resolve_to_type",
    "split_str",
    "partition_str",
    "split_into_args_kwargs",
    "args_to_dict",
    "try_literal_eval",
    "starts_with",
    "str_to_collection",
    # Conversion utilities
    "type_to_default_value",
    "resolve_properties",
    "isnan",
    "to_collection",
    # Connection utilities
    "parse_connection_uri",
    "validate_protocol",
    "sanitize_connection_params_for_logging",
    "sanitize_connection_string_for_logging",
    "log_env",
]
