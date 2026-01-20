"""Schema utilities for JSON schema manipulation.

Provides functions for combining and extracting JSON schemas from graph models.
"""

import json
from typing import Any

from .schema_merge import merge_schemas_by_title


def wrap_in_defs(
    schemas: dict[str, dict[str, Any]],
    schema_id: str = "https://cypher-graphdb.com/schemas/graph.schema.json",
    title: str = "Graph Data Model",
    description: str = "Combined graph schema with node and edge definitions",
) -> dict[str, Any]:
    """Wrap merged schemas in $defs structure.

    Takes a dictionary of merged schemas and wraps them in a JSON Schema
    with $defs structure.

    Args:
        schemas: Dictionary mapping title to merged schema
        schema_id: URI identifier for the combined schema
        title: Title for the combined schema
        description: Description for the combined schema

    Returns:
        Combined schema dictionary with $defs structure
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": schema_id,
        "title": title,
        "description": description,
        "$defs": schemas,
    }


def combine_schemas(
    schemas: list[dict[str, Any]],
    schema_id: str = "https://cypher-graphdb.com/schemas/graph.schema.json",
    title: str = "Graph Data Model",
    description: str = "Combined graph schema with node and edge definitions",
) -> dict[str, Any]:
    """Combine multiple JSON schemas into a single enriched schema.

    This function takes a list of JSON schemas and combines them into a single
    schema using the $defs format for better organization and validation.
    Schemas with the same title are properly merged (union of properties).

    Args:
        schemas: List of JSON schema dictionaries to combine
        schema_id: URI identifier for the combined schema
        title: Title for the combined schema
        description: Description for the combined schema

    Returns:
        Combined schema dictionary with $defs structure

    Example:
        >>> schemas = [{...}, {...}]  # List of schema dicts with title, type, etc.
        >>> combined = combine_schemas(schemas)
        >>> "$defs" in combined
        True
    """
    # Step 1: Merge schemas by title
    merged_schemas = merge_schemas_by_title(schemas)

    # Step 2: Wrap in $defs structure
    return wrap_in_defs(merged_schemas, schema_id, title, description)


def extract_schemas_from_model_infos(model_infos: list[Any]) -> list[dict[str, Any]]:
    """Extract JSON schemas from a list of GraphModelInfo objects.

    Args:
        model_infos: List of GraphModelInfo objects

    Returns:
        List of JSON schema dictionaries

    Raises:
        ValueError: If no JSON schema is available for a model
    """
    schemas = []
    for model_info in model_infos:
        json_schema = model_info.graph_schema.json_schema if model_info.graph_schema else None
        if not json_schema:
            raise ValueError(f"No JSON schema available for model: {model_info.label_}")

        # Handle case where json_schema might be a string (JSON serialized)
        if isinstance(json_schema, str):
            try:
                json_schema = json.loads(json_schema)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON schema string for model {model_info.label_}: {e}") from e

        schemas.append(json_schema)

    return schemas
