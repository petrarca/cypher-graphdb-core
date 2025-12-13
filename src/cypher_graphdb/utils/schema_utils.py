"""Schema utilities for JSON schema manipulation.

Provides functions for combining, extracting, and manipulating
JSON schemas from graph models.
"""

import json
from typing import Any


def combine_schemas(
    schemas: list[dict[str, Any]],
    title: str = "Graph Data Model",
    description: str = "Complete data model for graph management including nodes, edges, and their relationships.",
    schema_id: str = "https://cypher-graphdb.com/schemas/graph.schema.json",
) -> dict[str, Any]:
    """Combine multiple JSON schemas into a single enriched schema using $defs format.

    This function creates an enriched JSON Schema Draft 2020-12 compliant schema
    that nests all individual schemas under the $defs property.

    Args:
        schemas: List of individual JSON schema objects to combine
        title: Title for the combined schema (default: "Graph Data Model")
        description: Description for the combined schema
        schema_id: $id value for the combined schema

    Returns:
        Combined schema in enriched $defs format

    Example:
        >>> schemas = [{"title": "Product", "type": "object", ...}, {"title": "Company", "type": "object", ...}]
        >>> combined = combine_schemas(schemas)
        >>> combined["title"]
        'Graph Data Model'
        >>> combined["$defs"]["Product"]["title"]
        'Product'
    """
    enriched_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": schema_id,
        "title": title,
        "description": description,
        "$defs": {},
    }

    for schema in schemas:
        schema_title = schema.get("title")
        if schema_title:
            enriched_schema["$defs"][schema_title] = schema

    return enriched_schema


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
