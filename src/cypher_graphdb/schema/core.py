"""schema module: JSON schema generation for graph model classes.

Provides GraphObjectSchema for generating and managing JSON schemas
from GraphNode and GraphEdge model classes.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, model_serializer

from .. import utils
from ..models import GraphEdge, GraphNode, GraphObjectType


@dataclass
class GraphSchemaContext:
    """Context metadata used when enriching graph model schemas."""

    label: str
    metadata: dict[str, Any]
    graph_type: GraphObjectType
    relations: list[Any]
    display: Any | None = None  # DisplayConfig to avoid circular import
    source: str | None = None  # Source type: "model" (Python) or "schema" (JSON)


def _filter_internal_fields(schema: dict[str, Any], context: GraphSchemaContext | None) -> dict[str, Any]:
    """Remove internal graph database fields from the schema properties.

    Args:
        schema: The JSON schema dictionary to filter.
        context: Graph schema context containing type information.

    Returns:
        Filtered schema with internal fields removed.
    """
    from .. import config

    if not isinstance(schema, dict) or "properties" not in schema:
        return schema

    # Determine which fields to exclude based on type
    if context and context.graph_type == GraphObjectType.EDGE:
        fields_to_remove = set(config.EDGE_FIELDS)
    else:
        fields_to_remove = set(config.NODE_FIELDS)

    # Filter properties
    filtered_schema = dict(schema)
    filtered_schema["properties"] = {k: v for k, v in schema["properties"].items() if k not in fields_to_remove}

    # Filter required array if present
    if "required" in filtered_schema:
        filtered_schema["required"] = [field for field in filtered_schema["required"] if field not in fields_to_remove]

    return filtered_schema


def _build_graph_extension(context: GraphSchemaContext) -> dict[str, Any]:
    """Build the x-graph extension dictionary from context.

    Args:
        context: Graph schema context with metadata.

    Returns:
        Dictionary for x-graph extension.
    """
    graph_type = context.graph_type.name if hasattr(context.graph_type, "name") else context.graph_type

    extension: dict[str, Any] = {
        "type": graph_type,
        "label": context.label,
        "metadata": utils.to_collection(context.metadata or {}),
    }

    if hasattr(context, "source") and context.source is not None:
        extension["source"] = context.source

    if context.display is not None:
        if hasattr(context.display, "model_dump"):
            extension["display"] = context.display.model_dump(exclude_none=True)
        else:
            extension["display"] = utils.to_collection(context.display)

    if context.relations:
        normalized_relations: list[Any] = []
        for relation in context.relations:
            if hasattr(relation, "model_dump"):
                normalized_relations.append(relation.model_dump())
            else:
                normalized_relations.append(utils.to_collection(relation))
        extension["relations"] = normalized_relations

    return extension


def build_json_schema(
    graph_model: type[GraphNode | GraphEdge] | None,
    *,
    base_schema: dict[str, Any] | None = None,
    context: GraphSchemaContext | None = None,
) -> dict[str, Any]:
    """Generate a JSON schema and enrich with graph metadata.

    Args:
        graph_model: Model class to derive base schema from.
        base_schema: Precomputed schema to reuse.
        context: Optional metadata for x-graph extension.

    Returns:
        JSON schema dictionary enriched with graph metadata.
    """

    if base_schema is not None:
        schema_source = base_schema
    elif graph_model is not None:
        schema_source = graph_model.model_json_schema()
        schema_source = _filter_internal_fields(schema_source, context)
    else:
        schema_source = {}

    schema = utils.to_collection(schema_source) or {}

    if not isinstance(schema, dict):
        schema = {"schema": schema}

    if context is None:
        return schema

    extension = _build_graph_extension(context)

    enriched_schema = dict(schema)
    enriched_schema["x-graph"] = extension

    ordered_schema = utils.order_dict(enriched_schema, ("title", "type", "properties", "required"))

    return ordered_schema


class GraphObjectSchema(BaseModel):
    """Manage JSON schema generation for graph model classes with optional enrichment."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    graph_model: type[GraphNode | GraphEdge]
    _json_schema: dict[str, Any] | None = None
    context_provider: Callable[[], GraphSchemaContext | None] | None = None

    def _resolve_context(self) -> GraphSchemaContext | None:
        """Fetch context on demand using the configured provider, if any."""
        if self.context_provider is not None:
            return self.context_provider()

        return None

    @property
    def json_schema(self) -> dict[str, Any] | None:
        """Get the JSON schema, generating it from the model if not cached."""
        if self.graph_model is None and self._json_schema is None:
            return None

        context = self._resolve_context()

        return build_json_schema(self.graph_model, base_schema=self._json_schema, context=context)

    @json_schema.setter
    def json_schema(self, value: dict[str, Any] | None):
        """Set a custom JSON schema, overriding auto-generation."""
        assert value is None or isinstance(value, dict)

        self._json_schema = value

    @property
    def has_schema(self) -> bool:
        """Return True if a custom schema has been set."""
        return self._json_schema is not None

    @model_serializer
    def serialize_model(self, _) -> dict[str, Any]:
        """Serialize the schema with ordered keys (title, type, properties, required first)."""
        return utils.order_dict(
            utils.to_collection(self.json_schema),
            ("title", "type", "properties", "required"),
        )
