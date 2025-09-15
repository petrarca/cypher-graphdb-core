"""schema module: JSON schema generation for graph model classes.

Provides GraphObjectSchema for generating and managing JSON schemas
from GraphNode and GraphEdge model classes.
"""

from typing import Any

from pydantic import BaseModel, model_serializer

from . import utils
from .models import GraphEdge, GraphNode


class GraphObjectSchema(BaseModel):
    """Manages JSON schema generation for graph model classes."""

    graph_model: type[GraphNode | GraphEdge]
    _json_schema: dict[str, Any] | None = None

    @property
    def json_schema(self) -> dict[str, Any] | None:
        """Get the JSON schema, generating it from the model if not cached."""
        if self._json_schema is not None:
            return self._json_schema
        else:
            return self.graph_model.model_json_schema() if self.graph_model else None

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
        """Serialize the schema with ordered keys (title, type first)."""
        return utils.order_dict(utils.to_collection(self.json_schema), ("title", "type"))
