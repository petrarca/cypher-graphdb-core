"""Model information module: Metadata and schema information for graph models.

This module provides classes for storing metadata, schema information,
and relationship data for graph model classes (nodes and edges).
"""

from typing import Any

from pydantic import BaseModel, model_serializer

from . import config, utils
from .models import GraphEdge, GraphNode, GraphObjectType
from .schema import GraphObjectSchema


class GraphModelInfo(BaseModel):
    """Base class for graph model metadata and schema information.

    Stores metadata, schema, and type information for graph model classes.

    Attributes:
        label_: Graph label for the model.
        metadata: Additional metadata dictionary.
        graph_model: The actual model class (GraphNode or GraphEdge).
        graph_schema: Generated schema information.
    """

    label_: str
    metadata: dict[str, Any] = {}
    graph_model: type[GraphNode | GraphEdge]
    graph_schema: GraphObjectSchema = None

    def model_post_init(self, _):
        """Initialize schema after model creation."""
        self.graph_schema = GraphObjectSchema(graph_model=self.graph_model)

    @property
    def type_(self) -> str:
        """Get the graph object type.

        Returns:
            Type string (overridden in subclasses).
        """
        return GraphObjectType.UNDEFINED

    @property
    def fields(self) -> dict[str, dict[str, Any]]:
        if self.graph_model is None:
            return ()

        fields_to_remove: set[str] = config.EDGE_FIELDS if self.type_ == GraphObjectType.EDGE else config.NODE_FIELDS

        return utils.remove_from_dict(self.graph_model.model_fields, fields_to_remove)

    @model_serializer
    def serialize_model(self, info) -> dict[str, Any]:
        graph_model = self.graph_model

        _fields = self.fields if info.context and info.context.get("with_detailed_fields", False) else list(self.fields.keys())

        return {
            "type_": self.type_,
            "label_": self.label_,
            "metadata": utils.to_collection(self.metadata),
            "graph_model": f"{graph_model.__module__}.{graph_model.__name__}" if graph_model else None,
            "has_schema": self.graph_schema.has_schema,
            "fields": _fields,
        }

    def __hash__(self):
        """Get hash value based on label."""
        return hash(self.label_)

    def __eq__(self, other):
        """Check equality based on label."""
        return self.label_ == other


class GraphRelationInfo(BaseModel):
    """Information about relationships between graph node types.

    Attributes:
        rel_type_name: Name of the relationship type.
        to_type_name: Name of the target node type.
    """

    rel_type_name: str
    to_type_name: str


class GraphNodeInfo(GraphModelInfo):
    """Metadata and schema information for graph node models.

    Extends GraphModelInfo with relationship information specific to nodes.

    Attributes:
        relations: List of relationships this node type can have.
    """

    relations: list[GraphRelationInfo] = []

    @property
    def type_(self) -> str:
        """Get the graph object type for nodes.

        Returns:
            NODE type constant.
        """
        return GraphObjectType.NODE

    @model_serializer
    def serialize_model(self, info: Any) -> dict[str, Any]:
        """Serialize node model information including relationships.

        Args:
            info: Serialization context information.

        Returns:
            Dictionary representation including relations.
        """
        result = super().serialize_model(info)
        result.update({"relations": utils.to_collection(self.relations)})

        return result


class GraphEdgeInfo(GraphModelInfo):
    """Metadata and schema information for graph edge models.

    Specialized version of GraphModelInfo for edge types.
    """

    @property
    def type_(self) -> str:
        """Get the graph object type for edges.

        Returns:
            EDGE type constant.
        """
        return GraphObjectType.EDGE
