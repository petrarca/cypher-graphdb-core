"""Model information module: Metadata and schema information for graph models.

This module provides classes for storing metadata, schema information,
and relationship data for graph model classes (nodes and edges).
"""

from typing import Any

from pydantic import BaseModel, model_serializer
from pydantic.fields import FieldInfo, PydanticUndefined

from . import config, utils
from .cardinality import Cardinality
from .display import DisplayConfig
from .models import GraphEdge, GraphNode, GraphObjectType
from .schema import GraphObjectSchema, GraphSchemaContext


class GraphModelInfo(BaseModel):
    """Base class for graph model metadata and schema information.

    Attributes:
        label_: Graph label for the model.
        graph_model: The actual model class (GraphNode or GraphEdge).
        graph_schema: Generated schema information.
        source: Source location URI of the model file.
        display: Display configuration for UI rendering.
    """

    label_: str
    graph_model: type[GraphNode | GraphEdge]
    graph_schema: GraphObjectSchema | None = None
    source: str | None = None
    display: DisplayConfig | None = None

    def model_post_init(self, __context: Any) -> None:
        """Initialize schema after model creation."""
        super().model_post_init(__context)
        self._ensure_graph_schema()

    def _ensure_graph_schema(self) -> None:
        """Guarantee `graph_schema` exists and points at the current model."""
        if self.graph_schema is None:
            self.graph_schema = GraphObjectSchema(graph_model=self.graph_model, context_provider=self._build_schema_context)
            return

        self.graph_schema.graph_model = self.graph_model
        self.graph_schema.context_provider = self._build_schema_context

    def _build_schema_context(self) -> GraphSchemaContext:
        """Assemble schema context metadata used by `GraphObjectSchema`."""
        relations = getattr(self, "relations", None)
        graph_model_ref = None
        if self.graph_model is not None:
            graph_model_ref = f"{self.graph_model.__module__}.{self.graph_model.__name__}"

        return GraphSchemaContext(
            label=self.label_,
            metadata={},
            graph_type=self.type_,
            relations=list(relations or []),
            graph_model_ref=graph_model_ref,
            display=self.display,
        )

    @property
    def type_(self) -> str:
        """Get the graph object type.

        Returns:
            Type string (overridden in subclasses).
        """
        return GraphObjectType.UNDEFINED

    @property
    def fields(self) -> dict[str, dict[str, Any]]:
        """Expose model fields excluding reserved core GraphDB attributes."""
        if self.graph_model is None:
            return ()

        fields_to_remove: set[str] = config.EDGE_FIELDS if self.type_ == GraphObjectType.EDGE else config.NODE_FIELDS

        return utils.remove_from_dict(self.graph_model.model_fields, fields_to_remove)

    @model_serializer
    def serialize_model(self, info) -> dict[str, Any]:
        """Serialize schema and optional field details for the model."""
        graph_model = self.graph_model

        _fields = self._serialize_fields(info.context if info else None)

        return {
            "type_": self.type_,
            "label_": self.label_,
            "graph_model": f"{graph_model.__module__}.{graph_model.__name__}" if graph_model else None,
            "source": self.source,
            "fields": _fields,
        }

    def _serialize_fields(self, context: Any) -> list[str] | dict[str, Any]:
        fields = self.fields

        if not context or not context.get("with_detailed_fields", False):
            return list(fields.keys()) if isinstance(fields, dict) else list(fields)

        return {name: self._serialize_field_details(field) for name, field in fields.items()}

    @staticmethod
    def _serialize_field_details(field: FieldInfo) -> dict[str, Any]:
        required = GraphModelInfo._is_field_required(field)
        serialized: dict[str, Any] = {
            "annotation": GraphModelInfo._stringify_annotation(field.annotation),
            "required": required,
        }

        default = getattr(field, "default", PydanticUndefined)
        default_factory = getattr(field, "default_factory", PydanticUndefined)

        if default is not PydanticUndefined:
            serialized["default"] = utils.to_collection(default)
        elif default_factory is not PydanticUndefined:
            serialized["default_factory"] = GraphModelInfo._describe_callable(default_factory)

        json_schema_extra = getattr(field, "json_schema_extra", None)
        if json_schema_extra:
            serialized["json_schema_extra"] = utils.to_collection(json_schema_extra)

        return serialized

    @staticmethod
    def _describe_callable(factory: Any) -> str:
        qualname = getattr(factory, "__qualname__", None)
        module = getattr(factory, "__module__", None)
        if qualname:
            return f"{module}.{qualname}" if module else qualname
        return repr(factory)

    @staticmethod
    def _is_field_required(field: FieldInfo) -> bool:
        if hasattr(field, "is_required"):
            return field.is_required()  # type: ignore[call-arg]

        default = getattr(field, "default", PydanticUndefined)
        default_factory = getattr(field, "default_factory", PydanticUndefined)
        return default is PydanticUndefined and default_factory is PydanticUndefined

    @staticmethod
    def _stringify_annotation(annotation: Any) -> str | None:
        if annotation is None:
            return None

        if isinstance(annotation, type):
            return annotation.__name__

        return str(annotation)

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
        cardinality: Relationship cardinality (ONE or MANY), defaults to MANY.
        form_field: Whether this relation appears as a field in forms.
    """

    rel_type_name: str
    to_type_name: str
    cardinality: Cardinality = Cardinality.MANY
    form_field: bool = False


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
