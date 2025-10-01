"""models module: Core graph data structures and mixins.

Provides GraphNode, GraphEdge, GraphPath, and Graph classes with mixins for
identifiers, labels, and properties management.
"""

import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_serializer

from . import config, utils

# Type aliases for query results
TabularResult = list[tuple[Any, ...]]
"""Type alias for tabular query results.

Represents query results in tabular format where each row is a tuple that can contain:
- GraphObject instances (GraphNode, GraphEdge, GraphPath)
- Scalar values (int, str, float, bool, etc.)
- Collections (lists, dicts) with mixed content
- Any other values returned by Cypher queries

Examples:
    - Simple scalar query: [(42,), ("hello",), (3.14,)]
    - Mixed content: [(GraphNode(...), "name", 25), (GraphEdge(...), "type", True)]
    - Complex data: [({"key": "value"}, [1, 2, 3], None)]
"""


class DictAccessMixin:
    """Mixin to provide dictionary-like access to graph objects.

    Allows accessing properties and special fields using dict syntax:
    - node['property_name'] -> accesses properties_['property_name']
    - node['label_'] -> accesses the label_ field
    - node['id_'] -> accesses the id_ field
    - edge['start_id_'] -> accesses the start_id_ field
    - edge['end_id_'] -> accesses the end_id_ field

    Internal fields like 'properties_' are not accessible via dict syntax.
    Note: This does not interfere with Pydantic's built-in dict conversion.
    """

    def __getitem__(self, key: str) -> Any:
        """Get a property or special field value using dictionary syntax."""
        # Special fields that should be accessed directly
        if key in ("label_", "id_", "start_id_", "end_id_"):
            return getattr(self, key, None)

        # Prevent access to internal fields via our dict interface
        if key in ("properties_", "type_"):
            raise KeyError(f"'{key}' is not accessible via dictionary syntax")

        # Access properties from the properties_ dict
        if hasattr(self, "properties_") and key in self.properties_:
            return self.properties_[key]

        # If not found anywhere, raise KeyError
        raise KeyError(f"'{key}' not found in properties or special fields")

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a property or special field value using dictionary syntax."""
        # Special fields that should be set directly
        if key in ("label_", "id_", "start_id_", "end_id_"):
            setattr(self, key, value)
            return

        # Prevent setting internal fields via our dict interface
        if key in ("properties_", "type_"):
            raise KeyError(f"'{key}' cannot be set via dictionary syntax")

        # Set properties in the properties_ dict
        if hasattr(self, "properties_"):
            self.properties_[key] = value
            return

        # If properties_ doesn't exist, raise an error
        raise AttributeError("Object does not have a properties_ attribute")

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in properties or special fields."""
        # Check special fields
        if key in ("label_", "id_", "start_id_", "end_id_"):
            return hasattr(self, key) and getattr(self, key) is not None

        # Don't allow checking for internal fields via our dict interface
        if key in ("properties_", "type_"):
            return False

        # Check properties
        if hasattr(self, "properties_"):
            return key in self.properties_

        return False

    def get(self, key: str, default: Any = None) -> Any:
        """Get a property or special field value with a default."""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        """Return an iterator over all accessible keys."""
        keys = set()

        # Add special fields that exist and are not None
        for field in ("label_", "id_", "start_id_", "end_id_"):
            if hasattr(self, field) and getattr(self, field) is not None:
                keys.add(field)

        # Add properties
        if hasattr(self, "properties_"):
            keys.update(self.properties_.keys())

        return keys

    def items(self):
        """Return an iterator over (key, value) pairs."""
        for key in self.keys():
            yield key, self[key]

    def values(self):
        """Return an iterator over all accessible values."""
        for key in self.keys():
            yield self[key]


class GraphObjectType(Enum):
    """Enumeration of graph object types."""

    UNDEFINED = 0
    GRAPH = 1
    NODE = 2
    EDGE = 3
    PATH = 4


class GraphObject(BaseModel):
    """Base class for all graph objects."""

    pass


class GraphPropertiesMixin:
    """Mixin for managing dynamic properties and model field resolution."""

    properties_: dict[str, Any] = {}

    def flatten_properties(self):
        """Merge model fields with dynamic properties into a single dict."""
        # Manually extract model fields to avoid recursive model_dump() call
        result = {}

        # Get all model fields defined in the class (excluding internal fields)
        for field_name in self.__class__.model_fields:
            if not field_name.endswith("_") or field_name == config.PROP_GID:
                value = getattr(self, field_name, None)
                # Only include non-None values and simple types
                # to avoid recursion
                is_graph_type = isinstance(value, (GraphNode, GraphEdge, GraphPath, Graph))
                if value is not None and not is_graph_type:
                    result[field_name] = value

        # Merge dynamic properties from properties_ dict
        if hasattr(self, "properties_"):
            for key, value in self.properties_.items():
                if key not in result:
                    result[key] = value

        # Remove internal fields (ending with _ except those starting with _)
        for k in list(result.keys()):
            if k == config.PROP_GID:
                continue
            if k.endswith("_") and not k.startswith("_"):
                del result[k]

        return result

    def resolve_model_properties(self, default_from_values: bool = False) -> dict[str, Field]:
        """Extract model fields and dynamic properties as Pydantic Field definitions."""
        # Access model_fields from the class instead of the instance (Pydantic v2.11+ recommendation)
        result = {name: field for name, field in self.__class__.model_fields.items() if not name.startswith("_")}
        for v in ("id_", "label_", "properties_", "start_id_", "end_id_"):
            result.pop(v, None)

        for name, field in result.items():
            value = self.__dict__.get(name)
            field.default = value if default_from_values else utils.type_to_default_value(type(value))

        for name, value in self.properties_.items():
            if name in ("gid_",):
                continue

            # default= value => is_required == False
            default_value = value if default_from_values else utils.type_to_default_value(type(value))
            # Avoid deprecated Field(type=...) usage (Pydantic v2 deprecation).
            # Store original type name as metadata.
            result[name] = Field(
                default=default_value,
                json_schema_extra={"py_type": type(value).__name__},
            )

        return result

    @property
    def gid_(self):
        """Get the global identifier from properties, if present."""
        return self.properties_[config.PROP_GID] if self.has_gid else None

    @property
    def has_gid(self):
        """Return True if the object has a global identifier."""
        return config.PROP_GID in self.properties_

    def create_gid_if_missing(self):
        """Generate and assign a new GID if one doesn't exist."""
        if not self.has_gid:
            self.properties_[config.PROP_GID] = utils.generate_unique_string_id(config.GID_LENGTH)

    def _resolve_properties(self):
        """Hook for subclasses to resolve properties during object init."""
        # intentionally empty
        return None


class GraphIdentifierMixin:
    """Mixin for managing numeric graph object identifiers."""

    id_: int | None = None

    @property
    def has_id(self):
        """Return True if the object has a numeric ID."""
        return self.id_ is not None

    def bind_id(self, id_):
        """Assign a numeric ID to this object."""
        self.id_ = id_
        return self.id_

    def unbind_id(self):
        """Remove and return the current numeric ID."""
        result = self.id_
        self.id_ = None
        return result

    def _resolve_id(self):
        """Hook for subclasses to resolve ID during object init."""
        # intentionally empty
        return None


class GraphLabelMixin:
    """Mixin for managing graph object labels and type inference."""

    label_: str | None = None

    @classmethod
    def is_valid_label(cls, label: str) -> bool:
        """Check if a label string is valid for any graph object type."""
        return cls.label_to_obj_type(label) != GraphObjectType.UNDEFINED

    @classmethod
    def label_to_obj_type(cls, label) -> GraphObjectType:
        """Infer GraphObjectType from label naming conventions."""
        if isinstance(label, str):
            if re.match(r"^[A-Z_]*$", label):
                return GraphObjectType.EDGE

            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*", label):
                return GraphObjectType.NODE

        return GraphObjectType.UNDEFINED

    def _resolve_label(self):
        """Set label from graph_info_ if not already assigned."""
        if self.label_ is None:
            if hasattr(self, "graph_info_"):
                self.label_ = self.graph_info_.label_
            else:
                raise RuntimeError(f"Graph object {self} does not have a label!")


class GraphNode(
    GraphObject,
    GraphIdentifierMixin,
    GraphLabelMixin,
    GraphPropertiesMixin,
    DictAccessMixin,
):
    """Graph node with ID, label, and dynamic properties."""

    @property
    def type_(self):
        """Return GraphObjectType.NODE."""
        return GraphObjectType.NODE

    def __init_subclass__(cls, **kwargs):
        """Prevent inheriting graph_info_ from parent classes."""
        # prevent from inheriting this attribute
        if hasattr(cls, "graph_info_"):
            cls.graph_info_ = None

    def resolve(self) -> GraphObject:
        """Resolve ID, label, and properties for this node."""
        self._resolve_id()
        self._resolve_label()
        self._resolve_properties()

        return self

    @model_serializer
    def serialize_model(self, info) -> dict[str, Any]:
        """Serialize node; optionally wrap with type info."""
        with_type = info.context.get("with_type", True) if info.context else True

        model = {
            "id_": self.id_,
            "label_": self.label_,
            "properties_": (self.flatten_properties() if self.__class__ != GraphNode else self.properties_),
        }

        return {"node": model} if with_type else model

    def __hash__(self):
        """Hash based on node ID."""
        return hash(self.id_)


class GraphEdge(
    GraphObject,
    GraphIdentifierMixin,
    GraphLabelMixin,
    GraphPropertiesMixin,
    DictAccessMixin,
):
    """Directed graph edge connecting two nodes."""

    start_id_: int | None = None
    end_id_: int | None = None

    def __init_subclass__(cls, **kwargs):
        """Prevent inheriting graph_info_ from parent classes."""
        # prevent from inheriting this attribute
        if hasattr(cls, "graph_info_"):
            cls.graph_info_ = None

    @classmethod
    def build(cls, start_node: Any, end_node: Any, *args: Any, **kwargs: Any) -> "GraphEdge":
        """Create an edge between two nodes or IDs."""
        _start_id = start_node.id_ if isinstance(start_node, GraphNode) else start_node if isinstance(start_node, int) else None
        _end_id = end_node.id_ if isinstance(end_node, GraphNode) else end_node if isinstance(end_node, int) else None
        return cls(*args, **kwargs, start_id_=_start_id, end_id_=_end_id)

    def resolve(self) -> GraphObject:
        """Resolve ID, label, and properties for this edge."""
        self._resolve_id()
        self._resolve_label()
        self._resolve_properties()

        return self

    @model_serializer
    def serialize_model(self, info) -> dict[str, Any]:
        """Serialize edge; optionally wrap with type info."""
        with_type = info.context.get("with_type", True) if info.context else True

        model = {
            "id_": self.id_,
            "label_": self.label_,
            "start_id_": self.start_id_,
            "end_id_": self.end_id_,
            "properties_": (self.flatten_properties() if self.__class__ != GraphEdge else self.properties_),
        }

        return {"edge": model} if with_type else model

    @property
    def type_(self):
        """Return GraphObjectType.EDGE."""
        return GraphObjectType.EDGE

    def __hash__(self):
        """Hash based on edge ID."""
        return hash(self.id_)


class GraphPath(GraphObject):
    """Represents a sequence of connected graph entities (nodes and edges)."""

    entities: list[GraphObject] = []

    @property
    def type_(self):
        """Return GraphObjectType.PATH."""
        return GraphObjectType.PATH

    def __iter__(self):
        """Iterate over entities in the path."""
        return self.entities.__iter__()

    def __len__(self):
        """Return the number of entities in the path."""
        return self.entities.__len__()

    def __getitem__(self, index):
        """Get an entity by index."""
        return self.entities[index]

    def append(self, obj: GraphObject):
        """Add an entity to the end of the path."""
        self.entities.append(obj)

    @model_serializer
    def serialize_model(self, info) -> list | dict[str, Any]:
        """Serialize path; optionally wrap with type info."""
        with_type = info.context.get("with_type", True) if info.context else True

        model = [entity.model_dump(context=info.context) for entity in self.entities]

        return {"path": model} if with_type else model


class Graph(GraphObject):
    """Container for nodes and edges with indexing and merging capabilities."""

    nodes: dict[int, GraphNode] = {}
    edges: dict[int, GraphEdge] = {}

    @property
    def type_(self):
        """Return GraphObjectType.GRAPH."""
        return GraphObjectType.GRAPH

    @property
    def is_empty(self):
        """Return True if the graph contains no nodes or edges."""
        return not self.edges and not self.nodes

    def clear(self):
        """Remove all nodes and edges from the graph."""
        self.nodes.clear()
        self.edges.clear()

    def __getitem__(self, key: str) -> GraphNode | GraphEdge:
        """Get a node or edge by GID (Global ID) for subscriptable access.

        Only supports string GIDs, not numeric IDs.
        Use graph.nodes[id] or graph.edges[id] for numeric ID lookups.
        """
        if not isinstance(key, str):
            raise TypeError(
                f"Graph subscriptable access only supports string GIDs, "
                f"not {type(key).__name__}. "
                "Use graph.nodes[id] or graph.edges[id] for numeric access."
            )

        # Search nodes first
        for node in self.nodes.values():
            if node.gid_ == key:
                return node

        # Search edges
        for edge in self.edges.values():
            if edge.gid_ == key:
                return edge

        raise KeyError(f"No node or edge found with GID '{key}'")

    def merge(self, value, clear_before_merge: bool = False) -> None:
        """Merge another graph object or collection into this graph."""
        if value is not None:
            # ignore merging into itself
            if value is self:
                return

            if clear_before_merge:
                self.clear()

            self._merge_into_graph(value)

    @model_serializer
    def serialize_model(self, info) -> dict[str, Any] | list[dict[str, Any]]:
        with_type = info.context.get("with_type", False) if info.context else False

        # Convert dictionary values to lists for serialization
        _nodes = [n.model_dump(context={"with_type": not with_type}) for n in self.nodes.values()]
        _edges = [e.model_dump(context={"with_type": not with_type}) for e in self.edges.values()]

        if with_type:
            return {"nodes": _nodes, "edges": _edges}
        else:
            return _nodes + _edges

    def to_result(self) -> list[tuple[GraphObject | GraphEdge, ...]]:
        # Combine nodes and edges into a single list
        all_entities = list(self.nodes.values()) + list(self.edges.values())
        return [(v,) for v in all_entities]

    def grouped_nodes(self):
        return self._group_by_label(list(self.nodes.values()))

    def grouped_edges(self):
        return self._group_by_label(list(self.edges.values()))

    def grouped_entities(self):
        # combine the two dictionaries nodes and edge labels do not intersect
        result = {}
        result.update(self.grouped_nodes())
        result.update(self.grouped_edges())

        return result

    def _group_by_label(self, entities: list[GraphObject]) -> dict[str, list[GraphObject]]:
        result = {}

        for entity in entities.copy():
            result.setdefault(entity.label_, []).append(entity)

        return result

    def _merge_into_graph(self, value):
        if isinstance(value, GraphNode):
            self._append_node(value)
        elif isinstance(value, GraphEdge):
            self._append_edge(value)
        elif isinstance(value, GraphPath):
            self._append_path(value)
        elif isinstance(value, tuple | list):
            for val in value:
                self._merge_into_graph(val)
        else:
            # ignore scalar values
            pass

    def _append_node(self, value: GraphNode):
        if self._can_add_node(value):
            # Type checker: _can_add_node ensures value.id_ is not None
            assert value.id_ is not None
            self.nodes[value.id_] = value

    def _append_edge(self, value: GraphEdge):
        if self._can_add_edge(value):
            # Type checker: _can_add_edge ensures value.id_ is not None
            assert value.id_ is not None
            self.edges[value.id_] = value

    def _append_path(self, value: GraphPath):
        for entity in value.entities:
            self._merge_into_graph(entity)

    def _can_add_node(self, value: GraphNode) -> bool:
        """Check if node can be added (has ID and not duplicate)."""
        return value.id_ is not None and value.id_ not in self.nodes

    def _can_add_edge(self, value: GraphEdge) -> bool:
        """Check if edge can be added (has ID and not duplicate)."""
        return value.id_ is not None and value.id_ not in self.edges
