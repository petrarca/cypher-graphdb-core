"""decorators module: Class decorators for registering typed graph model classes.

Provides @node, @edge, and @relation decorators to register GraphNode and GraphEdge
subclasses with the global model provider.
"""

from typing import Any

from .modelinfo import GraphEdgeInfo, GraphNodeInfo, GraphRelationInfo
from .modelprovider import ModelProvider, model_provider
from .models import GraphEdge, GraphNode


def node(label: str = None, provider: ModelProvider = None) -> Any:
    """Decorator to register a GraphNode subclass with optional label.

    Args:
        label: Graph label for this node type (defaults to class name).
        provider: Model provider to register with (defaults to global provider).

    Returns:
        Class decorator that registers the node class.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphNode):
            raise RuntimeError(f"@node decorator can only be applied to GraphNode models. {cls.__name__} is not.")

        nonlocal provider

        # might already by defined by @relation
        node_info = cls.graph_info_ if hasattr(cls, "graph_info_") else None

        # derive label from class name if not explicitly defined
        label_ = cls.__name__ if label is None else label

        if provider is None:
            provider = model_provider

        if node_info:
            # Occurs when @relation is used after @node, so relation decorator is called before (!) @node.
            # Also required because label might might override default and provider needs to be updated when already registered.
            model_provider.remove(node_info)

            # will be registered under this label in the provider
            node_info.label_ = label_
        else:
            node_info = GraphNodeInfo(label_=label_, graph_model=cls)
            cls.graph_info_ = node_info

        # register in the factory
        provider.register(node_info)

        return cls

    return decorator


def edge(label: str = None, provider: ModelProvider = None) -> Any:
    """Decorator to register a GraphEdge subclass with optional label.

    Args:
        label: Graph label for this edge type (defaults to class name).
        provider: Model provider to register with (defaults to global provider).

    Returns:
        Class decorator that registers the edge class.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphEdge):
            raise RuntimeError(f"@edge decorator can only be applied to GraphEdge models. {cls.__name__} is not.")

        nonlocal provider

        # derive label from class name if not explicitly defined
        label_ = cls.__name__ if label is None else label

        edge_info = GraphEdgeInfo(label_=label_, graph_model=cls)
        cls.graph_info_ = edge_info

        if provider is None:
            provider = model_provider

        # register in the factory
        provider.register(edge_info)

        return cls

    return decorator


def relation(rel_type: GraphEdge | str, to_type: Any = GraphNode | str) -> Any:
    """Decorator to define relationships from a node type to other node types.

    Args:
        rel_type: GraphEdge class or string label for the relationship type.
        to_type: GraphNode class or string label for the target node type.

    Returns:
        Class decorator that adds relationship info to the node's metadata.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphNode):
            raise RuntimeError(f"@node decorator can only be applied to GraphNode models. {cls.__name__} is not.")

        if not hasattr(cls, "graph_info_") or cls.graph_info_ is None:
            node()(cls)

        node_info = cls.graph_info_

        # resolve relationship type name (label for the edge)
        rel_type_name = rel_type.graph_info_.label_ if isinstance(rel_type, GraphEdge) else rel_type

        # resolve type name (label) of target node
        to_type_name = to_type.graph_info_.label_ if isinstance(to_type, GraphNode) else to_type

        rel_info = GraphRelationInfo(rel_type_name=rel_type_name, to_type_name=to_type_name)

        node_info.relations.append(rel_info)

        return cls

    return decorator
