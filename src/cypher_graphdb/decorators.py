"""decorators module: Class decorators for registering typed graph model classes.

Provides @node, @edge, and @relation decorators to register GraphNode and GraphEdge
subclasses with the global model provider.
"""

from typing import Any

from . import config
from .cardinality import Cardinality
from .display import DisplayConfig
from .modelinfo import GraphEdgeInfo, GraphNodeInfo, GraphRelationInfo
from .modelprovider import ModelProvider, model_provider
from .models import GraphEdge, GraphNode


def node(
    label: str = None,
    display: DisplayConfig = None,
    provider: ModelProvider = None,
) -> Any:
    """Decorator to register a GraphNode subclass with optional label.

    Args:
        label: Graph label for this node type (defaults to class name).
        display: Display configuration for UI rendering.
        provider: Model provider to register with (defaults to global).

    Returns:
        Class decorator that registers the node class.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphNode):
            msg = f"@node decorator can only be applied to GraphNode models. {cls.__name__} is not."
            raise RuntimeError(msg)

        nonlocal provider

        # might already by defined by @relation
        node_info = cls.graph_info_ if hasattr(cls, "graph_info_") else None

        # derive label from class name if not explicitly defined
        label_ = cls.__name__ if label is None else label

        if provider is None:
            provider = model_provider

        if node_info:
            # Occurs when @relation is used after @node, so relation
            # decorator is called before (!) @node.
            # Also required because label might override default and
            # provider needs to be updated when already registered.
            model_provider.remove(node_info)

            # will be registered under this label in the provider
            node_info.label_ = label_
            node_info.display = display
            node_info.source = config.MODEL_SOURCE_MODEL
        else:
            node_info = GraphNodeInfo(
                label_=label_,
                graph_model=cls,
                display=display,
                source=config.MODEL_SOURCE_MODEL,
            )
            cls.graph_info_ = node_info

        # register in the factory
        provider.register(node_info)

        return cls

    return decorator


def edge(
    label: str = None,
    display: DisplayConfig = None,
    provider: ModelProvider = None,
) -> Any:
    """Decorator to register a GraphEdge subclass with optional label.

    Args:
        label: Graph label for this edge type (defaults to class name).
        display: Display configuration for UI rendering.
        provider: Model provider to register with (defaults to global).

    Returns:
        Class decorator that registers the edge class.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphEdge):
            msg = f"@edge decorator can only be applied to GraphEdge models. {cls.__name__} is not."
            raise RuntimeError(msg)

        nonlocal provider

        # derive label from class name if not explicitly defined
        label_ = cls.__name__ if label is None else label

        edge_info = GraphEdgeInfo(
            label_=label_,
            graph_model=cls,
            display=display,
            source=config.MODEL_SOURCE_MODEL,
        )
        cls.graph_info_ = edge_info

        if provider is None:
            provider = model_provider

        # register in the factory
        provider.register(edge_info)

        return cls

    return decorator


def relation(
    rel_type: GraphEdge | str,
    to_type: Any = GraphNode | str,
    cardinality: Cardinality = Cardinality.ONE_TO_MANY,
    form_field: bool = False,
    description: str | None = None,
) -> Any:
    """Decorator to define relationships from a node type to other node types.

    Args:
        rel_type: GraphEdge class or string label for the relationship type.
        to_type: GraphNode class or string label for the target node type.
        cardinality: Relationship cardinality (ONE_TO_ONE or ONE_TO_MANY),
            defaults to ONE_TO_MANY.
        form_field: Whether relation appears as form field (default: False).
        description: Optional description of the relationship.

    Returns:
        Class decorator that adds relationship info to the node's metadata.

    """

    def decorator(cls: Any) -> Any:
        if not issubclass(cls, GraphNode):
            msg = f"@node decorator can only be applied to GraphNode models. {cls.__name__} is not."
            raise RuntimeError(msg)

        if not hasattr(cls, "graph_info_") or cls.graph_info_ is None:
            node()(cls)

        node_info = cls.graph_info_

        # resolve relationship type name (label for the edge)
        rel_type_name = rel_type.graph_info_.label_ if isinstance(rel_type, GraphEdge) else rel_type

        # resolve type name (label) of target node
        to_type_name = to_type.graph_info_.label_ if isinstance(to_type, GraphNode) else to_type

        rel_info = GraphRelationInfo(
            rel_type_name=rel_type_name,
            to_type_name=to_type_name,
            cardinality=cardinality,
            form_field=form_field,
            description=description,
        )

        node_info.relations.append(rel_info)

        return cls

    return decorator


def extend_relations(
    target_label: str,
    relations: list[GraphRelationInfo],
    provider: ModelProvider = None,
) -> None:
    """Add relations to an already-registered node type.

    This function allows extending a node's relations without redefining the class.
    Useful for modular schema composition where extensions add relations to base models.

    Args:
        target_label: Label of the node type to extend.
        relations: List of GraphRelationInfo objects to add.
        provider: ModelProvider instance (defaults to global model_provider).

    Raises:
        ValueError: If target_label is not a registered node.

    Example:
        >>> import shared_model.graph_model  # Triggers base registration
        >>> extend_relations("Product", [
        ...     GraphRelationInfo(
        ...         rel_type_name="HAS_TECH_STACK",
        ...         to_type_name="TechnologyStack",
        ...         cardinality=Cardinality.ONE_TO_ONE,
        ...         description="Links product to its technology stack",
        ...     ),
        ... ])
    """
    if provider is None:
        provider = model_provider

    node_info = provider.get(target_label)

    if node_info is None:
        msg = f"'{target_label}' is not registered. Import the base model first."
        raise ValueError(msg)

    if not isinstance(node_info, GraphNodeInfo):
        msg = f"'{target_label}' is not a node type (found {type(node_info).__name__})."
        raise ValueError(msg)

    # Add relations, deduplicating by (rel_type_name, to_type_name)
    existing_keys = {(r.rel_type_name, r.to_type_name) for r in node_info.relations}

    for rel in relations:
        key = (rel.rel_type_name, rel.to_type_name)
        if key not in existing_keys:
            node_info.relations.append(rel)
            existing_keys.add(key)
