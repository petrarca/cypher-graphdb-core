"""decorators module: Class decorators for registering typed graph model classes.

Provides @node, @edge, and @relation decorators to register GraphNode and GraphEdge
subclasses with the global model provider.
"""

from typing import Any, TypedDict

from . import config
from .cardinality import Cardinality
from .display import DisplayConfig
from .modelinfo import GraphEdgeInfo, GraphNodeInfo, GraphRelationInfo
from .modelprovider import ModelProvider, model_provider
from .models import GraphEdge, GraphNode


def _collect_inherited_relations(cls: type[GraphNode]) -> list[GraphRelationInfo]:
    """Collect relations from all parent classes in inheritance hierarchy.

    Args:
        cls: The class to collect inherited relations for

    Returns:
        List of relation info objects from all parent classes
    """
    inherited_relations = []

    # Walk through inheritance hierarchy (skip self)
    for base in cls.__mro__[1:]:
        if hasattr(base, "graph_info_") and base.graph_info_:
            parent_relations = base.graph_info_.relations
            inherited_relations.extend(parent_relations)

    return inherited_relations


def _detect_relation_conflicts(
    inherited_relations: list[GraphRelationInfo], current_relations: list[GraphRelationInfo], class_name: str
) -> list[str]:
    """Detect conflicts between inherited and current relations.

    Args:
        inherited_relations: Relations inherited from parent classes
        current_relations: Relations defined on current class
        class_name: Name of the class for error reporting

    Returns:
        List of conflict description strings
    """
    conflicts = []

    # Create lookup for inherited relations
    inherited_lookup = {(rel.rel_type_name, rel.to_type_name): rel for rel in inherited_relations}

    # Check each current relation against inherited ones
    for current_rel in current_relations:
        key = (current_rel.rel_type_name, current_rel.to_type_name)
        if key in inherited_lookup:
            inherited_rel = inherited_lookup[key]
            conflict = _detect_relation_conflict_detail(current_rel, inherited_rel, class_name)
            if conflict:
                conflicts.append(conflict)

    return conflicts


def _detect_relation_conflict_detail(
    current_rel: GraphRelationInfo, inherited_rel: GraphRelationInfo, class_name: str
) -> str | None:
    """Detect specific conflicts between two relations."""
    conflicts = []

    # Cardinality conflict
    if current_rel.cardinality != inherited_rel.cardinality:
        conflicts.append(f"cardinality: {inherited_rel.cardinality} (inherited) vs {current_rel.cardinality} (current)")

    # Description conflict
    if current_rel.description and inherited_rel.description and current_rel.description != inherited_rel.description:
        conflicts.append(f"description: '{inherited_rel.description}' (inherited) vs '{current_rel.description}' (current)")

    # Form field conflict
    if current_rel.form_field != inherited_rel.form_field:
        conflicts.append(f"form_field: {inherited_rel.form_field} (inherited) vs {current_rel.form_field} (current)")

    if conflicts:
        return f"Relation '{current_rel.rel_type_name}' -> '{current_rel.to_type_name}' conflict in {class_name}: " + ", ".join(
            conflicts
        )

    return None


def _warn_relation_conflicts(conflicts: list[str], class_name: str, stacklevel: int = 3) -> None:
    """Emit warnings for relation conflicts."""
    if conflicts:
        import warnings

        conflict_msg = f"Relation inheritance conflicts detected in {class_name}:\n" + "\n".join(
            f"  - {conflict}" for conflict in conflicts
        )
        warnings.warn(conflict_msg, UserWarning, stacklevel=stacklevel)


def node(
    label: str = None,
    display: DisplayConfig = None,
    provider: ModelProvider = None,
    inherit_relations: bool = True,
) -> Any:
    """Decorator to register a GraphNode subclass with optional label.

    Args:
        label: Graph label for this node type (defaults to class name).
        display: Display configuration for UI rendering.
        provider: Model provider to register with (defaults to global).
        inherit_relations: Whether to inherit relations from parent classes (defaults to True).

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

        # Collect inherited relations from parent classes (if enabled)
        inherited_relations = _collect_inherited_relations(cls) if inherit_relations else []

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

            # Add inherited relations to existing relations
            node_info.relations = inherited_relations + node_info.relations
        else:
            node_info = GraphNodeInfo(
                label_=label_,
                graph_model=cls,
                display=display,
                source=config.MODEL_SOURCE_MODEL,
                relations=inherited_relations,  # Include inherited relations
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
    rel_type: type[GraphEdge] | str,
    to_type: type[GraphNode] | str,
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

        # Check for conflicts with inherited relations
        inherited_relations = _collect_inherited_relations(cls)
        if inherited_relations:
            conflicts = _detect_relation_conflicts(inherited_relations, [rel_info], cls.__name__)
            _warn_relation_conflicts(conflicts, cls.__name__, stacklevel=4)

        node_info.relations.append(rel_info)

        return cls

    return decorator


def _resolve_label(target: type[GraphNode] | type[GraphEdge] | str, kind: str) -> str:
    """Resolve a class or string to its label."""
    if isinstance(target, str):
        return target
    if hasattr(target, "graph_info_") and target.graph_info_ is not None:
        return target.graph_info_.label_
    raise ValueError(f"Cannot resolve {kind} label from {target}. Ensure it's decorated with @node or @edge.")


def _get_validated_node_info(target_label: str, provider: ModelProvider) -> GraphNodeInfo:
    """Get and validate that target is a registered node."""
    node_info = provider.get(target_label)

    if node_info is None:
        msg = f"'{target_label}' is not registered. Import the base model first."
        raise ValueError(msg)

    if not isinstance(node_info, GraphNodeInfo):
        msg = f"'{target_label}' is not a node type (found {type(node_info).__name__})."
        raise ValueError(msg)

    return node_info


def _add_relation_if_not_exists(
    node_info: GraphNodeInfo,
    rel_type_name: str,
    to_type_name: str,
    cardinality: Cardinality = Cardinality.ONE_TO_MANY,
    form_field: bool = False,
    description: str | None = None,
) -> bool:
    """Add relation to node info if it doesn't already exist. Returns True if added."""
    # Check for duplicate
    existing_keys = {(r.rel_type_name, r.to_type_name) for r in node_info.relations}
    key = (rel_type_name, to_type_name)

    if key not in existing_keys:
        rel_info = GraphRelationInfo(
            rel_type_name=rel_type_name,
            to_type_name=to_type_name,
            cardinality=cardinality,
            form_field=form_field,
            description=description,
        )
        node_info.relations.append(rel_info)
        return True

    return False


def extend_relation(
    target: type[GraphNode] | str,
    rel_type: type[GraphEdge] | str,
    to_type: type[GraphNode] | str,
    cardinality: Cardinality = Cardinality.ONE_TO_MANY,
    form_field: bool = False,
    description: str | None = None,
    provider: ModelProvider = None,
) -> None:
    """Add a single relation to an already-registered node type.

    This function allows extending a node's relations without redefining the class.
    Useful for modular schema composition where extensions add relations to base models.

    Args:
        target: GraphNode class or string label of the node type to extend.
        rel_type: GraphEdge class or string label for the relationship type.
        to_type: GraphNode class or string label for the target node type.
        cardinality: Relationship cardinality (ONE_TO_ONE or ONE_TO_MANY).
        form_field: Whether relation appears as form field (default: False).
        description: Optional description of the relationship.
        provider: ModelProvider instance (defaults to global model_provider).

    Raises:
        ValueError: If target is not a registered node.

    Example:
        >>> import shared_model.graph_model  # Triggers base registration
        >>> extend_relation(
        ...     Product,  # or "Product"
        ...     rel_type=HasTechStack,  # or "HAS_TECH_STACK"
        ...     to_type=TechnologyStack,  # or "TechnologyStack"
        ...     cardinality=Cardinality.ONE_TO_ONE,
        ...     description="Links product to its technology stack",
        ... )
    """
    if provider is None:
        provider = model_provider

    target_label = _resolve_label(target, "target")
    rel_type_name = _resolve_label(rel_type, "rel_type")
    to_type_name = _resolve_label(to_type, "to_type")

    node_info = _get_validated_node_info(target_label, provider)
    _add_relation_if_not_exists(node_info, rel_type_name, to_type_name, cardinality, form_field, description)


class RelationSpec(TypedDict, total=False):
    """Specification for a relation, supporting classes or strings."""

    rel_type: type[GraphEdge] | str
    to_type: type[GraphNode] | str
    cardinality: Cardinality
    form_field: bool
    description: str | None


def extend_relations(
    target: type[GraphNode] | str,
    relations: list[GraphRelationInfo | RelationSpec],
    provider: ModelProvider = None,
) -> None:
    """Add multiple relations to an already-registered node type.

    This function allows extending a node's relations without redefining the class.
    Useful for modular schema composition where extensions add relations to base models.

    Args:
        target: GraphNode class or string label of the node type to extend.
        relations: List of GraphRelationInfo objects or RelationSpec dicts.
            RelationSpec dicts can use classes for rel_type and to_type.
        provider: ModelProvider instance (defaults to global model_provider).

    Raises:
        ValueError: If target is not a registered node.

    Example:
        >>> extend_relations(Product, [
        ...     {"rel_type": HasTechStack, "to_type": TechnologyStack},
        ...     {"rel_type": "OWNED_BY", "to_type": Company, "cardinality": Cardinality.ONE_TO_ONE},
        ... ])
    """
    if provider is None:
        provider = model_provider

    target_label = _resolve_label(target, "target")
    node_info = _get_validated_node_info(target_label, provider)

    for rel in relations:
        if isinstance(rel, GraphRelationInfo):
            _add_relation_if_not_exists(
                node_info,
                rel.rel_type_name,
                rel.to_type_name,
                rel.cardinality,
                rel.form_field,
                rel.description,
            )
        else:
            # RelationSpec dict - resolve classes if needed
            rel_type_name = _resolve_label(rel["rel_type"], "rel_type")
            to_type_name = _resolve_label(rel["to_type"], "to_type")
            _add_relation_if_not_exists(
                node_info,
                rel_type_name,
                to_type_name,
                rel.get("cardinality", Cardinality.ONE_TO_MANY),
                rel.get("form_field", False),
                rel.get("description"),
            )
