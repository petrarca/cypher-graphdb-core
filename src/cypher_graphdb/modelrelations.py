"""modelrelations module: pure helpers for graph model relation handling.

Provider-agnostic building blocks used by ``ModelProvider``'s registration
methods (``node`` / ``edge`` / ``relation`` / ``extend_relation``). Deliberately
free of any ``ModelProvider`` import so the dependency runs one way
(``modelprovider`` -> ``modelrelations``) and no import cycle can form.
"""

from __future__ import annotations

import warnings
from typing import TypedDict

from .cardinality import Cardinality
from .modelinfo import GraphNodeInfo, GraphRelationInfo
from .models import GraphEdge, GraphNode


class RelationSpec(TypedDict, total=False):
    """Specification for a relation, supporting classes or strings."""

    rel_type: type[GraphEdge] | str
    to_type: type[GraphNode] | str
    cardinality: Cardinality
    form_field: bool
    description: str | None


def collect_inherited_relations(cls: type[GraphNode]) -> list[GraphRelationInfo]:
    """Collect relations from all parent classes in the inheritance hierarchy.

    Args:
        cls: The class to collect inherited relations for.

    Returns:
        List of relation info objects from all parent classes.
    """
    inherited: list[GraphRelationInfo] = []

    # Walk the inheritance hierarchy (skip self).
    for base in cls.__mro__[1:]:
        if hasattr(base, "graph_info_") and base.graph_info_:
            inherited.extend(base.graph_info_.relations)

    return inherited


def _conflict_detail(current: GraphRelationInfo, inherited: GraphRelationInfo, class_name: str) -> str | None:
    """Describe the conflicts between two same-key relations, or None if compatible."""
    conflicts: list[str] = []

    if current.cardinality != inherited.cardinality:
        conflicts.append(f"cardinality: {inherited.cardinality} (inherited) vs {current.cardinality} (current)")

    if current.description and inherited.description and current.description != inherited.description:
        conflicts.append(f"description: '{inherited.description}' (inherited) vs '{current.description}' (current)")

    if current.form_field != inherited.form_field:
        conflicts.append(f"form_field: {inherited.form_field} (inherited) vs {current.form_field} (current)")

    if not conflicts:
        return None

    return f"Relation '{current.rel_type_name}' -> '{current.to_type_name}' conflict in {class_name}: " + ", ".join(conflicts)


def detect_relation_conflicts(
    inherited_relations: list[GraphRelationInfo],
    current_relations: list[GraphRelationInfo],
    class_name: str,
) -> list[str]:
    """Detect conflicts between inherited and current relations.

    Args:
        inherited_relations: Relations inherited from parent classes.
        current_relations: Relations defined on the current class.
        class_name: Name of the class, for error reporting.

    Returns:
        List of conflict description strings.
    """
    lookup = {(rel.rel_type_name, rel.to_type_name): rel for rel in inherited_relations}

    conflicts: list[str] = []
    for current in current_relations:
        inherited = lookup.get((current.rel_type_name, current.to_type_name))
        if inherited is None:
            continue
        if detail := _conflict_detail(current, inherited, class_name):
            conflicts.append(detail)

    return conflicts


def warn_relation_conflicts(conflicts: list[str], class_name: str, stacklevel: int = 3) -> None:
    """Emit a single ``UserWarning`` summarising relation inheritance conflicts."""
    if not conflicts:
        return

    message = f"Relation inheritance conflicts detected in {class_name}:\n" + "\n".join(f"  - {c}" for c in conflicts)
    warnings.warn(message, UserWarning, stacklevel=stacklevel)


def resolve_label(target: type[GraphNode] | type[GraphEdge] | str, kind: str) -> str:
    """Resolve a model class (or plain string) to its graph label.

    Args:
        target: A ``@node``/``@edge`` registered class, or a label string.
        kind: Role of the target ('target', 'rel_type', 'to_type'), for errors.

    Returns:
        The resolved label.

    Raises:
        ValueError: If the class carries no ``graph_info_`` (not registered).
    """
    if isinstance(target, str):
        return target
    if getattr(target, "graph_info_", None) is not None:
        return target.graph_info_.label_
    raise ValueError(f"Cannot resolve {kind} label from {target}. Ensure it is registered via @<provider>.node/.edge.")


def add_relation_if_not_exists(
    node_info: GraphNodeInfo,
    rel_type_name: str,
    to_type_name: str,
    cardinality: Cardinality = Cardinality.ONE_TO_MANY,
    form_field: bool = False,
    description: str | None = None,
) -> bool:
    """Append a relation to ``node_info`` unless an identical key already exists.

    Returns:
        True if the relation was added, False if it was already present.
    """
    existing = {(r.rel_type_name, r.to_type_name) for r in node_info.relations}
    if (rel_type_name, to_type_name) in existing:
        return False

    node_info.relations.append(
        GraphRelationInfo(
            rel_type_name=rel_type_name,
            to_type_name=to_type_name,
            cardinality=cardinality,
            form_field=form_field,
            description=description,
        )
    )
    return True
