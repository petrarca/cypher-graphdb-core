"""Schema merging utilities for combining multiple graph schemas.

Provides functions for merging multiple combined schemas (with $defs structure)
into a single unified schema, with conflict detection and detailed reporting.
"""

import json
from typing import Any


def _deep_copy_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Create a deep copy of a schema to avoid mutating the original."""
    return json.loads(json.dumps(schema))


class MergeAction:
    """Represents a single merge action performed during schema merging."""

    def __init__(self, type_name: str, action: str, details: str = ""):
        self.type_name = type_name
        self.action = action  # "added", "merged"
        self.details = details

    def __repr__(self) -> str:
        if self.details:
            return f"{self.type_name}: {self.action} ({self.details})"
        return f"{self.type_name}: {self.action}"


class MergeConflict:
    """Represents a merge conflict detected during schema merging."""

    def __init__(self, type_name: str, conflict_type: str, message: str):
        self.type_name = type_name
        self.conflict_type = conflict_type  # "label_mismatch", "type_mismatch"
        self.message = message

    def __repr__(self) -> str:
        return f"{self.type_name}: {self.conflict_type} - {self.message}"


class SchemaMergeResult:
    """Result of a schema merge operation."""

    def __init__(self):
        self.success: bool = True
        self.schema: dict[str, Any] = {"$defs": {}}
        self.actions: list[MergeAction] = []
        self.conflicts: list[MergeConflict] = []

    def add_action(self, type_name: str, action: str, details: str = "") -> None:
        self.actions.append(MergeAction(type_name, action, details))

    def add_conflict(self, type_name: str, conflict_type: str, message: str) -> None:
        self.conflicts.append(MergeConflict(type_name, conflict_type, message))
        self.success = False

    @property
    def has_conflicts(self) -> bool:
        return len(self.conflicts) > 0


def _collect_definitions(combined_schemas: list[dict[str, Any]]) -> dict[str, list[dict]]:
    """Collect all definitions by type name from multiple schemas."""
    all_defs: dict[str, list[dict]] = {}
    for schema in combined_schemas:
        for type_name, definition in schema.get("$defs", {}).items():
            all_defs.setdefault(type_name, []).append(definition)
    return all_defs


def _check_definition_conflicts(type_name: str, definitions: list[dict], result: SchemaMergeResult) -> None:
    """Check for label/type conflicts in definitions with same type_name."""
    labels = {d.get("x-graph", {}).get("label") for d in definitions if d.get("x-graph", {}).get("label")}
    types = {d.get("x-graph", {}).get("type") for d in definitions if d.get("x-graph", {}).get("type")}

    if len(labels) > 1:
        result.add_conflict(type_name, "label_mismatch", f"Different labels: {labels}")
    if len(types) > 1:
        result.add_conflict(type_name, "type_mismatch", f"Different types: {types}")


def _merge_definition(
    existing: dict[str, Any],
    definition: dict[str, Any],
) -> list[str]:
    """Merge a single definition into existing, return list of merge details."""
    merge_details = []

    # Merge properties
    new_props = definition.get("properties", {})
    added_props = [p for p in new_props if p not in existing.get("properties", {})]
    for prop_name, prop_def in new_props.items():
        existing.setdefault("properties", {})[prop_name] = prop_def
    if added_props:
        merge_details.append(f"properties: +{added_props}")

    # Merge required
    existing_required = set(existing.get("required", []))
    new_required = set(definition.get("required", []))
    added_required = new_required - existing_required
    if added_required:
        existing["required"] = sorted(existing_required | new_required)
        merge_details.append(f"required: +{list(added_required)}")

    # Merge relations
    existing_x_graph = existing.setdefault("x-graph", {})
    existing_rels = existing_x_graph.setdefault("relations", [])
    existing_rel_keys = {(r.get("rel_type_name"), r.get("to_type_name")) for r in existing_rels}

    added_rels = []
    for rel in definition.get("x-graph", {}).get("relations", []):
        key = (rel.get("rel_type_name"), rel.get("to_type_name"))
        if key not in existing_rel_keys:
            existing_rels.append(rel)
            existing_rel_keys.add(key)
            added_rels.append(f"{key[0]}->{key[1]}")
    if added_rels:
        merge_details.append(f"relations: +{added_rels}")

    return merge_details


def merge_combined_schemas(
    combined_schemas: list[dict[str, Any]],
) -> SchemaMergeResult:
    """Merge multiple combined schemas (with $defs structure) into one.

    Two-pass processing:
    1. Validation pass: Check for conflicts (label mismatches, type mismatches)
    2. Merge pass: Perform actual merging if no conflicts

    Args:
        combined_schemas: List of combined schema objects (each with $defs structure)

    Returns:
        SchemaMergeResult with success status, merged schema, actions, and conflicts
    """
    result = SchemaMergeResult()

    if not combined_schemas:
        return result

    # Pass 1: Validation
    all_defs = _collect_definitions(combined_schemas)
    for type_name, definitions in all_defs.items():
        if len(definitions) > 1:
            _check_definition_conflicts(type_name, definitions, result)
    if result.has_conflicts:
        return result

    # Pass 2: Merge
    merged = _deep_copy_schema(combined_schemas[0])
    merged_defs = merged.setdefault("$defs", {})

    for type_name in merged_defs:
        result.add_action(type_name, "added", "from schema 0")

    for schema_idx, schema in enumerate(combined_schemas[1:], start=1):
        for type_name, definition in schema.get("$defs", {}).items():
            if type_name not in merged_defs:
                merged_defs[type_name] = _deep_copy_schema(definition)
                result.add_action(type_name, "added", f"from schema {schema_idx}")
                continue

            merge_details = _merge_definition(merged_defs[type_name], definition)
            if merge_details:
                result.add_action(type_name, "merged", ", ".join(merge_details))

    result.schema = merged
    return result
