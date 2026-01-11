"""Schema merging utilities for combining multiple graph schemas.

Provides functions for merging multiple combined schemas (with $defs structure)
into a single unified schema, with conflict detection and detailed reporting.
"""

import json
import warnings
from typing import Any


def _deep_copy_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Create a deep copy of a schema to avoid mutating the original."""
    return json.loads(json.dumps(schema))


def merge_schemas_by_title(schemas: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Merge individual schemas by title.

    Takes a list of individual schemas and merges schemas with the same title.
    Returns a dictionary mapping title to merged schema.

    Args:
        schemas: List of individual JSON schema dictionaries

    Returns:
        Dictionary mapping title to merged schema

    Example:
        >>> schemas = [
        ...     {'title': 'Product', 'properties': {'name': {}}},
        ...     {'title': 'Product', 'properties': {'price': {}}}
        ... ]
        >>> merged = merge_schemas_by_title(schemas)
        >>> merged['Product']['properties']  # doctest: +SKIP
        {'name': {}, 'price': {}}
    """
    # Group schemas by title
    schemas_by_title: dict[str, list[dict[str, Any]]] = {}
    for schema in schemas:
        schema_title = schema.get("title")
        if schema_title:
            if schema_title not in schemas_by_title:
                schemas_by_title[schema_title] = []
            schemas_by_title[schema_title].append(schema)

    # Merge schemas with the same title
    merged_schemas: dict[str, dict[str, Any]] = {}
    for title, schema_list in schemas_by_title.items():
        if len(schema_list) == 1:
            # Single schema - use as-is
            merged_schemas[title] = schema_list[0]
        else:
            # Multiple schemas with same title - merge them
            merged_schemas[title] = _merge_individual_schemas(schema_list)

    return merged_schemas


def _merge_individual_schemas(schemas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple individual schemas with the same title.

    Takes the union of all properties, required fields, and other schema elements.
    The first schema provides the base structure (type, etc.).
    Schema-level descriptions are merged (concatenated) to preserve context from all sources.
    """
    if not schemas:
        raise ValueError("Cannot merge empty list of schemas")

    if len(schemas) == 1:
        return schemas[0]

    # Start with the first schema as base
    merged = schemas[0].copy()

    # Merge schema-level descriptions
    merged_description = _merge_descriptions(schemas)
    if merged_description:
        merged["description"] = merged_description

    # Merge properties
    merged["properties"] = _merge_properties(schemas)

    # Merge required fields
    merged["required"] = _merge_required_fields(schemas)

    # Merge x-graph extensions
    x_graph = _merge_x_graph_extensions(schemas)
    if x_graph:
        merged["x-graph"] = x_graph

    return merged


def _merge_descriptions(schemas: list[dict[str, Any]]) -> str | None:
    """Merge schema-level descriptions from multiple schemas.

    Collects unique descriptions and concatenates them with '. ' separator.
    This preserves context from derived classes that add domain-specific information.

    Args:
        schemas: List of schemas to merge descriptions from

    Returns:
        Merged description string or None if no descriptions found
    """
    descriptions = []
    seen = set()

    for schema in schemas:
        desc = schema.get("description")
        if desc and desc not in seen:
            descriptions.append(desc)
            seen.add(desc)

    if not descriptions:
        return None

    if len(descriptions) == 1:
        return descriptions[0]

    # Concatenate descriptions, ensuring proper sentence separation
    merged = []
    for desc in descriptions:
        desc = desc.strip()
        if desc:
            # Add period if not present
            if not desc.endswith((".", "!", "?")):
                desc += "."
            merged.append(desc)

    return " ".join(merged) if merged else None


def _merge_properties(schemas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge properties from multiple schemas (union) with conflict detection."""
    all_properties = {}
    conflicts = []

    for schema_idx, schema in enumerate(schemas):
        properties = schema.get("properties", {})
        schema_source = f"schema {schema_idx + 1}"

        for prop_name, prop_def in properties.items():
            if prop_name in all_properties:
                # Check for conflicts
                existing_def = all_properties[prop_name]
                conflict = _detect_property_conflict(prop_name, existing_def, prop_def, schema_source)
                if conflict:
                    conflicts.append(conflict)
                # Last wins behavior (keep existing for now, could be changed)
                continue

            all_properties[prop_name] = prop_def

    # Report conflicts
    if conflicts:
        conflict_msg = "Property conflicts detected during schema merging:\n" + "\n".join(f"  - {c}" for c in conflicts)
        warnings.warn(conflict_msg, UserWarning, stacklevel=3)

    return all_properties


def _detect_property_conflict(prop_name: str, existing_def: dict[str, Any], new_def: dict[str, Any], source: str) -> str | None:
    """Detect conflicts between property definitions."""
    conflicts = []

    # Type conflict
    existing_type = existing_def.get("type")
    new_type = new_def.get("type")
    if existing_type and new_type and existing_type != new_type:
        conflicts.append(f"type: {existing_type} vs {new_type}")

    # Format conflict
    existing_format = existing_def.get("format")
    new_format = new_def.get("format")
    if existing_format and new_format and existing_format != new_format:
        conflicts.append(f"format: {existing_format} vs {new_format}")

    # Description conflict
    existing_desc = existing_def.get("description")
    new_desc = new_def.get("description")
    if existing_desc and new_desc and existing_desc != new_desc:
        conflicts.append(f"description: '{existing_desc}' vs '{new_desc}'")

    # Constraint conflicts (maxLength, minLength, etc.)
    for constraint in ["maxLength", "minLength", "minimum", "maximum", "pattern"]:
        existing_val = existing_def.get(constraint)
        new_val = new_def.get(constraint)
        if existing_val is not None and new_val is not None and existing_val != new_val:
            conflicts.append(f"{constraint}: {existing_val} vs {new_val}")

    if conflicts:
        return f"Property '{prop_name}' conflict in {source}: " + ", ".join(conflicts)

    return None


def _merge_required_fields(schemas: list[dict[str, Any]]) -> list[str]:
    """Merge required fields from multiple schemas (union)."""
    all_required = set()
    for schema in schemas:
        required = schema.get("required", [])
        all_required.update(required)
    return sorted(all_required)


def _merge_x_graph_extensions(schemas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge x-graph extensions from multiple schemas."""
    x_graph_merged = {}
    all_relations = []

    for schema in schemas:
        x_graph = schema.get("x-graph", {})
        for key, value in x_graph.items():
            if key in ["label", "type"]:
                # These should be the same, use the first non-empty value
                if key not in x_graph_merged and value:
                    x_graph_merged[key] = value
            elif key == "relations":
                # Collect all relations for merging
                relations = value if isinstance(value, list) else []
                all_relations.extend(relations)
            else:
                # For other fields, take the last non-empty value
                if value:
                    x_graph_merged[key] = value

    # Merge relations (deduplicate by rel_type and to_type)
    if all_relations:
        x_graph_merged["relations"] = _merge_relations(all_relations)

    return x_graph_merged


def _merge_relations(relations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge relations from multiple schemas with conflict detection."""
    seen = {}
    merged_relations = []
    conflicts = []

    for rel in relations:
        # Create a unique key for deduplication
        key = (rel.get("rel_type_name"), rel.get("to_type_name"))

        if key in seen:
            # Check for conflicts
            existing_rel = seen[key]
            conflict = _detect_relation_conflict(key, existing_rel, rel)
            if conflict:
                conflicts.append(conflict)
            # Skip duplicate (keep first one)
            continue

        seen[key] = rel
        merged_relations.append(rel)

    # Report conflicts
    if conflicts:
        conflict_msg = "Relation conflicts detected during schema merging:\n" + "\n".join(f"  - {c}" for c in conflicts)
        warnings.warn(conflict_msg, UserWarning, stacklevel=3)

    return merged_relations


def _detect_relation_conflict(key: tuple[str, str], existing_rel: dict[str, Any], new_rel: dict[str, Any]) -> str | None:
    """Detect conflicts between relation definitions."""
    rel_type, to_type = key
    conflicts = []

    # Cardinality conflict
    existing_card = existing_rel.get("cardinality")
    new_card = new_rel.get("cardinality")
    if existing_card and new_card and existing_card != new_card:
        conflicts.append(f"cardinality: {existing_card} vs {new_card}")

    # Description conflict
    existing_desc = existing_rel.get("description")
    new_desc = new_rel.get("description")
    if existing_desc and new_desc and existing_desc != new_desc:
        conflicts.append(f"description: '{existing_desc}' vs '{new_desc}'")

    # Form field conflict
    existing_form = existing_rel.get("form_field")
    new_form = new_rel.get("form_field")
    if existing_form is not None and new_form is not None and existing_form != new_form:
        conflicts.append(f"form_field: {existing_form} vs {new_form}")

    if conflicts:
        return f"Relation '{rel_type}' -> '{to_type}' conflict: " + ", ".join(conflicts)

    return None


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
