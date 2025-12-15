"""Tests for schema merge utilities."""

from cypher_graphdb.utils import MergeAction, MergeConflict, SchemaMergeResult, merge_combined_schemas


class TestSchemaMergeResult:
    """Tests for SchemaMergeResult class."""

    def test_initial_state(self):
        """Test initial state of SchemaMergeResult."""
        result = SchemaMergeResult()
        assert result.success is True
        assert result.schema == {"$defs": {}}
        assert result.actions == []
        assert result.conflicts == []
        assert result.has_conflicts is False

    def test_add_action(self):
        """Test adding actions to result."""
        result = SchemaMergeResult()
        result.add_action("Product", "added", "from schema 0")

        assert len(result.actions) == 1
        assert result.actions[0].type_name == "Product"
        assert result.actions[0].action == "added"
        assert result.actions[0].details == "from schema 0"

    def test_add_conflict_sets_success_false(self):
        """Test that adding a conflict sets success to False."""
        result = SchemaMergeResult()
        assert result.success is True

        result.add_conflict("Product", "label_mismatch", "Different labels")

        assert result.success is False
        assert result.has_conflicts is True
        assert len(result.conflicts) == 1


class TestMergeAction:
    """Tests for MergeAction class."""

    def test_repr_with_details(self):
        """Test string representation with details."""
        action = MergeAction("Product", "merged", "properties: +['key']")
        assert repr(action) == "Product: merged (properties: +['key'])"

    def test_repr_without_details(self):
        """Test string representation without details."""
        action = MergeAction("Product", "added")
        assert repr(action) == "Product: added"


class TestMergeConflict:
    """Tests for MergeConflict class."""

    def test_repr(self):
        """Test string representation."""
        conflict = MergeConflict("Product", "label_mismatch", "Different labels: {'A', 'B'}")
        assert repr(conflict) == "Product: label_mismatch - Different labels: {'A', 'B'}"


class TestMergeCombinedSchemas:
    """Tests for merge_combined_schemas function."""

    def test_empty_input(self):
        """Test merging empty list of schemas."""
        result = merge_combined_schemas([])
        assert result.success is True
        assert result.schema == {"$defs": {}}

    def test_single_schema(self):
        """Test merging single schema returns it unchanged."""
        schema = {
            "$defs": {
                "Product": {
                    "title": "Product",
                    "properties": {"name": {"type": "string"}},
                    "x-graph": {"label": "Product", "type": "NODE", "relations": []},
                }
            }
        }

        result = merge_combined_schemas([schema])

        assert result.success is True
        assert "Product" in result.schema["$defs"]
        assert len(result.actions) == 1
        assert result.actions[0].action == "added"

    def test_merge_disjoint_schemas(self):
        """Test merging schemas with no overlapping types."""
        schema_a = {"$defs": {"Product": {"title": "Product", "x-graph": {"label": "Product", "type": "NODE"}}}}
        schema_b = {"$defs": {"Company": {"title": "Company", "x-graph": {"label": "Company", "type": "NODE"}}}}

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is True
        assert "Product" in result.schema["$defs"]
        assert "Company" in result.schema["$defs"]

    def test_merge_properties(self):
        """Test that properties are merged (union)."""
        schema_a = {
            "$defs": {
                "Product": {
                    "title": "Product",
                    "properties": {"name": {"type": "string"}},
                    "x-graph": {"label": "Product", "type": "NODE", "relations": []},
                }
            }
        }
        schema_b = {
            "$defs": {
                "Product": {
                    "title": "Product",
                    "properties": {"key": {"type": "string"}},
                    "x-graph": {"label": "Product", "type": "NODE", "relations": []},
                }
            }
        }

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is True
        props = result.schema["$defs"]["Product"]["properties"]
        assert "name" in props
        assert "key" in props

    def test_merge_relations(self):
        """Test that relations are merged (union)."""
        schema_a = {
            "$defs": {
                "Product": {
                    "title": "Product",
                    "x-graph": {
                        "label": "Product",
                        "type": "NODE",
                        "relations": [{"rel_type_name": "HAS_CATEGORY", "to_type_name": "Category"}],
                    },
                }
            }
        }
        schema_b = {
            "$defs": {
                "Product": {
                    "title": "Product",
                    "x-graph": {
                        "label": "Product",
                        "type": "NODE",
                        "relations": [{"rel_type_name": "OWNED_BY", "to_type_name": "Company"}],
                    },
                }
            }
        }

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is True
        relations = result.schema["$defs"]["Product"]["x-graph"]["relations"]
        assert len(relations) == 2
        rel_types = {r["rel_type_name"] for r in relations}
        assert rel_types == {"HAS_CATEGORY", "OWNED_BY"}

    def test_merge_relations_deduplicates(self):
        """Test that duplicate relations are not added."""
        rel = {"rel_type_name": "HAS_CATEGORY", "to_type_name": "Category"}
        schema_a = {"$defs": {"Product": {"x-graph": {"label": "Product", "type": "NODE", "relations": [rel]}}}}
        schema_b = {"$defs": {"Product": {"x-graph": {"label": "Product", "type": "NODE", "relations": [rel]}}}}

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is True
        relations = result.schema["$defs"]["Product"]["x-graph"]["relations"]
        assert len(relations) == 1

    def test_conflict_label_mismatch(self):
        """Test that label mismatch is detected as conflict."""
        schema_a = {"$defs": {"Product": {"x-graph": {"label": "Product", "type": "NODE"}}}}
        schema_b = {"$defs": {"Product": {"x-graph": {"label": "Item", "type": "NODE"}}}}

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is False
        assert result.has_conflicts is True
        assert len(result.conflicts) == 1
        assert result.conflicts[0].conflict_type == "label_mismatch"

    def test_conflict_type_mismatch(self):
        """Test that type mismatch (NODE vs EDGE) is detected as conflict."""
        schema_a = {"$defs": {"Product": {"x-graph": {"label": "Product", "type": "NODE"}}}}
        schema_b = {"$defs": {"Product": {"x-graph": {"label": "Product", "type": "EDGE"}}}}

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is False
        assert result.has_conflicts is True
        assert len(result.conflicts) == 1
        assert result.conflicts[0].conflict_type == "type_mismatch"

    def test_merge_required_fields(self):
        """Test that required fields are merged (union)."""
        schema_a = {"$defs": {"Product": {"required": ["name"], "x-graph": {"label": "Product", "type": "NODE"}}}}
        schema_b = {"$defs": {"Product": {"required": ["key"], "x-graph": {"label": "Product", "type": "NODE"}}}}

        result = merge_combined_schemas([schema_a, schema_b])

        assert result.success is True
        required = result.schema["$defs"]["Product"]["required"]
        assert set(required) == {"name", "key"}

    def test_actions_track_merges(self):
        """Test that merge actions are tracked correctly."""
        schema_a = {
            "$defs": {
                "Product": {
                    "properties": {"name": {"type": "string"}},
                    "x-graph": {"label": "Product", "type": "NODE", "relations": []},
                }
            }
        }
        schema_b = {
            "$defs": {
                "Product": {
                    "properties": {"key": {"type": "string"}},
                    "x-graph": {
                        "label": "Product",
                        "type": "NODE",
                        "relations": [{"rel_type_name": "HAS_CAT", "to_type_name": "Cat"}],
                    },
                }
            }
        }

        result = merge_combined_schemas([schema_a, schema_b])

        # Should have: added (from schema 0), merged (properties + relations)
        assert len(result.actions) == 2
        merge_action = [a for a in result.actions if a.action == "merged"][0]
        assert "properties" in merge_action.details
        assert "relations" in merge_action.details
