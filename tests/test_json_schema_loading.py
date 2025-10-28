"""Tests for JSON schema-based model loading."""

import pytest

from cypher_graphdb.cardinality import Cardinality
from cypher_graphdb.modelinfo import GraphEdgeInfo, GraphNodeInfo
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge, GraphNode, GraphObjectType


@pytest.fixture
def provider():
    """Create a fresh ModelProvider for each test."""
    return ModelProvider()


class TestBasicSchemaLoading:
    """Test basic JSON schema loading functionality."""

    def test_load_simple_node_schema(self, provider):
        """Test loading a simple node model from JSON schema."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)

        assert len(loaded) == 1
        assert loaded[0].label_ == "Person"
        assert loaded[0].type_ == GraphObjectType.NODE
        assert loaded[0].graph_model is not None
        assert issubclass(loaded[0].graph_model, GraphNode)

    def test_load_simple_edge_schema(self, provider):
        """Test loading a simple edge model from JSON schema."""
        schemas = [
            {
                "title": "KNOWS",
                "type": "object",
                "properties": {"since": {"type": "integer"}},
                "x-graph": {"type": "EDGE", "label": "KNOWS"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)

        assert len(loaded) == 1
        assert loaded[0].label_ == "KNOWS"
        assert loaded[0].type_ == GraphObjectType.EDGE
        assert loaded[0].graph_model is not None
        assert issubclass(loaded[0].graph_model, GraphEdge)

    def test_load_multiple_schemas(self, provider):
        """Test loading multiple schemas at once."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            },
            {
                "title": "Company",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Company"},
            },
            {
                "title": "WORKS_FOR",
                "type": "object",
                "properties": {"since": {"type": "integer"}},
                "x-graph": {"type": "EDGE", "label": "WORKS_FOR"},
            },
        ]

        loaded = provider.load_from_json_schemas(schemas)

        assert len(loaded) == 3
        # Verify sorting: nodes first, then edges
        assert loaded[0].type_ == GraphObjectType.NODE
        assert loaded[1].type_ == GraphObjectType.NODE
        assert loaded[2].type_ == GraphObjectType.EDGE

    def test_empty_schema_list(self, provider):
        """Test loading empty schema list."""
        loaded = provider.load_from_json_schemas([])
        assert len(loaded) == 0


class TestSchemaValidation:
    """Test schema validation and error handling."""

    def test_missing_x_graph_extension(self, provider):
        """Test that schema without x-graph extension is rejected."""
        schemas = [{"title": "Person", "type": "object", "properties": {"name": {"type": "string"}}}]

        loaded = provider.load_from_json_schemas(schemas)
        # Should skip invalid schema and continue
        assert len(loaded) == 0

    def test_invalid_graph_type(self, provider):
        """Test that invalid graph type is rejected."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "INVALID", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 0

    def test_missing_label(self, provider):
        """Test that schema without label is rejected."""
        schemas = [{"type": "object", "properties": {"name": {"type": "string"}}, "x-graph": {"type": "NODE"}}]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 0

    def test_label_from_title_fallback(self, provider):
        """Test that label falls back to title if x-graph.label is missing."""
        schemas = [{"title": "Person", "type": "object", "properties": {"name": {"type": "string"}}, "x-graph": {"type": "NODE"}}]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 1
        assert loaded[0].label_ == "Person"


class TestModelGeneration:
    """Test that generated models work correctly."""

    def test_generated_model_has_fields(self, provider):
        """Test that generated model has correct fields."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}, "email": {"type": "string"}},
                "required": ["name", "age"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        model_class = loaded[0].graph_model

        # Check that fields exist
        assert "name" in model_class.model_fields
        assert "age" in model_class.model_fields
        assert "email" in model_class.model_fields

    def test_generated_model_validation(self, provider):
        """Test that generated model validates data correctly."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer", "minimum": 0}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        PersonModel = loaded[0].graph_model

        # Valid instance
        person = PersonModel(name="Alice", age=30)
        assert person.name == "Alice"
        assert person.age == 30

        # Missing required field should raise validation error
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PersonModel(age=30)

    def test_generated_model_inherits_from_graph_node(self, provider):
        """Test that generated node model inherits from GraphNode."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        PersonModel = loaded[0].graph_model

        # Should be a GraphNode subclass
        assert issubclass(PersonModel, GraphNode)

        # Should have GraphNode methods
        person = PersonModel(name="Alice")
        assert hasattr(person, "resolve")
        assert hasattr(person, "flatten_properties")

    def test_generated_model_inherits_from_graph_edge(self, provider):
        """Test that generated edge model inherits from GraphEdge."""
        schemas = [
            {
                "title": "KNOWS",
                "type": "object",
                "properties": {"since": {"type": "integer"}},
                "x-graph": {"type": "EDGE", "label": "KNOWS"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        KnowsModel = loaded[0].graph_model

        # Should be a GraphEdge subclass
        assert issubclass(KnowsModel, GraphEdge)

        # Should have GraphEdge methods
        knows = KnowsModel(start_id_=1, end_id_=2, since=2020)
        assert hasattr(knows, "resolve")
        assert knows.start_id_ == 1
        assert knows.end_id_ == 2


class TestRelationsParsing:
    """Test parsing of relations from x-graph extension."""

    def test_node_with_relations(self, provider):
        """Test loading node with relations."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {
                    "type": "NODE",
                    "label": "Person",
                    "relations": [
                        {"rel_type_name": "KNOWS", "to_type_name": "Person", "cardinality": "ONE_TO_MANY", "form_field": False},
                        {
                            "rel_type_name": "WORKS_FOR",
                            "to_type_name": "Company",
                            "cardinality": "ONE_TO_ONE",
                            "form_field": True,
                        },
                    ],
                },
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 1

        model_info = loaded[0]
        assert isinstance(model_info, GraphNodeInfo)
        assert len(model_info.relations) == 2

        # Check first relation
        rel1 = model_info.relations[0]
        assert rel1.rel_type_name == "KNOWS"
        assert rel1.to_type_name == "Person"
        assert rel1.cardinality == Cardinality.ONE_TO_MANY
        assert rel1.form_field is False

        # Check second relation
        rel2 = model_info.relations[1]
        assert rel2.rel_type_name == "WORKS_FOR"
        assert rel2.to_type_name == "Company"
        assert rel2.cardinality == Cardinality.ONE_TO_ONE
        assert rel2.form_field is True

    def test_edge_ignores_relations(self, provider):
        """Test that edge schemas ignore relations field."""
        schemas = [
            {
                "title": "KNOWS",
                "type": "object",
                "x-graph": {
                    "type": "EDGE",
                    "label": "KNOWS",
                    "relations": [{"rel_type_name": "INVALID", "to_type_name": "Invalid"}],  # Should be ignored
                },
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 1

        model_info = loaded[0]
        assert isinstance(model_info, GraphEdgeInfo)
        # Edges don't have relations attribute


class TestSourceTracking:
    """Test source URI tracking."""

    def test_default_source_uri(self, provider):
        """Test default source URI."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert loaded[0].source == "schema://dynamic"

    def test_custom_source_uri(self, provider):
        """Test custom source URI."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas, source_uri="database://server")
        assert loaded[0].source == "database://server"


class TestModelProviderIntegration:
    """Test integration with ModelProvider."""

    def test_loaded_models_are_registered(self, provider):
        """Test that loaded models are registered in provider."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        provider.load_from_json_schemas(schemas)

        # Should be able to retrieve model
        model_info = provider.get("Person")
        assert model_info is not None
        assert model_info.label_ == "Person"

    def test_duplicate_label_skipped(self, provider):
        """Test that duplicate labels are skipped."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        # Load first time
        loaded1 = provider.load_from_json_schemas(schemas)
        assert len(loaded1) == 1

        # Load again - should skip
        loaded2 = provider.load_from_json_schemas(schemas)
        assert len(loaded2) == 0

    def test_create_node_with_loaded_model(self, provider):
        """Test creating nodes using loaded models."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        provider.load_from_json_schemas(schemas)

        # Create node using provider
        node = provider.create_node("Person", {"name": "Alice", "age": 30}, id=1)

        assert node.label_ == "Person"
        assert node.id_ == 1
        assert node.name == "Alice"
        assert node.age == 30

    def test_create_edge_with_loaded_model(self, provider):
        """Test creating edges using loaded models."""
        schemas = [
            {
                "title": "KNOWS",
                "type": "object",
                "properties": {"since": {"type": "integer"}},
                "x-graph": {"type": "EDGE", "label": "KNOWS"},
            }
        ]

        provider.load_from_json_schemas(schemas)

        # Create edge using provider
        edge = provider.create_edge("KNOWS", start_id=1, end_id=2, props={"since": 2020}, id=10)

        assert edge.label_ == "KNOWS"
        assert edge.id_ == 10
        assert edge.start_id_ == 1
        assert edge.end_id_ == 2
        assert edge.since == 2020


class TestSchemaGeneration:
    """Test that schemas can be generated from loaded models."""

    def test_schema_generation_from_loaded_model(self, provider):
        """Test that JSON schema can be generated from a loaded model."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        provider.load_from_json_schemas(schemas)
        model_info = provider.get("Person")

        # Generate schema
        generated_schema = model_info.graph_schema.json_schema

        assert generated_schema is not None
        assert "x-graph" in generated_schema
        assert generated_schema["x-graph"]["type"] == "NODE"
        assert generated_schema["x-graph"]["label"] == "Person"
        assert "properties" in generated_schema

    def test_schema_roundtrip(self, provider):
        """Test that schema can be loaded, exported, and reloaded."""
        original_schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person", "relations": []},
            }
        ]

        # Load original schemas
        provider.load_from_json_schemas(original_schemas)

        # Export schemas
        exported = provider.model_dump()
        person_export = exported["Person"]

        # Verify exported structure
        assert person_export["label_"] == "Person"
        assert person_export["type_"] == "NODE"


class TestComplexSchemas:
    """Test loading complex schemas with various features."""

    def test_schema_with_nested_objects(self, provider):
        """Test loading schema with nested object properties."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "address": {"type": "object", "properties": {"street": {"type": "string"}, "city": {"type": "string"}}},
                },
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 1

        PersonModel = loaded[0].graph_model
        person = PersonModel(name="Alice", address={"street": "Main St", "city": "NYC"})
        assert person.name == "Alice"
        # Nested objects are Pydantic models, access via attribute
        assert person.address.city == "NYC"

    def test_schema_with_array_properties(self, provider):
        """Test loading schema with array properties."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "tags": {"type": "array", "items": {"type": "string"}}},
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        assert len(loaded) == 1

        PersonModel = loaded[0].graph_model
        person = PersonModel(name="Alice", tags=["developer", "python"])
        assert person.name == "Alice"
        assert len(person.tags) == 2

    def test_schema_with_optional_fields(self, provider):
        """Test loading schema with optional fields."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "email": {"type": "string"}, "phone": {"type": "string"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        loaded = provider.load_from_json_schemas(schemas)
        PersonModel = loaded[0].graph_model

        # Should work with only required field
        person = PersonModel(name="Alice")
        assert person.name == "Alice"

        # Should work with optional fields
        person2 = PersonModel(name="Bob", email="bob@example.com")
        assert person2.email == "bob@example.com"


class TestReplaceExisting:
    """Test replace_existing parameter functionality."""

    def test_replace_existing_false_skips_duplicates(self, provider):
        """Test that replace_existing=False skips duplicate models."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        # Load first time
        loaded1 = provider.load_from_json_schemas(schemas, source_uri="db://test")
        assert len(loaded1) == 1
        assert loaded1[0].graph_model.model_fields["age"].annotation is int

        # Modify schema (change age to string)
        schemas[0]["properties"]["age"] = {"type": "string"}

        # Load again without replace_existing - should skip
        loaded2 = provider.load_from_json_schemas(schemas, source_uri="db://test", replace_existing=False)
        assert len(loaded2) == 0  # Skipped because already exists

        # Model should still have integer age (not updated)
        person_info = provider.get("Person")
        assert person_info.graph_model.model_fields["age"].annotation is int

    def test_replace_existing_true_updates_models(self, provider):
        """Test that replace_existing=True updates existing models."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
                "x-graph": {"type": "NODE", "label": "Person"},
            }
        ]

        # Load first time
        loaded1 = provider.load_from_json_schemas(schemas, source_uri="db://test")
        assert len(loaded1) == 1
        assert loaded1[0].graph_model.model_fields["age"].annotation is int

        # Modify schema (change age to string)
        schemas[0]["properties"]["age"] = {"type": "string"}

        # Load again with replace_existing=True
        loaded2 = provider.load_from_json_schemas(schemas, source_uri="db://test", replace_existing=True)
        assert len(loaded2) == 1

        # Model should now have string age (updated)
        person_info = provider.get("Person")
        assert person_info.graph_model.model_fields["age"].annotation is str

    def test_replace_existing_removes_deleted_schemas(self, provider):
        """Test that replace_existing=True removes models not in new schema set."""
        schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            },
            {
                "title": "Company",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Company"},
            },
        ]

        # Load both models
        loaded1 = provider.load_from_json_schemas(schemas, source_uri="db://test")
        assert len(loaded1) == 2
        assert provider.get("Person") is not None
        assert provider.get("Company") is not None

        # Load again with only Person (Company removed from schemas)
        schemas_reduced = [schemas[0]]
        loaded2 = provider.load_from_json_schemas(schemas_reduced, source_uri="db://test", replace_existing=True)
        assert len(loaded2) == 1

        # Person should exist, Company should be removed
        assert provider.get("Person") is not None
        assert provider.get("Company") is None

    def test_replace_existing_only_affects_matching_source(self, provider):
        """Test that replace_existing only removes models with matching source prefix."""
        # Load from database source
        db_schemas = [
            {
                "title": "Person",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Person"},
            },
        ]
        provider.load_from_json_schemas(db_schemas, source_uri="db://metadata")

        # Load from file source
        file_schemas = [
            {
                "title": "Company",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Company"},
            },
        ]
        provider.load_from_json_schemas(file_schemas, source_uri="file:///models.py")

        # Both should exist
        assert provider.get("Person") is not None
        assert provider.get("Company") is not None

        # Reload database source with replace_existing=True
        new_db_schemas = [
            {
                "title": "Product",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "x-graph": {"type": "NODE", "label": "Product"},
            },
        ]
        provider.load_from_json_schemas(new_db_schemas, source_uri="db://metadata", replace_existing=True)

        # Person (db://) should be removed, Company (file://) should remain
        assert provider.get("Person") is None
        assert provider.get("Company") is not None
        assert provider.get("Product") is not None
