"""Tests comparing JSON schema-based models with decorator-based models.

This test suite verifies that models loaded from JSON schemas have the same
internal structure and behavior as models defined using @node, @edge, and @relation decorators.
"""

import pytest

from cypher_graphdb.cardinality import Cardinality
from cypher_graphdb.decorators import edge, node, relation
from cypher_graphdb.display import DisplayConfig
from cypher_graphdb.modelprovider import ModelProvider
from cypher_graphdb.models import GraphEdge, GraphNode


@pytest.fixture
def provider():
    """Create a fresh ModelProvider for each test."""
    return ModelProvider()


class TestDecoratorVsSchemaEquivalence:
    """Test that decorator-based and schema-based models are equivalent."""

    def test_simple_node_equivalence(self, provider):
        """Test that simple node models are equivalent."""

        # Define using decorators
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str
            age: int

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name", "age"],
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema])

        # Compare model info
        decorator_info = provider.get("PersonDecorator")
        schema_info = provider.get("PersonSchema")

        # Both should be NODE type
        assert decorator_info.type_ == schema_info.type_

        # Both should have graph_model
        assert decorator_info.graph_model is not None
        assert schema_info.graph_model is not None

        # Both should be GraphNode subclasses
        assert issubclass(decorator_info.graph_model, GraphNode)
        assert issubclass(schema_info.graph_model, GraphNode)

        # Both should have the same fields
        decorator_fields = set(decorator_info.fields.keys())
        schema_fields = set(schema_info.fields.keys())
        assert decorator_fields == schema_fields

    def test_simple_edge_equivalence(self, provider):
        """Test that simple edge models are equivalent."""

        # Define using decorators
        @edge(label="KNOWS_DECORATOR", provider=provider)
        class KnowsDecorator(GraphEdge):
            since: int

        # Define using JSON schema
        schema = {
            "title": "KNOWS_SCHEMA",
            "type": "object",
            "properties": {"since": {"type": "integer"}},
            "required": ["since"],
            "x-graph": {"type": "EDGE", "label": "KNOWS_SCHEMA"},
        }
        provider.load_from_json_schemas([schema])

        # Compare model info
        decorator_info = provider.get("KNOWS_DECORATOR")
        schema_info = provider.get("KNOWS_SCHEMA")

        # Both should be EDGE type
        assert decorator_info.type_ == schema_info.type_

        # Both should have graph_model
        assert decorator_info.graph_model is not None
        assert schema_info.graph_model is not None

        # Both should be GraphEdge subclasses
        assert issubclass(decorator_info.graph_model, GraphEdge)
        assert issubclass(schema_info.graph_model, GraphEdge)

    def test_node_with_relations_equivalence(self, provider):
        """Test that nodes with relations are equivalent."""

        # Define using decorators (note: decorators are applied bottom-up, so order is reversed)
        @node(label="PersonDecorator", provider=provider)
        @relation(rel_type="WORKS_FOR", to_type="Company", cardinality=Cardinality.ONE_TO_ONE, form_field=True)
        @relation(rel_type="KNOWS", to_type="PersonDecorator", cardinality=Cardinality.ONE_TO_MANY, form_field=False)
        class PersonDecorator(GraphNode):
            name: str

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "x-graph": {
                "type": "NODE",
                "label": "PersonSchema",
                "relations": [
                    {"rel_type_name": "KNOWS", "to_type_name": "PersonSchema", "cardinality": "ONE_TO_MANY", "form_field": False},
                    {"rel_type_name": "WORKS_FOR", "to_type_name": "Company", "cardinality": "ONE_TO_ONE", "form_field": True},
                ],
            },
        }
        provider.load_from_json_schemas([schema])

        # Compare relations
        decorator_info = provider.get("PersonDecorator")
        schema_info = provider.get("PersonSchema")

        assert len(decorator_info.relations) == len(schema_info.relations)

        # Compare first relation
        dec_rel1 = decorator_info.relations[0]
        sch_rel1 = schema_info.relations[0]
        assert dec_rel1.rel_type_name == sch_rel1.rel_type_name
        assert dec_rel1.cardinality == sch_rel1.cardinality
        assert dec_rel1.form_field == sch_rel1.form_field

        # Compare second relation
        dec_rel2 = decorator_info.relations[1]
        sch_rel2 = schema_info.relations[1]
        assert dec_rel2.rel_type_name == sch_rel2.rel_type_name
        assert dec_rel2.cardinality == sch_rel2.cardinality
        assert dec_rel2.form_field == sch_rel2.form_field

    def test_node_creation_equivalence(self, provider):
        """Test that node creation works the same for both approaches."""

        # Define using decorators
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str
            age: int

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name", "age"],
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema])

        # Create nodes using provider
        node_decorator = provider.create_node("PersonDecorator", {"name": "Alice", "age": 30}, id=1)
        node_schema = provider.create_node("PersonSchema", {"name": "Bob", "age": 25}, id=2)

        # Both should have same structure
        assert node_decorator.label_ == "PersonDecorator"
        assert node_schema.label_ == "PersonSchema"
        assert node_decorator.name == "Alice"
        assert node_schema.name == "Bob"
        assert node_decorator.age == 30
        assert node_schema.age == 25

    def test_edge_creation_equivalence(self, provider):
        """Test that edge creation works the same for both approaches."""

        # Define using decorators
        @edge(label="KNOWS_DECORATOR", provider=provider)
        class KnowsDecorator(GraphEdge):
            since: int

        # Define using JSON schema
        schema = {
            "title": "KNOWS_SCHEMA",
            "type": "object",
            "properties": {"since": {"type": "integer"}},
            "required": ["since"],
            "x-graph": {"type": "EDGE", "label": "KNOWS_SCHEMA"},
        }
        provider.load_from_json_schemas([schema])

        # Create edges using provider
        edge_decorator = provider.create_edge("KNOWS_DECORATOR", start_id=1, end_id=2, props={"since": 2020}, id=10)
        edge_schema = provider.create_edge("KNOWS_SCHEMA", start_id=3, end_id=4, props={"since": 2021}, id=11)

        # Both should have same structure
        assert edge_decorator.label_ == "KNOWS_DECORATOR"
        assert edge_schema.label_ == "KNOWS_SCHEMA"
        assert edge_decorator.since == 2020
        assert edge_schema.since == 2021
        assert edge_decorator.start_id_ == 1
        assert edge_schema.start_id_ == 3

    def test_validation_equivalence(self, provider):
        """Test that validation works the same for both approaches."""

        # Define using decorators
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str
            age: int

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name", "age"],
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema])

        # Both should validate correctly
        PersonDecoratorModel = provider.get("PersonDecorator").graph_model
        PersonSchemaModel = provider.get("PersonSchema").graph_model

        # Valid instances
        person_dec = PersonDecoratorModel(name="Alice", age=30)
        person_sch = PersonSchemaModel(name="Bob", age=25)
        assert person_dec.name == "Alice"
        assert person_sch.name == "Bob"

        # Both should fail validation for missing required field
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PersonDecoratorModel(age=30)

        with pytest.raises(ValidationError):
            PersonSchemaModel(age=25)

    def test_schema_generation_equivalence(self, provider):
        """Test that both approaches generate similar schemas."""

        # Define using decorators
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str
            age: int

        # Define using JSON schema
        schema_input = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name", "age"],
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema_input])

        # Get generated schemas
        decorator_info = provider.get("PersonDecorator")
        schema_info = provider.get("PersonSchema")

        decorator_schema = decorator_info.graph_schema.json_schema
        schema_schema = schema_info.graph_schema.json_schema

        # Both should have x-graph extension
        assert "x-graph" in decorator_schema
        assert "x-graph" in schema_schema

        # Both should have same graph type
        assert decorator_schema["x-graph"]["type"] == "NODE"
        assert schema_schema["x-graph"]["type"] == "NODE"

        # Both should have properties
        assert "properties" in decorator_schema
        assert "properties" in schema_schema

    def test_optional_fields_equivalence(self, provider):
        """Test that optional fields work the same way."""

        # Define using decorators
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str
            email: str | None = None

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}, "email": {"type": "string"}},
            "required": ["name"],
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema])

        # Both should work with only required field
        PersonDecoratorModel = provider.get("PersonDecorator").graph_model
        PersonSchemaModel = provider.get("PersonSchema").graph_model

        person_dec = PersonDecoratorModel(name="Alice")
        person_sch = PersonSchemaModel(name="Bob")

        assert person_dec.name == "Alice"
        assert person_sch.name == "Bob"

        # Both should work with optional field
        person_dec2 = PersonDecoratorModel(name="Charlie", email="charlie@example.com")
        person_sch2 = PersonSchemaModel(name="Dave", email="dave@example.com")

        assert person_dec2.email == "charlie@example.com"
        assert person_sch2.email == "dave@example.com"

    def test_display_config_equivalence(self, provider):
        """Test that display configuration works for both approaches."""
        display_config = DisplayConfig(labelProperty="name", sortProperty="name", sortOrder="ASC")

        # Define using decorators
        @node(label="PersonDecorator", display=display_config, provider=provider)
        class PersonDecorator(GraphNode):
            name: str

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {
                "type": "NODE",
                "label": "PersonSchema",
                "display": {"labelProperty": "name", "sortProperty": "name", "sortOrder": "ASC"},
            },
        }
        provider.load_from_json_schemas([schema])

        # Compare display configs
        decorator_info = provider.get("PersonDecorator")
        schema_info = provider.get("PersonSchema")

        assert decorator_info.display is not None
        assert schema_info.display is not None
        assert decorator_info.display.labelProperty == schema_info.display.labelProperty
        assert decorator_info.display.sortProperty == schema_info.display.sortProperty
        assert decorator_info.display.sortOrder == schema_info.display.sortOrder

    def test_source_tracking_difference(self, provider):
        """Test that source tracking correctly distinguishes the two approaches."""

        # Define using decorators (source will be None initially)
        @node(label="PersonDecorator", provider=provider)
        class PersonDecorator(GraphNode):
            name: str

        # Define using JSON schema
        schema = {
            "title": "PersonSchema",
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "x-graph": {"type": "NODE", "label": "PersonSchema"},
        }
        provider.load_from_json_schemas([schema], source_uri="schema://test")

        # Check sources
        decorator_info = provider.get("PersonDecorator")
        schema_info = provider.get("PersonSchema")

        # Decorator-based has no source initially
        assert decorator_info.source is None

        # Schema-based has the provided source
        assert schema_info.source == "schema://test"
