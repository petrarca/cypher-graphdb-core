"""Tests for schema_utils module."""

import json

import pytest

from cypher_graphdb.utils.schema_utils import combine_schemas, extract_schemas_from_model_infos


class TestCombineSchemas:
    """Tests for combine_schemas function."""

    def test_combine_empty_schemas(self):
        """Test combining empty list of schemas."""
        result = combine_schemas([])

        assert result["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert result["$id"] == "https://cypher-graphdb.com/schemas/graph.schema.json"
        assert result["title"] == "Graph Data Model"
        assert result["$defs"] == {}

    def test_combine_single_schema(self):
        """Test combining a single schema."""
        schema = {
            "title": "Product",
            "type": "object",
            "properties": {"name": {"type": "string"}},
        }

        result = combine_schemas([schema])

        assert "Product" in result["$defs"]
        assert result["$defs"]["Product"]["title"] == "Product"
        assert result["$defs"]["Product"]["type"] == "object"

    def test_combine_multiple_schemas(self):
        """Test combining multiple schemas."""
        schemas = [
            {"title": "Product", "type": "object", "properties": {"name": {"type": "string"}}},
            {"title": "Company", "type": "object", "properties": {"founded": {"type": "integer"}}},
            {"title": "OWNS", "type": "object", "properties": {"since": {"type": "string"}}},
        ]

        result = combine_schemas(schemas)

        assert len(result["$defs"]) == 3
        assert "Product" in result["$defs"]
        assert "Company" in result["$defs"]
        assert "OWNS" in result["$defs"]

    def test_combine_with_custom_title(self):
        """Test combining schemas with custom title."""
        schemas = [{"title": "Test", "type": "object"}]

        result = combine_schemas(schemas, title="Custom Model")

        assert result["title"] == "Custom Model"

    def test_combine_with_custom_description(self):
        """Test combining schemas with custom description."""
        schemas = [{"title": "Test", "type": "object"}]

        result = combine_schemas(schemas, description="Custom description")

        assert result["description"] == "Custom description"

    def test_combine_with_custom_schema_id(self):
        """Test combining schemas with custom schema ID."""
        schemas = [{"title": "Test", "type": "object"}]

        result = combine_schemas(schemas, schema_id="https://example.com/schema.json")

        assert result["$id"] == "https://example.com/schema.json"

    def test_combine_schema_without_title_is_skipped(self):
        """Test that schemas without title are skipped."""
        schemas = [
            {"title": "Product", "type": "object"},
            {"type": "object", "properties": {}},  # No title
        ]

        result = combine_schemas(schemas)

        assert len(result["$defs"]) == 1
        assert "Product" in result["$defs"]

    def test_combine_preserves_x_graph_extension(self):
        """Test that x-graph extension is preserved."""
        schema = {
            "title": "Product",
            "type": "object",
            "x-graph": {"type": "NODE", "label": "Product", "relations": []},
        }

        result = combine_schemas([schema])

        assert result["$defs"]["Product"]["x-graph"]["type"] == "NODE"
        assert result["$defs"]["Product"]["x-graph"]["label"] == "Product"


class MockGraphSchema:
    """Mock GraphSchema for testing."""

    def __init__(self, json_schema):
        self.json_schema = json_schema


class MockModelInfo:
    """Mock GraphModelInfo for testing."""

    def __init__(self, label: str, json_schema: dict | str | None):
        self.label_ = label
        self.graph_schema = MockGraphSchema(json_schema) if json_schema is not None else None


class TestExtractSchemasFromModelInfos:
    """Tests for extract_schemas_from_model_infos function."""

    def test_extract_empty_list(self):
        """Test extracting from empty list."""
        result = extract_schemas_from_model_infos([])
        assert result == []

    def test_extract_single_model(self):
        """Test extracting schema from single model."""
        model_info = MockModelInfo("Product", {"title": "Product", "type": "object"})

        result = extract_schemas_from_model_infos([model_info])

        assert len(result) == 1
        assert result[0]["title"] == "Product"

    def test_extract_multiple_models(self):
        """Test extracting schemas from multiple models."""
        model_infos = [
            MockModelInfo("Product", {"title": "Product", "type": "object"}),
            MockModelInfo("Company", {"title": "Company", "type": "object"}),
        ]

        result = extract_schemas_from_model_infos(model_infos)

        assert len(result) == 2

    def test_extract_raises_on_missing_schema(self):
        """Test that ValueError is raised when model has no schema."""
        model_info = MockModelInfo("Product", None)

        with pytest.raises(ValueError, match="No JSON schema available for model: Product"):
            extract_schemas_from_model_infos([model_info])

    def test_extract_handles_json_string(self):
        """Test extracting schema from JSON string."""
        json_schema = json.dumps({"title": "Product", "type": "object"})
        model_info = MockModelInfo("Product", json_schema)

        result = extract_schemas_from_model_infos([model_info])

        assert len(result) == 1
        assert result[0]["title"] == "Product"

    def test_extract_raises_on_invalid_json_string(self):
        """Test that ValueError is raised for invalid JSON string."""
        model_info = MockModelInfo("Product", "not valid json")

        with pytest.raises(ValueError, match="Invalid JSON schema string for model Product"):
            extract_schemas_from_model_infos([model_info])
