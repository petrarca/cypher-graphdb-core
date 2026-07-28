"""Tests for ModelProvider schema generation functionality."""

import tempfile
from pathlib import Path

import pytest

PREAMBLE = "from cypher_graphdb import GraphEdge, GraphNode, ModelProvider\n\nmp = ModelProvider()\n"


class TestGenerateSchemasFromPath:
    """Tests for ModelProvider.generate_schemas_from_path method."""

    def test_generate_schemas_from_single_file(self, mp):
        """Test generating schemas from a single model file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir, "product.py")
            model_file.write_text(
                PREAMBLE
                + """
@mp.node()
class ProductSchema(GraphNode):
    '''A product node.'''
    name: str
    price: float
""",
                encoding="utf-8",
            )

            schemas = mp.generate_schemas_from_path(str(model_file))

            assert len(schemas) == 1
            assert schemas[0]["title"] == "ProductSchema"
            assert "x-graph" in schemas[0]
            assert schemas[0]["x-graph"]["type"] == "NODE"

    def test_generate_schemas_from_directory(self, mp):
        """Test generating schemas from a directory with multiple models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "node_models.py").write_text(
                PREAMBLE
                + """
@mp.node()
class CategorySchema(GraphNode):
    '''A category node.'''
    name: str
""",
                encoding="utf-8",
            )

            (Path(tmpdir) / "edge_models.py").write_text(
                PREAMBLE
                + """
@mp.edge()
class BELONGS_TO_SCHEMA(GraphEdge):
    '''A belongs to edge.'''
    since: str
""",
                encoding="utf-8",
            )

            schemas = mp.generate_schemas_from_path(str(tmpdir))

            assert len(schemas) == 2
            labels = [s.get("x-graph", {}).get("label") or s.get("title") for s in schemas]
            assert "CategorySchema" in labels
            assert "BELONGS_TO_SCHEMA" in labels

    def test_generate_schemas_combined(self, mp):
        """Test generating combined schema with $defs format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir, "item.py")
            model_file.write_text(
                PREAMBLE
                + """
@mp.node()
class ItemSchema(GraphNode):
    '''An item node.'''
    name: str
""",
                encoding="utf-8",
            )

            combined = mp.generate_schemas_from_path(str(model_file), combine=True)

            assert "$schema" in combined
            assert "$defs" in combined
            assert "ItemSchema" in combined["$defs"]
            assert combined["title"] == "Graph Data Model"

    def test_generate_schemas_nonexistent_path_raises(self, mp):
        """Test that FileNotFoundError is raised for nonexistent path."""
        with pytest.raises(FileNotFoundError):
            mp.generate_schemas_from_path("/nonexistent/path/models.py")

    def test_generate_schemas_empty_directory(self, mp):
        """Test generating schemas from empty directory returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            schemas = mp.generate_schemas_from_path(str(tmpdir))
            assert schemas == []

    def test_generate_schemas_combined_empty_returns_empty_dict(self, mp):
        """Test generating combined schema from empty directory returns empty dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            combined = mp.generate_schemas_from_path(str(tmpdir), combine=True)
            assert combined == {}
