"""Tests for extend_relations and extend_relation functions."""

import pytest

from cypher_graphdb import Cardinality, GraphEdge, GraphNode, edge, extend_relation, extend_relations, node
from cypher_graphdb.modelinfo import GraphNodeInfo, GraphRelationInfo
from cypher_graphdb.modelprovider import ModelProvider


class TestExtendRelations:
    """Tests for the extend_relations function."""

    def test_extend_relations_adds_new_relation(self):
        """Test that extend_relations adds a new relation to an existing node."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        # Verify no relations initially
        node_info = provider.get("Product")
        assert isinstance(node_info, GraphNodeInfo)
        assert len(node_info.relations) == 0

        # Extend with a new relation
        extend_relations(
            "Product",
            [
                GraphRelationInfo(
                    rel_type_name="HAS_CATEGORY",
                    to_type_name="Category",
                    cardinality=Cardinality.ONE_TO_ONE,
                )
            ],
            provider=provider,
        )

        # Verify relation was added
        assert len(node_info.relations) == 1
        assert node_info.relations[0].rel_type_name == "HAS_CATEGORY"
        assert node_info.relations[0].to_type_name == "Category"
        assert node_info.relations[0].cardinality == Cardinality.ONE_TO_ONE

    def test_extend_relations_with_description(self):
        """Test that extend_relations preserves relation description."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relations(
            "Product",
            [
                GraphRelationInfo(
                    rel_type_name="USES_TECHNOLOGY",
                    to_type_name="Technology",
                    description="Links product to technologies it uses",
                )
            ],
            provider=provider,
        )

        node_info = provider.get("Product")
        assert node_info.relations[0].description == "Links product to technologies it uses"

    def test_extend_relations_deduplicates(self):
        """Test that extend_relations doesn't add duplicate relations."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        rel = GraphRelationInfo(
            rel_type_name="HAS_CATEGORY",
            to_type_name="Category",
        )

        # Add same relation twice
        extend_relations("Product", [rel], provider=provider)
        extend_relations("Product", [rel], provider=provider)

        node_info = provider.get("Product")
        assert len(node_info.relations) == 1

    def test_extend_relations_multiple_relations(self):
        """Test extending with multiple relations at once."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relations(
            "Product",
            [
                GraphRelationInfo(rel_type_name="HAS_CATEGORY", to_type_name="Category"),
                GraphRelationInfo(rel_type_name="OWNED_BY", to_type_name="Company"),
                GraphRelationInfo(rel_type_name="USES_TECH", to_type_name="Technology"),
            ],
            provider=provider,
        )

        node_info = provider.get("Product")
        assert len(node_info.relations) == 3

    def test_extend_relations_raises_for_unregistered_label(self):
        """Test that extend_relations raises ValueError for unregistered label."""
        provider = ModelProvider()

        with pytest.raises(ValueError, match="'UnknownNode' is not registered"):
            extend_relations(
                "UnknownNode",
                [GraphRelationInfo(rel_type_name="REL", to_type_name="Target")],
                provider=provider,
            )

    def test_extend_relations_raises_for_edge_type(self):
        """Test that extend_relations raises ValueError when target is an edge."""

        provider = ModelProvider()

        @edge(provider=provider)
        class MyEdge(GraphEdge):
            weight: int

        with pytest.raises(ValueError, match="'MyEdge' is not a node type"):
            extend_relations(
                "MyEdge",
                [GraphRelationInfo(rel_type_name="REL", to_type_name="Target")],
                provider=provider,
            )

    def test_extend_relations_with_class_target(self):
        """Test that extend_relations accepts a class as target."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relations(
            Product,  # Class instead of string
            [GraphRelationInfo(rel_type_name="HAS_CATEGORY", to_type_name="Category")],
            provider=provider,
        )

        node_info = provider.get("Product")
        assert len(node_info.relations) == 1
        assert node_info.relations[0].rel_type_name == "HAS_CATEGORY"


class TestExtendRelation:
    """Tests for the extend_relation function (singular)."""

    def test_extend_relation_with_strings(self):
        """Test extend_relation with all string arguments."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relation(
            "Product",
            rel_type="HAS_CATEGORY",
            to_type="Category",
            cardinality=Cardinality.ONE_TO_ONE,
            description="Product category",
            provider=provider,
        )

        node_info = provider.get("Product")
        assert len(node_info.relations) == 1
        rel = node_info.relations[0]
        assert rel.rel_type_name == "HAS_CATEGORY"
        assert rel.to_type_name == "Category"
        assert rel.cardinality == Cardinality.ONE_TO_ONE
        assert rel.description == "Product category"

    def test_extend_relation_with_classes(self):
        """Test extend_relation with class arguments."""
        provider = ModelProvider()

        @node(provider=provider)
        class Category(GraphNode):
            name: str

        @edge(provider=provider)
        class HasCategory(GraphEdge):
            pass

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relation(
            Product,
            rel_type=HasCategory,
            to_type=Category,
            provider=provider,
        )

        node_info = provider.get("Product")
        assert len(node_info.relations) == 1
        rel = node_info.relations[0]
        assert rel.rel_type_name == "HasCategory"
        assert rel.to_type_name == "Category"

    def test_extend_relation_deduplicates(self):
        """Test that extend_relation doesn't add duplicate relations."""
        provider = ModelProvider()

        @node(provider=provider)
        class Product(GraphNode):
            name: str

        extend_relation("Product", rel_type="HAS_CAT", to_type="Cat", provider=provider)
        extend_relation("Product", rel_type="HAS_CAT", to_type="Cat", provider=provider)

        node_info = provider.get("Product")
        assert len(node_info.relations) == 1

    def test_extend_relation_raises_for_unregistered_label(self):
        """Test that extend_relation raises ValueError for unregistered label."""
        provider = ModelProvider()

        with pytest.raises(ValueError, match="'UnknownNode' is not registered"):
            extend_relation(
                "UnknownNode",
                rel_type="REL",
                to_type="Target",
                provider=provider,
            )
