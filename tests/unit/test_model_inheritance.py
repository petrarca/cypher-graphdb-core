"""Test model inheritance: relation inheritance and schema description merging."""

import pytest

from cypher_graphdb import GraphNode, node, relation
from cypher_graphdb.utils import combine_schemas


@pytest.fixture(autouse=True)
def _cleanup(cleanup_model_provider):  # noqa: ARG001 - fixture parameter intentionally unused
    """Auto-use the shared cleanup fixture."""


class TestRelationInheritance:
    """Test that relation inheritance copies metadata identically to manual definition."""

    def test_inherited_relation_shares_object_reference(self):
        """Test that inherited relations share the same object as parent."""

        # Base class with relation
        @node()
        class Product(GraphNode):
            name: str

        Product = relation(
            rel_type="HAS_CATEGORY", to_type="Category", cardinality="ONE_TO_MANY", description="Product has categories"
        )(Product)

        # Inherited class
        @node(label="Product")
        class TechProduct(Product):
            tech_stack: str

        # Get relations
        product_rel = Product.graph_info_.relations[0]
        inherited_rel = TechProduct.graph_info_.relations[0]

        # Should share the same object reference
        assert product_rel is inherited_rel, "Inherited relation should share parent's object"

    def test_manual_relation_creates_new_object(self):
        """Test that manually defined relations create new objects."""

        # Base class with relation
        @node()
        class Product(GraphNode):
            name: str

        Product = relation(
            rel_type="HAS_CATEGORY", to_type="Category", cardinality="ONE_TO_MANY", description="Product has categories"
        )(Product)

        # Manual class (no inheritance)
        @node(label="Product", inherit_relations=False)
        class ManualProduct(GraphNode):
            name: str

        ManualProduct = relation(
            rel_type="HAS_CATEGORY", to_type="Category", cardinality="ONE_TO_MANY", description="Product has categories"
        )(ManualProduct)

        # Get relations
        product_rel = Product.graph_info_.relations[0]
        manual_rel = ManualProduct.graph_info_.relations[0]

        # Should create separate object
        assert product_rel is not manual_rel, "Manual relation should create new object"

    def test_inherited_and_manual_relations_have_identical_attributes(self):
        """Test that inherited and manual relations have identical attributes."""

        # Base class with relation
        @node()
        class Product(GraphNode):
            name: str

        Product = relation(
            rel_type="HAS_CATEGORY",
            to_type="Category",
            cardinality="ONE_TO_MANY",
            description="Product has categories",
            form_field=True,
        )(Product)

        # Inherited class
        @node(label="Product")
        class TechProduct(Product):
            tech_stack: str

        # Manual class
        @node(label="Product", inherit_relations=False)
        class ManualProduct(GraphNode):
            name: str

        ManualProduct = relation(
            rel_type="HAS_CATEGORY",
            to_type="Category",
            cardinality="ONE_TO_MANY",
            description="Product has categories",
            form_field=True,
        )(ManualProduct)

        # Get relations
        product_rel = Product.graph_info_.relations[0]
        inherited_rel = TechProduct.graph_info_.relations[0]
        manual_rel = ManualProduct.graph_info_.relations[0]

        # All should have identical attributes
        assert product_rel.rel_type_name == inherited_rel.rel_type_name == manual_rel.rel_type_name
        assert product_rel.to_type_name == inherited_rel.to_type_name == manual_rel.to_type_name
        assert product_rel.cardinality == inherited_rel.cardinality == manual_rel.cardinality
        assert product_rel.description == inherited_rel.description == manual_rel.description
        assert product_rel.form_field == inherited_rel.form_field == manual_rel.form_field

    def test_schema_generation_treats_inherited_and_manual_identically(self):
        """Test that schema generation treats inherited and manual relations identically."""
        from cypher_graphdb.modelprovider import model_provider
        from cypher_graphdb.utils.schema_utils import extract_schemas_from_model_infos

        # Base class with relation
        @node()
        class Product(GraphNode):
            name: str

        Product = relation(
            rel_type="HAS_CATEGORY", to_type="Category", cardinality="ONE_TO_MANY", description="Product has categories"
        )(Product)

        # Inherited class
        @node(label="Product")
        class TechProduct(Product):
            tech_stack: str

        # Manual class
        @node(label="Product", inherit_relations=False)
        class ManualProduct(GraphNode):
            name: str

        ManualProduct = relation(
            rel_type="HAS_CATEGORY", to_type="Category", cardinality="ONE_TO_MANY", description="Product has categories"
        )(ManualProduct)

        # Register models
        model_provider.register(Product)
        model_provider.register(TechProduct)
        model_provider.register(ManualProduct)

        # Generate schemas
        product_schema = extract_schemas_from_model_infos([model_provider.get("Product")])[0]
        techproduct_schema = extract_schemas_from_model_infos([model_provider.get("Product")])[0]
        manualproduct_schema = extract_schemas_from_model_infos([model_provider.get("Product")])[0]

        # All should have same number of relations
        product_relations = product_schema.get("x-graph", {}).get("relations", [])
        techproduct_relations = techproduct_schema.get("x-graph", {}).get("relations", [])
        manualproduct_relations = manualproduct_schema.get("x-graph", {}).get("relations", [])

        assert len(product_relations) == len(techproduct_relations) == len(manualproduct_relations)

        # Relations should have same structure
        if product_relations and techproduct_relations and manualproduct_relations:
            # Use correct key names (rel_type_name and to_type_name)
            assert (
                product_relations[0]["rel_type_name"]
                == techproduct_relations[0]["rel_type_name"]
                == manualproduct_relations[0]["rel_type_name"]
            )
            assert (
                product_relations[0]["to_type_name"]
                == techproduct_relations[0]["to_type_name"]
                == manualproduct_relations[0]["to_type_name"]
            )

    def test_schema_description_merging(self):
        """Test that schema-level descriptions are merged during schema combination."""
        # Two schemas with different descriptions for the same title
        schemas = [
            {
                "title": "Product",
                "description": "A product in the catalog",
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            {
                "title": "Product",
                "description": "Technology-specific product",
                "type": "object",
                "properties": {"tech_stack": {"type": "string"}},
            },
        ]

        merged = combine_schemas(schemas)
        product_def = merged["$defs"]["Product"]

        # Descriptions should be merged
        assert "A product in the catalog" in product_def["description"]
        assert "Technology-specific product" in product_def["description"]

    def test_schema_description_no_duplication(self):
        """Test that identical descriptions are not duplicated."""
        schemas = [
            {
                "title": "Company",
                "description": "A company entity",
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            {
                "title": "Company",
                "description": "A company entity",  # Same description
                "type": "object",
                "properties": {"industry": {"type": "string"}},
            },
        ]

        merged = combine_schemas(schemas)
        company_def = merged["$defs"]["Company"]

        # Should not duplicate
        assert company_def["description"] == "A company entity"

    def test_schema_description_partial(self):
        """Test that missing descriptions are handled gracefully."""
        schemas = [
            {
                "title": "Category",
                "description": "Product category",
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            {
                "title": "Category",
                # No description
                "type": "object",
                "properties": {"code": {"type": "string"}},
            },
        ]

        merged = combine_schemas(schemas)
        category_def = merged["$defs"]["Category"]

        # Should keep the existing description
        assert category_def["description"] == "Product category"
