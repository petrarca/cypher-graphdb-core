"""Test @extend_relation decorator functionality."""

from cypher_graphdb import GraphNode


class TestExtendRelationDecorator:
    """Test the @extend_relation decorator functionality."""

    def test_extend_relation_decorator_adds_relation_to_source_class(self, mp):
        """Test that @extend_relation decorator adds relation to source class."""

        # Create source class
        @mp.node()
        class Product(GraphNode):
            name: str

        # Use decorator on target class
        @mp.extend_relation(Product, "HAS_TECH_STACK", description="Product's tech stack")
        class TechnologyStack(GraphNode):
            name: str
            components: list[str]

        # Verify relation was added to Product (source class)
        assert len(Product.graph_info_.relations) == 1

        rel = Product.graph_info_.relations[0]
        assert rel.rel_type_name == "HAS_TECH_STACK"
        assert rel.to_type_name == "TechnologyStack"
        assert rel.description == "Product's tech stack"
        assert rel.cardinality == "ONE_TO_MANY"

        # Target class should have no relations
        assert len(TechnologyStack.graph_info_.relations) == 0

    def test_extend_relation_decorator_without_node_decorator(self, mp):
        """Test that @extend_relation applies @node if not already applied."""

        # Create source class
        @mp.node()
        class Product(GraphNode):
            name: str

        # Use decorator on target class without @node
        @mp.extend_relation(Product, "HAS_CATEGORY")
        class Category(GraphNode):
            name: str

        # Verify @node was applied to Category
        assert hasattr(Category, "graph_info_")
        assert Category.graph_info_.label_ == "Category"

        # Verify relation was added to Product
        assert len(Product.graph_info_.relations) == 1
        rel = Product.graph_info_.relations[0]
        assert rel.rel_type_name == "HAS_CATEGORY"
        assert rel.to_type_name == "Category"

    def test_extend_relation_decorator_with_cardinality(self, mp):
        """Test decorator with custom cardinality."""

        @mp.node()
        class Product(GraphNode):
            name: str

        @mp.extend_relation(Product, "HAS_PRIMARY_SUPPLIER", cardinality="ONE_TO_ONE", description="Primary supplier for product")
        class Supplier(GraphNode):
            name: str

        rel = Product.graph_info_.relations[0]
        assert rel.cardinality == "ONE_TO_ONE"
        assert rel.description == "Primary supplier for product"

    def test_extend_relation_decorator_multiple_decorators(self, mp):
        """Test multiple @extend_relation decorators on same class."""

        @mp.node()
        class Product(GraphNode):
            name: str

        # Apply multiple decorators
        @mp.extend_relation(Product, "HAS_CATEGORY", description="Product category")
        @mp.extend_relation(Product, "HAS_SUPPLIER", description="Product supplier")
        class Category(GraphNode):  # noqa: F401  # unused class
            name: str

        @mp.extend_relation(Product, "HAS_MANUFACTURER", description="Product manufacturer")
        class Manufacturer(GraphNode):  # noqa: F401  # unused class
            name: str

        # Verify all relations were added to Product
        assert len(Product.graph_info_.relations) == 3

        relation_types = {rel.rel_type_name for rel in Product.graph_info_.relations}
        expected_types = {"HAS_CATEGORY", "HAS_SUPPLIER", "HAS_MANUFACTURER"}
        assert relation_types == expected_types

    def test_extend_relation_decorator_with_classes(self, mp):
        """Test decorator using class references instead of strings."""

        @mp.node()
        class Product(GraphNode):
            name: str

        @mp.node()
        class HasTechStack(GraphNode):  # noqa: F401  # unused class
            """Edge class for tech stack relationship."""

        @mp.extend_relation(Product, HasTechStack, description="Tech stack relation")
        class TechnologyStack(GraphNode):
            name: str

        # Verify relation was added using class reference
        rel = Product.graph_info_.relations[0]
        assert rel.rel_type_name == "HasTechStack"
        assert rel.to_type_name == "TechnologyStack"

    def test_extend_relation_function_still_works(self, mp):
        """Test that the function version of extend_relation still works."""

        @mp.node()
        class Product(GraphNode):
            name: str

        @mp.node()
        class Category(GraphNode):  # noqa: F401  # unused class
            name: str

        @mp.node()
        class Supplier(GraphNode):  # noqa: F401  # unused class
            name: str

        # Use as function (not decorator)
        mp.extend_relation(Product, "HAS_CATEGORY", Category, description="Product category")

        # Verify relation was added
        assert len(Product.graph_info_.relations) == 1
        rel = Product.graph_info_.relations[0]
        assert rel.rel_type_name == "HAS_CATEGORY"
        assert rel.to_type_name == "Category"
        assert rel.description == "Product category"
