#!/usr/bin/env python
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "loguru",
#   "pydantic",
#   "python-dotenv"
# ]
# ///
"""Test script for validating CypherGraphDB examples."""

import json

from cypher_graphdb import CypherGraphDB, GraphEdge, GraphNode, MatchEdgeCriteria, MatchNodeCriteria, edge, node, relation


# Define model classes for our examples - Typed approach
@node(metadata={"category": "software"})
@relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    name: str
    version: str | None = None
    description: str | None = None

    def is_stable(self) -> bool:
        """Check if product is stable."""
        return self.version and not self.version.endswith("-beta")


@node(label="Technology")
class Technology(GraphNode):
    name: str
    category: str | None = None

    def is_database(self) -> bool:
        """Check if this is a database technology."""
        return self.category == "Database"


@edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    since: int | None = None
    version: str | None = None


def test_basic_connection():
    """Test basic connection to the database."""
    print("Testing basic connection...")
    with CypherGraphDB() as db:
        db.connect()
        result = db.execute("MATCH (n) RETURN count(n)")
        print(f"Node count: {result}")
    print("Connection test completed.\n")


def test_fetch_nodes_untyped():
    """Test fetching nodes with different criteria using untyped approach."""
    print("Testing fetch_nodes (untyped approach)...")
    with CypherGraphDB() as db:
        db.connect()

        # Fetch by label
        products = db.fetch_nodes({"label_": "Product"})
        print(f"Found {len(products)} products")

        # Fetch by properties - using existing product
        product = db.fetch_nodes({"label_": "Product", "name": "CypherGraph"}, unnest_result=True)
        if product:
            print(f"Found product: {product.properties_.get('name')}")
            # With untyped approach, we need to access properties through properties_ dictionary
            print(f"Properties: {product.properties_}")

        # Advanced criteria with labels and projections
        criteria = MatchNodeCriteria(label_="Technology", projection_=["n.name", "id(n)"])
        technologies = db.fetch_nodes(criteria)
        print(f"Found {len(technologies)} technologies")
    print("Fetch nodes untyped test completed.\n")


def test_fetch_nodes_typed():
    """Test fetching nodes with different criteria using typed approach."""
    print("Testing fetch_nodes (typed approach)...")
    with CypherGraphDB() as db:
        db.connect()

        # Fetch by label using model class
        products = db.fetch_nodes({"label_": Product})
        print(f"Found {len(products)} products")

        # Fetch by properties - using existing product
        product = db.fetch_nodes({"label_": Product, "name": "CypherGraph"}, unnest_result=True)
        if product:
            # With typed approach, we can access properties directly as attributes
            print(f"Found product: {product.name}")
            if hasattr(product, "is_stable"):
                print(f"Is stable: {product.is_stable()}")

        # Advanced criteria with model class
        criteria = MatchNodeCriteria(label_=Technology, projection_=["n.name", "n.category", "id(n)"])
        technologies = db.fetch_nodes(criteria)
        print(f"Found {len(technologies)} technologies")
    print("Fetch nodes typed test completed.\n")


def test_fetch_edges_untyped():
    """Test fetching edges with different criteria using untyped approach."""
    print("Testing fetch_edges (untyped approach)...")
    with CypherGraphDB() as db:
        db.connect()

        # Fetch by edge type
        uses_relations = db.fetch_edges({"label_": "USES_TECHNOLOGY"})
        print(f"Found {len(uses_relations)} USES_TECHNOLOGY relationships")

        # Advanced criteria with start/end node filtering
        criteria = MatchEdgeCriteria(
            label_="USES_TECHNOLOGY",
            start_criteria_=MatchNodeCriteria(label_="Product", properties_={"name": "CypherGraph"}),
            end_criteria_=MatchNodeCriteria(label_="Technology"),
            fetch_nodes_=True,  # Include connected nodes in results
        )
        product_tech_relations = db.fetch_edges(criteria)
        print(f"Found {len(product_tech_relations)} technologies used by CypherGraph")

        # Print the technologies used by CypherGraph
        if product_tech_relations:
            for rel in product_tech_relations:
                if hasattr(rel, "end_node") and rel.end_node:
                    print(f"- CypherGraph uses {rel.end_node.properties_.get('name')}")
    print("Fetch edges untyped test completed.\n")


def test_fetch_edges_typed():
    """Test fetching edges with different criteria using typed approach."""
    print("Testing fetch_edges (typed approach)...")
    with CypherGraphDB() as db:
        db.connect()

        # Fetch by edge type using model class
        uses_relations = db.fetch_edges({"label_": UsesTechnology})
        print(f"Found {len(uses_relations)} USES_TECHNOLOGY relationships")

        # Advanced criteria with start/end node filtering using model classes
        criteria = MatchEdgeCriteria(
            label_=UsesTechnology,
            start_criteria_=MatchNodeCriteria(label_=Product, properties_={"name": "CypherGraph"}),
            end_criteria_=MatchNodeCriteria(label_=Technology),
            fetch_nodes_=True,  # Include connected nodes in results
        )
        product_tech_relations = db.fetch_edges(criteria)
        print(f"Found {len(product_tech_relations)} technologies used by CypherGraph")

        # Print the technologies used by CypherGraph with typed access
        if product_tech_relations:
            for rel in product_tech_relations:
                if hasattr(rel, "end_node") and rel.end_node:
                    # With typed approach, we can access properties directly
                    tech_name = rel.end_node.name if hasattr(rel.end_node, "name") else rel.end_node.properties_.get("name")
                    print(f"- CypherGraph uses {tech_name}")

                    # We can also access custom methods if the object is properly typed
                    if hasattr(rel.end_node, "is_database"):
                        is_db = rel.end_node.is_database()
                        print(f"  Is database technology: {is_db}")
    print("Fetch edges typed test completed.\n")


def test_execute():
    """Test execute with different result formatting."""
    print("Testing execute...")
    with CypherGraphDB() as db:
        db.connect()

        # Basic query execution
        result = db.execute("MATCH (n:Product) RETURN n.name, n.gid_ LIMIT 3")
        print(f"Basic result: {result}")

        # Get single result
        count = db.execute(
            "MATCH (n:Product) RETURN count(n)",
            unnest_result="rc",  # single row, single column -> scalar
        )
        print(f"Product count: {count}")

        # Get first column only
        names = db.execute("MATCH (n:Technology) RETURN n.name, n.gid_ ORDER BY n.name LIMIT 3", unnest_result="c")
        print(f"Technology names: {names}")

        # Complex analytical query
        query = """
            MATCH (p:Product)-[r:USES_TECHNOLOGY]->(t:Technology)
            RETURN t.name AS technology, count(p) AS product_count
            ORDER BY product_count DESC
            LIMIT 3
        """
        stats = db.execute(query)
        print(f"Technology usage stats: {stats}")
    print("Execute test completed.\n")


def test_model_comparison():
    """Compare untyped and typed approaches."""
    print("Testing model comparison...")
    with CypherGraphDB() as db:
        db.connect()

        print("\nUntyped approach:")
        # Untyped approach - working with raw dictionaries
        untyped_product = db.fetch_nodes({"label_": "Product", "name": "CypherGraph"}, unnest_result=True)
        if untyped_product:
            print(f"- Product name: {untyped_product.properties_.get('name')}")
            print("- Access requires using properties_ dictionary")
            print("- No validation or custom methods available")

        print("\nTyped approach:")
        # Typed approach - working with model instances
        typed_product = db.fetch_nodes({"label_": Product, "name": "CypherGraph"}, unnest_result=True)
        if typed_product:
            # Direct attribute access
            if hasattr(typed_product, "name"):
                print(f"- Product name: {typed_product.name}")
                print("- Direct attribute access available")

            # Custom methods
            if hasattr(typed_product, "is_stable"):
                print(f"- Custom method available: is_stable() = {typed_product.is_stable()}")

            # Type validation
            print("- Type validation through Pydantic models")
    print("Model comparison completed.\n")


def test_model_info():
    """Test model information."""
    with CypherGraphDB() as cdb:
        cdb.connect()

        result = list(cdb.model_provider.model_dump(context={"with_detailed_fields": True}).values())
        print(json.dumps(result, indent=2))


def test_model_json_schema():
    """Show how to access JSON Schema for registered models."""
    print("Generating JSON schema for registered models...")
    with CypherGraphDB() as cdb:
        cdb.connect()

        for label in cdb.model_provider:
            model_info = cdb.model_provider.get(label)
            if not model_info:
                continue

            schema = model_info.graph_schema.json_schema or {}

            print(f"\nSchema for {label}:")
            print(json.dumps(schema, indent=2))


def main():
    # print("Starting example tests...\n")

    # test_basic_connection()
    # test_fetch_nodes_untyped()
    # test_fetch_nodes_typed()
    # test_fetch_edges_untyped()
    # test_fetch_edges_typed()
    # test_execute()
    # test_model_comparison()

    # print("\nAll tests completed.")
    # test_model_info()
    test_model_json_schema()


if __name__ == "__main__":
    main()
