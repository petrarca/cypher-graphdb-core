"""Example: Loading graph models from the product-graph model file.

This example demonstrates loading models from graph_model.py and using them
with CypherGraphDB to create and query a product graph.
"""

from pathlib import Path

from cypher_graphdb.modelprovider import model_provider


def example_load_graph_model():
    """Example: Load the graph_model.py file from the same directory."""
    print("\n" + "=" * 60)
    print("Loading Product Graph Model")
    print("=" * 60)

    # Get the path to graph_model.py in the same directory
    model_file = Path(__file__).parent / "graph_model.py"

    print(f"\nLoading model file: {model_file}")

    # Load the model file
    loaded_models, path = model_provider.try_to_load_models(None, str(model_file))

    if loaded_models:
        print(f"\n✓ Successfully loaded {len(loaded_models)} model(s) from {path}")

        # Separate nodes and edges
        nodes = []
        edges = []

        for model_name in sorted(loaded_models):
            model_info = model_provider.get(model_name)
            if model_info.type_.name == "NODE":
                nodes.append(model_name)
            else:
                edges.append(model_name)

        print(f"\nNodes ({len(nodes)}):")
        for node in nodes:
            model_info = model_provider.get(node)
            print(f"  - {node} (label: {model_info.label_})")

        print(f"\nEdges ({len(edges)}):")
        for edge in edges:
            model_info = model_provider.get(edge)
            print(f"  - {edge} (label: {model_info.label_})")

        return True
    else:
        print(f"✗ Failed to load models from {path}")
        return False


def example_use_loaded_models():
    """Example: Using loaded models with CypherGraphDB."""
    print("\n" + "=" * 60)
    print("Using Loaded Models with CypherGraphDB")
    print("=" * 60)

    print("\nExample usage with the loaded product graph models:")
    print("""
    # Connect to database
    db = CypherGraphDB().connect()

    # Create typed nodes using loaded models
    product = db.create_or_merge(
        {"label_": "Product", "name": "MyApp"}
    )

    technology = db.create_or_merge(
        {"label_": "Technology", "name": "Python"}
    )

    # Create typed edge
    uses_tech = db.create_or_merge(
        {
            "label_": "USES_TECHNOLOGY",
            "start_id_": product.id_,
            "end_id_": technology.id_,
            "version": "3.11"
        }
    )

    # Query with type awareness
    products = db.fetch_nodes({"label_": "Product"})
    # Results will be Product instances with typed fields

    # Query relationships
    tech_stack = db.fetch_edges({
        "label_": "USES_TECHNOLOGY",
        "start_criteria_": {"label_": "Product", "name": "MyApp"}
    })
    """)


def example_inspect_model():
    """Example: Inspect a specific model's schema."""
    print("\n" + "=" * 60)
    print("Inspecting Model Schema")
    print("=" * 60)

    # Get Product model info
    model_info = model_provider.get("Product")
    if model_info:
        print("\nModel: Product")
        print(f"Label: {model_info.label_}")
        print(f"Type: {model_info.type_.name}")

        # Get schema if available
        if model_info.graph_schema:
            schema = model_info.graph_schema.json_schema
            if schema and "properties" in schema:
                print("\nProperties:")
                for prop_name, prop_def in schema["properties"].items():
                    prop_type = prop_def.get("type", "unknown")
                    required = "required" if prop_name in schema.get("required", []) else "optional"
                    print(f"  - {prop_name}: {prop_type} ({required})")

            # Show relations
            if model_info.graph_schema.relations:
                print("\nRelations:")
                for rel in model_info.graph_schema.relations:
                    print(f"  - {rel.rel_type} -> {rel.to_type} ({rel.cardinality.name})")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("CypherGraphDB Product Graph Model Loading Example")
    print("=" * 60)

    # Load the graph model
    if example_load_graph_model():
        # Show usage examples
        example_use_loaded_models()

        # Inspect a specific model
        example_inspect_model()

    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60 + "\n")
