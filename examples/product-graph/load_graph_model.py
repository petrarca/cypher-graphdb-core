"""Example: Loading graph models from the product-graph model file.

This example demonstrates loading models from graph_model.py.
"""

from pathlib import Path

from cypher_graphdb.modelprovider import model_provider


def main():
    """Load the graph_model.py file and display the loaded models."""
    print("\n" + "=" * 60)
    print("Loading Product Graph Model")
    print("=" * 60)

    # Get the path to graph_model.py in the same directory
    model_file = Path(__file__).parent / "graph_model.py"

    print(f"\nLoading model file: {model_file}")

    # Load the model file
    loaded_models = model_provider.try_to_load_models(None, str(model_file))

    if not loaded_models:
        print(f"✗ Failed to load models from {model_file}")
        return

    print(f"\n✓ Successfully loaded {len(loaded_models)} model(s)")

    # Separate and display nodes and edges
    nodes = [m for m in loaded_models if m.type_.name == "NODE"]
    edges = [m for m in loaded_models if m.type_.name == "EDGE"]

    print(f"\nNodes ({len(nodes)}):")
    for model_info in sorted(nodes, key=lambda m: m.label_):
        class_name = model_info.graph_model.__name__ if model_info.graph_model else "N/A"
        print(f"  - {model_info.label_} ({class_name})")

    print(f"\nEdges ({len(edges)}):")
    for model_info in sorted(edges, key=lambda m: m.label_):
        class_name = model_info.graph_model.__name__ if model_info.graph_model else "N/A"
        print(f"  - {model_info.label_} ({class_name})")

    # Show example: inspect Product model details
    print("\n" + "=" * 60)
    print("Product Model Details")
    print("=" * 60)

    product_model = model_provider.get("Product")
    if product_model and product_model.graph_schema:
        schema = product_model.graph_schema.json_schema
        if schema and "properties" in schema:
            print("\nProperties:")
            for prop_name, prop_def in schema["properties"].items():
                prop_type = prop_def.get("type", "unknown")
                is_required = prop_name in schema.get("required", [])
                req_label = "required" if is_required else "optional"
                print(f"  - {prop_name}: {prop_type} ({req_label})")

        # Show relations (only available for node models)
        if hasattr(product_model, "relations") and product_model.relations:
            print("\nRelations:")
            for rel in product_model.relations:
                rel_type = rel.rel_type_name
                to_type = rel.to_type_name
                cardinality = rel.cardinality.name
                print(f"  - {rel_type} -> {to_type} ({cardinality})")

    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
