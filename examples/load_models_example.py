"""Example: Loading graph models from files and directories.

This example demonstrates the enhanced model loading capabilities:
1. Loading a single model file
2. Loading all models from a directory
3. Using loaded models with CypherGraphDB
"""

import tempfile
from pathlib import Path

from cypher_graphdb import CypherGraphDB
from cypher_graphdb.modelprovider import model_provider


def create_example_models():
    """Create example model files in a temporary directory."""
    tmpdir = Path(tempfile.mkdtemp(prefix="cypher_models_"))

    # Create node models
    (tmpdir / "person.py").write_text("""
from cypher_graphdb import node, GraphNode

@node()
class Person(GraphNode):
    '''Represents a person.'''
    name: str
    age: int
    email: str = None
""", encoding="utf-8")

    (tmpdir / "company.py").write_text("""
from cypher_graphdb import node, GraphNode

@node()
class Company(GraphNode):
    '''Represents a company.'''
    name: str
    industry: str
    founded: int
""", encoding="utf-8")

    # Create edge models
    (tmpdir / "relationships.py").write_text("""
from cypher_graphdb import edge, GraphEdge

@edge()
class WORKS_FOR(GraphEdge):
    '''Employment relationship.'''
    position: str
    since: int

@edge()
class KNOWS(GraphEdge):
    '''Knows relationship between people.'''
    since: int = None
""", encoding="utf-8")

    return tmpdir


def example_load_single_file():
    """Example: Load a single model file."""
    print("\n" + "="*60)
    print("Example 1: Loading a Single Model File")
    print("="*60)

    # Create a temporary model file
    with tempfile.TemporaryDirectory() as tmpdir:
        model_file = Path(tmpdir) / "product.py"
        model_file.write_text("""
from cypher_graphdb import node, GraphNode

@node()
class Product(GraphNode):
    '''A product in the catalog.'''
    sku: str
    name: str
    price: float
    in_stock: bool = True
""", encoding="utf-8")

        print(f"\nCreated model file: {model_file}")

        # Load the single file
        loaded_models, path = model_provider.try_to_load_models(
            None, str(model_file)
        )

        if loaded_models:
            print(f"\n✓ Successfully loaded {len(loaded_models)} model(s):")
            for model_name in loaded_models:
                print(f"  - {model_name}")

            # Verify the model is registered
            model_info = model_provider.get("Product")
            print(f"\nModel info for 'Product':")
            print(f"  Label: {model_info.label_}")
            print(f"  Type: {model_info.type_}")
            print(f"  Has class: {model_info.graph_model is not None}")
        else:
            print(f"✗ Failed to load models from {path}")


def example_load_directory():
    """Example: Load all models from a directory."""
    print("\n" + "="*60)
    print("Example 2: Loading All Models from a Directory")
    print("="*60)

    models_dir = create_example_models()

    print(f"\nCreated models directory: {models_dir}")
    print("\nDirectory contents:")
    for file in sorted(models_dir.glob("*.py")):
        print(f"  - {file.name}")

    # Load all models from the directory
    loaded_models, path = model_provider.try_to_load_models(None, str(models_dir))

    if loaded_models:
        print(f"\n✓ Successfully loaded {len(loaded_models)} model(s):")
        for model_name in loaded_models:
            model_info = model_provider.get(model_name)
            model_type = "NODE" if model_info.type_.name == "NODE" else "EDGE"
            print(f"  - {model_name} ({model_type})")

        print(f"\nNote: Nodes are listed before edges due to sorting.")
    else:
        print(f"✗ Failed to load models from {path}")


def example_use_loaded_models():
    """Example: Using loaded models with CypherGraphDB."""
    print("\n" + "="*60)
    print("Example 3: Using Loaded Models with CypherGraphDB")
    print("="*60)

    models_dir = create_example_models()

    # Load models
    loaded_models, _ = model_provider.try_to_load_models(None, str(models_dir))
    print(f"\n✓ Loaded {len(loaded_models)} models")

    # Note: This example shows the API usage
    # Actual database operations would require a running graph database

    print("\nExample usage with CypherGraphDB:")
    print("""
    # Connect to database
    db = CypherGraphDB().connect()

    # Create typed nodes using loaded models
    person = db.create_or_merge(
        {"label_": "Person", "name": "Alice", "age": 30}
    )

    company = db.create_or_merge(
        {"label_": "Company", "name": "TechCorp", "industry": "Technology", "founded": 2015}
    )

    # Create typed edge
    works_for = db.create_or_merge(
        {
            "label_": "WORKS_FOR",
            "start_id_": person.id_,
            "end_id_": company.id_,
            "position": "Engineer",
            "since": 2020
        }
    )

    # Query with type awareness
    results = db.fetch_nodes({"label_": "Person"})
    # Results will be Person instances with typed fields
    """)


def example_mixed_loading():
    """Example: Loading specific files and directories."""
    print("\n" + "="*60)
    print("Example 4: Mixed Loading - File and Directory")
    print("="*60)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a models directory
        models_dir = tmpdir / "models"
        models_dir.mkdir()

        (models_dir / "user.py").write_text("""
from cypher_graphdb import node, GraphNode

@node()
class User(GraphNode):
    '''User account.'''
    username: str
    email: str
""", encoding="utf-8")

        # Create a separate model file
        extra_model = tmpdir / "admin.py"
        extra_model.write_text("""
from cypher_graphdb import node, GraphNode

@node()
class Admin(GraphNode):
    '''Administrator account.'''
    username: str
    level: int
""", encoding="utf-8")

        # Load directory first
        loaded1, _ = model_provider.try_to_load_models(None, str(models_dir))
        print(f"\n✓ Loaded from directory: {loaded1}")

        # Load specific file
        loaded2, _ = model_provider.try_to_load_models(None, str(extra_model))
        print(f"✓ Loaded from file: {loaded2}")

        print(f"\nTotal new models loaded: {len(loaded1) + len(loaded2)}")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("CypherGraphDB Model Loading Examples")
    print("="*60)

    example_load_single_file()
    example_load_directory()
    example_use_loaded_models()
    example_mixed_loading()

    print("\n" + "="*60)
    print("All examples completed!")
    print("="*60 + "\n")
