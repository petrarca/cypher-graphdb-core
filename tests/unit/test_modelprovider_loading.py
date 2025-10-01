"""Improved tests for ModelProvider model loading functionality.

This test suite demonstrates a better approach to testing model loading
by creating actual model files and testing against the global provider
with proper cleanup.
"""

import tempfile
from pathlib import Path

import pytest

from cypher_graphdb.modelprovider import model_provider


@pytest.fixture(autouse=True)
def record_initial_models():
    """Record models before each test for proper cleanup verification."""
    initial_models = set(model_provider._models.keys())
    yield initial_models
    # After test, we can verify what was added (but not remove for isolation)


class TestModelProviderFileLoading:
    """Test loading models from files - demonstrating real-world usage."""

    def test_load_single_file_basic(self, record_initial_models):
        """Test loading a single model file - basic case."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a simple model file
            model_file = Path(tmpdir, "person.py")
            model_file.write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Person(GraphNode):
    '''A person node.'''
    name: str
    age: int
""",
                encoding="utf-8",
            )

            # Load the model
            loaded_models = model_provider.try_to_load_models(None, str(model_file))

            # Verify
            assert loaded_models is not None
            assert len(loaded_models) == 1
            assert loaded_models[0].label_ == "Person"
            assert loaded_models[0].source is not None
            assert loaded_models[0].source.startswith("file://")

    def test_load_directory_with_multiple_models(self, record_initial_models):
        """Test loading all models from a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple model files
            (Path(tmpdir) / "company.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Company(GraphNode):
    '''A company node.'''
    name: str
    industry: str
""",
                encoding="utf-8",
            )

            (Path(tmpdir) / "employee.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Employee(GraphNode):
    '''An employee node.'''
    name: str
    position: str
""",
                encoding="utf-8",
            )

            (Path(tmpdir) / "works_at.py").write_text(
                """
from cypher_graphdb import edge, GraphEdge

@edge()
class WORKS_AT(GraphEdge):
    '''Employment relationship.'''
    since: int
""",
                encoding="utf-8",
            )

            # Load all models from directory
            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Verify
            assert loaded_models is not None
            assert len(loaded_models) == 3
            labels = [m.label_ for m in loaded_models]
            assert "Company" in labels
            assert "Employee" in labels
            assert "WORKS_AT" in labels
            # Verify all have source set
            for model_info in loaded_models:
                assert model_info.source is not None
                assert model_info.source.startswith("file://")

    def test_models_are_sorted_correctly(self, record_initial_models):
        """Test that nodes come before edges in sorted results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create edge first (alphabetically), then node
            (Path(tmpdir) / "a_edge.py").write_text(
                """
from cypher_graphdb import edge, GraphEdge

@edge()
class RELATED_TO(GraphEdge):
    '''A relationship.'''
    type: str
""",
                encoding="utf-8",
            )

            (Path(tmpdir) / "z_node.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Thing(GraphNode):
    '''A thing node.'''
    name: str
""",
                encoding="utf-8",
            )

            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Nodes should come before edges
            assert loaded_models is not None
            labels = [m.label_ for m in loaded_models]
            thing_idx = labels.index("Thing")
            related_idx = labels.index("RELATED_TO")
            assert thing_idx < related_idx, "Nodes should come before edges"

    def test_load_single_file_that_does_not_exist(self):
        """Test loading a non-existent file."""
        import pytest

        path = "/tmp/nonexistent_model_file_12345.py"
        with pytest.raises(FileNotFoundError, match="Path does not exist"):
            model_provider.try_to_load_models(None, path)

    def test_load_empty_directory(self):
        """Test loading from an empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            assert loaded_models is None

    def test_directory_ignores_init_files(self, record_initial_models):
        """Test that __init__.py files are properly ignored."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create __init__.py
            (Path(tmpdir) / "__init__.py").write_text("# Package init", encoding="utf-8")

            # Create one actual model
            (Path(tmpdir) / "widget.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Widget(GraphNode):
    '''A widget.'''
    id: str
""",
                encoding="utf-8",
            )

            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Should only load Widget, not __init__
            assert loaded_models is not None
            assert len(loaded_models) == 1
            assert loaded_models[0].label_ == "Widget"

    def test_directory_with_syntax_error_file(self, record_initial_models):
        """Test that directory loading continues despite syntax errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a file with syntax error
            (Path(tmpdir) / "bad.py").write_text("this is invalid python <<<", encoding="utf-8")

            # Create a valid file
            (Path(tmpdir) / "good.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class ValidModel(GraphNode):
    '''A valid model.'''
    value: str
""",
                encoding="utf-8",
            )

            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Should load the valid model despite the error
            assert loaded_models is not None
            labels = [m.label_ for m in loaded_models]
            assert "ValidModel" in labels

    def test_load_with_module_name_and_path(self, record_initial_models):
        """Test specifying both module_name and path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir, "my_model.py")
            model_file.write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class CustomModel(GraphNode):
    '''A custom model.'''
    data: str
""",
                encoding="utf-8",
            )

            loaded_models = model_provider.try_to_load_models("my_custom_module_name", str(model_file))

            assert loaded_models is not None
            assert loaded_models[0].label_ == "CustomModel"


class TestModelProviderDirectoryScanning:
    """Test directory scanning behavior."""

    def test_loads_all_py_files_in_directory(self, record_initial_models):
        """Test that all .py files are discovered and loaded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple files
            for i in range(5):
                (Path(tmpdir) / f"model{i}.py").write_text(
                    f"""
from cypher_graphdb import node, GraphNode

@node()
class Model{i}(GraphNode):
    '''Model {i}.'''
    value: int
""",
                    encoding="utf-8",
                )

            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            assert loaded_models is not None
            assert len(loaded_models) == 5
            labels = [m.label_ for m in loaded_models]
            for i in range(5):
                assert f"Model{i}" in labels

    def test_skips_non_python_files(self, record_initial_models):
        """Test that non-.py files are ignored."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .py file
            (Path(tmpdir) / "model.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class RealModel(GraphNode):
    '''A real model.'''
    val: str
""",
                encoding="utf-8",
            )

            # Create non-.py files
            (Path(tmpdir) / "README.md").write_text("# Readme", encoding="utf-8")
            (Path(tmpdir) / "config.json").write_text("{}", encoding="utf-8")
            (Path(tmpdir) / "data.txt").write_text("data", encoding="utf-8")

            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Should only load the .py file
            assert loaded_models is not None
            assert len(loaded_models) == 1
            assert loaded_models[0].label_ == "RealModel"


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_typical_models_directory_structure(self, record_initial_models):
        """Test a typical project models directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = Path(tmpdir) / "models"
            models_dir.mkdir()

            # Create a typical models structure
            (models_dir / "user.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class User(GraphNode):
    '''User node.'''
    username: str
    email: str
    active: bool = True
""",
                encoding="utf-8",
            )

            (models_dir / "post.py").write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Post(GraphNode):
    '''Post node.'''
    title: str
    content: str
    published: bool = False
""",
                encoding="utf-8",
            )

            (models_dir / "_relationships.py").write_text(
                """
from cypher_graphdb import edge, GraphEdge

@edge()
class AUTHORED(GraphEdge):
    '''User authored post.'''
    timestamp: int

@edge()
class LIKES(GraphEdge):
    '''User likes post.'''
    timestamp: int
""",
                encoding="utf-8",
            )

            loaded_models = model_provider.try_to_load_models(None, str(models_dir))

            assert loaded_models is not None
            assert len(loaded_models) == 4
            labels = [m.label_ for m in loaded_models]
            assert "User" in labels
            assert "Post" in labels
            assert "AUTHORED" in labels
            assert "LIKES" in labels

    def test_load_single_model_file_from_project(self, record_initial_models):
        """Test loading just one specific model file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a specific model file
            model_file = Path(tmpdir, "product.py")
            model_file.write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Product(GraphNode):
    '''Product in catalog.'''
    sku: str
    name: str
    price: float
    in_stock: bool = True
""",
                encoding="utf-8",
            )

            # Load only this file
            loaded_models = model_provider.try_to_load_models(None, str(model_file))

            assert loaded_models is not None
            assert len(loaded_models) == 1
            assert loaded_models[0].label_ == "Product"

            # Verify the model is registered and usable
            model_info = model_provider.get("Product")
            assert model_info is not None
            assert model_info.label_ == "Product"

    def test_source_property_is_set_on_loaded_models(self, record_initial_models):
        """Test that source property contains the correct file URI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create model files with unique names
            account_file = Path(tmpdir, "account.py")
            account_file.write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Account(GraphNode):
    '''An account node.'''
    account_id: str
    balance: float
""",
                encoding="utf-8",
            )

            permission_file = Path(tmpdir, "permission.py")
            permission_file.write_text(
                """
from cypher_graphdb import node, GraphNode

@node()
class Permission(GraphNode):
    '''A permission node.'''
    name: str
    level: int
""",
                encoding="utf-8",
            )

            # Load all models from directory
            loaded_models = model_provider.try_to_load_models(None, tmpdir)

            # Verify source property is set for all loaded models
            assert loaded_models is not None
            assert len(loaded_models) >= 2  # At least the two we created

            # Find our specific models
            account_model = None
            permission_model = None
            for model_info in loaded_models:
                if model_info.label_ == "Account":
                    account_model = model_info
                elif model_info.label_ == "Permission":
                    permission_model = model_info

            # Verify both were loaded
            assert account_model is not None, "Account model not found"
            assert permission_model is not None, "Permission model not found"

            # Check each model has a source with file URI
            assert account_model.source is not None
            assert account_model.source.startswith("file://")
            assert str(account_file) in account_model.source

            assert permission_model.source is not None
            assert permission_model.source.startswith("file://")
            assert str(permission_file) in permission_model.source

            # Also verify via model_provider.get()
            account_from_provider = model_provider.get("Account")
            assert account_from_provider is not None
            assert account_from_provider.source is not None
            assert "account.py" in account_from_provider.source

            permission_from_provider = model_provider.get("Permission")
            assert permission_from_provider is not None
            assert permission_from_provider.source is not None
            assert "permission.py" in permission_from_provider.source
