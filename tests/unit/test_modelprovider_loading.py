"""Tests for ModelProvider loading models from files, packages and directories.

Loading works by *discovering* the ``ModelProvider`` instances declared in the
loaded modules and adopting their models. Each test therefore writes real model
files that declare their own provider, and loads them into a fresh, isolated
provider -- exactly how a consumer does it.
"""

import tempfile
from pathlib import Path

import pytest

from cypher_graphdb import ModelProvider

PREAMBLE = "from cypher_graphdb import GraphEdge, GraphNode, ModelProvider\n\nmp = ModelProvider()\n"


def write_model(directory: Path, filename: str, body: str) -> Path:
    """Write a model file that declares its own provider, and return its path."""
    path = Path(directory) / filename
    path.write_text(PREAMBLE + body, encoding="utf-8")
    return path


class TestSingleFileLoading:
    """Loading one Python file."""

    def test_load_single_file(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = write_model(
                Path(tmpdir),
                "person.py",
                """
@mp.node()
class Person(GraphNode):
    name: str
    age: int
""",
            )

            loaded = mp.try_to_load_models(None, str(model_file))

            assert loaded is not None
            assert len(loaded) == 1
            assert loaded[0].label_ == "Person"
            assert loaded[0].source == "model"

    def test_load_with_explicit_module_name(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = write_model(
                Path(tmpdir),
                "my_model.py",
                """
@mp.node()
class CustomModel(GraphNode):
    data: str
""",
            )

            loaded = mp.try_to_load_models("my_custom_module_name", str(model_file))

            assert loaded is not None
            assert loaded[0].label_ == "CustomModel"

    def test_loaded_model_is_registered_and_usable(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = write_model(
                Path(tmpdir),
                "product.py",
                """
@mp.node()
class Product(GraphNode):
    sku: str
    price: float
    in_stock: bool = True
""",
            )

            loaded = mp.try_to_load_models(None, str(model_file))

            assert loaded is not None
            assert [m.label_ for m in loaded] == ["Product"]

            info = mp.get("Product")
            assert info is not None
            assert info.label_ == "Product"

            node = mp.create_node("Product", {"sku": "A-1", "price": 9.5})
            assert node.sku == "A-1"
            assert node.in_stock is True

    def test_multiple_providers_in_one_file_are_all_adopted(self, mp):
        """A module may declare several providers -- all of them are adopted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir) / "multi.py"
            model_file.write_text(
                """
from cypher_graphdb import GraphNode, ModelProvider

taxonomy = ModelProvider()
domain = ModelProvider()

@taxonomy.node()
class Category(GraphNode):
    name: str

@domain.node()
class Order(GraphNode):
    ref: str
""",
                encoding="utf-8",
            )

            loaded = mp.try_to_load_models(None, str(model_file))

            assert loaded is not None
            assert sorted(m.label_ for m in loaded) == ["Category", "Order"]

    def test_missing_file_raises(self, mp):
        with pytest.raises(FileNotFoundError, match="Path does not exist"):
            mp.try_to_load_models(None, "/tmp/nonexistent_model_file_12345.py")


class TestDirectoryLoading:
    """Loading a directory of loose .py files."""

    def test_load_directory_with_multiple_models(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_model(Path(tmpdir), "company.py", "\n@mp.node()\nclass Company(GraphNode):\n    name: str\n")
            write_model(Path(tmpdir), "employee.py", "\n@mp.node()\nclass Employee(GraphNode):\n    name: str\n")
            write_model(Path(tmpdir), "works_at.py", "\n@mp.edge()\nclass WORKS_AT(GraphEdge):\n    since: int\n")

            loaded = mp.try_to_load_models(None, tmpdir)

            assert loaded is not None
            assert sorted(m.label_ for m in loaded) == ["Company", "Employee", "WORKS_AT"]
            assert all(m.source == "model" for m in loaded)

    def test_nodes_are_sorted_before_edges(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_model(Path(tmpdir), "a_edge.py", "\n@mp.edge()\nclass RELATED_TO(GraphEdge):\n    type: str\n")
            write_model(Path(tmpdir), "z_node.py", "\n@mp.node()\nclass Thing(GraphNode):\n    name: str\n")

            loaded = mp.try_to_load_models(None, tmpdir)

            assert loaded is not None
            labels = [m.label_ for m in loaded]
            assert labels.index("Thing") < labels.index("RELATED_TO")

    def test_empty_directory_returns_none(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert mp.try_to_load_models(None, tmpdir) is None

    def test_non_python_files_are_skipped(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_model(Path(tmpdir), "model.py", "\n@mp.node()\nclass RealModel(GraphNode):\n    val: str\n")
            (Path(tmpdir) / "README.md").write_text("# Readme", encoding="utf-8")
            (Path(tmpdir) / "config.json").write_text("{}", encoding="utf-8")

            loaded = mp.try_to_load_models(None, tmpdir)

            assert loaded is not None
            assert [m.label_ for m in loaded] == ["RealModel"]

    def test_directory_with_broken_file_still_loads_the_rest(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "bad.py").write_text("this is invalid python <<<", encoding="utf-8")
            write_model(Path(tmpdir), "good.py", "\n@mp.node()\nclass ValidModel(GraphNode):\n    value: str\n")

            loaded = mp.try_to_load_models(None, tmpdir)

            assert loaded is not None
            assert "ValidModel" in [m.label_ for m in loaded]

    def test_all_files_in_a_directory_are_discovered(self, mp):
        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(5):
                write_model(Path(tmpdir), f"model{i}.py", f"\n@mp.node()\nclass Model{i}(GraphNode):\n    value: int\n")

            loaded = mp.try_to_load_models(None, tmpdir)

            assert loaded is not None
            assert sorted(m.label_ for m in loaded) == [f"Model{i}" for i in range(5)]


class TestPackageLoading:
    """Loading a directory that is a Python package (has __init__.py)."""

    def test_providers_shadowed_by_star_imports_are_still_found(self, mp):
        """``from .a import *`` shadows ``mp``; submodules must still be scanned."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg = Path(tmpdir) / "graph_models"
            pkg.mkdir()
            (pkg / "__init__.py").write_text("from .part_a import *\nfrom .part_b import *\n", encoding="utf-8")
            write_model(pkg, "part_a.py", "\n@mp.node()\nclass Beta(GraphNode):\n    x: str\n")
            write_model(pkg, "part_b.py", "\n@mp.node()\nclass Gamma(GraphNode):\n    y: str\n")

            loaded = mp.try_to_load_models(None, str(pkg))

            assert loaded is not None
            assert sorted(m.label_ for m in loaded) == ["Beta", "Gamma"]

    def test_same_named_package_in_a_different_directory_is_not_reused(self, mp):
        """A stale same-named package in sys.modules must not shadow a new path."""
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            pkg_a = Path(first) / "graph_models"
            pkg_a.mkdir()
            (pkg_a / "__init__.py").write_text("from .m import *\n", encoding="utf-8")
            write_model(pkg_a, "m.py", "\n@mp.node()\nclass FromFirst(GraphNode):\n    a: str\n")

            pkg_b = Path(second) / "graph_models"
            pkg_b.mkdir()
            (pkg_b / "__init__.py").write_text("from .m import *\n", encoding="utf-8")
            write_model(pkg_b, "m.py", "\n@mp.node()\nclass FromSecond(GraphNode):\n    b: str\n")

            first_provider = ModelProvider()
            first_loaded = first_provider.try_to_load_models(None, str(pkg_a))
            assert first_loaded is not None
            assert [m.label_ for m in first_loaded] == ["FromFirst"]

            second_loaded = mp.try_to_load_models(None, str(pkg_b))
            assert second_loaded is not None
            assert [m.label_ for m in second_loaded] == ["FromSecond"]


class TestIsolation:
    """Loading never leaks between independent providers."""

    def test_two_providers_loading_the_same_label_stay_independent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = write_model(
                Path(tmpdir),
                "shared.py",
                "\n@mp.node()\nclass Product(GraphNode):\n    key: str\n",
            )

            a, b = ModelProvider(), ModelProvider()
            a.try_to_load_models(None, str(model_file))

            assert "Product" in a
            assert "Product" not in b, "loading into one provider must not touch another"

    def test_loading_is_idempotent_across_the_import_cache(self, mp):
        """A second load of an already-imported package still yields its models."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg = Path(tmpdir) / "cached_models"
            pkg.mkdir()
            (pkg / "__init__.py").write_text("from .m import *\n", encoding="utf-8")
            write_model(pkg, "m.py", "\n@mp.node()\nclass Cached(GraphNode):\n    v: str\n")

            first = ModelProvider().try_to_load_models(None, str(pkg))
            second = mp.try_to_load_models(None, str(pkg))

            assert first is not None and [m.label_ for m in first] == ["Cached"]
            assert second is not None and [m.label_ for m in second] == ["Cached"]
