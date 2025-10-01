"""modelprovider module: Registry and factory for typed graph model classes.

Provides ModelProvider for registering, loading, and creating
GraphNode/GraphEdge instances from label-based class mappings.
"""

import collections.abc
import importlib.util
import os
import re
import traceback
from typing import Any, Final

from loguru import logger

from . import utils
from .modelinfo import GraphModelInfo
from .models import GraphEdge, GraphNode, GraphObject, GraphObjectType


class ModelProvider(collections.abc.Collection):
    """Registry and factory for typed graph model classes mapped by label."""

    def __init__(self, ctx: object = None) -> None:
        """Initialize an empty model registry.

        Args:
            ctx: Optional context object passed to callbacks.

        """
        self._models: GraphModelInfo = {}
        self._disabled = False

        # passed to callbacks
        self._ctx = ctx

    @classmethod
    def sort_model_names(cls, model_names: set[str] | list[str]) -> list[str]:
        """Sort model names with nodes first, then edges (ALL_CAPS pattern).

        Args:
            model_names: Collection of model label names.

        Returns:
            Sorted list with node names first, edge names second.

        """
        model_names = list(model_names)
        # match edges
        edge_names = [s for s in model_names if re.search(r"^[A-Z_]+$", s)]
        edge_names.sort()
        node_names = list(set(model_names) - set(edge_names))
        node_names.sort()

        return node_names + edge_names

    # Combine the sorted lists
    def register(self, modelinfo: GraphModelInfo):
        """Register a model class under its label."""
        self._models[modelinfo.label_] = modelinfo

    def remove(self, modelinfo: GraphModelInfo) -> bool:
        """Remove a registered model and return True if it existed."""
        return bool(self._models.pop(modelinfo.label_, None))

    def get(self, label: str) -> GraphModelInfo:
        """Get model info by label, returning None if not found."""
        return self._models.get(label, None)

    def keys(self) -> list[str]:
        """Return all registered model labels."""
        return self._models.keys()

    def values(self) -> list[GraphModelInfo]:
        """Return all registered model info objects."""
        return self._models.values()

    def items(self) -> dict[str, GraphModelInfo]:
        """Return label-to-modelinfo mapping."""
        return self._models.items()

    def get_model_class(self, label: str, default_type: GraphObject | GraphEdge = None) -> type[GraphObject]:
        """Get the model class for a label, falling back to default_type."""
        if self._disabled:
            return default_type

        if model_info := self.get(label):
            return model_info.graph_model or default_type

        return default_type

    def prepare_model_class(
        self, label: str, props: dict[str, Any], default_cls: type[GraphObject], strict: bool = False
    ) -> tuple[type[GraphObject], dict[str, Any], dict[str, Any]]:
        """Resolve model class and split properties into model fields vs. extra properties.

        Args:
            label: Graph object label to look up.
            props: Raw property dictionary.
            default_cls: Fallback class if no typed model found.
            strict: If True, raise error when no typed model exists.

        Returns:
            Tuple of (resolved_class, model_field_values, extra_properties).

        """
        if (cls := self.get_model_class(label)) is None and strict:
            raise RuntimeError(f"Missing typed class for label '{label}'")

        if (cls := cls or default_cls) is None:
            raise RuntimeError(f"Could not resolve class for label {label}")

        field_values, properties = utils.slice_model_properties(cls, props)

        return cls, field_values, properties

    def create_node(self, label: str, props: dict[str, Any], id: int | None = None) -> GraphNode:
        """Create a typed GraphNode instance using registered model class."""
        cls, field_values, properties = self.prepare_model_class(label, props, GraphNode)

        return cls(id_=id, label_=label, properties_=properties, **field_values)

    def create_edge(self, label: str, start_id: int, end_id: int, props: dict[str, Any], id: int | None = None) -> GraphEdge:
        """Create a typed GraphEdge instance using registered model class."""
        cls, field_values, properties = self.prepare_model_class(label, props, GraphEdge)

        return cls(id_=id, label_=label, start_id_=start_id, end_id_=end_id, properties_=properties, **field_values)

    def find(
        self, label: str, case_insenstive: bool = True, graph_objectype: GraphObjectType = None
    ) -> dict[str, GraphModelInfo]:
        """Find models with labels starting with the given prefix.

        Args:
            label: Label prefix to search for.
            case_insenstive: Whether to perform case-insensitive matching.
            graph_objectype: Optional filter by GraphObjectType (NODE/EDGE).

        Returns:
            Dictionary of matching label -> GraphModelInfo pairs.

        """
        result = {}

        if case_insenstive:
            label = label.upper()

        for lbl, modelinfo in self.items():
            if graph_objectype is not None and modelinfo.type_ != graph_objectype:
                continue

            if case_insenstive:
                lbl = lbl.upper()

            if lbl.startswith(label):
                result[modelinfo.label_] = modelinfo

        return result

    def _load_single_file(self, file_path: str, mod_name: str | None = None) -> bool:
        """Load a single Python module file.

        Args:
            file_path: Path to the Python file to load
            mod_name: Optional module name (defaults to filename without
                      extension)

        Returns:
            True if successful, False otherwise
        """
        if not mod_name:
            mod_name = (utils.split_path(file_path))[1]

        try:
            spec = importlib.util.spec_from_file_location(mod_name, file_path)
            if spec is None:
                logger.debug(f"Could not find module at {file_path=}")
                return False

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.debug(f"Successfully loaded module from {file_path}")
            return True
        # pylint: disable=W0718
        except Exception as e:
            logger.debug(f"Module could not be loaded. Reason {e}")
            tb = traceback.extract_tb(e.__traceback__)
            root_cause = tb[-1]
            logger.debug(f"File: {root_cause.filename}")
            logger.debug(f"Line: {root_cause.lineno}: {root_cause.line}")
            return False

    def _load_from_directory(self, path: str) -> bool:
        """Load all Python files from a directory.

        Args:
            path: Directory path to load models from

        Returns:
            True if at least one file was loaded successfully, False otherwise
        """
        logger.debug(f"Loading all model files from directory: {path}")
        py_files = sorted([f for f in os.listdir(path) if f.endswith(".py") and not f.startswith("__")])

        if not py_files:
            logger.debug(f"No Python files found in directory {path}")
            return False

        loaded_count = 0
        for py_file in py_files:
            file_path = os.path.join(path, py_file)
            mod_name = os.path.splitext(py_file)[0]
            if self._load_single_file(file_path, mod_name):
                loaded_count += 1

        if loaded_count == 0:
            logger.debug(f"No modules could be loaded from directory {path}")
            return False

        logger.debug(f"Successfully loaded {loaded_count} module(s) from {path}")
        return True

    def try_to_load_models(self, module_name: str | None, path: str | None = None) -> tuple[list[str] | None, str]:
        """Attempt to load model classes from a Python file or directory.

        This method supports two modes:
        1. Single file: If path points to a .py file, only that file is loaded
        2. Directory: If path points to a directory, all .py files in that
           directory are loaded

        Note: Models in the loaded files register with the model provider
        specified in their decorators (defaults to global model_provider).

        Args:
            module_name: Dotted module name to import (used if path not given).
            path: File path or directory path. If not provided, built from
                  module_name.

        Returns:
            Tuple of (newly_loaded_model_names, resolved_path).

        Examples:
            Load a single file:
            >>> provider.try_to_load_models(None, "./models/mymodel.py")

            Load all models from a directory:
            >>> provider.try_to_load_models(None, "./models")

            Load using module name:
            >>> provider.try_to_load_models("my.models.module")
        """
        if not module_name and not path:
            return (None, None)

        logger.debug(f"Try to load model {module_name=}, {path=}")
        logger.trace(f"cwd={os.getcwd()}")

        # Build path from module name if not provided
        if not path and module_name:
            path = f"./{module_name.replace('.', '/')}.py"

        # Track models before loading
        before = set(self._models.keys())
        success = False

        # Check if path is a file or directory
        if os.path.isfile(path):
            logger.debug(f"Loading single model file: {path}")
            mod_name = module_name or (utils.split_path(path))[1]
            success = self._load_single_file(path, mod_name)
        elif os.path.isdir(path):
            success = self._load_from_directory(path)
        else:
            logger.debug(f"Path does not exist or is invalid: {path}")
            return (None, path)

        if not success:
            return (None, path)

        # Return the newly loaded models
        newly_loaded = set(self._models.keys()) - before
        return (self.sort_model_names(newly_loaded), path)

    def model_dump(self, context: Any = None) -> dict[str, GraphModelInfo]:
        """Export all model info as a serializable dictionary."""
        model_names = self.sort_model_names(self._models.keys())
        return {
            model_name: utils.to_collection(self._models[model_name].model_dump(context=context)) for model_name in model_names
        }

    def disable(self):
        """Disable model resolution, falling back to default types."""
        self._disabled = True

    def enable(self):
        """Re-enable model resolution."""
        self._disabled = False

    def reset(self):
        """Remove all models without associated graph_model classes."""
        self._models = [item for item in self._models if item.graph_model is not None]

    def __contains__(self, x: object) -> bool:
        return x.label_ in self._models

    def __len__(self):
        return len(self._models)

    def __iter__(self):
        return iter(self._models)

    def __getitem__(self, key) -> GraphModelInfo:
        return self._models[key]

    @property
    def __dict__(self) -> dict[str, GraphModelInfo]:
        return self.model_dump()


# pylint: disable=C0103
model_provider: Final[ModelProvider] = ModelProvider()
