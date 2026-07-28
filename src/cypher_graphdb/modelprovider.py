"""modelprovider module: Registry and factory for typed graph model classes.

``ModelProvider`` is the single registration surface for typed graph models.
Models declare their provider explicitly::

    from cypher_graphdb import GraphNode, ModelProvider

    mp = ModelProvider()

    @mp.node()
    class Product(GraphNode):
        key: str

There is no global provider and no implicit registration: importing a model
module has no process-wide side effect, so two graphs can define the same label
without colliding. A provider is a plain value -- create it, pass it to a
connection or pool, and it travels with them.

Loading models from a path (file, package, or directory) works by *discovering*
the ``ModelProvider`` instances declared in the loaded modules and adopting
their models, so a module may declare several providers and load order never
matters.
"""

import collections.abc
import importlib
import importlib.util
import os
import pkgutil
import re
import sys
import threading
import traceback
from collections.abc import Callable
from types import ModuleType
from typing import Any, TypeVar

from loguru import logger

from . import config, modelrelations, utils
from .cardinality import Cardinality
from .display import DisplayConfig
from .modelinfo import GraphEdgeInfo, GraphModelInfo, GraphNodeInfo, GraphRelationInfo
from .models import GraphEdge, GraphNode, GraphObject, GraphObjectType

T = TypeVar("T")


class ModelProvider(collections.abc.Collection):
    """Registry and factory for typed graph model classes mapped by label."""

    def __init__(self, ctx: object = None) -> None:
        """Initialize an empty model registry.

        Args:
            ctx: Optional context object passed to callbacks.

        """
        self._models: GraphModelInfo = {}
        self._disabled = False
        self._lock = threading.RLock()

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
        with self._lock:
            self._models[modelinfo.label_] = modelinfo
            # Per-model registration is fine-grained internal mechanics that
            # fires for every @node/@edge class -- trace, not debug.
            logger.trace("Registered {} '{}' in provider {}", modelinfo.type_, modelinfo.label_, id(self))

    def adopt(self, other: ModelProvider, *, overwrite: bool = True) -> list[GraphModelInfo]:
        """Copy every model registered in ``other`` into this provider.

        Used to merge an independently declared provider (e.g. one discovered in
        a loaded module, or a shared set of infrastructure types) into this one.
        The model *classes* are shared, not copied -- only the label -> info
        mapping is extended.

        Args:
            other: Provider whose models should be adopted.
            overwrite: If False, labels already present here are left untouched.

        Returns:
            The model infos that were actually adopted.
        """
        adopted: list[GraphModelInfo] = []
        with self._lock:
            for info in other.values():
                if not overwrite and info.label_ in self._models:
                    continue
                self.register(info)
                adopted.append(info)
        return adopted

    def remove(self, modelinfo: GraphModelInfo) -> bool:
        """Remove a registered model and return True if it existed."""
        with self._lock:
            return bool(self._models.pop(modelinfo.label_, None))

    # -- registration decorators ---------------------------------------------

    def node(
        self,
        label: str | None = None,
        display: DisplayConfig | None = None,
        inherit_relations: bool = True,
        merge_keys: list[str] | None = None,
    ) -> Callable[[type[T]], type[T]]:
        """Register a ``GraphNode`` subclass with this provider (decorator).

        Usage::

            mp = ModelProvider()

            @mp.node()
            class Product(GraphNode):
                key: str

        Args:
            label: Graph label for this node type (defaults to the class name).
            display: Display configuration for UI rendering.
            inherit_relations: Whether to inherit relations from parent classes.
            merge_keys: Property names used as business keys for MERGE. When set,
                ``create_or_merge`` identifies existing nodes by these properties
                instead of ``gid_``. ``[]`` means MERGE on label alone
                (singleton); ``None`` keeps the standard ``gid_`` resolution.

        Returns:
            A class decorator that registers the node class.
        """

        def decorator(cls: type[T]) -> type[T]:
            if not issubclass(cls, GraphNode):
                raise RuntimeError(f"node() can only be applied to GraphNode models. {cls.__name__} is not.")

            label_ = cls.__name__ if label is None else label
            inherited = modelrelations.collect_inherited_relations(cls) if inherit_relations else []

            # ``relation()`` may have run first (decorators apply bottom-up) and
            # already created the info; reuse it so its relations survive.
            node_info = getattr(cls, "graph_info_", None)
            if node_info is not None:
                # Re-registering under a possibly different label: drop the old key.
                self.remove(node_info)
                node_info.label_ = label_
                node_info.display = display
                node_info.source = config.MODEL_SOURCE_MODEL
                node_info.merge_keys = merge_keys
                node_info.relations = inherited + node_info.relations
            else:
                node_info = GraphNodeInfo(
                    label_=label_,
                    graph_model=cls,
                    display=display,
                    source=config.MODEL_SOURCE_MODEL,
                    relations=inherited,
                    merge_keys=merge_keys,
                )
                cls.graph_info_ = node_info

            self.register(node_info)
            return cls

        return decorator

    def edge(
        self,
        label: str | None = None,
        display: DisplayConfig | None = None,
    ) -> Callable[[type[T]], type[T]]:
        """Register a ``GraphEdge`` subclass with this provider (decorator).

        Usage::

            @mp.edge(label="USES_TECHNOLOGY")
            class UsesTechnology(GraphEdge):
                version: str | None = None

        Args:
            label: Graph label for this edge type (defaults to the class name).
            display: Display configuration for UI rendering.

        Returns:
            A class decorator that registers the edge class.
        """

        def decorator(cls: type[T]) -> type[T]:
            if not issubclass(cls, GraphEdge):
                raise RuntimeError(f"edge() can only be applied to GraphEdge models. {cls.__name__} is not.")

            edge_info = GraphEdgeInfo(
                label_=cls.__name__ if label is None else label,
                graph_model=cls,
                display=display,
                source=config.MODEL_SOURCE_MODEL,
            )
            cls.graph_info_ = edge_info

            self.register(edge_info)
            return cls

        return decorator

    def relation(
        self,
        rel_type: type[GraphEdge] | str,
        to_type: type[GraphNode] | str,
        cardinality: Cardinality = Cardinality.ONE_TO_MANY,
        form_field: bool = False,
        description: str | None = None,
    ) -> Callable[[type[T]], type[T]]:
        """Declare an outbound relationship on a node type (decorator).

        Usage::

            @mp.node()
            @mp.relation(rel_type="CONTAINS", to_type="Module")
            class Source(GraphNode): ...

        Args:
            rel_type: Edge class or label string for the relationship type.
            to_type: Target node class or label string.
            cardinality: Relationship cardinality.
            form_field: Whether the relation appears as a form field.
            description: Optional description of the relationship.

        Returns:
            A class decorator that records the relation on the node's metadata.
        """

        def decorator(cls: type[T]) -> type[T]:
            if not issubclass(cls, GraphNode):
                raise RuntimeError(f"relation() can only be applied to GraphNode models. {cls.__name__} is not.")

            if getattr(cls, "graph_info_", None) is None:
                self.node()(cls)

            rel_info = GraphRelationInfo(
                rel_type_name=modelrelations.resolve_label(rel_type, "rel_type"),
                to_type_name=modelrelations.resolve_label(to_type, "to_type"),
                cardinality=cardinality,
                form_field=form_field,
                description=description,
            )

            if inherited := modelrelations.collect_inherited_relations(cls):
                conflicts = modelrelations.detect_relation_conflicts(inherited, [rel_info], cls.__name__)
                modelrelations.warn_relation_conflicts(conflicts, cls.__name__, stacklevel=4)

            cls.graph_info_.relations.append(rel_info)
            return cls

        return decorator

    def extend_relation(
        self,
        target: type[GraphNode] | str,
        rel_type: type[GraphEdge] | str,
        to_type: type[GraphNode] | str | None = None,
        cardinality: Cardinality = Cardinality.ONE_TO_MANY,
        form_field: bool = False,
        description: str | None = None,
    ) -> Callable[[type[T]], type[T]] | None:
        """Add a relation to an already-registered node type.

        Two calling styles:

        1. Function -- ``mp.extend_relation(Product, "HAS_STACK", TechStack)``
        2. Decorator -- ``@mp.extend_relation(Product, "HAS_STACK")`` on the
           target class, whose own class becomes ``to_type``.

        Args:
            target: The node type that gains the outbound relation.
            rel_type: Edge class or label string.
            to_type: Target node class/label. Omit when used as a decorator.
            cardinality: Relationship cardinality.
            form_field: Whether the relation appears as a form field.
            description: Optional description.

        Returns:
            ``None`` for function use, or the decorator for decorator use.
        """
        if to_type is not None:
            self._extend_relation(target, rel_type, to_type, cardinality, form_field, description)
            return None

        def decorator(cls: type[T]) -> type[T]:
            if getattr(cls, "graph_info_", None) is None:
                self.node()(cls)
            self._extend_relation(target, rel_type, cls, cardinality, form_field, description)
            return cls

        return decorator

    def extend_relations(
        self,
        target: type[GraphNode] | str,
        relations: list[GraphRelationInfo | modelrelations.RelationSpec],
    ) -> None:
        """Add multiple relations to an already-registered node type.

        Enables modular schema composition -- an extension adds relations to a
        base model without redefining the class::

            mp.extend_relations(Product, [
                {"rel_type": HasTechStack, "to_type": TechnologyStack},
                {"rel_type": "OWNED_BY", "to_type": Company,
                 "cardinality": Cardinality.ONE_TO_ONE},
            ])

        Args:
            target: Node class or label whose relations should be extended.
            relations: ``GraphRelationInfo`` objects or ``RelationSpec`` dicts.

        Raises:
            ValueError: If ``target`` is not a registered node in this provider.
        """
        node_info = self._require_node_info(modelrelations.resolve_label(target, "target"))

        for rel in relations:
            if isinstance(rel, GraphRelationInfo):
                modelrelations.add_relation_if_not_exists(
                    node_info,
                    rel.rel_type_name,
                    rel.to_type_name,
                    rel.cardinality,
                    rel.form_field,
                    rel.description,
                )
            else:
                modelrelations.add_relation_if_not_exists(
                    node_info,
                    modelrelations.resolve_label(rel["rel_type"], "rel_type"),
                    modelrelations.resolve_label(rel["to_type"], "to_type"),
                    rel.get("cardinality", Cardinality.ONE_TO_MANY),
                    rel.get("form_field", False),
                    rel.get("description"),
                )

    def _extend_relation(
        self,
        target: type[GraphNode] | str,
        rel_type: type[GraphEdge] | str,
        to_type: type[GraphNode] | str,
        cardinality: Cardinality,
        form_field: bool,
        description: str | None,
    ) -> None:
        """Resolve labels and append a single relation to a registered node."""
        node_info = self._require_node_info(modelrelations.resolve_label(target, "target"))
        modelrelations.add_relation_if_not_exists(
            node_info,
            modelrelations.resolve_label(rel_type, "rel_type"),
            modelrelations.resolve_label(to_type, "to_type"),
            cardinality,
            form_field,
            description,
        )

    def _require_node_info(self, label: str) -> GraphNodeInfo:
        """Return the registered ``GraphNodeInfo`` for ``label``, or raise."""
        info = self.get(label)
        if info is None:
            raise ValueError(f"'{label}' is not registered in this provider. Register the base model first.")
        if not isinstance(info, GraphNodeInfo):
            raise ValueError(f"'{label}' is not a node type (found {type(info).__name__}).")
        return info

    # -- lookup ---------------------------------------------------------------

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

    def create_node(self, label: str, props: dict[str, Any], id_: int | None = None) -> GraphNode:
        """Create a typed GraphNode instance using registered model class."""
        cls, field_values, properties = self.prepare_model_class(label, props, GraphNode)

        return cls(id_=id_, label_=label, properties_=properties, **field_values)

    def create_edge(self, label: str, start_id: int, end_id: int, props: dict[str, Any], id_: int | None = None) -> GraphEdge:
        """Create a typed GraphEdge instance using registered model class."""
        cls, field_values, properties = self.prepare_model_class(label, props, GraphEdge)

        return cls(id_=id_, label_=label, start_id_=start_id, end_id_=end_id, properties_=properties, **field_values)

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

    @staticmethod
    def discover_providers(module: ModuleType) -> list[ModelProvider]:
        """Find every ``ModelProvider`` declared in ``module`` and its submodules.

        Model modules declare their provider as a module-level value::

            mp = ModelProvider()

            @mp.node()
            class Product(GraphNode): ...

        Discovery reads that value back after import. It is therefore immune to
        Python's import cache: a module that was already imported (so its
        decorators will not fire again) still yields its provider.

        Packages are walked recursively, because a package's ``__init__.py``
        that does ``from .a import *`` / ``from .b import *`` leaves only the
        *last* ``mp`` visible at top level -- the submodules must be inspected to
        find them all. Providers are de-duplicated by identity.

        Args:
            module: The imported module or package to inspect.

        Returns:
            All distinct providers found, in discovery order.
        """
        found: list[ModelProvider] = []
        seen: set[int] = set()
        ModelProvider._discover_into(module, found, seen)
        return found

    @staticmethod
    def _discover_into(module: ModuleType, found: list[ModelProvider], seen: set[int]) -> None:
        """Collect providers from ``module``, recursing into a package's submodules."""
        for value in vars(module).values():
            if isinstance(value, ModelProvider) and id(value) not in seen:
                seen.add(id(value))
                found.append(value)

        # Packages only: walk submodules so shadowed providers are still found.
        if not hasattr(module, "__path__"):
            return

        for _, sub_name, _ in pkgutil.iter_modules(module.__path__):
            try:
                sub = importlib.import_module(f"{module.__name__}.{sub_name}")
            except Exception as e:  # noqa: BLE001 - a broken submodule must not abort discovery
                logger.debug("Could not import submodule {}.{}: {}", module.__name__, sub_name, e)
                continue
            ModelProvider._discover_into(sub, found, seen)

    def _adopt_discovered(self, module: ModuleType) -> list[str]:
        """Adopt all models from providers declared in ``module``.

        Returns:
            The labels adopted into this provider.
        """
        labels: list[str] = []
        for provider in self.discover_providers(module):
            if provider is self:
                continue
            labels.extend(info.label_ for info in self.adopt(provider))
        return labels

    @staticmethod
    def _log_load_failure(context: str, error: Exception) -> None:
        """Log why a module/package failed to load, including the root cause."""
        logger.debug("{} could not be loaded. Reason: {}", context, error)
        tb = traceback.extract_tb(error.__traceback__)
        if tb:
            root = tb[-1]
            logger.debug("File: {}", root.filename)
            logger.debug("Line: {}: {}", root.lineno, root.line)

    def _load_single_file(self, file_path: str, mod_name: str | None = None) -> tuple[bool, list[str]]:
        """Load a single Python module file and adopt its declared providers.

        Args:
            file_path: Path to the Python file to load.
            mod_name: Optional module name (defaults to the filename stem).

        Returns:
            Tuple of (success, list_of_adopted_model_labels).
        """
        if not mod_name:
            mod_name = (utils.split_path(file_path))[1]

        try:
            spec = importlib.util.spec_from_file_location(mod_name, file_path)
            if spec is None:
                logger.debug("Could not find module at file_path={}", file_path)
                return (False, [])

            module = importlib.util.module_from_spec(spec)
            # Register before exec so intra-module dataclass/pydantic lookups
            # and any relative self-import resolve against the same object.
            sys.modules[mod_name] = module
            spec.loader.exec_module(module)
            logger.debug("Successfully loaded module from {}", file_path)

            return (True, self._adopt_discovered(module))
        except Exception as e:  # noqa: BLE001 - surfaced to the caller as a load failure
            self._log_load_failure(f"Module {file_path}", e)
            return (False, [])

    @staticmethod
    def _package_matches(module: ModuleType, dir_path: str) -> bool:
        """Whether an already-imported package really lives at ``dir_path``."""
        paths = getattr(module, "__path__", None) or []
        target = os.path.realpath(dir_path)
        return any(os.path.realpath(p) == target for p in paths)

    @staticmethod
    def _purge_package(package_name: str) -> None:
        """Drop a package and all its submodules from ``sys.modules``."""
        prefix = f"{package_name}."
        for name in [n for n in sys.modules if n == package_name or n.startswith(prefix)]:
            sys.modules.pop(name, None)

    def _load_as_package(self, dir_path: str, package_name: str) -> tuple[bool, list[str]]:
        """Load a directory as a Python package and adopt its declared providers.

        Note: the caller must verify ``__init__.py`` exists before calling this.

        Unlike a plain import, an already-imported package is *not* reloaded --
        discovery reads the providers off the module object, so re-executing the
        decorators is unnecessary.

        Args:
            dir_path: Path to the package directory (must contain __init__.py).
            package_name: Name to use for the package.

        Returns:
            Tuple of (success, list_of_adopted_model_labels).
        """
        # Add the parent directory to sys.path so relative imports resolve.
        parent_dir = os.path.dirname(dir_path)
        path_added = False
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
            path_added = True

        try:
            cached = sys.modules.get(package_name)
            if cached is not None and not self._package_matches(cached, dir_path):
                # A different package of the same name is cached (common with
                # temp dirs and same-named model packages). Drop it so the
                # requested directory is the one actually imported.
                logger.debug("Cached package {} resolves elsewhere; re-importing from {}", package_name, dir_path)
                self._purge_package(package_name)
                cached = None

            module = cached or importlib.import_module(package_name)
            logger.debug("Successfully loaded package {} from {}", package_name, dir_path)
            return (True, self._adopt_discovered(module))
        except Exception as e:  # noqa: BLE001 - surfaced to the caller as a load failure
            self._log_load_failure(f"Package {package_name}", e)
            return (False, [])
        finally:
            if path_added and parent_dir in sys.path:
                sys.path.remove(parent_dir)

    def _load_from_directory(self, path: str) -> tuple[bool, dict[str, list[str]]]:
        """Load all Python files from a directory.

        If the directory contains an __init__.py, it will be loaded as a package
        to support relative imports between modules. Otherwise, files are loaded
        individually.

        Args:
            path: Directory path to load models from

        Returns:
            Tuple of (success, dict mapping file_path to list of model labels)
        """
        logger.debug(f"Loading all model files from directory: {path}")

        # If directory has __init__.py, try loading as a package first
        init_file = os.path.join(path, "__init__.py")
        if os.path.exists(init_file):
            package_name = os.path.basename(path)
            logger.debug(f"Found __init__.py, loading as package: {package_name}")
            success, newly_loaded = self._load_as_package(path, package_name)
            if success and newly_loaded:
                # Package loaded successfully and registered models
                file_to_models = {init_file: newly_loaded}
                return (True, file_to_models)
            if not newly_loaded:
                logger.debug("Package loaded but no models registered, falling back to individual file loading")
            else:
                logger.debug("Package loading failed, falling back to individual file loading")

        # Fall back to loading individual files
        py_files = sorted([f for f in os.listdir(path) if f.endswith(".py") and not f.startswith("__")])

        if not py_files:
            logger.debug(f"No Python files found in directory {path}")
            return (False, {})

        file_to_models: dict[str, list[str]] = {}
        loaded_count = 0
        for py_file in py_files:
            file_path = os.path.join(path, py_file)
            mod_name = os.path.splitext(py_file)[0]
            success, newly_loaded = self._load_single_file(file_path, mod_name)
            if success:
                loaded_count += 1
                if newly_loaded:
                    file_to_models[file_path] = newly_loaded

        if loaded_count == 0:
            logger.debug(f"No modules could be loaded from directory {path}")
            return (False, {})

        logger.debug(f"Successfully loaded {loaded_count} module(s) from {path}")
        return (True, file_to_models)

    def _update_source_and_collect(self, file_to_models: dict[str, list[str]]) -> list[GraphModelInfo]:
        """Update source property for loaded models and collect their info.

        Args:
            file_to_models: Mapping of file paths to model label lists

        Returns:
            List of GraphModelInfo objects sorted by label
        """
        loaded_model_infos: list[GraphModelInfo] = []
        for file_path, model_labels in file_to_models.items():
            # Only store filename (not full path) for security reasons
            os.path.basename(file_path)
            for label in model_labels:
                if model_info := self.get(label):
                    model_info.source = config.MODEL_SOURCE_MODEL
                    loaded_model_infos.append(model_info)

        # Sort by label names (nodes first, then edges)
        sorted_labels = self.sort_model_names([mi.label_ for mi in loaded_model_infos])
        sorted_infos = [next(mi for mi in loaded_model_infos if mi.label_ == label) for label in sorted_labels]

        return sorted_infos

    def try_to_load_models(self, module_name: str | None, path: str | None = None) -> list[GraphModelInfo] | None:
        """Attempt to load model classes from a Python file or directory.

        This method supports two modes:
        1. Single file: If path points to a .py file, only that file is loaded
        2. Directory: If path points to a directory, all .py files in that
           directory are loaded

        Models declare their own ``ModelProvider`` at module level; loading
        discovers those providers and adopts their models into this one.

        Args:
            module_name: Dotted module name to import (used if path not given).
            path: File path or directory path. If not provided, built from
                  module_name.

        Returns:
            List of loaded GraphModelInfo objects, or None if loading failed
            or no path provided.

        Examples:
            Load a single file:
            >>> provider.try_to_load_models(None, "./models/mymodel.py")

            Load all models from a directory:
            >>> provider.try_to_load_models(None, "./models")

            Load using module name:
            >>> provider.try_to_load_models("my.models.module")
        """
        if not module_name and not path:
            return None

        logger.debug(f"Try to load model {module_name=}, {path=}")
        logger.trace(f"cwd={os.getcwd()}")

        # Build path from module name if not provided
        if not path and module_name:
            path = f"./{module_name.replace('.', '/')}.py"

        # Validate path exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path does not exist: {path}")

        # Validate path is file or directory
        if not (os.path.isfile(path) or os.path.isdir(path)):
            raise ValueError(f"Path is neither a file nor a directory: {path}")

        # Resolve absolute path for source URI
        abs_path = os.path.abspath(path)

        with self._lock:
            # Track loading results
            file_to_models: dict[str, list[str]] = {}

            # Check if path is a file or directory
            if os.path.isfile(path):
                logger.debug(f"Loading single model file: {path}")
                mod_name = module_name or (utils.split_path(path))[1]
                success, newly_loaded = self._load_single_file(path, mod_name)
                if success and newly_loaded:
                    file_to_models[abs_path] = newly_loaded
            elif os.path.isdir(path):
                success, file_to_models_result = self._load_from_directory(path)
                if success:
                    # Convert relative paths to absolute
                    file_to_models = {os.path.abspath(fp): models for fp, models in file_to_models_result.items()}

            if not file_to_models:
                return None

            # Update source and collect model info
            sorted_model_infos = self._update_source_and_collect(file_to_models)

            return sorted_model_infos

    def generate_schemas_from_path(self, models_path: str, combine: bool = False) -> list[dict[str, Any]] | dict[str, Any]:
        """Generate JSON schemas from Python models at given path.

        Loads models from the specified path (file or directory) and extracts
        their JSON schemas. Optionally combines them into a single enriched schema.

        The loaded modules' own ``ModelProvider`` instances are discovered and
        adopted into this provider, so the result contains exactly the models
        declared under ``models_path`` -- never models belonging to another
        graph. Repeat calls with the same path are idempotent.

        Args:
            models_path: Path to Python file or directory with models
            combine: If True, return combined schema with $defs format.
                    If False (default), return list of individual schemas.

        Returns:
            List of individual JSON schemas, or combined schema if combine=True.
            Returns empty list/dict if no models found.

        Raises:
            FileNotFoundError: If models_path does not exist.
            ValueError: If models_path is neither a file nor a directory,
                       or if a model has no JSON schema.

        Examples:
            Generate individual schemas:
            >>> schemas = ModelProvider().generate_schemas_from_path("./models.py")
            >>> len(schemas)
            3

            Generate combined schema:
            >>> combined = ModelProvider().generate_schemas_from_path("./models/", combine=True)
            >>> combined["$defs"]["Product"]["title"]
            'Product'
        """
        from .utils.schema_utils import combine_schemas, extract_schemas_from_model_infos

        loaded_models = self.try_to_load_models(None, models_path)

        if not loaded_models:
            return {} if combine else []

        schemas = extract_schemas_from_model_infos(loaded_models)

        if combine:
            return combine_schemas(schemas)
        return schemas

    def model_dump(self, context: Any = None) -> dict[str, GraphModelInfo]:
        """Export all model info as a serializable dictionary."""
        model_names = self.sort_model_names(self._models.keys())
        return {
            model_name: utils.to_collection(self._models[model_name].model_dump(context=context)) for model_name in model_names
        }

    def disable(self):
        """Disable model resolution, falling back to default types."""
        with self._lock:
            self._disabled = True

    def enable(self):
        """Re-enable model resolution."""
        with self._lock:
            self._disabled = False

    def reset(self):
        """Remove all models without associated graph_model classes."""
        with self._lock:
            self._models = [item for item in self._models if item.graph_model is not None]

    def _parse_display_config(self, display_data: dict[str, Any] | None):
        """Parse display configuration from x-graph extension.

        Args:
            display_data: Display configuration dictionary from x-graph.

        Returns:
            DisplayConfig instance or None.
        """
        if display_data is None:
            return None

        from .display import DisplayConfig

        try:
            return DisplayConfig(**display_data)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to parse display config: {e}")
            return None

    def _extract_x_graph_metadata(self, schema: dict[str, Any]) -> tuple[str, str, list[GraphRelationInfo], Any]:
        """Extract metadata from x-graph extension in JSON schema.

        Args:
            schema: JSON schema dictionary with x-graph extension.

        Returns:
            Tuple of (label, graph_type, relations, display_config).

        Raises:
            ValueError: If required x-graph metadata is missing or invalid.
        """
        x_graph = schema.get("x-graph", {})

        if not x_graph:
            raise ValueError("Schema missing required 'x-graph' extension")

        # Extract graph type (NODE or EDGE)
        graph_type = x_graph.get("type")
        if graph_type not in ("NODE", "EDGE"):
            raise ValueError(f"Invalid or missing x-graph.type: {graph_type}. Must be 'NODE' or 'EDGE'")

        # Extract label (prefer x-graph.label, fallback to schema title)
        label = x_graph.get("label") or schema.get("title")
        if not label:
            raise ValueError("Schema missing both x-graph.label and title")

        # Extract relations (only for nodes)
        relations = []
        if graph_type == "NODE":
            for rel_data in x_graph.get("relations", []):
                try:
                    relations.append(GraphRelationInfo(**rel_data))
                except (TypeError, ValueError) as e:
                    logger.warning(f"Failed to parse relation {rel_data}: {e}")

        # Extract display config
        display = self._parse_display_config(x_graph.get("display"))

        return label, graph_type, relations, display

    def _create_typed_model_class(self, schema: dict[str, Any], label: str, graph_type: str) -> type[GraphNode | GraphEdge]:
        """Create a typed Pydantic model class from JSON schema.

        Args:
            schema: JSON schema dictionary (without x-graph extension).
            label: Model label to use as class name.
            graph_type: "NODE" or "EDGE".

        Returns:
            Generated Pydantic model class inheriting from GraphNode or GraphEdge.

        Raises:
            ImportError: If json-schema-to-pydantic is not installed.
            ValueError: If model generation fails.
        """
        try:
            from json_schema_to_pydantic import create_model
        except ImportError as e:
            raise ImportError(
                "json-schema-to-pydantic is required for loading models from JSON schemas. "
                "Install it with: pip install json-schema-to-pydantic"
            ) from e

        # Remove x-graph extension before passing to create_model
        clean_schema = {k: v for k, v in schema.items() if k != "x-graph"}

        try:
            # Generate base Pydantic model from schema
            base_model = create_model(clean_schema)
        except Exception as e:  # noqa: BLE001 - third-party library can raise various exceptions
            raise ValueError(f"Failed to generate Pydantic model from schema: {e}") from e

        # Create typed model class inheriting from GraphNode or GraphEdge
        if graph_type == "NODE":
            typed_model = type(label, (GraphNode, base_model), {})
        else:  # EDGE
            typed_model = type(label, (GraphEdge, base_model), {})

        return typed_model

    def clear_models_by_source(self, source: str | None) -> int:
        """Remove all models with matching source.

        Args:
            source: Source value to match ("model" for Python models,
                None for JSON schema models)

        Returns:
            Number of models removed
        """
        with self._lock:
            removed_count = 0
            labels_to_remove = [label for label, info in self._models.items() if info.source == source]
            for label in labels_to_remove:
                self._models.pop(label, None)
                removed_count += 1
            return removed_count

    def load_from_json_schemas(self, schemas: list[dict[str, Any]], replace_existing: bool = False) -> list[GraphModelInfo]:
        """Load models from JSON schemas with x-graph extensions.

        This method creates Pydantic model classes dynamically from JSON schemas
        and registers them with the model provider. The schemas must include
        x-graph extensions with metadata (type, label, relations, etc.).

        Models loaded from JSON schemas will have source=None (not set).

        Args:
            schemas: List of JSON Schema dictionaries with x-graph extensions.
                Each schema must have:
                - x-graph.type: "NODE" or "EDGE"
                - x-graph.label or title: Model label
                - Optional: x-graph.relations (for nodes)
                - Optional: x-graph.display
            replace_existing: If True, removes all existing models where
                source is None before loading new ones. Python models (source="model")
                are preserved. Useful for reloading scenarios.

        Returns:
            List of registered GraphModelInfo objects, sorted by label
            (nodes first, then edges).

        Raises:
            ValueError: If schema is invalid or missing required fields.
            ImportError: If json-schema-to-pydantic is not installed.

        Examples:
            Load a simple node model:
            >>> schemas = [{
            ...     "title": "Person",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "age": {"type": "integer"}
            ...     },
            ...     "required": ["name"],
            ...     "x-graph": {
            ...         "type": "NODE",
            ...         "label": "Person"
            ...     }
            ... }]
            >>> loaded = provider.load_from_json_schemas(schemas)
            >>> len(loaded)
            1

            Load models from server:
            >>> schemas = fetch_schemas_from_server()
            >>> provider.load_from_json_schemas(schemas, replace_existing=True)
        """
        if not schemas:
            logger.debug("No schemas provided to load")
            return []

        with self._lock:
            # Clear existing models from JSON schemas (source=None) if requested
            if replace_existing:
                removed_count = self.clear_models_by_source(None)
                if removed_count > 0:
                    logger.debug(f"Removed {removed_count} existing schema-based model(s)")

            loaded_models: list[GraphModelInfo] = []

            for schema in schemas:
                try:
                    # Extract x-graph metadata
                    label, graph_type, relations, display = self._extract_x_graph_metadata(schema)

                    # Check if model already exists. This is an expected,
                    # benign condition when re-loading schemas that overlap
                    # with already-registered types (e.g. metadata types
                    # pre-registered in every pool), so log at debug level.
                    if existing := self.get(label):
                        logger.debug(f"Model '{label}' already registered, skipping. Source: {existing.source}")
                        continue

                    # Create typed Pydantic model class
                    typed_model = self._create_typed_model_class(schema, label, graph_type)

                    # Create appropriate model info (source is not set for schema-based models)
                    if graph_type == "NODE":
                        model_info = GraphNodeInfo(
                            label_=label,
                            graph_model=typed_model,
                            relations=relations,
                            display=display,
                        )
                    else:  # EDGE
                        model_info = GraphEdgeInfo(
                            label_=label,
                            graph_model=typed_model,
                            display=display,
                        )

                    # Attach graph_info_ to the generated class
                    typed_model.graph_info_ = model_info

                    # Register with provider
                    self.register(model_info)
                    loaded_models.append(model_info)

                    logger.debug(f"Successfully loaded {graph_type} model '{label}' from schema")

                except (ValueError, ImportError) as e:  # Specific exceptions from schema loading
                    schema_title = schema.get("title", "unknown")
                    logger.error(f"Failed to load schema '{schema_title}': {e}")
                    # Continue processing other schemas
                    continue

            # Sort results (nodes first, then edges)
            if loaded_models:
                sorted_labels = self.sort_model_names([mi.label_ for mi in loaded_models])
                loaded_models = [next(mi for mi in loaded_models if mi.label_ == label) for label in sorted_labels]

            logger.info(f"Loaded {len(loaded_models)} model(s) from JSON schemas")
            return loaded_models

    def __contains__(self, x: object) -> bool:
        """Membership by label, or by a model info / model class.

        ``__iter__`` yields labels, so ``"Product" in provider`` is the natural
        spelling and is what the Collection contract implies. Passing a
        ``GraphModelInfo`` (or a registered model class) is also accepted.
        """
        if isinstance(x, str):
            return x in self._models
        label = getattr(x, "label_", None)
        if label is None:
            label = getattr(getattr(x, "graph_info_", None), "label_", None)
        return label in self._models if label is not None else False

    def __len__(self):
        return len(self._models)

    def __iter__(self):
        return iter(self._models)

    def __getitem__(self, key) -> GraphModelInfo:
        return self._models[key]

    @property
    def __dict__(self) -> dict[str, GraphModelInfo]:
        return self.model_dump()


def validate_node_properties(provider: ModelProvider, label: str, properties: dict) -> None:
    """Validate node properties against the registered ``@node`` model for ``label``.

    Instantiates the Pydantic model class registered for ``label`` in
    ``provider`` with the given ``properties``, so missing required fields and
    type errors are caught before the node is written to the graph.

    No-op when:
    - ``provider`` has no model registered for ``label`` (untyped nodes pass
      through unchecked).
    - The registered model class cannot be resolved (defensive).

    Args:
        provider: The ``ModelProvider`` to look up the model class from.
        label: Node label to validate against.
        properties: Raw property dict to validate.

    Raises:
        pydantic.ValidationError: If the properties are invalid for the
            registered model. The caller decides how to surface this error
            (e.g. HTTP 400, CLI message, or re-raise).
    """
    if provider.get(label) is None:
        return
    model_class = provider.get_model_class(label)
    if model_class is None:
        return
    model_class(**properties)
