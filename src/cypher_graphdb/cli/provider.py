"""CLI provider module: Provides data sources for CLI auto-completion.

This module defines provider interfaces and implementations for supplying
data to the CLI auto-completion system, including graph names, variables,
and configuration properties.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from cypher_graphdb import GraphObject, ModelProvider


class GraphDBProvider(ABC):
    """Abstract base class for graph database operations provider."""

    @abstractmethod
    def get_graphs(self) -> tuple[str]:
        pass


class GraphDataProvider(ABC):
    """Abstract base class for graph data access provider."""

    @abstractmethod
    def fetch_graph_obj(self, graph_obj_ref: str, graph_obj_type: str | None = None) -> GraphObject | None:
        pass


class VarProvider(ABC):
    """Abstract base class for variable management provider."""

    @abstractmethod
    def get_var(self, varname: str | None) -> dict:
        pass

    @abstractmethod
    def get_varnames(self) -> tuple[str]:
        pass

    @abstractmethod
    def set_var(self, name: str, value: object) -> dict:
        pass


class ConfigProvider(ABC):
    """Abstract base class for configuration provider."""

    @abstractmethod
    def get_props(self) -> tuple[str]:
        pass


@dataclass
class CLIProviders:
    """Container for all CLI service providers."""

    model_provider: ModelProvider
    var_provider: VarProvider
    config_provider: ConfigProvider
    graphdb_provider: GraphDBProvider
    graphdata_provider: GraphDataProvider
