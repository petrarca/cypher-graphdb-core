"""Structural typing contract shared by CypherGraphDB's mixins.

Every mixin in this package (``ConnectionMixin``, ``BatchMixin``,
``SchemaMixin``, ...) is only ever combined with ``CypherGraphDB`` itself,
which is the class that actually provides ``_backend``, ``settings``,
``execute``, and the other members the mixin method bodies reference.

Because mixins don't inherit from ``CypherGraphDB`` (it's the other way
around), mypy has no way to know those members exist unless the mixin
declares the dependency explicitly. ``CypherGraphDBHost`` is that
declaration: each mixin module does

    if TYPE_CHECKING:
        from ._host import CypherGraphDBHost as _Host
    else:
        _Host = object

    class SomeMixin(_Host):
        ...

so that, for type-checking purposes only, ``self`` inside the mixin's
methods is known to provide this interface. At runtime ``_Host`` is plain
``object``, so this has zero effect on the MRO or behaviour.
"""

from typing import Any, Protocol

from ..backend import CypherBackend
from ..cypherparser import ParsedCypherQuery
from ..cypherquery import CypherQuery
from ..models import TabularResult
from ..settings import Settings


class CypherGraphDBHost(Protocol):
    """Members that ``CypherGraphDB`` provides to its mixins."""

    _backend: CypherBackend

    @property
    def settings(self) -> Settings: ...

    @property
    def read_only(self) -> bool: ...

    def execute(
        self,
        cypher_cmd: str | ParsedCypherQuery | CypherQuery,
        unnest_result: str | bool | None = None,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> Any: ...

    def _execute_on_backend(
        self,
        parsed_query: ParsedCypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> tuple[Any, Any]: ...

    def _before_execute(self, parsed_query: ParsedCypherQuery) -> bool: ...

    def _after_execute(self, result: Any, parsed_query: ParsedCypherQuery) -> None: ...

    def _parse_and_execute(
        self,
        cypher_cmd: str | ParsedCypherQuery | CypherQuery,
        fetch_one: bool = False,
        raw_data: bool = False,
        params: dict | None = None,
    ) -> TabularResult | None: ...
