"""Pagination mixin for CypherGraphDB.

Provides windowed (offset/limit) access to a query result with an optional
exact total count, mirroring the capability-based design of ``StreamMixin``.

Backends that declare ``BackendCapability.PAGINATION_SUPPORT`` provide a native
implementation (e.g. outer-SQL ``OFFSET/LIMIT`` for AGE, Cypher ``SKIP/LIMIT``
for Memgraph). The ``is_safe_to_window`` guard further restricts native dispatch
to queries that can be safely windowed (explicit RETURN, no existing pagination
or UNION). All other cases use a backend-agnostic **cache-and-slice** fallback:
the query is materialized once (bounded by a hard cap), then pages are served
by slicing the materialized result. Slicing guarantees **stable order across
pages** without requiring the user to add ``ORDER BY``.

``want_total`` behaviour:
    - For backends that declare ``EXACT_COUNT`` (e.g. AGE), ``want_total=True``
      fetches an exact count; ``want_total=False`` skips it and relies on
      ``has_more`` alone.
    - For backends that do **not** declare ``EXACT_COUNT`` (e.g. Memgraph),
      the parameter is ignored and ``total`` is always ``None``.
    - The fallback always produces an exact ``total`` regardless of
      ``want_total`` (it already has the full result in memory).
    - Callers should treat ``total`` as a *hint*, not a guarantee.
"""

import warnings
from typing import Any

from ..backend import BackendCapability
from ..cypherparser import ParsedCypherQuery, parse_cypher_query
from ..utils.column_utils import resolve_column_names
from .pagination import Page

# Default hard cap on rows materialized by the fallback. Protects the server
# (and browser) from an accidentally unbounded query. The page's ``truncated``
# flag signals when this cap was hit.
DEFAULT_MAX_MATERIALIZED_ROWS = 50_000

# Reserved parameter names injected by the Memgraph native path for SKIP/LIMIT.
_RESERVED_PAGE_PARAMS = frozenset({"__cypher_page_skip__", "__cypher_page_limit__"})


class PaginationMixin:
    """Mixin adding windowed pagination to CypherGraphDB.

    Adds :meth:`execute_cypher_page` to the ``CypherGraphDB`` facade. Routing:

    - **Native path**: backend declares ``PAGINATION_SUPPORT`` *and* the query
      passes :meth:`~cypher_graphdb.cypherparser.ParsedCypherQuery.is_safe_to_window`.
    - **Fallback**: everything else — materializes once, slices, exact total.

    Both paths respect ``on_before_execute`` / ``on_after_execute`` hooks.
    """

    def execute_cypher_page(
        self,
        cypher_query: str | ParsedCypherQuery,
        offset: int = 0,
        limit: int = 100,
        want_total: bool = True,
        raw_data: bool = False,
        params: dict | None = None,
        max_rows: int = DEFAULT_MAX_MATERIALIZED_ROWS,
    ) -> Page:
        """Execute a Cypher query and return a single windowed ``Page`` of rows.

        Args:
            cypher_query: Cypher query string or pre-parsed ParsedCypherQuery.
            offset: Zero-based index of the first row to return.
            limit: Maximum number of rows to return in the window.
            want_total: Hint to attempt an exact total count. Honoured by
                backends that declare ``EXACT_COUNT``; silently ignored by
                others (``total`` will be ``None``). The fallback always
                provides an exact total.
            raw_data: If True, return raw rows without row-factory processing.
            params: Optional bound parameters (e.g. ``{"name": "Alice"}``).
                Must not contain the reserved keys ``__cypher_page_skip__`` or
                ``__cypher_page_limit__`` (used internally by the Memgraph path).
            max_rows: Hard cap on rows materialized by the fallback path.
                Queries that exceed this cap set ``Page.truncated = True``.
                Not used by backends with native pagination.

        Returns:
            A ``Page`` with the row window plus pagination metadata.

        Raises:
            ValueError: If ``offset < 0``, ``limit < 0``, or ``params``
                contains reserved pagination keys.

        Notes:
            The fallback slices a single materialized result, so order is
            stable across pages even without an explicit ``ORDER BY``. Native
            backend implementations re-issue the query per page and therefore
            require a deterministic ``ORDER BY`` for stable paging.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if limit < 0:
            raise ValueError("limit must be >= 0")
        if params and _RESERVED_PAGE_PARAMS & params.keys():
            raise ValueError(f"params must not contain reserved keys: {_RESERVED_PAGE_PARAMS}")

        assert self._backend

        parsed_query = parse_cypher_query(cypher_query) if isinstance(cypher_query, str) else cypher_query

        # Honour on_before_execute hook (e.g. read-only enforcement, audit).
        if not self._before_execute(parsed_query):
            # Cancelled by hook -- return an empty page rather than raising.
            return Page(rows=[], offset=offset, limit=limit, total=0, has_more=False)

        # Native dispatch: only when the backend supports it AND the query
        # shape is safe to window (no existing SKIP/LIMIT, no RETURN *, etc.)
        use_native = self._backend.has_capability(BackendCapability.PAGINATION_SUPPORT) and parsed_query.is_safe_to_window()

        if use_native:
            # Only pass want_total=True to backends that can actually honour it.
            # For backends without EXACT_COUNT (e.g. Memgraph), suppress the
            # hint so backends don't waste effort on a count they can't produce.
            effective_want_total = want_total and self._backend.get_capability(BackendCapability.EXACT_COUNT)
            page = self._backend.execute_cypher_page(
                parsed_query,
                offset=offset,
                limit=limit,
                want_total=effective_want_total,
                raw_data=raw_data,
                params=params,
            )
        else:
            page = self._fallback_execute_cypher_page(
                parsed_query,
                offset=offset,
                limit=limit,
                raw_data=raw_data,
                params=params,
                max_rows=max_rows,
            )

        self._after_execute(page.rows, parsed_query)
        return page

    def _fallback_execute_cypher_page(
        self,
        parsed_query: ParsedCypherQuery,
        offset: int,
        limit: int,
        raw_data: bool,
        params: dict | None,
        max_rows: int,
    ) -> Page:
        """Cache-and-slice fallback for queries that cannot use native pagination.

        Materializes the result once (capped at ``max_rows``), slices the
        requested window, and computes an exact total. Emits a ``UserWarning``
        so callers can distinguish native from fallback in tests/logging.
        """
        warnings.warn(
            "Backend does not support native pagination. Using cache-and-slice fallback (full materialization).",
            UserWarning,
            stacklevel=4,  # user → execute_cypher_page → _fallback → warnings.warn
        )

        result, exec_stats = self._execute_on_backend(parsed_query, raw_data=raw_data, params=params)
        rows = list(result) if result else []

        truncated = False
        if len(rows) > max_rows:
            rows = rows[:max_rows]
            truncated = True

        total = len(rows)
        window = rows[offset : offset + limit]
        has_more = (offset + len(window)) < total

        col_names = _resolve_col_names(parsed_query, result, exec_stats)

        return Page(
            rows=window,
            offset=offset,
            limit=limit,
            total=total,
            has_more=has_more,
            col_names=col_names,
            truncated=truncated,
        )

    @staticmethod
    def _resolve_page_col_names(
        parsed_query: ParsedCypherQuery,
        result: Any,
        exec_stats: Any,
    ) -> dict[str, str] | None:
        """Resolve display column names for a page.

        Backends delegate here instead of duplicating resolution logic.
        """
        return _resolve_col_names(parsed_query, result, exec_stats)


def _resolve_col_names(
    parsed_query: ParsedCypherQuery,
    result: Any,
    exec_stats: Any,
) -> dict[str, str] | None:
    """Module-level helper for col_names resolution, shared by mixin and backends."""
    if not parsed_query or not parsed_query.return_arguments:
        return None
    col_count = getattr(exec_stats, "col_count", 0) or 0
    return resolve_column_names(parsed_query.return_arguments, result, col_count)
