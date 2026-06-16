"""Pagination mixin for CypherGraphDB.

Provides windowed (offset/limit) access to a query result with an optional
exact total count, mirroring the capability-based design of ``StreamMixin``.

Backends that declare ``BackendCapability.PAGINATION_SUPPORT`` provide a native
implementation (e.g. SQL ``OFFSET/LIMIT`` for AGE, Cypher ``SKIP/LIMIT`` for
Memgraph). All other backends transparently use a backend-agnostic
**cache-and-slice** fallback: the query is materialized once (bounded by a hard
cap), then pages are served by slicing the materialized result. Slicing one
materialized result guarantees a **stable order across pages** without
requiring the user to add ``ORDER BY``.
"""

import contextlib
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


class PaginationMixin:
    """Mixin adding windowed pagination to CypherGraphDB."""

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
            want_total: If True, attempt to report an exact total row count.
                When False (or when a backend cannot do so cheaply), only
                ``has_more`` is guaranteed.
            raw_data: If True, return raw rows without row-factory processing.
            params: Optional bound parameters (e.g. {"name": "Alice"}).
            max_rows: Hard cap on rows materialized by the fallback path.
                Ignored by backends with native pagination.

        Returns:
            A ``Page`` with the row window plus pagination metadata.

        Notes:
            The fallback path slices a single materialized result, so order is
            stable across pages even without an explicit ``ORDER BY``. Native
            backend implementations re-issue the query per page and therefore
            require a deterministic ``ORDER BY`` for stable paging.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if limit < 0:
            raise ValueError("limit must be >= 0")

        assert self._backend

        parsed_query = parse_cypher_query(cypher_query) if isinstance(cypher_query, str) else cypher_query

        supports_pagination = False
        with contextlib.suppress(NotImplementedError):
            supports_pagination = self._backend.get_capability(BackendCapability.PAGINATION_SUPPORT)

        if supports_pagination:
            return self._backend.execute_cypher_page(
                parsed_query,
                offset=offset,
                limit=limit,
                want_total=want_total,
                raw_data=raw_data,
                params=params,
            )

        return self._fallback_execute_cypher_page(
            parsed_query,
            offset=offset,
            limit=limit,
            raw_data=raw_data,
            params=params,
            max_rows=max_rows,
        )

    def _fallback_execute_cypher_page(
        self,
        parsed_query: ParsedCypherQuery,
        offset: int,
        limit: int,
        raw_data: bool,
        params: dict | None,
        max_rows: int,
    ) -> Page:
        """Cache-and-slice fallback for backends without native pagination.

        Materializes the result once (capped at ``max_rows``), then slices the
        requested window. Exact total = number of materialized rows (flagged
        ``truncated`` if the cap was hit).
        """
        warnings.warn(
            "Backend does not support native pagination. Using cache-and-slice fallback (full materialization).",
            UserWarning,
            stacklevel=3,
        )

        result, exec_stats = self._execute_on_backend(parsed_query, raw_data=raw_data, params=params)
        rows = list(result) if result else []

        truncated = False
        if max_rows is not None and len(rows) > max_rows:
            rows = rows[:max_rows]
            truncated = True

        total = len(rows)
        window = rows[offset : offset + limit]
        has_more = (offset + len(window)) < total

        col_names = self._resolve_page_col_names(parsed_query, result, exec_stats)

        return Page(
            rows=window,
            offset=offset,
            limit=limit,
            returned=len(window),
            total=total,
            has_more=has_more,
            col_names=col_names,
            truncated=truncated,
        )

    @staticmethod
    def _resolve_page_col_names(parsed_query: ParsedCypherQuery, result: Any, exec_stats: Any) -> dict[str, str] | None:
        """Resolve display column names for a page, if determinable."""
        if not parsed_query or not parsed_query.return_arguments:
            return None
        col_count = getattr(exec_stats, "col_count", 0) or 0
        return resolve_column_names(parsed_query.return_arguments, result, col_count)
