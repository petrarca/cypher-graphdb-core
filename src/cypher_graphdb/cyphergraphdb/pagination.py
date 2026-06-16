"""Pagination container for windowed Cypher query results.

Defines the immutable ``Page`` model returned by ``execute_cypher_page``. The
shape is driven by what a result-table visualization needs: a row window
(``offset``/``limit``/``returned``), an exact ``total`` when the backend can
provide one (else a definite ``has_more``), and ``col_names`` so headers can be
rendered without fetching the whole result.
"""

from typing import Any

from pydantic import BaseModel, Field

from ..models import TabularResult


class Page(BaseModel):
    """Immutable container for one window of a query result.

    Attributes:
        rows: The result rows for this window (already sliced to ``limit``).
        offset: Zero-based index of the first row in this window.
        limit: Maximum number of rows requested for this window.
        returned: Actual number of rows in ``rows`` (``<= limit``).
        total: Exact total row count for the full query, or ``None`` when the
            backend cannot determine it without extra cost (then rely on
            ``has_more``).
        has_more: True if rows exist beyond this window. Always known.
        col_names: Mapping of return argument -> display column name, when
            resolvable; otherwise ``None``.
        truncated: True if the underlying result was capped (e.g. by a
            materialization cap in the fallback) and ``total`` reflects the cap
            rather than the true total.
    """

    rows: TabularResult = Field(default_factory=list, description="Rows for this window")
    offset: int = Field(0, ge=0, description="Index of the first row in this window")
    limit: int = Field(..., ge=0, description="Maximum rows requested for this window")
    returned: int = Field(0, ge=0, description="Actual rows returned in this window")
    total: int | None = Field(None, description="Exact total row count, or None if unknown")
    has_more: bool = Field(False, description="Whether rows exist beyond this window")
    col_names: dict[str, str] | None = Field(None, description="Return argument -> column display name")
    truncated: bool = Field(False, description="Whether the result was capped before counting")

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    def is_empty(self) -> bool:
        """Return True if this window contains no rows."""
        return self.returned == 0

    def model_post_init(self, _context: Any) -> None:  # noqa: D401
        """Backfill ``returned`` from ``rows`` when not explicitly provided."""
        # Frozen models forbid normal attribute assignment; use object.__setattr__.
        if self.returned == 0 and self.rows:
            object.__setattr__(self, "returned", len(self.rows))
