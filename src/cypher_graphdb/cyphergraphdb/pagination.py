"""Pagination container for windowed Cypher query results.

Defines the immutable ``Page`` model returned by ``execute_cypher_page``. The
shape is driven by what a result-table visualization needs: a row window
(``offset``/``limit``/``returned``), an exact ``total`` when the backend can
provide one (else a definite ``has_more``), and ``col_names`` so headers can be
rendered without fetching the whole result.

API contract:
    - ``has_more`` is always known (never ``None``). It is authoritative for
      deciding whether to request the next page.
    - ``total`` is ``None`` when the backend cannot cheaply produce an exact
      count (e.g. Memgraph native path). Callers must treat it as a hint, not
      a guarantee.
    - ``want_total=True`` is a *hint* to the backend, not a guarantee: some
      backends (e.g. Memgraph) always return ``total=None`` regardless.
    - ``truncated=True`` means the underlying result was capped before counting,
      so ``total`` reflects the cap rather than the true unbounded total.
      ``truncated`` and ``total is None`` are mutually exclusive (enforced).
    - ``returned`` always equals ``len(rows)`` (enforced by model validator).
"""

from pydantic import BaseModel, Field, computed_field, model_validator

from ..models import TabularResult


class Page(BaseModel):
    """Immutable container for one window of a query result.

    Attributes:
        rows: The result rows for this window (already sliced to ``limit``).
        offset: Zero-based index of the first row in this window.
        limit: Maximum number of rows requested for this window.
        total: Exact total row count for the full query, or ``None`` when the
            backend cannot determine it without extra cost (rely on ``has_more``
            instead).  Populated by backends that declare ``EXACT_COUNT``.
        has_more: True if rows exist beyond this window. Always known and
            always exact — never an estimate.
        col_names: Mapping of return argument -> display column name, when
            resolvable; otherwise ``None``.
        truncated: True when the underlying result was capped (e.g. by the
            fallback's ``max_rows``). When ``True``, ``total`` reflects the cap
            and ``has_more`` may be misleadingly ``False`` for rows beyond it.
            ``truncated=True`` requires ``total`` to be set (not ``None``).
    """

    rows: TabularResult = Field(default_factory=list, description="Rows for this window")
    offset: int = Field(0, ge=0, description="Index of the first row in this window")
    limit: int = Field(..., ge=0, description="Maximum rows requested for this window")
    total: int | None = Field(None, description="Exact total row count, or None if unknown")
    has_more: bool = Field(False, description="Whether rows exist beyond this window")
    col_names: dict[str, str] | None = Field(None, description="Return argument -> column display name")
    truncated: bool = Field(False, description="Whether the result was capped before counting")

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    @computed_field  # type: ignore[prop-decorator]
    @property
    def returned(self) -> int:
        """Number of rows in this window. Always equals ``len(rows)``."""
        return len(self.rows)

    @model_validator(mode="after")
    def _check_invariants(self) -> Page:
        if self.truncated and self.total is None:
            raise ValueError("truncated=True requires total to be set (it reflects the cap value)")
        return self

    def is_empty(self) -> bool:
        """Return True if this window contains no rows."""
        # Defined as a plain method (not a computed_field) so Pydantic's
        # frozen-model __setattr__ guard does not interfere with attribute lookup.
        return len(self.rows) == 0
