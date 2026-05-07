"""Direct SQL bulk writer for Apache AGE label tables.

Bypasses the Cypher parser by INSERTing directly into AGE's PostgreSQL label
tables using _graphid() + nextval() for ID generation and ::agtype for
properties. This is the same pattern AGE's own test suite uses
(regress/sql/age_global_graph.sql) and is 10-30x faster than Cypher UNWIND
for large bulk loads.

The writer is stateless: it receives a connection and graph_name, performs
the INSERT, and returns. Transaction control (commit/rollback) is the
caller's responsibility.
"""

import psycopg
from loguru import logger
from psycopg.sql import SQL, Identifier

from cypher_graphdb.utils import chunk_list

from .ageserializer import escape_value
from .agesqlbuilder import SQLBuilder


class AGEBulkWriter:
    """Direct SQL bulk writer for AGE label tables.

    Args:
        connection: An open psycopg connection to the AGE database.
        graph_name: The AGE graph name (= PostgreSQL schema name).
    """

    def __init__(self, connection: psycopg.Connection, graph_name: str) -> None:
        self._conn = connection
        self._graph_name = graph_name

    # -- Public API -----------------------------------------------------------

    def bulk_insert_nodes(self, label: str, rows: list[dict], batch_size: int = 200) -> int:
        """Insert nodes via direct SQL INSERT into the AGE label table.

        Creates the vlabel if it doesn't exist yet (AGE normally does this
        lazily on first Cypher CREATE).

        Uses multi-row INSERT with inline agtype literals (not parameterized)
        because AGE's agtype_in text-input function does not interpret JSON
        escape sequences correctly when received as psycopg text parameters.
        The agtype values are built using the same escape_value function as
        the Cypher UNWIND path, ensuring identical storage.

        Args:
            label: Vertex label name (e.g. "Method").
            rows: List of property dicts, one per node.
            batch_size: Number of rows per multi-row INSERT.

        Returns:
            Total number of nodes inserted.
        """
        if not rows:
            return 0

        label_id = self._ensure_label(label, kind="v")

        total = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(rows, batch_size):
                values = ", ".join(
                    f"(ag_catalog._graphid({label_id}, nextval('{self._seq_name(label)}'::regclass)), "
                    f"{self._to_agtype_literal(row)}::ag_catalog.agtype)"
                    for row in batch
                )
                cursor.execute(
                    SQL("INSERT INTO {schema}.{table} (id, properties) VALUES {vals}").format(
                        schema=Identifier(self._graph_name),
                        table=Identifier(label),
                        vals=SQL(values),
                    )
                )
                total += len(batch)

        logger.debug("Direct SQL: {} {} nodes inserted", total, label)
        return total

    def bulk_insert_edges(
        self,
        label: str,
        edges: list[dict],
        src_label: str,
        dst_label: str,
        src_ref_prop: str = "id",
        dst_ref_prop: str = "id",
        batch_size: int = 500,
    ) -> int:
        """Insert edges via direct SQL INSERT into the AGE edge label table.

        Pre-resolves source and destination graphids via a single SQL query
        per label, then INSERTs edges with known start_id/end_id -- no Cypher
        MATCH at write time.

        Edges whose src or dst reference cannot be resolved are silently
        skipped (same behaviour as Cypher MATCH -- unmatched patterns produce
        no rows).

        Args:
            label: Edge label name (e.g. "CALLS").
            edges: List of dicts with at least "src" and "dst" keys.
            src_label: Vertex label of source nodes.
            dst_label: Vertex label of destination nodes.
            src_ref_prop: Property name on source nodes to match "src" values.
            dst_ref_prop: Property name on destination nodes to match "dst" values.
            batch_size: Number of rows per executemany batch.

        Returns:
            Total number of edges inserted (excludes skipped).
        """
        if not edges:
            return 0

        label_id = self._ensure_label(label, kind="e")

        # Build {ref_value: graphid} maps for source and destination nodes
        src_map = self._build_graphid_index(src_label, src_ref_prop)
        if src_label == dst_label and src_ref_prop == dst_ref_prop:
            dst_map = src_map
        else:
            dst_map = self._build_graphid_index(dst_label, dst_ref_prop)

        total = 0
        skipped = 0
        with self._conn.cursor() as cursor:
            for batch in chunk_list(edges, batch_size):
                value_parts = []
                for edge in batch:
                    src_gid = src_map.get(edge["src"])
                    dst_gid = dst_map.get(edge["dst"])
                    if src_gid is None or dst_gid is None:
                        skipped += 1
                        continue
                    props = {k: v for k, v in edge.items() if k not in ("src", "dst")}
                    value_parts.append(
                        f"(ag_catalog._graphid({label_id}, nextval('{self._seq_name(label)}'::regclass)), "
                        f"'{src_gid}'::ag_catalog.graphid, '{dst_gid}'::ag_catalog.graphid, "
                        f"{self._to_agtype_literal(props)}::ag_catalog.agtype)"
                    )
                if value_parts:
                    values = ", ".join(value_parts)
                    cursor.execute(
                        SQL("INSERT INTO {schema}.{table} (id, start_id, end_id, properties) VALUES {vals}").format(
                            schema=Identifier(self._graph_name),
                            table=Identifier(label),
                            vals=SQL(values),
                        )
                    )
                    total += len(value_parts)

        if skipped:
            logger.debug("Direct SQL: {} {} edges inserted, {} skipped (unresolved endpoints)", total, label, skipped)
        else:
            logger.debug("Direct SQL: {} {} edges inserted", total, label)
        return total

    # -- Internal helpers -----------------------------------------------------

    def _ensure_label(self, label: str, kind: str = "v") -> int:
        """Look up the numeric label_id, creating the label if needed.

        Args:
            label: Label name (e.g. "Method", "CALLS").
            kind: 'v' for vertex, 'e' for edge.

        Returns:
            The integer label_id for the label.
        """
        lookup_sql = SQLBuilder.lookup_label_id_sql(self._graph_name)
        with self._conn.cursor() as cursor:
            cursor.execute(lookup_sql, (self._graph_name, label))
            row = cursor.fetchone()
            if row:
                return row[0]

            # Label doesn't exist -- create it (AGE normally does this lazily via Cypher CREATE)
            if kind == "v":
                cursor.execute(SQLBuilder.create_vlabel(self._graph_name, label))
            else:
                cursor.execute(SQLBuilder.create_elabel(self._graph_name, label))
            self._conn.commit()

            cursor.execute(lookup_sql, (self._graph_name, label))
            row = cursor.fetchone()

        if not row:
            raise ValueError(f"Failed to create label '{label}' in graph '{self._graph_name}'")
        return row[0]

    def _seq_name(self, label: str) -> str:
        """Return the sequence name for a label's entry IDs."""
        return f'{self._graph_name}."{label}_id_seq"'

    @staticmethod
    def _to_agtype_literal(props: dict) -> str:
        """Serialize a dict to a PostgreSQL string literal for ``::agtype`` cast.

        The agtype text-input parser interprets ``\\"`` as an escaped double
        quote inside strings (same as JSON). When the literal is embedded in a
        standard PostgreSQL ``'...'`` string, backslashes are passed through
        verbatim — so the ``\\"`` from ``escape_value`` reaches the agtype
        parser intact. This produces identical stored values to the Cypher
        UNWIND path.

        Single quotes inside values are escaped as ``''`` (PostgreSQL standard).

        Note: standard ``'...'`` strings (not ``E'...'``) are used intentionally.
        ``E'...'`` would consume the ``\\`` escape layer before agtype sees it,
        breaking ``\\"`` → ``"`` prematurely.
        """
        parts = ", ".join(f"{escape_value(k)}: {escape_value(v)}" for k, v in props.items())
        inner = "{" + parts + "}"
        # Escape single quotes for PostgreSQL standard string literal
        inner = inner.replace("'", "''")
        return "'" + inner + "'"

    def _build_graphid_index(self, label: str, ref_prop: str) -> dict[str, int]:
        """Load a {ref_prop_value: graphid} mapping for all nodes of a label.

        Values returned by agtype_access_operator are agtype-quoted strings
        (e.g. '"mth:foo.bar"'), so we strip the surrounding quotes.
        """
        sql_stmt = SQLBuilder.lookup_node_graphids_sql(self._graph_name, label, ref_prop)
        with self._conn.cursor() as cursor:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
        return {str(r[0]).strip('"'): r[1] for r in rows}
