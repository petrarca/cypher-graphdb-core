"""AGE SQL builder module: Generate SQL queries for Apache AGE operations.

This module provides the SQLBuilder class for generating SQL queries that
interface with Apache AGE graph functionality, including graph creation,
Cypher query execution, and full-text search operations.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import sql

from cypher_graphdb.cypherparser import ParsedCypherQuery

from .ageserializer import escape_string_inner


@dataclass(frozen=True)
class PageSql:
    """SQL pair produced by :meth:`SQLBuilder.create_cypher_page_sql`.

    Using a named dataclass makes the two members self-documenting at every
    call site instead of relying on positional tuple unpacking.
    """

    page: sql.Composed
    """Outer SELECT with OFFSET/LIMIT applied; preserves the agtype column spec."""
    count: sql.Composed
    """Outer SELECT count(*) for the exact total row count."""


class SQLBuilder:
    """SQL query builder for Apache AGE graph operations.

    Provides class methods for generating SQL queries that interact with
    Apache AGE's graph functionality, including graph management and
    Cypher query execution.
    """

    @classmethod
    def load_age(cls) -> sql.SQL:
        return sql.SQL("LOAD 'age';")

    @classmethod
    def set_search_path(cls) -> sql.SQL:
        return sql.SQL("SET search_path = ag_catalog, '$user', public;")

    @classmethod
    def graph_exists(cls, graph_name: str) -> sql.Composed:
        return sql.SQL("SELECT count(*) FROM ag_graph WHERE name={graph_name}").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def create_graph(cls, graph_name: str) -> sql.Composed:
        return sql.SQL("SELECT create_graph({graph_name});").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def drop_graph(cls, graph_name: str) -> sql.Composed:
        return sql.SQL("SELECT drop_graph({graph_name}, true);").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def create_vlabel(cls, graph_name: str, vlabel: str) -> sql.Composed:
        return sql.SQL("SELECT create_vlabel({graph_name}, {vlabel});").format(
            graph_name=sql.Literal(graph_name), vlabel=sql.Literal(vlabel)
        )

    @classmethod
    def create_elabel(cls, graph_name: str, elabel: str) -> sql.Composed:
        return sql.SQL("SELECT create_elabel({graph_name}, {elabel});").format(
            graph_name=sql.Literal(graph_name), elabel=sql.Literal(elabel)
        )

    @classmethod
    def create_cypher_sql(
        cls, graph_name: str, cypher_query: ParsedCypherQuery, params: dict | None = None
    ) -> tuple[sql.SQL, tuple | None]:
        """Create SQL for executing a Cypher query via AGE.

        Args:
            graph_name: Name of the graph.
            cypher_query: Parsed Cypher query.
            params: Optional dictionary of parameters.

        Returns:
            Tuple of (SQL statement, params tuple for psycopg execute).
            For prepared statements, returns SQL with $1 placeholder.
            For regular queries, returns SQL with %s placeholder and params as tuple.
        """
        assert isinstance(cypher_query, ParsedCypherQuery)

        return_arguments = {}
        columns = []
        for k, v in cypher_query.return_arguments.items():
            if v.startswith("ag_catalog.agtype_out"):
                rt = "text"
            else:
                rt = "agtype"
                # Quote column names to handle special characters like parentheses
                columns.append(sql.Identifier(k).as_string(None))
            return_arguments[k] = rt

        # select <columns>
        column_list = ",".join(columns)
        # return (...)
        # Quote column names in result types as well
        result_types = ",".join([f"{sql.Identifier(k).as_string(None)} {rt}" for k, rt in return_arguments.items()])

        # Handle queries without RETURN clause (CREATE, DELETE, MERGE, etc.)
        if not result_types:
            # Use a dummy column for queries that don't return data
            column_list = "result"
            result_types = "result agtype"

        # Strip trailing semicolon from Cypher query - AGE doesn't accept it in cypher() function
        cypher_clean = cypher_query.parsed_query.rstrip(";").strip()

        # Build cypher() function call with optional params.
        # Use a tagged dollar-quote ($age_cypher$...$age_cypher$) instead of bare
        # $$ so that property values containing $$ (e.g. file paths, Angular
        # identifiers) do not prematurely terminate the quote block.
        # The tag 'age_cypher' cannot appear in user data (source paths, class
        # names, field names) so it is safe as a delimiter.
        if params:
            cypher_call = f"cypher('{graph_name}', $age_cypher$ {cypher_clean} $age_cypher$, $1)"
        else:
            cypher_call = f"cypher('{graph_name}', $age_cypher$ {cypher_clean} $age_cypher$)"

        # TODO: select explicit fields from return argmentents: select p1, ..
        return (sql.SQL(f"SELECT {column_list} FROM {cypher_call} as ({result_types})"), None)

    @classmethod
    def create_cypher_page_sql(
        cls,
        graph_name: str,
        cypher_query: ParsedCypherQuery,
        offset: int,
        limit: int,
        params: dict | None = None,
    ) -> PageSql:
        """Build a :class:`PageSql` for windowed execution of a Cypher query.

        Wraps the inner ``SELECT ... FROM cypher(...)`` produced by
        :meth:`create_cypher_sql` in an outer SQL ``SELECT`` so we never edit
        the user's Cypher text:

        - ``PageSql.page`` applies ``OFFSET/LIMIT`` via ``sql.Literal`` and
          returns the same columns as the inner select (so the AGE agtype row
          factory is unaffected by the wrapping).
        - ``PageSql.count`` is ``SELECT count(*) FROM (<inner>) __count`` and
          returns a single plain integer (no agtype factory needed).

        ``OFFSET``/``LIMIT`` are embedded as ``sql.Literal`` values rather than
        raw f-string integers, keeping the composable safe and consistent with
        the rest of the builder.  The inner ``$1`` placeholder (when ``params``
        is provided) is preserved inside the subquery so prepared-statement
        parameter positions remain correct.

        The caller is responsible for only using this on queries that are safe
        to window (see :meth:`~cypher_graphdb.cypherparser.ParsedCypherQuery.is_safe_to_window`).

        Args:
            graph_name: Name of the graph.
            cypher_query: Parsed Cypher query (must have an explicit RETURN projection).
            offset: Zero-based row offset for the window.
            limit: Maximum rows in the window.
            params: Optional parameters (prepared-statement path uses $1).

        Returns:
            :class:`PageSql` with ``page`` and ``count`` composables.
        """
        inner_sql, _ = cls.create_cypher_sql(graph_name, cypher_query, params)
        page = sql.SQL("SELECT * FROM ({}) AS __page OFFSET {} LIMIT {}").format(
            inner_sql, sql.Literal(offset), sql.Literal(limit)
        )
        count = sql.SQL("SELECT count(*) FROM ({}) AS __count").format(inner_sql)
        return PageSql(page=page, count=count)

    @classmethod
    def create_fts_sql(cls, graph_name: str, cypher_query: ParsedCypherQuery, fts_query: str, language: str) -> sql.Composed:
        inner_sql, _ = cls.create_cypher_sql(graph_name, cypher_query)

        where_parts = []
        for k, v in cypher_query.return_arguments.items():
            if v.startswith("ag_catalog.agtype_out"):
                where_parts.append(f"to_tsvector('{language}',{k}) @@ to_tsquery('{fts_query}')")

        where_clause = " OR ".join(where_parts)

        return inner_sql + sql.SQL(f"WHERE {where_clause}")

    @classmethod
    def resolve_graphs(cls) -> sql.SQL:
        return sql.SQL("SELECT name FROM ag_catalog.ag_graph")

    @classmethod
    def resolve_labels(cls, graph_name: str) -> sql.SQL:
        query = "SELECT name, kind, relation FROM ag_catalog.ag_label"
        if graph_name:
            query += f" WHERE relation::text like '{graph_name}.%'"

        return sql.SQL(query)

    @classmethod
    def resolve_label_count(cls, relation: str) -> sql.SQL:
        return sql.SQL(f"SELECT count(*) FROM {relation}")

    # ── Index management SQL ──────────────────────────────────────────────

    @classmethod
    def get_label_tables(cls, graph_name: str) -> sql.Composed:
        """List all label tables in the graph schema via pg_class."""
        return sql.SQL(
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = {graph_name} AND c.relkind = 'r'"
        ).format(graph_name=sql.Placeholder())

    @classmethod
    def create_gin_index(cls, graph_name: str, label: str) -> sql.Composed:
        """Create a GIN index on the properties column of a label table."""
        idx_name = f"{graph_name}_{label}_props_gin"
        return sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {schema}.{table} USING gin(properties)").format(
            idx=sql.Identifier(idx_name),
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
        )

    @classmethod
    def create_expression_index(cls, graph_name: str, label: str, property_name: str) -> sql.Composed:
        """Create a btree expression index on a single property of a label table.

        AGE Cypher uses ``agtype_access_operator`` to extract property values.
        GIN indexes on the whole ``properties`` column do NOT accelerate these
        lookups. A btree expression index on the same access operator expression
        turns sequential scans into index scans (280x measured speedup).

        Args:
            graph_name: Graph name (used as schema).
            label: Node label (table name within the schema).
            property_name: Property name to index (e.g. "symbol", "qualified_name").
        """
        idx_name = f"{graph_name}_{label}_{property_name}_expr"
        # The expression must exactly match what AGE generates in its WHERE clause:
        #   agtype_access_operator(VARIADIC ARRAY[properties, '"prop_name"'::agtype])
        expr = f"""ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '"{property_name}"'::agtype])"""
        return sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {schema}.{table} USING btree ({expr})").format(
            idx=sql.Identifier(idx_name),
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
            expr=sql.SQL(expr),
        )

    @classmethod
    def drop_gin_index(cls, graph_name: str, label: str) -> sql.Composed:
        """Drop a GIN index on the properties column of a label table."""
        idx_name = f"{graph_name}_{label}_props_gin"
        return sql.SQL("DROP INDEX IF EXISTS {schema}.{idx}").format(
            schema=sql.Identifier(graph_name),
            idx=sql.Identifier(idx_name),
        )

    @classmethod
    def drop_expression_index(cls, graph_name: str, label: str, property_name: str) -> sql.Composed:
        """Drop a btree expression index on a single property of a label table."""
        idx_name = f"{graph_name}_{label}_{property_name}_expr"
        return sql.SQL("DROP INDEX IF EXISTS {schema}.{idx}").format(
            schema=sql.Identifier(graph_name),
            idx=sql.Identifier(idx_name),
        )

    # ── Direct SQL bulk insert (bypasses Cypher parser) ─────────────────

    @classmethod
    def lookup_label_id_sql(cls) -> sql.SQL:
        """SQL to look up the numeric label_id for a label name within a graph.

        Returns a parameterised query with two bind positions: (graph_name, label_name).
        """
        return sql.SQL(
            "SELECT l.id FROM ag_catalog.ag_label l "
            "JOIN ag_catalog.ag_graph g ON g.graphid = l.graph "
            "WHERE g.name = %s AND l.name = %s"
        )

    @staticmethod
    def _prop_access(prop: str) -> sql.Composed:
        """agtype_access_operator expression reading one property, matching the expression index."""
        return sql.SQL("ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, {prop}::ag_catalog.agtype])").format(
            prop=sql.Literal(f'"{prop}"'),
        )

    @classmethod
    def lookup_node_graphids_sql(cls, graph_name: str, label: str, ref_props: Sequence[str]) -> sql.Composed:
        """SQL to look up graphids of all nodes of a label by one or more property values.

        Returns ``(ref_value_0, ..., ref_value_n, graphid)`` for every row in the
        label table -- one value column per entry in ``ref_props``, in order, so
        the caller can build a composite key. Use
        ``lookup_node_graphids_filtered_sql`` when only a subset is needed
        (avoids scanning hundreds of thousands of rows).
        """
        value_cols = sql.SQL(", ").join(cls._prop_access(p) + sql.SQL("::text") for p in ref_props)
        return sql.SQL("SELECT {cols}, id FROM {schema}.{table}").format(
            cols=value_cols,
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
        )

    @classmethod
    def lookup_node_graphids_filtered_sql(
        cls, graph_name: str, label: str, ref_props: Sequence[str], ref_values: set
    ) -> sql.Composed:
        """SQL to look up graphids for a specific set of (composite) property values.

        ``ref_props`` is the ordered list of key properties; ``ref_values`` is a
        set of value tuples (one element per prop, same order).

        A single prop uses ``WHERE access IN (v, ...)`` -- the fast path that hits
        the expression index. A composite key uses ``WHERE (a0 = v0 AND a1 = v1)
        OR (...) OR ...``: AGE's agtype does not support SQL row-value comparison
        (``(a, b) IN ((x, y))`` raises "cannot cast agtype string to type
        boolean"), so the key is expanded into per-tuple AND groups.

        Values are cast to ``::ag_catalog.agtype`` to match the expression index
        type. Values containing double quotes or backslashes are escaped for the
        agtype JSON string format before being wrapped via sql.Literal -- identical
        escaping to the Cypher UNWIND path.
        """

        def agtype_literal(v: str) -> sql.SQL:
            return sql.SQL(sql.Literal('"' + escape_string_inner(v) + '"').as_string(None) + "::ag_catalog.agtype")

        value_cols = sql.SQL(", ").join(cls._prop_access(p) + sql.SQL("::text") for p in ref_props)

        # Match each key tuple as an AND of per-prop equalities, OR'd across tuples.
        # agtype equality must use the ag_catalog.= operator -- a plain "=" (or a
        # ::text cast) raises "cannot cast agtype string to type boolean". This
        # uniform form covers both the single-prop and composite cases.
        accessors = [cls._prop_access(p) for p in ref_props]
        groups = [
            sql.SQL("(")
            + sql.SQL(" AND ").join(
                sql.SQL("{a} OPERATOR(ag_catalog.=) {v}").format(a=accessors[i], v=agtype_literal(v))
                for i, v in enumerate(value_tuple)
            )
            + sql.SQL(")")
            for value_tuple in ref_values
        ]
        where = sql.SQL(" OR ").join(groups)

        return sql.SQL("SELECT {cols}, id FROM {schema}.{table} WHERE {where}").format(
            cols=value_cols,
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
            where=where,
        )

    @classmethod
    def list_indexes(cls, graph_name: str) -> sql.Composed:
        """List all user-created indexes in the graph schema via pg_indexes."""
        return sql.SQL(
            "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = {graph_name} ORDER BY tablename, indexname"
        ).format(graph_name=sql.Placeholder())
