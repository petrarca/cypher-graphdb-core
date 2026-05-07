"""AGE SQL builder module: Generate SQL queries for Apache AGE operations.

This module provides the SQLBuilder class for generating SQL queries that
interface with Apache AGE graph functionality, including graph creation,
Cypher query execution, and full-text search operations.
"""

from psycopg import sql

from cypher_graphdb.cypherparser import ParsedCypherQuery


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
    def graph_exists(cls, graph_name: str) -> sql.SQL:
        return sql.SQL("SELECT count(*) FROM ag_graph WHERE name={graph_name}").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def create_graph(cls, graph_name: str) -> sql.SQL:
        return sql.SQL("SELECT create_graph({graph_name});").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def drop_graph(cls, graph_name: str) -> sql.SQL:
        return sql.SQL("SELECT drop_graph({graph_name}, true);").format(graph_name=sql.Literal(graph_name))

    @classmethod
    def create_vlabel(cls, graph_name: str, vlabel: str) -> sql.SQL:
        return sql.SQL("SELECT create_vlabel({graph_name}, {vlabel});").format(
            graph_name=sql.Literal(graph_name), vlabel=sql.Literal(vlabel)
        )

    @classmethod
    def create_elabel(cls, graph_name: str, elabel: str) -> sql.SQL:
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
    def create_fts_sql(cls, graph_name: str, cypher_query: ParsedCypherQuery, fts_query: str, language: str) -> sql.SQL:
        sql_, _ = cls.create_cypher_sql(graph_name, cypher_query)

        where_parts = []
        for k, v in cypher_query.return_arguments.items():
            if v.startswith("ag_catalog.agtype_out"):
                where_parts.append(f"to_tsvector('{language}',{k}) @@ to_tsquery('{fts_query}')")

        where_parts = " OR ".join(where_parts)

        sql_ = sql_ + sql.SQL(f"WHERE {where_parts}")

        return sql_

    @classmethod
    def resolve_graphs(cls) -> sql.SQL:
        return sql.SQL("SELECT name FROM ag_catalog.ag_graph")

    @classmethod
    def resolve_labels(cls, graph_name: str) -> sql.SQL:
        sql_ = sql.SQL("SELECT name, kind, relation FROM ag_catalog.ag_label")
        if graph_name:
            sql_ = sql_ + sql.SQL(f" WHERE relation::text like '{graph_name}.%'")

        return sql_

    @classmethod
    def resolve_label_count(cls, relation: str) -> sql.SQL:
        return sql.SQL(f"SELECT count(*) FROM {relation}")

    # ── Index management SQL ──────────────────────────────────────────────

    @classmethod
    def get_label_tables(cls, graph_name: str) -> sql.SQL:
        """List all label tables in the graph schema via pg_class."""
        return sql.SQL(
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = {graph_name} AND c.relkind = 'r'"
        ).format(graph_name=sql.Placeholder())

    @classmethod
    def create_gin_index(cls, graph_name: str, label: str) -> sql.SQL:
        """Create a GIN index on the properties column of a label table."""
        idx_name = f"{graph_name}_{label}_props_gin"
        return sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {schema}.{table} USING gin(properties)").format(
            idx=sql.Identifier(idx_name),
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
        )

    @classmethod
    def create_expression_index(cls, graph_name: str, label: str, property_name: str) -> sql.SQL:
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
    def drop_gin_index(cls, graph_name: str, label: str) -> sql.SQL:
        """Drop a GIN index on the properties column of a label table."""
        idx_name = f"{graph_name}_{label}_props_gin"
        return sql.SQL("DROP INDEX IF EXISTS {schema}.{idx}").format(
            schema=sql.Identifier(graph_name),
            idx=sql.Identifier(idx_name),
        )

    @classmethod
    def drop_expression_index(cls, graph_name: str, label: str, property_name: str) -> sql.SQL:
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

    @classmethod
    def lookup_node_graphids_sql(cls, graph_name: str, label: str, ref_prop: str) -> sql.SQL:
        """SQL to look up graphids of all nodes of a label by a property value.

        Returns (ref_value, graphid) pairs for every row in the label table.
        Use ``lookup_node_graphids_filtered_sql`` when only a subset of values
        is needed (avoids scanning hundreds of thousands of rows).
        """
        return sql.SQL(
            "SELECT ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, {prop}::ag_catalog.agtype])::text, id "
            "FROM {schema}.{table}"
        ).format(
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
            prop=sql.Literal(f'"{ref_prop}"'),
        )

    @classmethod
    def lookup_node_graphids_filtered_sql(cls, graph_name: str, label: str, ref_prop: str, ref_values: set[str]) -> sql.SQL:
        """SQL to look up graphids for a specific set of property values.

        Uses a WHERE ... IN (...) clause with the same agtype_access_operator
        expression as the expression index, so the btree index is used for
        the filter. For 500 ref values against a 200K+ row table this turns
        a sequential scan into 500 index lookups.

        The ref values are cast to ``::ag_catalog.agtype`` to match the
        expression index type exactly.
        """
        # Build the IN list: each value is a double-quoted agtype string literal
        in_values = ", ".join(sql.Literal(f'"{v}"').as_string(None) + "::ag_catalog.agtype" for v in ref_values)
        return sql.SQL(
            "SELECT ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, {prop}::ag_catalog.agtype])::text, id "
            "FROM {schema}.{table} "
            "WHERE ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, {prop}::ag_catalog.agtype]) IN ({in_vals})"
        ).format(
            schema=sql.Identifier(graph_name),
            table=sql.Identifier(label),
            prop=sql.Literal(f'"{ref_prop}"'),
            in_vals=sql.SQL(in_values),
        )

    @classmethod
    def list_indexes(cls, graph_name: str) -> sql.SQL:
        """List all user-created indexes in the graph schema via pg_indexes."""
        return sql.SQL(
            "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = {graph_name} ORDER BY tablename, indexname"
        ).format(graph_name=sql.Placeholder())
