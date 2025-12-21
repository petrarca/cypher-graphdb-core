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

        # Build cypher() function call with optional params
        # For prepared statements, use $1 placeholder for agtype parameter
        if params:
            cypher_call = f"cypher('{graph_name}', $$ {cypher_clean} $$, $1)"
        else:
            cypher_call = f"cypher('{graph_name}', $$ {cypher_clean} $$)"

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
