"""Cypher query builder module: Generate Cypher queries for graph operations.

This module provides the CypherBuilder class for programmatically generating
parameterized Cypher queries for common graph database operations like creating,
merging, fetching, and deleting nodes and edges.

All methods that inline property values return a ``(query, params)`` tuple so
that the caller can pass parameters directly to the database driver, avoiding
Cypher injection and parse errors caused by special characters in values.

Methods that only reference graph IDs (integers) continue to return a plain
``str`` — IDs are always safe to inline.
"""


def _props_to_params(properties: dict, prefix: str = "p_") -> tuple[str, dict]:
    """Convert a properties dict to a parameterized Cypher fragment and a params dict.

    Returns:
        A tuple of (cypher_fragment, params) where cypher_fragment is a Cypher
        map literal using ``$param_key`` placeholders (e.g. ``{name: $p_name}``),
        and params maps each ``p_<key>`` to its value.

    Example:
        >>> _props_to_params({"name": "Alice", "age": 30})
        ('{name: $p_name, age: $p_age}', {'p_name': 'Alice', 'p_age': 30})
    """
    if not properties:
        return "{}", {}
    params = {f"{prefix}{k}": v for k, v in properties.items()}
    fragment = "{" + ", ".join(f"{k}: ${prefix}{k}" for k in properties) + "}"
    return fragment, params


def _set_to_params(properties: dict, node_alias: str, prefix: str = "p_") -> tuple[str, dict]:
    """Convert a properties dict to a parameterized SET clause and a params dict.

    Returns:
        A tuple of (set_clause, params) where set_clause is a comma-separated
        list of ``alias.key = $p_key`` assignments, and params maps each
        ``p_<key>`` to its value. Returns an empty string and empty dict if
        properties is empty.

    Example:
        >>> _set_to_params({"name": "Bob"}, "n")
        ('n.name = $p_name', {'p_name': 'Bob'})
    """
    if not properties:
        return "", {}
    params = {f"{prefix}{k}": v for k, v in properties.items()}
    clause = ", ".join(f"{node_alias}.{k} = ${prefix}{k}" for k in properties)
    return clause, params


class CypherBuilder:
    """Utility class for building parameterized Cypher query strings.

    Methods that inline property values return ``tuple[str, dict]``:
    ``(cypher_query, params)``. The caller is responsible for passing params
    to the database driver.

    Methods that only reference graph IDs (safe integers) return ``str``.
    """

    @classmethod
    def create_node(cls, label: str, properties: dict) -> tuple[str, dict]:
        """Build a parameterized CREATE node query.

        Returns:
            (query, params) tuple.

        Example:
            >>> query, params = CypherBuilder.create_node("Person", {"name": "Alice", "age": 30})
            >>> # query: 'CREATE (n:Person {name: $p_name, age: $p_age}) RETURN id(n)'
            >>> # params: {'p_name': 'Alice', 'p_age': 30}
        """
        props_fragment, params = _props_to_params(properties)
        return f"CREATE (n:{label} {props_fragment}) RETURN id(n)", params

    @classmethod
    def merge_node_by_id(cls, node_id: int, properties: dict) -> tuple[str, dict]:
        """Build a parameterized MATCH + SET node query by ID.

        Returns:
            (query, params) tuple.
        """
        set_clause, params = _set_to_params(properties, "n")
        query = f"MATCH (n) WHERE id(n) = {node_id} SET {set_clause} RETURN n"
        return query, params

    @classmethod
    def create_edge(cls, label: str, start_id: int, end_id: int, properties: dict) -> tuple[str, dict]:
        """Build a parameterized CREATE edge query.

        Returns:
            (query, params) tuple.
        """
        props_fragment, params = _props_to_params(properties)
        prop_clause = f" {props_fragment}" if properties else ""
        query = (
            f"MATCH (s) WHERE id(s) = {start_id} "
            f"MATCH (t) WHERE id(t) = {end_id} "
            f"CREATE (s)-[e:{label}{prop_clause}]->(t) "
            f"RETURN id(e)"
        )
        return query, params

    @classmethod
    def merge_edge_by_id(cls, edge_id: int, properties: dict) -> tuple[str, dict]:
        """Build a parameterized MATCH + SET edge query by ID.

        Returns:
            (query, params) tuple.
        """
        set_clause, params = _set_to_params(properties, "e")
        set_stmt = f"SET {set_clause} " if set_clause else ""
        query = f"MATCH (s)-[e]->(t) WHERE id(e) = {edge_id} {set_stmt}RETURN e"
        return query, params

    @classmethod
    def fetch_node_by_criteria(cls, criteria) -> tuple[str, dict]:
        """Build a parameterized MATCH node query from criteria.

        Returns:
            (query, params) tuple. params is empty when criteria only uses IDs.
        """
        prefix = criteria.prefix_ if criteria.prefix_ else "n"
        match_stmt, params = cls._match_node_by_criteria_parameterized(criteria, prefix)
        projection_stmt = cls._build_projection_stmt(criteria, prefix)
        return f"{match_stmt} RETURN {projection_stmt}", params

    @classmethod
    def fetch_nodes_by_ids(cls, node_ids: list[int]) -> str:
        """Build a MATCH query for multiple nodes by ID. Returns plain str (IDs are safe)."""
        id_sequence = ",".join([str(id) for id in node_ids])
        return f"MATCH (n) WHERE id(n) IN [{id_sequence}] RETURN n"

    @classmethod
    def delete_node_by_criteria(cls, criteria, detach: bool) -> tuple[str, dict]:
        """Build a parameterized DELETE node query.

        Returns:
            (query, params) tuple.
        """
        prefix = criteria.prefix_ if criteria.prefix_ else "n"
        match_stmt, params = cls._match_node_by_criteria_parameterized(criteria, prefix)
        detach_stmt = " DETACH" if detach else ""
        return f"{match_stmt}{detach_stmt} DELETE {prefix} RETURN id({prefix})", params

    @classmethod
    def fetch_edge_by_criteria(cls, criteria) -> tuple[str, dict]:
        """Build a parameterized MATCH edge query.

        Returns:
            (query, params) tuple.
        """
        prefix = criteria.prefix_ if criteria.prefix_ else "v"
        match_stmt, params = cls._match_edge_by_criteria_parameterized(criteria, prefix)
        projection_stmt = cls._build_projection_stmt(criteria, prefix)

        if hasattr(criteria, "fetch_nodes_") and criteria.fetch_nodes_:
            if hasattr(criteria, "start_criteria_") and criteria.start_criteria_:
                start_projection = cls._build_projection_stmt(criteria.start_criteria_, criteria.start_criteria_.get_prefix("s"))
            else:
                start_projection = "s"

            if hasattr(criteria, "end_criteria_") and criteria.end_criteria_:
                end_projection = cls._build_projection_stmt(criteria.end_criteria_, criteria.end_criteria_.get_prefix("e"))
            else:
                end_projection = "e"

            projection_stmt = f"{start_projection},{projection_stmt},{end_projection}"

        return f"{match_stmt} RETURN {projection_stmt}", params

    @classmethod
    def delete_edge_by_criteria(cls, criteria) -> tuple[str, dict]:
        """Build a parameterized DELETE edge query.

        Returns:
            (query, params) tuple.
        """
        match_stmt, params = cls._match_edge_by_criteria_parameterized(criteria, "v")
        return f"{match_stmt} DELETE v RETURN id(v)", params

    @classmethod
    def _build_projection_stmt(cls, criteria, prefix) -> str:
        if criteria.has_projection:
            return ",".join(f"{prefix}.{val}" if "(" not in val else val for val in criteria.projection_)
        else:
            return prefix

    @classmethod
    def _match_node_by_criteria_parameterized(cls, criteria, prefix) -> tuple[str, dict]:
        """Build a parameterized MATCH node clause from criteria.

        Returns:
            (match_clause, params) tuple.
        """
        node_criteria, params = cls._criteria_builder_parameterized(criteria, prefix)
        where_condition = f"WHERE {node_criteria[0]}" if node_criteria[0] is not None else ""
        return f"MATCH ({node_criteria[2]}{node_criteria[1]}) {where_condition}", params

    @classmethod
    def _match_edge_by_criteria_parameterized(cls, criteria, prefix) -> tuple[str, dict]:
        """Build a parameterized MATCH edge clause.

        Collects params from edge criteria, start node criteria, and end
        node criteria. Param keys are derived from the criteria prefix via
        _criteria_builder_parameterized (format: _c{prefix}_{key}). The
        default prefixes v/s/e are distinct, so no collisions occur in
        normal usage. Avoid setting custom prefix_ to the same value across
        edge and start/end criteria when all have properties.

        Returns:
            (match_clause, params) tuple.
        """
        edge_criteria, edge_params = cls._criteria_builder_parameterized(criteria, prefix)

        start_criteria, start_params = (
            cls._criteria_builder_parameterized(criteria.start_criteria_, "s")
            if hasattr(criteria, "start_criteria_") and criteria.start_criteria_
            else ((None, "", "s"), {})
        )
        end_criteria, end_params = (
            cls._criteria_builder_parameterized(criteria.end_criteria_, "e")
            if hasattr(criteria, "end_criteria_") and criteria.end_criteria_
            else ((None, "", "e"), {})
        )

        # Merge all params (prefixes ensure no key collisions)
        all_params = {**edge_params, **start_params, **end_params}

        conditions = [c[0] for c in (edge_criteria, start_criteria, end_criteria) if c[0] is not None]
        where_condition = "WHERE " + " AND ".join(conditions) if conditions else ""

        edge_part = f"{edge_criteria[2]}{edge_criteria[1]}"
        start_node_part = f"{start_criteria[2]}{start_criteria[1]}"
        end_node_part = f"{end_criteria[2]}{end_criteria[1]}"

        return f"MATCH ({start_node_part})-[{edge_part}]->({end_node_part}) {where_condition}", all_params

    @classmethod
    def _build_label_properties_parameterized(cls, criteria, with_label: bool, prefix: str) -> tuple[str, dict]:
        """Build label+properties fragment with parameterized values.

        Returns:
            (fragment, params) tuple.
        """
        if criteria is None:
            return "", {}

        label = (f":{criteria.label_}" if criteria.label_ else "") if with_label else ""
        params = {}
        props_fragment = ""
        if criteria.properties_ and len(criteria.properties_):
            props_fragment, params = _props_to_params(criteria.properties_, prefix=f"_c{prefix}_")

        return f"{label} {props_fragment}".rstrip(), params

    @classmethod
    def _criteria_builder_parameterized(cls, criteria, prefix) -> tuple[tuple, dict]:
        """Build criteria tuple with parameterized property values.

        Returns:
            ((where_condition, label_props, prefix), params) tuple.
        """
        if criteria is None:
            return (None, "", prefix), {}

        prefix = criteria.get_prefix(prefix)

        if criteria.id_ is not None:
            return (f"id({prefix})={criteria.id_}", "", prefix), {}

        label_cond = f"label({prefix}) in [{','.join(f'"{lbl}"' for lbl in criteria.label_)}]" if criteria.has_labels else None
        label_props, params = cls._build_label_properties_parameterized(criteria, label_cond is None, prefix)

        return (label_cond, label_props, prefix), params
