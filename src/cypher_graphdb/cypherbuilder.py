"""Cypher query builder module: Generate Cypher queries for graph operations.

This module provides the CypherBuilder class for programmatically generating
Cypher queries for common graph database operations like creating, merging,
fetching, and deleting nodes and edges.
"""

from .utils import dict_to_non_quoted_json, dict_to_value_pairs


class CypherBuilder:
    """Utility class for building Cypher query strings."""

    @classmethod
    def create_node(cls, label, properties) -> str:
        props = dict_to_non_quoted_json(properties)

        return f"CREATE(n:{label} {str(props)}) RETURN id(n)"

    @classmethod
    def merge_node_by_id(cls, node_id, properties) -> str:
        # create value pairs with prefix "n."
        values = dict_to_value_pairs(properties, "n.")

        return f"""
            MATCH (n)
            WHERE id(n) = {node_id}
            SET {values}
            RETURN n
        """

    @classmethod
    def create_edge(cls, label, start_id, end_id, properties) -> str:
        props = dict_to_non_quoted_json(properties)

        return f"""
            MATCH (s) WHERE id(s) = {start_id}
            MATCH (t) WHERE id(t) = {end_id}
            CREATE (s)-[e:{label} {props}]->(t)
            RETURN id(e)
        """

    @classmethod
    def merge_edge_by_id(cls, edge_id, properties) -> str:
        # create value pairs with prefix "e."
        values = dict_to_value_pairs(properties, "e.")

        if values:
            set_clause = f"SET {values}"
        else:
            set_clause = ""

        return f"""
            MATCH (s)-[e]->(t)
            WHERE id(e) = {edge_id}
            {set_clause}
            RETURN e
        """

    @classmethod
    def fetch_node_by_criteria(cls, criteria) -> str:
        prefix = criteria.prefix_ if criteria.prefix_ else "n"

        match_stmt = cls._match_node_by_criteria(criteria, prefix)
        projection_stmt = cls._build_projection_stmt(criteria, prefix)

        return f"{match_stmt} RETURN {projection_stmt}"

    @classmethod
    def fetch_nodes_by_ids(cls, node_ids: list[int]) -> str:
        id_sequence = ",".join([str(id) for id in node_ids])

        return f"MATCH (n) WHERE id(n) IN [{id_sequence}] RETURN n"

    @classmethod
    def delete_node_by_criteria(cls, criteria, detach: bool) -> str:
        prefix = criteria.prefix_ if criteria.prefix_ else "n"

        match_stmt = cls._match_node_by_criteria(criteria, prefix)
        detach_stmt = " DETACH" if detach else ""

        return f"{match_stmt}{detach_stmt} DELETE {prefix} RETURN id({prefix})"

    @classmethod
    def fetch_edge_by_criteria(cls, criteria) -> str:
        prefix = criteria.prefix_ if criteria.prefix_ else "v"
        match_stmt = cls._match_edge_by_criteria(criteria, prefix)

        # projection statement for the edge
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

        return f"{match_stmt} RETURN {projection_stmt}"

    @classmethod
    def delete_edge_by_criteria(cls, criteria) -> str:
        match_stmt = cls._match_edge_by_criteria(criteria, "v")

        return f"{match_stmt} DELETE v RETURN id(v)"

    @classmethod
    def _build_projection_stmt(cls, criteria, prefix) -> str:
        if criteria.has_projection:
            return ",".join(f"{prefix}.{val}" if "(" not in val else val for val in criteria.projection_)
        else:
            return prefix

    @classmethod
    def _match_node_by_criteria(cls, criteria, prefix) -> str:
        node_criteria = cls._criteria_builder(criteria, prefix)

        where_condition = f"WHERE {node_criteria[0]}" if node_criteria[0] is not None else ""

        return f"MATCH ({node_criteria[2]}{node_criteria[1]}) {where_condition}"

    @classmethod
    def _match_edge_by_criteria(cls, criteria, prefix) -> str:
        edge_criteria = cls._criteria_builder(criteria, prefix)

        start_criteria = (
            cls._criteria_builder(criteria.start_criteria_, "s") if hasattr(criteria, "start_criteria_") else (None, "", "s")
        )
        end_criteria = (
            cls._criteria_builder(criteria.end_criteria_, "e") if hasattr(criteria, "end_criteria_") else (None, "", "e")
        )

        ids = []
        if edge_criteria[0] is not None:
            ids.append(edge_criteria[0])
        if start_criteria[0] is not None:
            ids.append(start_criteria[0])
        if end_criteria[0] is not None:
            ids.append(end_criteria[0])

        where_condition = "WHERE " + " AND ".join(ids) if len(ids) > 0 else ""

        edge_part = f"{edge_criteria[2]}{edge_criteria[1]}"
        start_node_part = f"{start_criteria[2]}{start_criteria[1]}"
        end_node_part = f"{end_criteria[2]}{end_criteria[1]}"

        return f"MATCH ({start_node_part})-[{edge_part}]->({end_node_part}) {where_condition}"

    @classmethod
    def _build_label_properties(cls, criteria, with_label: bool) -> str:
        if criteria is None:
            return ""

        label = (f":{criteria.label_}" if criteria.label_ else "") if with_label else ""
        props = dict_to_non_quoted_json(criteria.properties_) if criteria.properties_ and len(criteria.properties_) else ""

        return f"{label} {props}"

    @classmethod
    def _criteria_builder(cls, criteria, prefix):
        if criteria is None:
            return (None, "", prefix)

        prefix = criteria.get_prefix(prefix)

        if criteria.id_ is not None:
            return (f"id({prefix})={criteria.id_}", "", prefix)

        label_cond = f"label({prefix}) in [{','.join(f'"{lbl}"' for lbl in criteria.label_)}]" if criteria.has_labels else None

        return (label_cond, cls._build_label_properties(criteria, label_cond is None), prefix)
