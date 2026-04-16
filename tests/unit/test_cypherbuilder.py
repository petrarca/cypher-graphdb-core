import pytest

from cypher_graphdb.cypherbuilder import CypherBuilder


class MockCriteria:
    """Mock criteria for testing builder methods."""

    def __init__(
        self,
        id_=None,
        label_=None,
        properties_=None,
        prefix_=None,
        projection_=None,
        has_labels=False,
        fetch_nodes_=False,
        start_criteria_=None,
        end_criteria_=None,
    ):
        self.id_ = id_
        self.label_ = label_
        self.properties_ = properties_
        self.prefix_ = prefix_
        self.projection_ = projection_
        self.has_labels = has_labels
        self.fetch_nodes_ = fetch_nodes_
        self.start_criteria_ = start_criteria_
        self.end_criteria_ = end_criteria_
        self.model_fields = {
            "fetch_nodes_": fetch_nodes_,
            "start_criteria_": start_criteria_ is not None,
            "end_criteria_": end_criteria_ is not None,
        }

    @property
    def has_projection(self):
        return self.projection_ is not None

    def get_prefix(self, default):
        return self.prefix_ if self.prefix_ else default


# ---------------------------------------------------------------------------
# Basic node/edge creation and merging -- return (query, params) tuples
# ---------------------------------------------------------------------------


def test_create_node():
    query, params = CypherBuilder.create_node("Person", {"name": "Alice", "age": 30})
    assert "CREATE (n:Person" in query
    assert "$p_name" in query
    assert "$p_age" in query
    assert "RETURN id(n)" in query
    assert params["p_name"] == "Alice"
    assert params["p_age"] == 30


def test_create_node_empty_properties():
    query, params = CypherBuilder.create_node("Empty", {})
    assert "CREATE (n:Empty {})" in query
    assert params == {}


def test_create_edge():
    query, params = CypherBuilder.create_edge("KNOWS", 1, 2, {"since": 2020, "weight": 0.8})
    assert "MATCH (s) WHERE id(s) = 1" in query
    assert "MATCH (t) WHERE id(t) = 2" in query
    assert "CREATE (s)-[e:KNOWS" in query
    assert "$p_since" in query
    assert "RETURN id(e)" in query
    assert params["p_since"] == 2020
    assert params["p_weight"] == 0.8


def test_create_edge_empty_properties():
    query, params = CypherBuilder.create_edge("SIMPLE", 1, 2, {})
    assert "CREATE (s)-[e:SIMPLE]->(t)" in query
    assert params == {}


def test_merge_node_by_id():
    query, params = CypherBuilder.merge_node_by_id(123, {"name": "Bob", "active": True})
    assert "MATCH (n)" in query
    assert "WHERE id(n) = 123" in query
    assert "n.name = $p_name" in query
    assert "n.active = $p_active" in query
    assert "RETURN n" in query
    assert params["p_name"] == "Bob"
    assert params["p_active"] is True


def test_merge_edge_by_id():
    query, params = CypherBuilder.merge_edge_by_id(456, {"weight": 0.9})
    assert "MATCH (s)-[e]->(t)" in query
    assert "WHERE id(e) = 456" in query
    assert "e.weight = $p_weight" in query
    assert "RETURN e" in query
    assert params["p_weight"] == 0.9


def test_merge_edge_by_id_empty_properties():
    query, params = CypherBuilder.merge_edge_by_id(456, {})
    assert "MATCH (s)-[e]->(t)" in query
    assert "SET" not in query
    assert "RETURN e" in query
    assert params == {}


# ---------------------------------------------------------------------------
# Fetch operations
# ---------------------------------------------------------------------------


def test_fetch_nodes_by_ids():
    # Returns plain str -- IDs are safe to inline
    cypher = CypherBuilder.fetch_nodes_by_ids([10, 20, 30])
    assert cypher == "MATCH (n) WHERE id(n) IN [10,20,30] RETURN n"


def test_fetch_node_by_criteria_simple():
    criteria = MockCriteria(id_=42)
    query, params = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "MATCH (n)" in query
    assert "WHERE id(n)=42" in query
    assert "RETURN n" in query
    assert params == {}


def test_fetch_node_by_criteria_with_projection():
    criteria = MockCriteria(id_=42, projection_=["name", "id(n)", "age"])
    query, params = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "RETURN n.name,id(n),n.age" in query


def test_fetch_node_by_criteria_with_custom_prefix():
    criteria = MockCriteria(id_=42, prefix_="person")
    query, params = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "MATCH (person)" in query
    assert "WHERE id(person)=42" in query
    assert "RETURN person" in query


def test_fetch_node_by_criteria_with_properties():
    criteria = MockCriteria(label_="License", properties_={"name": '{file = "LICENSE"}'})
    query, params = CypherBuilder.fetch_node_by_criteria(criteria)
    # Value must NOT be inlined -- it must be a $param reference
    assert '{file = "LICENSE"}' not in query
    assert "$" in query
    assert any(v == '{file = "LICENSE"}' for v in params.values())


def test_fetch_edge_by_criteria_simple():
    # Returns plain str
    criteria = MockCriteria(id_=101)
    cypher = CypherBuilder.fetch_edge_by_criteria(criteria)
    assert "MATCH (s)-[v]->(e)" in cypher
    assert "WHERE id(v)=101" in cypher
    assert "RETURN v" in cypher


def test_fetch_edge_by_criteria_with_nodes():
    # Returns plain str
    start_criteria = MockCriteria(id_=1)
    end_criteria = MockCriteria(id_=2)
    criteria = MockCriteria(
        id_=101,
        fetch_nodes_=True,
        start_criteria_=start_criteria,
        end_criteria_=end_criteria,
    )
    cypher = CypherBuilder.fetch_edge_by_criteria(criteria)
    assert "RETURN s,v,e" in cypher
    assert "WHERE id(v)=101 AND id(s)=1 AND id(e)=2" in cypher


# ---------------------------------------------------------------------------
# Delete operations -- return plain str (ID-based, safe)
# ---------------------------------------------------------------------------


def test_delete_node_by_criteria():
    criteria = MockCriteria(id_=99)
    cypher = CypherBuilder.delete_node_by_criteria(criteria, detach=False)
    assert "MATCH (n)" in cypher
    assert "WHERE id(n)=99" in cypher
    assert " DELETE n RETURN id(n)" in cypher
    assert "DETACH" not in cypher


def test_delete_node_by_criteria_detach():
    criteria = MockCriteria(id_=99)
    cypher = CypherBuilder.delete_node_by_criteria(criteria, detach=True)
    assert "DETACH DELETE n RETURN id(n)" in cypher


def test_delete_edge_by_criteria():
    criteria = MockCriteria(id_=200)
    cypher = CypherBuilder.delete_edge_by_criteria(criteria)
    assert "MATCH (s)-[v]->(e)" in cypher
    assert "WHERE id(v)=200" in cypher
    assert "DELETE v RETURN id(v)" in cypher


# ---------------------------------------------------------------------------
# Helper method tests
# ---------------------------------------------------------------------------


def test_build_projection_stmt_with_projection():
    criteria = MockCriteria(projection_=["name", "id(n)", "age"])
    result = CypherBuilder._build_projection_stmt(criteria, "n")
    assert result == "n.name,id(n),n.age"


def test_build_projection_stmt_without_projection():
    criteria = MockCriteria()
    result = CypherBuilder._build_projection_stmt(criteria, "n")
    assert result == "n"


def test_criteria_builder_with_id():
    (cond, props, prefix), params = CypherBuilder._criteria_builder_parameterized(MockCriteria(id_=42), "n")
    assert cond == "id(n)=42"
    assert props == ""
    assert prefix == "n"
    assert params == {}


def test_criteria_builder_with_labels():
    criteria = MockCriteria(label_=["Person", "User"], has_labels=True)
    (cond, props, prefix), params = CypherBuilder._criteria_builder_parameterized(criteria, "n")
    assert "label(n) in" in cond
    assert '"Person"' in cond
    assert '"User"' in cond


def test_criteria_builder_none():
    result, params = CypherBuilder._criteria_builder_parameterized(None, "n")
    assert result == (None, "", "n")
    assert params == {}


# ---------------------------------------------------------------------------
# Special character safety tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "special_value",
    [
        "ibxtlib.3.1lib.lib",
        '{file = "LICENSE"}',
        "O'Brien",
        'value with "quotes"',
        "C:\\path\\to\\file",
        "multi\nline\nvalue",
    ],
)
def test_create_node_special_characters(special_value):
    """Property values with special characters must never be inlined into the query."""
    query, params = CypherBuilder.create_node("Node", {"key": special_value})
    assert special_value not in query
    assert params["p_key"] == special_value


@pytest.mark.parametrize(
    "special_value",
    [
        "ibxtlib.3.1lib.lib",
        '{file = "LICENSE"}',
        "O'Brien",
    ],
)
def test_fetch_node_by_criteria_special_characters(special_value):
    """Criteria property values with special characters must never be inlined."""
    criteria = MockCriteria(label_="Node", properties_={"name": special_value})
    query, params = CypherBuilder.fetch_node_by_criteria(criteria)
    assert special_value not in query
    assert any(v == special_value for v in params.values())


@pytest.mark.parametrize(
    "node_ids,expected",
    [
        ([1], "MATCH (n) WHERE id(n) IN [1] RETURN n"),
        ([1, 2, 3], "MATCH (n) WHERE id(n) IN [1,2,3] RETURN n"),
        ([], "MATCH (n) WHERE id(n) IN [] RETURN n"),
    ],
)
def test_fetch_nodes_by_ids_parametrized(node_ids, expected):
    result = CypherBuilder.fetch_nodes_by_ids(node_ids)
    assert result == expected
