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
# Basic node/edge creation and merging
# ---------------------------------------------------------------------------


def test_create_node():
    cypher = CypherBuilder.create_node("Person", {"name": "Alice", "age": 30})
    assert "CREATE(n:Person" in cypher
    assert 'name: "Alice"' in cypher
    assert "age: 30" in cypher
    assert "RETURN id(n)" in cypher


def test_create_edge():
    cypher = CypherBuilder.create_edge("KNOWS", 1, 2, {"since": 2020, "weight": 0.8})
    assert "MATCH (s) WHERE id(s) = 1" in cypher
    assert "MATCH (t) WHERE id(t) = 2" in cypher
    assert "CREATE (s)-[e:KNOWS" in cypher
    assert "since: 2020" in cypher
    assert "RETURN id(e)" in cypher


def test_merge_node_by_id():
    cypher = CypherBuilder.merge_node_by_id(123, {"name": "Bob", "active": True})
    assert "MATCH (n)" in cypher
    assert "WHERE id(n) = 123" in cypher
    assert 'SET n.name="Bob",n.active=True' in cypher
    assert "RETURN n" in cypher


def test_merge_edge_by_id():
    cypher = CypherBuilder.merge_edge_by_id(456, {"weight": 0.9})
    assert "MATCH (s)-[e]->(t)" in cypher
    assert "WHERE id(e) = 456" in cypher
    assert "SET e.weight=0.9" in cypher
    assert "RETURN e" in cypher


# ---------------------------------------------------------------------------
# Fetch operations
# ---------------------------------------------------------------------------


def test_fetch_nodes_by_ids():
    cypher = CypherBuilder.fetch_nodes_by_ids([10, 20, 30])
    assert cypher == "MATCH (n) WHERE id(n) IN [10,20,30] RETURN n"


def test_fetch_node_by_criteria_simple():
    criteria = MockCriteria(id_=42)
    cypher = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "MATCH (n)" in cypher
    assert "WHERE id(n)=42" in cypher
    assert "RETURN n" in cypher


def test_fetch_node_by_criteria_with_projection():
    criteria = MockCriteria(id_=42, projection_=["name", "id(n)", "age"])
    cypher = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "RETURN n.name,id(n),n.age" in cypher


def test_fetch_node_by_criteria_with_custom_prefix():
    criteria = MockCriteria(id_=42, prefix_="person")
    cypher = CypherBuilder.fetch_node_by_criteria(criteria)
    assert "MATCH (person)" in cypher
    assert "WHERE id(person)=42" in cypher
    assert "RETURN person" in cypher


def test_fetch_edge_by_criteria_simple():
    criteria = MockCriteria(id_=101)
    cypher = CypherBuilder.fetch_edge_by_criteria(criteria)
    assert "MATCH (s)-[v]->(e)" in cypher
    assert "WHERE id(v)=101" in cypher
    assert "RETURN v" in cypher


def test_fetch_edge_by_criteria_with_nodes():
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
    assert "WHERE id(v)=101, id(s)=1, id(e)=2" in cypher


# ---------------------------------------------------------------------------
# Delete operations
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


def test_build_label_properties_empty():
    result = CypherBuilder._build_label_properties(None, True)
    assert result == ""


def test_build_label_properties_with_label_and_props():
    criteria = MockCriteria(label_="Person", properties_={"name": "Alice"})
    result = CypherBuilder._build_label_properties(criteria, True)
    assert ":Person" in result
    assert 'name: "Alice"' in result


def test_criteria_builder_with_id():
    criteria = MockCriteria(id_=42)
    result = CypherBuilder._criteria_builder(criteria, "n")
    assert result[0] == "id(n)=42"
    assert result[1] == ""
    assert result[2] == "n"


def test_criteria_builder_with_labels():
    criteria = MockCriteria(label_=["Person", "User"], has_labels=True)
    result = CypherBuilder._criteria_builder(criteria, "n")
    assert "label(n) in" in result[0]
    assert '"Person"' in result[0]
    assert '"User"' in result[0]


def test_criteria_builder_none():
    result = CypherBuilder._criteria_builder(None, "n")
    assert result == (None, "", "n")


# ---------------------------------------------------------------------------
# Edge cases and complex scenarios
# ---------------------------------------------------------------------------


def test_create_node_empty_properties():
    cypher = CypherBuilder.create_node("Empty", {})
    assert "CREATE(n:Empty {})" in cypher


def test_create_edge_empty_properties():
    cypher = CypherBuilder.create_edge("SIMPLE", 1, 2, {})
    assert "CREATE (s)-[e:SIMPLE {}]->(t)" in cypher


def test_match_edge_by_criteria_complex():
    start_criteria = MockCriteria(label_=["Person"], has_labels=True, prefix_="start")
    end_criteria = MockCriteria(id_=99, prefix_="end")
    edge_criteria = MockCriteria(properties_={"weight": 0.5}, prefix_="rel")

    # Mock the edge criteria structure manually for complex test
    edge_criteria.start_criteria_ = start_criteria
    edge_criteria.end_criteria_ = end_criteria
    edge_criteria.model_fields = {
        "start_criteria_": True,
        "end_criteria_": True,
    }

    cypher = CypherBuilder._match_edge_by_criteria(edge_criteria, "rel")
    assert "MATCH (start" in cypher
    assert ")-[rel" in cypher
    assert "]->(end)" in cypher
    assert "label(start)" in cypher
    assert "id(end)=99" in cypher
    assert "weight: 0.5" in cypher


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
