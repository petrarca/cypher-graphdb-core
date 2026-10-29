"""Integration tests for CypherGraphDB.expand_node against both backends.

Verifies the node-neighborhood expansion primitive is backend-agnostic
(runs identically on Memgraph and AGE): directions, rel-type filtering,
the unique-node cap with ``has_more``, and the empty result for a node
with no neighbors.
"""

import pytest

from cypher_graphdb import ExpandResult, GraphEdge, GraphNode

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def clean_db(test_db):
    """Provide a clean database connection with teardown."""
    yield test_db
    test_db.execute("MATCH (n) DETACH DELETE n")
    test_db.commit()


def _center_id(db, name: str) -> int:
    """Return the database id of the uniquely-named Person node."""
    rows = db.execute(f"MATCH (p:Person {{name: '{name}'}}) RETURN id(p)", unnest_result=True)
    return rows[0] if isinstance(rows, (list, tuple)) else rows


def _seed_star(db) -> int:
    """Create a small star: Alice -[:KNOWS]-> Bob, Carol; Dave -[:MANAGES]-> Alice.

    Returns Alice's node id (the center for expansion).
    """
    db.execute(
        """
        CREATE (a:Person {name: 'Alice'})
        CREATE (b:Person {name: 'Bob'})
        CREATE (c:Person {name: 'Carol'})
        CREATE (d:Person {name: 'Dave'})
        CREATE (a)-[:KNOWS]->(b)
        CREATE (a)-[:KNOWS]->(c)
        CREATE (d)-[:MANAGES]->(a)
        """
    )
    db.commit()
    return _center_id(db, "Alice")


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_both_returns_all_neighbors(clean_db):
    center = _seed_star(clean_db)

    result = clean_db.expand_node(center)

    assert isinstance(result, ExpandResult)
    assert all(isinstance(n, GraphNode) for n in result.nodes)
    assert all(isinstance(e, GraphEdge) for e in result.edges)
    names = {n.properties_.get("name") for n in result.nodes}
    assert names == {"Bob", "Carol", "Dave"}
    assert len(result.edges) == 3
    assert result.has_more is False


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_out_only(clean_db):
    center = _seed_star(clean_db)

    result = clean_db.expand_node(center, direction="out")

    names = {n.properties_.get("name") for n in result.nodes}
    assert names == {"Bob", "Carol"}


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_in_only(clean_db):
    center = _seed_star(clean_db)

    result = clean_db.expand_node(center, direction="in")

    names = {n.properties_.get("name") for n in result.nodes}
    assert names == {"Dave"}


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_rel_type_filter(clean_db):
    center = _seed_star(clean_db)

    result = clean_db.expand_node(center, rel_types=["MANAGES"])

    names = {n.properties_.get("name") for n in result.nodes}
    assert names == {"Dave"}
    assert all(e.label_ == "MANAGES" for e in result.edges)


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_limit_sets_has_more(clean_db):
    center = _seed_star(clean_db)

    result = clean_db.expand_node(center, limit=1)

    assert len(result.nodes) == 1
    assert result.has_more is True


@pytest.mark.parametrize("test_db", ["memgraph_db", "neo4j_db", "age_db"], indirect=True)
def test_expand_node_without_neighbors_is_empty(clean_db):
    clean_db.execute("CREATE (a:Person {name: 'Loner'})")
    clean_db.commit()
    center = _center_id(clean_db, "Loner")

    result = clean_db.expand_node(center)

    assert result.nodes == []
    assert result.edges == []
    assert result.has_more is False
