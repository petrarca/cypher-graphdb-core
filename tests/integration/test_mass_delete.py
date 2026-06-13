"""Integration tests for label introspection and bulk delete primitives.

Covers ``node_labels()``, ``edge_types()`` and ``mass_delete(include=/exclude=)``
against every supported backend (Memgraph and AGE) via the parametrized
``test_db`` fixture. These primitives drive backend SQL/Cypher (AGE resolves
labels from its catalog and rejects label predicates / list functions in a
single WHERE clause), so per-backend coverage guarantees the convention-agnostic
filtering works everywhere -- any backend added later is covered by adding it to
the parametrize list.
"""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def clean_db(test_db):
    """Provide a clean database with teardown after the test."""
    test_db.execute("MATCH (n) DETACH DELETE n")
    test_db.commit()
    yield test_db
    test_db.execute("MATCH (n) DETACH DELETE n")
    test_db.commit()


def _seed(db):
    """Create a small mixed graph: domain nodes + an infra-style node."""
    db.execute(
        """
        CREATE (a:Product {key: 'p1'})
        CREATE (b:Component {key: 'c1'})
        CREATE (c:Component {key: 'c2'})
        CREATE (d:Technology {name: 't1'})
        CREATE (m:_GraphModel {name: 'g'})
        CREATE (a)-[:HAS_COMPONENT]->(b)
        CREATE (a)-[:HAS_COMPONENT]->(c)
        CREATE (b)-[:USES_TECHNOLOGY]->(d)
        """
    )
    db.commit()


class TestNodeLabels:
    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_node_labels(self, clean_db):
        _seed(clean_db)
        labels = clean_db.node_labels()
        for expected in ("Product", "Component", "Technology", "_GraphModel"):
            assert expected in labels, f"{expected} missing from {labels}"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_edge_types(self, clean_db):
        _seed(clean_db)
        types = clean_db.edge_types()
        for expected in ("HAS_COMPONENT", "USES_TECHNOLOGY"):
            assert expected in types, f"{expected} missing from {types}"


class TestMassDelete:
    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_delete_all_nodes(self, clean_db):
        _seed(clean_db)
        deleted = clean_db.mass_delete()
        clean_db.commit()
        assert deleted == 5
        assert clean_db.execute("MATCH (n) RETURN count(n)", unnest_result=True) == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_exclude_preserves_infra(self, clean_db):
        """The key behaviour: wipe domain nodes, keep _-prefixed infra node."""
        _seed(clean_db)
        infra = [lbl for lbl in clean_db.node_labels() if lbl.startswith("_")]
        assert "_GraphModel" in infra

        deleted = clean_db.mass_delete(exclude=infra)
        clean_db.commit()

        assert deleted == 4  # Product, 2x Component, Technology
        assert clean_db.execute("MATCH (n) RETURN count(n)", unnest_result=True) == 1
        assert clean_db.execute("MATCH (m:_GraphModel) RETURN count(m)", unnest_result=True) == 1
        # Domain nodes gone.
        assert clean_db.execute("MATCH (n:Product) RETURN count(n)", unnest_result=True) == 0

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_include_only(self, clean_db):
        """Only the included labels are deleted; others remain untouched."""
        _seed(clean_db)
        deleted = clean_db.mass_delete(include=["Component"])
        clean_db.commit()

        assert deleted == 2
        assert clean_db.execute("MATCH (n:Component) RETURN count(n)", unnest_result=True) == 0
        assert clean_db.execute("MATCH (n:Product) RETURN count(n)", unnest_result=True) == 1
        assert clean_db.execute("MATCH (n:Technology) RETURN count(n)", unnest_result=True) == 1
        assert clean_db.execute("MATCH (m:_GraphModel) RETURN count(m)", unnest_result=True) == 1

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_include_intersect_exclude(self, clean_db):
        """exclude is applied after include."""
        _seed(clean_db)
        deleted = clean_db.mass_delete(include=["Product", "Component"], exclude=["Component"])
        clean_db.commit()

        assert deleted == 1  # only Product
        assert clean_db.execute("MATCH (n:Product) RETURN count(n)", unnest_result=True) == 0
        assert clean_db.execute("MATCH (n:Component) RETURN count(n)", unnest_result=True) == 2

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_include_unknown_label_noop(self, clean_db):
        """Including a label not present in the graph deletes nothing."""
        _seed(clean_db)
        deleted = clean_db.mass_delete(include=["DoesNotExist"])
        clean_db.commit()
        assert deleted == 0
        assert clean_db.execute("MATCH (n) RETURN count(n)", unnest_result=True) == 5
