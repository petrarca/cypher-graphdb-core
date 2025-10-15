"""Integration tests for CypherGraphDB with Memgraph and Apache AGE."""

import pytest


def test_integration_example():
    """Example integration test with automatic marker."""
    # This test will automatically get the 'integration' marker
    # because it's in the tests/integration directory
    assert True  # Placeholder test


@pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
def test_connection(test_db):
    """Test basic connection to graph database (Memgraph and Apache AGE)."""
    # Test that we can execute a simple query (no CREATE, just RETURN)
    result = test_db.execute("RETURN 1 AS test_value", unnest_result=True)
    print(f"Backend: {test_db.backend.name}, Result: {result}")

    # Check that we got a result
    assert result is not None
    assert result == 1


@pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
def test_crud_operations(test_db):
    """Test basic CRUD operations (Memgraph and Apache AGE)."""
    # First create test data
    test_db.execute("""
        CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})
    """)

    # Read the Alice user
    result = test_db.execute(
        """
        MATCH (p:Person {name: 'Alice'})
        RETURN p.name AS name, p.age AS age, p.email AS email
    """,
        unnest_result=True,
    )

    print(f"Backend: {test_db.backend.name}, Result: {result}")
    assert result is not None
    # Result is a tuple: ('Alice', 30, 'alice@example.com')
    assert result[0] == "Alice"  # name
    assert result[1] == 30  # age
    assert result[2] == "alice@example.com"  # email


@pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
def test_relationships(test_db):
    """Test querying relationships (Memgraph and Apache AGE)."""
    # Create test data
    test_db.execute("""
        CREATE (p1:Person {name: 'Alice', age: 30})
        CREATE (p2:Person {name: 'Bob', age: 25})
        CREATE (c:Company {name: 'TechCorp'})
        CREATE (p1)-[:WORKS_FOR {role: 'Engineer'}]->(c)
        CREATE (p2)-[:WORKS_FOR {role: 'Designer'}]->(c)
    """)

    # Query the relationship data
    result = test_db.execute(
        """
        MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
        RETURN p.name AS person, r.role AS role, c.name AS company
        ORDER BY p.name
    """,
        unnest_result=True,
    )

    print(f"Backend: {test_db.backend.name}, Result: {result}")
    assert len(result) == 2  # Alice and Bob
    # Results are tuples: [('Alice', 'Engineer', 'TechCorp'),
    #                      ('Bob', 'Designer', 'TechCorp')]
    assert result[0][0] == "Alice"  # person
    assert result[0][1] == "Engineer"  # role
    assert result[0][2] == "TechCorp"  # company
    assert result[1][0] == "Bob"  # person
    assert result[1][1] == "Designer"  # role
    assert result[1][2] == "TechCorp"  # company
