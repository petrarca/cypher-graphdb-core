"""Integration tests for CypherGraphDB with Memgraph backend."""

import pytest


@pytest.fixture(scope="module")
def test_db(memgraph_db):
    """Initialize test data once per module."""
    # Create test data that will be available for all tests in this module
    memgraph_db.execute("""
        CREATE (p1:Person {name: 'Alice', age: 30, email: 'alice@example.com'})
        CREATE (p2:Person {name: 'Bob', age: 25, email: 'bob@example.com'})
        CREATE (c:Company {name: 'TechCorp', founded: 2010})
        CREATE (p1)-[:WORKS_FOR {since: 2020, role: 'Engineer'}]->(c)
        CREATE (p2)-[:WORKS_FOR {since: 2021, role: 'Designer'}]->(c)
        CREATE (p1)-[:KNOWS {since: 2019}]->(p2)
    """)

    # Return the database connection for tests to use
    yield memgraph_db


def test_integration_example():
    """Example integration test with automatic marker."""
    # This test will automatically get the 'integration' marker
    # because it's in the tests/integration directory
    assert True  # Placeholder test


def test_memgraph_connection(test_db):
    """Test basic connection to Memgraph test container."""
    # Test that we can execute a simple query
    result = test_db.execute("RETURN 1 AS test_value", unnest_result=True)
    print(result)

    # Check that we got a result
    assert result is not None
    assert result == 1


def test_memgraph_crud_operations(test_db):
    """Test basic CRUD operations against Memgraph."""
    # The test data is already created by the module_test_data fixture
    # We can query the existing data instead of creating new data

    # Read the existing Alice user
    result = test_db.execute(
        """
        MATCH (p:Person {name: 'Alice'})
        RETURN p.name AS name, p.age AS age, p.email AS email
    """,
        unnest_result=True,
    )

    print(result)
    assert result is not None
    # Result is a tuple: ('Alice', 30, 'alice@example.com')
    assert result[0] == "Alice"  # name
    assert result[1] == 30  # age
    assert result[2] == "alice@example.com"  # email


def test_memgraph_relationships(test_db):
    """Test querying relationships with the pre-created data."""
    # Query the relationship data
    result = test_db.execute(
        """
        MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
        RETURN p.name AS person, r.role AS role, c.name AS company
        ORDER BY p.name
    """,
        unnest_result=True,
    )

    print(result)
    assert len(result) == 2  # Alice and Bob
    # Results are tuples: [('Alice', 'Engineer', 'TechCorp'),
    #                      ('Bob', 'Designer', 'TechCorp')]
    assert result[0][0] == "Alice"  # person
    assert result[0][1] == "Engineer"  # role
    assert result[0][2] == "TechCorp"  # company
    assert result[1][0] == "Bob"  # person
    assert result[1][1] == "Designer"  # role
    assert result[1][2] == "TechCorp"  # company
