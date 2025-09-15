"""Shared fixtures and utilities for integration tests."""

import pytest
from testcontainers.core.container import DockerContainer

from cypher_graphdb import CypherGraphDB


@pytest.fixture(scope="session")
def memgraph_container(request):
    """Provide a Memgraph container for integration tests."""
    # Use the Memgraph platform image
    container = DockerContainer("memgraph/memgraph-platform:latest")
    container.with_exposed_ports(7687)  # Bolt port
    container.start()

    def cleanup():
        container.stop()

    request.addfinalizer(cleanup)

    # Get the mapped port and host
    bolt_port = container.get_exposed_port(7687)
    host = container.get_container_host_ip()

    yield {"host": host, "port": bolt_port, "uri": f"bolt://{host}:{bolt_port}"}


@pytest.fixture(scope="module")
def memgraph_db(memgraph_container):
    """Provide a CypherGraphDB instance connected to test Memgraph."""
    container_info = memgraph_container
    connect_params = {"host": container_info["host"], "port": container_info["port"]}

    with CypherGraphDB(backend="memgraph", connect_params=connect_params) as db:
        # Clean the database before each test
        db.execute("MATCH (n) DETACH DELETE n")
        yield db


@pytest.fixture(scope="function")
def sample_data(memgraph_db):
    """Create sample data in the test database."""
    # Create some sample nodes and relationships
    memgraph_db.execute("""
        CREATE (p1:Person {name: 'Alice', age: 30})
        CREATE (p2:Person {name: 'Bob', age: 25})
        CREATE (c:Company {name: 'TechCorp'})
        CREATE (p1)-[:WORKS_FOR {since: 2020}]->(c)
        CREATE (p2)-[:WORKS_FOR {since: 2021}]->(c)
        CREATE (p1)-[:KNOWS]->(p2)
    """)

    yield memgraph_db
