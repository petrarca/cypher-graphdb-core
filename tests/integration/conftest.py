"""Shared fixtures and utilities for integration tests."""

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer

from cypher_graphdb import CypherGraphDB


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    """Automatically add backend-specific markers to parametrized tests."""
    for item in items:
        # Check if test is parametrized with backend fixtures
        if hasattr(item, "callspec") and "test_db" in item.callspec.params:
            backend = item.callspec.params["test_db"]
            if "memgraph" in backend:
                item.add_marker(pytest.mark.integration_memgraph)
            elif "age" in backend:
                item.add_marker(pytest.mark.integration_age)


@pytest.fixture(scope="session")
def memgraph_container(request):
    """Provide a Memgraph container for integration tests."""
    # Use the Memgraph mage image
    container = DockerContainer("memgraph/memgraph-mage:latest")
    container.with_exposed_ports(7687)  # Bolt port
    container.start()

    def cleanup():
        container.stop()

    request.addfinalizer(cleanup)

    # Get the mapped port and host
    bolt_port = container.get_exposed_port(7687)
    host = container.get_container_host_ip()

    yield {
        "host": host,
        "port": bolt_port,
        "uri": f"bolt://{host}:{bolt_port}",
    }


@pytest.fixture(scope="session")
def age_container(request):
    """Provide an Apache AGE (PostgreSQL) container for integration tests."""
    # Use PostgresContainer for proper health checks - no sleep needed!
    container = PostgresContainer(
        image="apache/age:latest",
        username="postgres",
        password="postgres",
        dbname="test_db",
    )

    container.start()

    def cleanup():
        container.stop()

    request.addfinalizer(cleanup)

    # Get connection details
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    user = "postgres"
    password = "postgres"
    dbname = "test_db"

    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    yield {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
        "connection_url": connection_url,
    }


@pytest.fixture(scope="module")
def memgraph_db(memgraph_container):  # noqa: ARG001
    """Provide a CypherGraphDB instance connected to test Memgraph."""
    container_info = memgraph_container
    connect_params = {
        "host": container_info["host"],
        "port": container_info["port"],
        "read_only": False,  # Explicitly set to false
    }

    db = CypherGraphDB(
        backend="memgraph",
        connect_params=connect_params,
    )

    # Clean the database before each test
    db.execute("MATCH (n) DETACH DELETE n")
    yield db

    db.disconnect()


@pytest.fixture(scope="module")
def age_db(age_container):  # noqa: ARG001
    """Provide a CypherGraphDB instance connected to test Apache AGE."""
    container_info = age_container
    graph_name = "test_graph"

    # Create connection - pass host/port/dbname as connect_params
    # AGE backend passes these to psycopg.conninfo.make_conninfo()
    db = CypherGraphDB(
        backend="age",
        connect_params={
            "host": container_info["host"],
            "port": container_info["port"],
            "dbname": container_info["dbname"],
            "user": container_info["user"],
            "password": container_info["password"],
            "graph_name": graph_name,
            "create_graph": True,  # Create graph if it doesn't exist
            "read_only": False,
        },
    )

    # Clean the database before each test
    db.execute("MATCH (n) DETACH DELETE n")

    yield db

    db.disconnect()


@pytest.fixture(scope="function")
def memgraph_db_readonly(memgraph_container):  # noqa: ARG001
    """Provide a read-only CypherGraphDB instance."""
    container_info = memgraph_container
    connect_params = {
        "host": container_info["host"],
        "port": container_info["port"],
        "read_only": True,
    }

    db = CypherGraphDB(backend="memgraph", connect_params=connect_params)
    yield db
    db.disconnect()


@pytest.fixture(scope="function")
def age_db_readonly(age_container):  # noqa: ARG001
    """Provide a read-only Apache AGE CypherGraphDB instance."""
    container_info = age_container
    graph_name = "test_graph"

    db = CypherGraphDB(
        backend="age",
        connect_params={
            "host": container_info["host"],
            "port": container_info["port"],
            "dbname": container_info["dbname"],
            "user": container_info["user"],
            "password": container_info["password"],
            "graph_name": graph_name,
            "read_only": True,
        },
    )
    yield db
    db.disconnect()


@pytest.fixture
def test_db(request):
    """Parametrized fixture that returns memgraph_db or age_db."""
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="function")
def sample_data(memgraph_db):  # noqa: ARG001
    """Create sample data in the test database."""
    # Create some sample nodes and relationships
    memgraph_db.execute(
        """
        CREATE (p1:Person {name: 'Alice', age: 30})
        CREATE (p2:Person {name: 'Bob', age: 25})
        CREATE (c:Company {name: 'TechCorp'})
        CREATE (p1)-[:WORKS_FOR {since: 2020}]->(c)
        CREATE (p2)-[:WORKS_FOR {since: 2021}]->(c)
        CREATE (p1)-[:KNOWS]->(p2)
    """
    )

    yield memgraph_db
