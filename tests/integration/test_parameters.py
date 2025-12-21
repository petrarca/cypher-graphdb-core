"""Integration tests for parameterized query execution.

Tests parameter binding functionality for Memgraph and Apache AGE backends.
AGE now supports parameterized queries using PostgreSQL prepared statements.
"""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def db_with_data(request):
    """Create test data in the database."""
    db = request.getfixturevalue(request.param)
    db.execute("""
        CREATE (p1:Product {key: 'prod-001', name: 'Widget', price: 100})
        CREATE (p2:Product {key: 'prod-002', name: 'Gadget', price: 200})
        CREATE (p3:Product {key: 'prod-003', name: 'Gizmo', price: 150})
    """)
    yield db
    db.execute("MATCH (n) DETACH DELETE n")


class TestParameterBinding:
    """Test parameter binding in execute queries."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_single_parameter(self, test_db):
        """Test query with a single parameter."""
        # Create test data
        test_db.execute("CREATE (p:Product {key: 'test-key', name: 'TestProduct', price: 99})")

        # Query with parameter
        result = test_db.execute(
            "MATCH (p:Product {key: $key}) RETURN p.name AS name, p.price AS price",
            params={"key": "test-key"},
            unnest_result=True,
        )

        assert result is not None
        assert result[0] == "TestProduct"
        assert result[1] == 99

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_multiple_parameters(self, test_db):
        """Test query with multiple parameters."""
        # Create test data
        test_db.execute("CREATE (p:Product {key: 'multi-key', name: 'MultiProduct', price: 150})")

        # Query with multiple parameters
        result = test_db.execute(
            "MATCH (p:Product {key: $key, name: $name}) RETURN p.price AS price",
            params={"key": "multi-key", "name": "MultiProduct"},
            unnest_result=True,
        )

        assert result == 150

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_parameter_in_where_clause(self, test_db):
        """Test parameter in WHERE clause."""
        # Clean up any existing Product nodes first
        test_db.execute("MATCH (p:Product) DETACH DELETE p")

        # Create test data
        test_db.execute("""
            CREATE (p1:Product {name: 'Cheap', price: 50})
            CREATE (p2:Product {name: 'Medium', price: 100})
            CREATE (p3:Product {name: 'Expensive', price: 200})
        """)

        # Query with parameter in WHERE
        result = test_db.execute(
            "MATCH (p:Product) WHERE p.price > $min_price RETURN p.name ORDER BY p.price",
            params={"min_price": 75},
        )

        assert len(result) == 2
        assert result[0][0] == "Medium"
        assert result[1][0] == "Expensive"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_string_parameter(self, test_db):
        """Test string parameter binding."""
        test_db.execute("CREATE (p:Person {name: 'Alice', city: 'New York'})")

        result = test_db.execute(
            "MATCH (p:Person {name: $name}) RETURN p.city",
            params={"name": "Alice"},
            unnest_result="rc",
        )

        assert result == "New York"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_integer_parameter(self, test_db):
        """Test integer parameter binding."""
        test_db.execute("CREATE (p:Person {name: 'Bob', age: 30})")

        result = test_db.execute(
            "MATCH (p:Person) WHERE p.age = $age RETURN p.name",
            params={"age": 30},
            unnest_result="rc",
        )

        assert result == "Bob"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_no_params_still_works(self, test_db):
        """Test that queries without params still work (backward compatibility)."""
        test_db.execute("CREATE (p:Product {name: 'NoParamProduct'})")

        result = test_db.execute(
            "MATCH (p:Product {name: 'NoParamProduct'}) RETURN p.name",
            unnest_result="rc",
        )

        assert result == "NoParamProduct"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_empty_params_dict(self, test_db):
        """Test that empty params dict works."""
        test_db.execute("CREATE (p:Product {name: 'EmptyParamsProduct'})")

        result = test_db.execute(
            "MATCH (p:Product {name: 'EmptyParamsProduct'}) RETURN p.name",
            params={},
            unnest_result="rc",
        )

        assert result == "EmptyParamsProduct"


class TestParameterBindingWithStats:
    """Test parameter binding with execute_with_stats."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_execute_with_stats_and_params(self, test_db):
        """Test execute_with_stats with parameters."""
        test_db.execute("CREATE (p:Product {key: 'stats-key', name: 'StatsProduct', price: 250})")

        result = test_db.execute_with_stats(
            "MATCH (p:Product {key: $key}) RETURN p.name AS name, p.price AS price",
            params={"key": "stats-key"},
            unnest_result=True,
        )

        assert result.data is not None
        assert result.data[0] == "StatsProduct"
        assert result.data[1] == 250
        assert result.exec_statistics is not None
        assert result.exec_statistics.exec_time >= 0


class TestParameterTypes:
    """Test various parameter types."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_boolean_parameter(self, test_db):
        """Test boolean parameter binding."""
        test_db.execute("CREATE (p:Item {name: 'ActiveItem', active: true})")
        test_db.execute("CREATE (p:Item {name: 'InactiveItem', active: false})")

        result = test_db.execute(
            "MATCH (i:Item {active: $is_active}) RETURN i.name",
            params={"is_active": True},
            unnest_result="rc",
        )

        assert result == "ActiveItem"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_float_parameter(self, test_db):
        """Test float parameter binding."""
        test_db.execute("CREATE (p:Product {name: 'FloatProduct', rating: 4.5})")

        result = test_db.execute(
            "MATCH (p:Product) WHERE p.rating >= $min_rating RETURN p.name",
            params={"min_rating": 4.0},
            unnest_result="rc",
        )

        assert result == "FloatProduct"

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_null_parameter(self, test_db):
        """Test null parameter binding."""
        test_db.execute("CREATE (p:Product {name: 'NullProduct', description: null})")

        # Query for products where description is null
        result = test_db.execute(
            "MATCH (p:Product {name: $name}) RETURN p.description",
            params={"name": "NullProduct"},
            unnest_result="rc",
        )

        assert result is None


class TestParameterCaching:
    """Test prepared statement caching performance."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_loop_same_query_cache_performance(self, test_db):
        """Test executing same query multiple times to verify cache performance."""
        # Create test data
        test_db.execute("CREATE (p:Product {id: 1, name: 'Test1', price: 100})")
        test_db.execute("CREATE (p:Product {id: 2, name: 'Test2', price: 200})")
        test_db.execute("CREATE (p:Product {id: 3, name: 'Test3', price: 300})")

        # Execute same query 10 times with same parameters
        query = "MATCH (p:Product {id: $id}) RETURN p.name, p.price"
        params = {"id": 2}

        results = []
        for i in range(10):
            result = test_db.execute(query, params=params, unnest_result=True)
            results.append(result)
            print(f"Execution {i + 1}: {result}")

        # All results should be identical
        for result in results[1:]:
            assert result == results[0], "All executions should return same result"

        # Verify the expected result (handle both tuple and list formats)
        expected = ["Test2", 200]
        if isinstance(results[0], tuple):
            assert list(results[0]) == expected
        else:
            assert results[0] == expected

    @pytest.mark.parametrize("test_db", ["age_db"], indirect=True)  # pylint: disable=redefined-outer-name
    def test_age_cache_trace_logging(self, test_db):
        """Test AGE cache with trace logging enabled to see cache behavior."""
        # Create test data
        test_db.execute("CREATE (p:CacheTest {key: 'test', value: 42})")

        # Execute same query multiple times to see cache hits/misses
        query = "MATCH (p:CacheTest {key: $key}) RETURN p.value"
        params = {"key": "test"}

        print("=== Testing AGE Prepared Statement Cache ===")

        # First execution should be cache miss
        print("First execution (should be cache miss):")
        result1 = test_db.execute(query, params=params, unnest_result=True)
        print(f"Result: {result1}")

        # Subsequent executions should be cache hits
        for i in range(2, 6):
            print(f"Execution {i} (should be cache hit):")
            result = test_db.execute(query, params=params, unnest_result=True)
            print(f"Result: {result}")
            assert result == result1, "All results should be identical"

        print("=== Cache test completed ===")
