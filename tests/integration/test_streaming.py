"""Integration tests for streaming functionality.

Tests streaming read operations for Memgraph backend, including:
- Basic chunked streaming
- Early termination
- Memory efficiency
- Error handling
- Various query types
"""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def memgraph_with_data(memgraph_db):
    """Create test data in Memgraph for streaming tests."""
    # Create test data
    memgraph_db.execute("""
        CREATE (p1:Person {name: 'Alice', age: 30, city: 'New York'})
        CREATE (p2:Person {name: 'Bob', age: 25, city: 'San Francisco'})
        CREATE (p3:Person {name: 'Charlie', age: 35, city: 'Chicago'})
        CREATE (p4:Person {name: 'Diana', age: 28, city: 'Boston'})
        CREATE (p5:Person {name: 'Eve', age: 32, city: 'Seattle'})
        CREATE (c1:Company {name: 'TechCorp', industry: 'Technology'})
        CREATE (c2:Company {name: 'FinanceCo', industry: 'Finance'})
        CREATE (p1)-[:WORKS_FOR {since: 2020, role: 'Engineer'}]->(c1)
        CREATE (p2)-[:WORKS_FOR {since: 2021, role: 'Analyst'}]->(c1)
        CREATE (p3)-[:WORKS_FOR {since: 2019, role: 'Manager'}]->(c2)
        CREATE (p4)-[:KNOWS {since: 2022}]->(p5)
    """)
    yield memgraph_db
    # Cleanup
    memgraph_db.execute("MATCH (n) DETACH DELETE n")


class TestMemgraphStreaming:
    """Test streaming read functionality for Memgraph backend."""

    def test_stream_nodes_basic(self, memgraph_with_data):
        """Test basic streaming of nodes with small chunks."""
        chunks = list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name", chunk_size=2))

        # Should get 3 chunks: 2, 2, 1 persons
        assert len(chunks) == 3
        assert len(chunks[0]) == 2  # Alice, Bob
        assert len(chunks[1]) == 2  # Charlie, Diana
        assert len(chunks[2]) == 1  # Eve

        # Verify total count
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 5

    def test_stream_early_termination(self, memgraph_with_data):
        """Test early termination of streaming."""
        chunk_count = 0
        total_rows = 0

        for chunk in memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name", chunk_size=2):
            chunk_count += 1
            total_rows += len(chunk)

            # Stop after 2 chunks
            if chunk_count >= 2:
                break

        # Should have processed only first 2 chunks (4 persons)
        assert chunk_count == 2
        assert total_rows == 4

    def test_stream_large_chunks(self, memgraph_with_data):
        """Test streaming with chunk size larger than result."""
        chunks = list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name", chunk_size=10))

        # Should get 1 chunk with all data
        assert len(chunks) == 1
        assert len(chunks[0]) == 5

    def test_stream_relationships(self, memgraph_with_data):
        """Test streaming of relationship data."""
        chunks = list(
            memgraph_with_data.execute_cypher_stream(
                "MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) RETURN p, r, c ORDER BY p.name", chunk_size=2
            )
        )

        # Should get 2 chunks: 2, 1 relationships
        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 3

    def test_stream_empty_result(self, memgraph_with_data):
        """Test streaming with empty result set."""
        chunks = list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person {name: 'NonExistent'}) RETURN p", chunk_size=5))

        # Should get empty list (no chunks yielded)
        assert len(chunks) == 0

    def test_stream_with_aggregation(self, memgraph_with_data):
        """Test streaming with aggregation queries."""
        chunks = list(
            memgraph_with_data.execute_cypher_stream(
                "MATCH (p:Person) RETURN p.city, count(p) as count ORDER BY count DESC", chunk_size=2
            )
        )

        # Should get 3 chunks: 2, 2, 1 cities
        assert len(chunks) == 3
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 5  # 5 different cities

    def test_stream_memory_efficiency(self, memgraph_with_data):
        """Test that streaming is memory efficient by processing large datasets."""
        # Create more test data
        for i in range(100):
            memgraph_with_data.execute(f"""
                CREATE (p:Person {{name: 'Person{i}', age: {20 + i % 40}, city: 'City{i % 10}'}})
            """)

        try:
            # Stream all 105 people (5 original + 100 new)
            chunk_count = 0
            total_processed = 0

            for chunk in memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name", chunk_size=10):
                chunk_count += 1
                chunk_size = len(chunk)
                total_processed += chunk_size

                # Verify chunk size doesn't exceed requested size
                assert chunk_size <= 10

                # Process chunk (simulate memory-intensive operation)
                processed_data = [str(item) for item in chunk]
                assert len(processed_data) == chunk_size

            # Should have processed all 105 people
            assert total_processed == 105
            assert chunk_count == 11  # 10 chunks of 10 + 1 chunk of 5

        finally:
            # Cleanup the extra data
            memgraph_with_data.execute("MATCH (p:Person) WHERE p.name STARTS WITH 'Person' DELETE p")

    def test_stream_error_handling(self, memgraph_with_data):
        """Test error handling in streaming operations."""
        import mgclient

        # Test with invalid Cypher syntax
        with pytest.raises(mgclient.DatabaseError):  # Should raise parsing error
            list(memgraph_with_data.execute_cypher_stream("INVALID CYPHER SYNTAX", chunk_size=5))

        # Test with division by zero error
        with pytest.raises(mgclient.DatabaseError):  # Should raise runtime error
            list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN 1/0", chunk_size=5))

    def test_stream_different_chunk_sizes(self, memgraph_with_data):
        """Test streaming with various chunk sizes."""
        # Test chunk size of 1
        chunks = list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 3", chunk_size=1))
        assert len(chunks) == 3
        assert all(len(chunk) == 1 for chunk in chunks)

        # Test chunk size of 3
        chunks = list(memgraph_with_data.execute_cypher_stream("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 3", chunk_size=3))
        assert len(chunks) == 1
        assert len(chunks[0]) == 3

    def test_stream_with_age_filter(self, memgraph_with_data):
        """Test streaming queries with age filtering."""
        chunks = list(
            memgraph_with_data.execute_cypher_stream("MATCH (p:Person) WHERE p.age >= 30 RETURN p ORDER BY p.name", chunk_size=2)
        )

        # Should get persons aged 30+: Alice (30), Charlie (35), Eve (32)
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 3

    def test_stream_raw_data_mode(self, memgraph_with_data):
        """Test streaming with raw_data=True."""
        chunks = list(
            memgraph_with_data.execute_cypher_stream(
                "MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 3", chunk_size=2, raw_data=True
            )
        )

        # Should get raw data without processing
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 3

        # Raw data should be tuples
        for chunk in chunks:
            for row in chunk:
                assert isinstance(row, tuple)
