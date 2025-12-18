"""Stream mixin for CypherGraphDB.

Provides streaming query execution capabilities for large result sets.
"""

from collections.abc import Iterator
from typing import Any

from ..cypherparser import ParsedCypherQuery, parse_cypher_query


class StreamMixin:
    """Mixin class that adds streaming capabilities to CypherGraphDB."""

    def execute_cypher_stream(
        self,
        cypher_query: str | ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
    ) -> Iterator[list[Any]]:
        """Execute a Cypher query and yield results in chunks.

        This method provides memory-efficient streaming of large query results
        by fetching data in configurable chunks instead of loading everything
        into memory at once.

        Args:
            cypher_query: Cypher query string or ParsedCypherQuery to execute
            chunk_size: Number of rows to fetch per chunk (default: 1000)
            raw_data: If True, return raw data without row factory processing

        Yields:
            Lists of result rows (chunks). Each chunk contains up to chunk_size rows.

        Example:
            ```python
            db = CypherGraphDB()
            db.connect("bolt://localhost:7687")

            # Stream all nodes in chunks of 100
            for chunk in db.execute_cypher_stream(
                "MATCH (n) RETURN n LIMIT 10000",
                chunk_size=100
            ):
                print(f"Received {len(chunk)} nodes")
                # Process chunk...
            ```

        Benefits:
            - Memory efficient: Only one chunk in memory at a time
            - Progressive processing: Data available as soon as first chunk arrives
            - Scalable: Can handle arbitrarily large result sets
            - Flexible: Configurable chunk size based on use case
        """
        assert self._backend

        # Parse query if string provided
        if isinstance(cypher_query, str):
            parsed_query = parse_cypher_query(cypher_query)
        else:
            parsed_query = cypher_query

        # Delegate to backend streaming implementation
        # Each backend handles its own streaming strategy
        yield from self._backend.execute_cypher_stream(parsed_query, chunk_size=chunk_size, raw_data=raw_data)
