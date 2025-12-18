"""Stream mixin for CypherGraphDB.

Provides streaming query execution capabilities for large result sets.
"""

import contextlib
import warnings
from collections.abc import Iterator
from typing import Any

from ..backend import BackendCapability
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

        For backends that don't support native streaming (e.g., Apache AGE),
        this method falls back to executing the query normally and chunking
        the results.

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
            - Backward compatible: Falls back to chunking for unsupported backends
        """
        assert self._backend

        # Parse query if string provided
        if isinstance(cypher_query, str):
            parsed_query = parse_cypher_query(cypher_query)
        else:
            parsed_query = cypher_query

        # Check if backend supports native streaming (capability must exist AND be True)
        supports_streaming = False
        with contextlib.suppress(NotImplementedError):
            supports_streaming = self._backend.get_capability(BackendCapability.STREAMING_SUPPORT)

        if supports_streaming:
            # Use native streaming
            yield from self._backend.execute_cypher_stream(parsed_query, chunk_size=chunk_size, raw_data=raw_data)
        else:
            # Fallback: execute query and chunk results
            yield from self._fallback_execute_cypher_stream(parsed_query, chunk_size=chunk_size, raw_data=raw_data)

    def _fallback_execute_cypher_stream(
        self,
        cypher_query: ParsedCypherQuery,
        chunk_size: int = 1000,
        raw_data: bool = False,
    ) -> Iterator[list[Any]]:
        """Fallback streaming for backends without native streaming support.

        Executes the query normally and yields results in chunks.

        Args:
            cypher_query: Parsed Cypher query to execute.
            chunk_size: Number of rows to yield per chunk.
            raw_data: If True, return raw data without processing.

        Yields:
            Lists of result rows (chunks).
        """
        warnings.warn(
            "Backend does not support native streaming. Using chunked fallback execution.",
            UserWarning,
            stacklevel=3,
        )

        # Execute query normally
        result, _ = self._backend.execute_cypher(cypher_query, raw_data=raw_data)

        # Yield results in chunks
        for i in range(0, len(result), chunk_size):
            chunk = result[i : i + chunk_size]
            yield chunk
