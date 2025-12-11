"""Full-text search mixin for CypherGraphDB."""

from typing import Any

from loguru import logger

from .. import utils
from ..cypherparser import ParsedCypherQuery
from ..models import TabularResult


class SearchMixin:
    """Mixin providing full-text search methods for CypherGraphDB."""

    def search(
        self, parsed_query: ParsedCypherQuery, fts_query: str, language: str = None, unnest_result: str | bool = None
    ) -> Any | TabularResult:
        """Perform full-text search on graph data using backend-specific search capabilities.

        Executes full-text search queries against indexed graph content. The search
        behavior depends on the backend's search implementation and indexing configuration.

        Args:
            parsed_query: Parsed Cypher query providing search context and filters
            fts_query: Full-text search query string with search terms
            language: Optional language hint for search optimization (e.g., "english")
            unnest_result: Result formatting (same as execute method)

        Returns:
            Search results formatted according to unnest_result parameter

        Examples:
            ```python
            # Using context manager (recommended)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # Basic text search in product descriptions
                context_query = cdb.parse("MATCH (p:Product) RETURN p")
            results = cdb.search(
                context_query,
                "graph database client",
                language="english"
            )

            # Search with specific language
            spanish_results = cdb.search(
                context_query,
                "base de datos grafo",
                language="spanish",
                unnest_result="c"  # Just the matching content
            )

            # Technology search with filters
            tech_query = cdb.parse('''
                MATCH (t:Technology)
                WHERE t.category = "Database"
                RETURN t
            ''')
            database_techs = cdb.search(
                tech_query,
                "graph database nosql",
                unnest_result=True
            )
            ```

        Note:
            Full-text search requires proper indexing configuration on the
            backend. Search capabilities and syntax vary by database backend
            (Neo4j, Memgraph, etc.).
        """
        assert self._backend
        logger.debug(f"Search fts_query={fts_query} unnest_result={unnest_result}\ncypher_query={parsed_query.submitted_query}")

        result, _ = self._backend.fulltext_search(
            parsed_query,
            fts_query,
            language,
        )

        return utils.unnest_result(result, unnest_result)
