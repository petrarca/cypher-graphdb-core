"""SQL execution mixin for CypherGraphDB."""

from typing import Any

from loguru import logger

from .. import utils
from ..models import TabularResult
from .result import QueryResult


class SqlMixin:
    """Mixin providing SQL execution methods for CypherGraphDB."""

    def execute_sql(
        self,
        sql_str: str,
        unnest_result: str | bool = None,
        fetch_one=False,
        raw_data=False,
    ) -> Any | TabularResult:
        """Execute a raw SQL command and return results, with optional unnesting."""
        assert self._backend

        logger.debug(f"Execute SQL {unnest_result=}, {fetch_one=}): \n{sql_str}")
        result = self._execute_sql(sql_str, fetch_one, raw_data)

        return utils.unnest_result(result, unnest_result)

    def execute_sql_with_stats(
        self,
        sql_str: str,
        unnest_result: str | bool = None,
        fetch_one: bool = False,
        raw_data: bool = False,
    ) -> QueryResult:
        """Execute a SQL command and return immutable QueryResult with statistics.

        Provides complete query execution information for SQL queries without mutable state,
        ensuring thread safety and clean state ownership.

        Args:
            sql_str: SQL query string to execute
            unnest_result: Result formatting option (same as execute_sql method)
            fetch_one: Optimize for single result (stops after first match)
            raw_data: Return raw database results without object transformation

        Returns:
            QueryResult containing data, execution statistics, SQL statistics, and no parsed query

        Examples:
            ```python
            # Using context manager (recommended approach)
            with CypherGraphDB() as cdb:
                cdb.connect()

                # SQL query with full information
                result = cdb.execute_sql_with_stats("SELECT * FROM products LIMIT 5")
                data = result.data  # Query results
                exec_stats = result.exec_statistics  # Execution metrics
                sql_stats = result.sql_statistics  # SQL-specific info including column names

                # Access SQL column information
                if sql_stats and sql_stats.col_names:
                    print(f"Columns: {sql_stats.col_names}")
            ```
        """
        assert self._backend

        logger.debug(f"Execute SQL with stats {unnest_result=}, {fetch_one=}: \n{sql_str}")

        # Execute the SQL query and get statistics
        result, exec_stats, sql_stats = self._backend.execute_sql(sql_str, fetch_one=fetch_one, raw_data=raw_data)

        # Create immutable QueryResult
        return QueryResult(
            data=utils.unnest_result(result, unnest_result),
            exec_statistics=exec_stats,
            sql_statistics=sql_stats,  # SQL statistics populated for SQL queries
            parsed_query=None,  # No parsed query for SQL queries
        )

    def _execute_sql(self, sql_str: str, fetch_one: bool, raw_data: bool) -> TabularResult:
        result, _, _ = self._backend.execute_sql(sql_str, fetch_one, raw_data)

        return result
