"""Query result container for CypherGraphDB operations."""

from typing import Any

from pydantic import BaseModel, Field

from ..backend import ExecStatistics, SqlStatistics
from ..cypherparser import ParsedCypherQuery
from ..models import TabularResult


class QueryResult(BaseModel):
    """Immutable container for query execution results and metadata.

    Provides complete query execution information without mutable state,
    ensuring thread safety and clean state ownership.

    Attributes:
        data: Query results formatted according to unnest_result parameter
        exec_statistics: Execution metrics and statistics
        sql_statistics: SQL-specific statistics (SQL queries only)
        parsed_query: Parsed query analysis and metadata
    """

    data: Any | TabularResult = Field(..., description="Query results data")
    exec_statistics: ExecStatistics = Field(..., description="Execution statistics")
    sql_statistics: SqlStatistics | None = Field(None, description="SQL statistics (SQL queries only)")
    parsed_query: ParsedCypherQuery | None = Field(None, description="Parsed query analysis")

    model_config = {"frozen": True}  # Makes the model immutable

    def is_empty(self) -> bool:
        """Check if the result contains no data.

        Returns:
            True if the result contains no data, False otherwise
        """
        return not self.data

    def has_graph_data(self) -> bool:
        """Check if the result contains graph data (nodes/edges).

        Returns:
            True if the result contains graph data, False otherwise
        """
        return self.exec_statistics.has_graph_data()

    def has_tabular_data(self) -> bool:
        """Check if the result contains tabular data.

        Returns:
            True if the result contains tabular data, False otherwise
        """
        return self.exec_statistics.has_tabular_data()
