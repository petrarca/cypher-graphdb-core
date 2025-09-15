"""Tests for proper resource management in DuckDBSource and RowCollector."""

import unittest
from unittest.mock import MagicMock, patch

from cypher_graphdb.tools.duckdb_source import DuckDBSource
from cypher_graphdb.tools.row_collector import RowCollector


class TestDuckDBSourceResourceManagement(unittest.TestCase):
    """Test proper resource management in DuckDBSource."""

    @patch("cypher_graphdb.tools.duckdb_source.duckdb")
    def test_close_method(self, mock_duckdb):
        """Test that close() properly closes the DuckDB connection."""
        # Setup
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        # Create and close DuckDBSource
        source = DuckDBSource("SELECT 1")
        source.close()

        # Verify connection was closed
        mock_con.close.assert_called_once()
        self.assertIsNone(source._con)

    @patch("cypher_graphdb.tools.duckdb_source.duckdb")
    def test_context_manager(self, mock_duckdb):
        """Test that context manager properly closes the DuckDB connection."""
        # Setup
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        # Use DuckDBSource as context manager
        with DuckDBSource("SELECT 1") as source:
            pass

        # Verify connection was closed
        mock_con.close.assert_called_once()
        self.assertIsNone(source._con)


class TestRowCollectorResourceManagement(unittest.TestCase):
    """Test proper resource management in RowCollector."""

    @patch("cypher_graphdb.tools.row_collector.duckdb")
    def test_close_method(self, mock_duckdb):
        """Test that close() properly closes the DuckDB connection."""
        # Setup
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_db = MagicMock()

        # Create and close RowCollector
        collector = RowCollector(mock_db)
        collector.close()

        # Verify connection was closed
        mock_con.close.assert_called_once()
        self.assertIsNone(collector._con)

    @patch("cypher_graphdb.tools.row_collector.duckdb")
    def test_context_manager(self, mock_duckdb):
        """Test that context manager properly closes the DuckDB connection."""
        # Setup
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_db = MagicMock()

        # Use RowCollector as context manager
        with RowCollector(mock_db) as collector:
            pass

        # Verify connection was closed
        mock_con.close.assert_called_once()
        self.assertIsNone(collector._con)


if __name__ == "__main__":
    unittest.main()
