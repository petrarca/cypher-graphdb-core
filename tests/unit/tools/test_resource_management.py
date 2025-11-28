"""Tests for proper resource management in CsvSource and RowCollector."""

import unittest
from unittest.mock import MagicMock, patch

from cypher_graphdb.tools.csv_source import CsvSource
from cypher_graphdb.tools.row_collector import RowCollector


class TestCsvSourceResourceManagement(unittest.TestCase):
    """Test proper resource management in CsvSource."""

    @patch("cypher_graphdb.tools.csv_source.open", create=True)
    @patch("cypher_graphdb.tools.csv_source.csv")
    def test_close_method(self, mock_csv, mock_open):
        """Test that close() properly closes the file handle."""
        # Setup
        mock_file = MagicMock()
        mock_open.return_value = mock_file

        # Mock DictReader with fieldnames attribute
        mock_reader = MagicMock()
        mock_reader.fieldnames = ["col1", "col2"]
        mock_csv.DictReader.return_value = mock_reader
        mock_csv.Sniffer.return_value.sniff.return_value = "dummy"

        # Create and close CsvSource
        source = CsvSource("dummy.csv")
        source.close()

        # Verify file was closed
        mock_file.close.assert_called_once()

    @patch("cypher_graphdb.tools.csv_source.open", create=True)
    @patch("cypher_graphdb.tools.csv_source.csv")
    def test_context_manager(self, mock_csv, mock_open):
        """Test that context manager properly closes the file handle."""
        # Setup
        mock_file = MagicMock()
        mock_open.return_value = mock_file

        # Mock DictReader with fieldnames attribute
        mock_reader = MagicMock()
        mock_reader.fieldnames = ["col1", "col2"]
        mock_csv.DictReader.return_value = mock_reader
        mock_csv.Sniffer.return_value.sniff.return_value = "dummy"

        # Use CsvSource as context manager
        with CsvSource("dummy.csv"):
            pass

        # Verify file was closed
        mock_file.close.assert_called_once()


class TestRowCollectorResourceManagement(unittest.TestCase):
    """Test proper resource management in RowCollector (pure Python implementation)."""

    def test_close_method_no_op(self):
        """Test that close() is a no-op for pure Python implementation."""
        # Setup
        mock_db = MagicMock()

        # Create and close RowCollector
        collector = RowCollector(mock_db)
        collector.close()

        # Verify no exceptions and close() is callable
        self.assertIsNone(collector.close())

    def test_context_manager_no_op(self):
        """Test that context manager works for pure Python implementation."""
        # Setup
        mock_db = MagicMock()

        # Use RowCollector as context manager
        with RowCollector(mock_db) as collector:
            # Verify collector is properly initialized
            self.assertIsNotNone(collector._node_cache)
            self.assertEqual(collector.db, mock_db)

        # Verify no exceptions occurred
        self.assertTrue(True)  # If we get here, context manager worked


if __name__ == "__main__":
    unittest.main()
