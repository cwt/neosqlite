import pytest
import neosqlite
from unittest.mock import patch, MagicMock


def test_custom_tokenizer_parameter():
    """Test that custom tokenizer parameter is accepted."""
    # Mock the database connection to avoid trying to load actual extensions
    with patch("neosqlite.connection.sqlite3") as mock_sqlite:
        # Configure the mock to behave like a real sqlite3 connection
        mock_db = MagicMock()
        mock_sqlite.connect.return_value = mock_db
        mock_db.isolation_level = None

        # This test just verifies that the parameter is accepted
        # We mock the extension loading to avoid filesystem access
        conn = neosqlite.Connection(
            ":memory:", tokenizers=[("icu", "/path/to/libfts5_icu.so")]
        )

        # Verify that the tokenizer was stored correctly
        assert conn._tokenizers == [("icu", "/path/to/libfts5_icu.so")]

        # Verify that enable_load_extension was called
        mock_db.enable_load_extension.assert_called_once_with(True)

        # Verify that execute was called to load the extension
        mock_db.execute.assert_called_with(
            "SELECT load_extension('/path/to/libfts5_icu.so')"
        )


def test_create_indexes_with_tokenizer():
    """Test that create_indexes accepts tokenizer parameter."""
    with patch("neosqlite.connection.sqlite3") as mock_sqlite:
        # Configure the mock to behave like a real sqlite3 connection
        mock_db = MagicMock()
        mock_sqlite.connect.return_value = mock_db
        mock_db.isolation_level = None

        conn = neosqlite.Connection(":memory:")
        collection = conn["test"]

        # Mock the database methods to avoid actual FTS operations
        mock_db.execute.return_value = None
        mock_db.fetchone.return_value = None

        # Test with dict format
        indexes = [
            {"key": "content", "fts": True, "tokenizer": "icu"},
            {"key": "title", "fts": True},
        ]

        # This should not raise an exception
        collection.create_indexes(indexes)

        # Verify that create_index was called with the correct parameters
        # The first call should have tokenizer='icu'
        # The second call should have tokenizer=None (default)
