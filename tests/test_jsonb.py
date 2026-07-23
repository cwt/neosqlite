"""
Tests for JSONB features and fallback behavior.
"""

import pytest

import neosqlite

# Try to import pysqlite3 for consistent JSON/JSONB support
try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


def test_import_with_pysqlite3():
    """Test that the module correctly uses pysqlite3 when available."""

    # This test would pass in an environment where pysqlite3 is available
    # which is our current test environment
    import neosqlite.collection as neosqlite_module

    # In our current environment, pysqlite3 should be available
    try:
        import pysqlite3.dbapi2

        # If we can import it, neosqlite should be using it
        assert neosqlite_module.sqlite3 == pysqlite3.dbapi2
    except ImportError:
        # If pysqlite3 is not available, neosqlite should fall back to standard sqlite3
        import sqlite3

        assert neosqlite_module.sqlite3 == sqlite3


def test_jsonb_table_creation():
    """Test that tables are created with JSONB column when supported."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_jsonb_collection"]

        # Check the table schema
        cursor = collection.db.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (collection.name,),
        )
        row = cursor.fetchone()
        table_sql = row[0] if row else ""

        # Check if JSONB is supported in this SQLite installation
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        # If JSONB is supported, the column should be JSONB
        # Otherwise, it should be TEXT
        if jsonb_supported:
            assert "data JSONB" in table_sql
        else:
            assert "data TEXT" in table_sql


def test_jsonb_operations():
    """Test JSONB operations with enhanced SQLite support."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_operations_test"]

        # Verify that JSONB is actually supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert a document with nested data
        doc = {
            "name": "Test User",
            "profile": {
                "age": 30,
                "settings": {"theme": "dark", "notifications": True},
            },
            "tags": ["python", "sqlite", "jsonb"],
        }

        result = collection.insert_one(doc)
        assert result.inserted_id is not None
        from neosqlite.objectid import ObjectId

        assert isinstance(result.inserted_id, ObjectId)

        # Retrieve and verify the document using the ObjectId
        found_doc = collection.find_one({"_id": result.inserted_id})
        assert found_doc["name"] == "Test User"
        assert found_doc["profile"]["age"] == 30
        assert found_doc["profile"]["settings"]["theme"] == "dark"
        assert found_doc["tags"] == ["python", "sqlite", "jsonb"]

        # Test updates
        collection.update_one(
            {"name": "Test User"}, {"$set": {"profile.settings.theme": "light"}}
        )

        updated_doc = collection.find_one({"_id": result.inserted_id})
        assert updated_doc["profile"]["settings"]["theme"] == "light"


def test_jsonb_query_operations():
    """Test querying JSONB data."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_query_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert test documents
        docs = [
            {
                "name": "Alice",
                "age": 25,
                "city": "New York",
                "skills": ["Python", "SQL"],
            },
            {
                "name": "Bob",
                "age": 30,
                "city": "San Francisco",
                "skills": ["JavaScript", "HTML"],
            },
            {
                "name": "Charlie",
                "age": 35,
                "city": "New York",
                "skills": ["Python", "JSONB"],
            },
        ]

        collection.insert_many(docs)

        # Test simple queries
        assert collection.count_documents({"city": "New York"}) == 2
        assert collection.count_documents({"age": {"$gt": 30}}) == 1

        # Test find_one
        user = collection.find_one({"name": "Alice"})
        assert user["age"] == 25
        assert "Python" in user["skills"]


def test_jsonb_indexing():
    """Test JSONB indexing capabilities."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_index_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert test documents
        docs = [
            {
                "name": "Alice",
                "profile": {"age": 25, "department": "Engineering"},
            },
            {"name": "Bob", "profile": {"age": 30, "department": "Marketing"}},
            {
                "name": "Charlie",
                "profile": {"age": 35, "department": "Engineering"},
            },
        ]

        collection.insert_many(docs)

        # Create an index on a nested field
        collection.create_index("profile.department")

        # Verify the index was created
        indexes = collection.list_indexes()
        assert any("profile_department" in idx for idx in indexes)

        # Test querying with the index
        engineers = list(collection.find({"profile.department": "Engineering"}))
        assert len(engineers) == 2


"""
Tests for JSONB support utilities to improve coverage.
"""

from unittest.mock import Mock

from neosqlite.collection.jsonb_support import (
    _contains_text_operator,
    _get_json_function_prefix,
    clear_jsonb_cache,
    should_use_json_functions,
    sqlite3,  # Import the same sqlite3 that the module uses
    supports_jsonb,
)


def test_get_json_function_prefix():
    """Test the _get_json_function_prefix function."""
    assert _get_json_function_prefix(True) == "jsonb"
    assert _get_json_function_prefix(False) == "json"


def test_should_use_json_functions_jsonb_not_supported():
    """Test should_use_json_functions when JSONB is not supported."""
    # When JSONB is not supported, it should always return True (use json functions)
    assert should_use_json_functions(query=None, jsonb_supported=False) is True
    assert (
        should_use_json_functions(query={"name": "test"}, jsonb_supported=False)
        is True
    )


def test_should_use_json_functions_jsonb_supported_no_query():
    """Test should_use_json_functions when JSONB is supported but no query provided."""
    # When JSONB is supported and no query is provided, it should return False (use jsonb functions)
    assert should_use_json_functions(query=None, jsonb_supported=True) is False


def test_should_use_json_functions_jsonb_supported_with_text_search():
    """Test should_use_json_functions when JSONB is supported but query has text search."""
    # Query with $text should return True (use json functions for FTS compatibility)
    query_with_text = {"$text": {"$search": "test"}}
    assert (
        should_use_json_functions(query=query_with_text, jsonb_supported=True)
        is True
    )


def test_should_use_json_functions_jsonb_supported_without_text_search():
    """Test should_use_json_functions when JSONB is supported and no text search."""
    # Query without $text should return False (use jsonb functions)
    query_without_text = {"name": "test"}
    assert (
        should_use_json_functions(
            query=query_without_text, jsonb_supported=True
        )
        is False
    )


def test_contains_text_operator_simple():
    """Test _contains_text_operator with simple $text query."""
    query = {"$text": {"$search": "test"}}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_not_text():
    """Test _contains_text_operator with non-text query."""
    query = {"name": "test"}
    assert _contains_text_operator(query) is False


def test_contains_text_operator_nested_in_and():
    """Test _contains_text_operator with $text nested in $and."""
    query = {"$and": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_or():
    """Test _contains_text_operator with $text nested in $or."""
    query = {"$or": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_nor():
    """Test _contains_text_operator with $text nested in $nor."""
    query = {"$nor": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_not():
    """Test _contains_text_operator with $text nested in $not."""
    query = {"$not": {"$text": {"$search": "search"}}}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_deeply_nested():
    """Test _contains_text_operator with deeply nested $text."""
    query = {
        "$and": [
            {"$or": [{"field": "value"}, {"$text": {"$search": "search"}}]}
        ]
    }
    assert _contains_text_operator(query) is True


def test_contains_text_operator_non_dict_input():
    """Test _contains_text_operator with non-dict input."""
    assert _contains_text_operator("not_a_dict") is False
    assert _contains_text_operator(["not", "a", "dict"]) is False
    assert _contains_text_operator(123) is False
    assert _contains_text_operator(None) is False


def test_contains_text_operator_empty_dict():
    """Test _contains_text_operator with empty dict."""
    assert _contains_text_operator({}) is False


def test_contains_text_operator_complex_nested_without_text():
    """Test _contains_text_operator with complex nesting without $text."""
    query = {
        "$and": [
            {"$or": [{"field1": "value1"}, {"field2": "value2"}]},
            {"$nor": [{"field3": "value3"}]},
        ]
    }
    assert _contains_text_operator(query) is False


def test_supports_jsonb_with_mock_connection():
    """Test supports_jsonb with a mock connection that simulates JSONB support."""
    # Clear cache to avoid stale results from other tests
    clear_jsonb_cache()

    # Test successful execution (JSONB supported)
    mock_connection_success = Mock()
    mock_connection_success.execute.return_value = Mock()
    assert supports_jsonb(mock_connection_success) is True

    # Clear cache before testing failure path
    clear_jsonb_cache()

    # Test failed execution (JSONB not supported)
    mock_connection_fail_for_test = Mock()
    mock_connection_fail_for_test.execute.side_effect = (
        sqlite3.OperationalError("no such function: jsonb")
    )
    assert supports_jsonb(mock_connection_fail_for_test) is False


"""
Tests for JSONB features with temporary table aggregation.
"""


# Try to import pysqlite3 for consistent JSON/JSONB support
try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


def test_jsonb_temporary_table_results():
    """Test that temporary table aggregation correctly handles JSONB data retrieval."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_temp_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert test documents
        docs = [
            {
                "name": "Alice",
                "orders": [
                    {"item": "laptop", "price": 1000},
                    {"item": "mouse", "price": 25},
                ],
            },
            {
                "name": "Bob",
                "orders": [
                    {"item": "monitor", "price": 300},
                    {"item": "keyboard", "price": 75},
                ],
            },
        ]

        collection.insert_many(docs)

        # Test a pipeline that uses temporary tables and would trigger the JSONB bug
        pipeline = [
            {"$unwind": "$orders"},
            {"$sort": {"orders.price": -1}},
            {"$limit": 3},
        ]

        # This should not raise a UnicodeDecodeError
        results = list(collection.aggregate(pipeline))

        # Verify we get results
        assert len(results) == 3

        # Verify the results are correctly decoded
        for doc in results:
            assert isinstance(doc, dict)
            assert "name" in doc
            assert "orders" in doc
            assert isinstance(
                doc["orders"], dict
            )  # After unwind, orders is a single object
            assert "item" in doc["orders"]
            assert "price" in doc["orders"]


def test_get_results_from_table_with_jsonb():
    """Test the _get_results_from_table method directly with JSONB data."""
    from neosqlite.collection.temporary_table_aggregation import (
        TemporaryTableAggregationProcessor,
    )

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["jsonb_direct_test"]

        # Verify that JSONB is supported
        try:
            conn.db.execute('SELECT jsonb(\'{"key": "value"}\')')
            jsonb_supported = True
        except sqlite3.OperationalError:
            jsonb_supported = False

        if not jsonb_supported:
            pytest.skip("JSONB not supported in this SQLite installation")

        # Insert a test document
        doc = {"name": "Test", "value": 42}
        collection.insert_one(doc)

        # Create a temporary table processor
        processor = TemporaryTableAggregationProcessor(collection)

        # Verify that JSONB is supported for this processor as well
        if not processor.jsonb.jsonb_supported:
            pytest.skip("JSONB not supported for temporary table processor")

        # Create a temporary table with JSONB data
        processor.db.execute(
            "CREATE TEMP TABLE test_jsonb_table (id INTEGER, data JSONB)"
        )
        processor.db.execute(
            "INSERT INTO test_jsonb_table (id, data) VALUES (1, jsonb(?))",
            (neosqlite.collection.json_helpers.neosqlite_json_dumps(doc),),
        )

        # This should not raise a UnicodeDecodeError
        results = processor._get_results_from_table("test_jsonb_table")

        # Verify the results
        assert len(results) == 1
        assert results[0]["name"] == "Test"
        assert results[0]["value"] == 42
