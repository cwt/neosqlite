"""
Tests for JSONB features with temporary table aggregation.
"""

import neosqlite
import pytest

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
        if not processor._jsonb_supported:
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
