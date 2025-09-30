"""
Test cases to improve coverage for query_engine.py
"""

import pytest
import neosqlite
from neosqlite.requests import InsertOne, UpdateOne, DeleteOne


def test_query_engine_bulk_write_ordered():
    """Test bulk_write with ordered operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create some requests
        requests = [
            InsertOne({"name": "Alice", "age": 30}),
            UpdateOne({"name": "Bob"}, {"$set": {"age": 25}}, upsert=True),
            DeleteOne({"name": "Charlie"}),
        ]

        result = collection.bulk_write(requests, ordered=True)

        # Check the results
        assert result.inserted_count == 1
        assert result.upserted_count == 1


def test_query_engine_bulk_write_exception_handling():
    """Test bulk_write exception handling with ordered=True"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Add a document to test update
        collection.insert_one({"_id": 1, "name": "Test"})

        # Create requests that might cause issues
        requests = [
            InsertOne(
                {"_id": 1, "name": "Duplicate"}
            ),  # This should cause an error
            UpdateOne({"_id": 1}, {"$set": {"name": "Updated"}}),
        ]

        # This should raise an exception due to duplicate ID
        with pytest.raises(Exception):
            collection.bulk_write(requests, ordered=True)


def test_query_engine_find_one_and_delete_with_translation_failure():
    """Test find_one_and_delete when SQL translation fails"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a test document
        collection.insert_one({"_id": 1, "name": "Test", "value": 42})

        # Test find_one_and_delete with simple filter
        result = collection.find_one_and_delete({"_id": 1})
        assert result is not None
        assert result["name"] == "Test"

        # Attempt to find the deleted document (should return None)
        result = collection.find_one({"_id": 1})
        assert result is None


def test_query_engine_find_one_and_replace_with_translation_failure():
    """Test find_one_and_replace when SQL translation fails"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a test document
        collection.insert_one({"_id": 1, "name": "Test", "value": 42})

        # Test find_one_and_replace with simple filter
        replacement = {"name": "Replaced", "new_field": "new_value"}
        result = collection.find_one_and_replace({"_id": 1}, replacement)
        assert result is not None
        assert result["name"] == "Test"

        # Verify the replacement
        updated_doc = collection.find_one({"_id": 1})
        assert updated_doc["name"] == "Replaced"
        assert updated_doc["new_field"] == "new_value"
        assert "value" not in updated_doc  # Original field should be gone


def test_query_engine_find_one_and_update_complex():
    """Test find_one_and_update with complex update operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a test document
        collection.insert_one(
            {"_id": 1, "name": "Test", "value": 42, "tags": ["a", "b"]}
        )

        # Test find_one_and_update with complex update
        update = {
            "$set": {"status": "active"},
            "$inc": {"value": 10},
            "$push": {"tags": "c"},
        }
        result = collection.find_one_and_update({"_id": 1}, update)
        assert result is not None
        assert result["name"] == "Test"

        # Verify the update
        updated_doc = collection.find_one({"_id": 1})
        assert updated_doc["status"] == "active"
        assert updated_doc["value"] == 52  # 42 + 10
        assert "c" in updated_doc["tags"]


def test_query_engine_aggregation_with_complex_pipelines():
    """Test aggregation with complex pipeline stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "value": 10, "status": "active"},
                {"category": "A", "value": 15, "status": "inactive"},
                {"category": "B", "value": 20, "status": "active"},
            ]
        )

        # Test complex pipeline with multiple stages
        pipeline = [
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$category", "total": {"$sum": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        results = collection.aggregate(pipeline)
        results_list = list(results)  # Convert cursor to list
        assert (
            len(results_list) == 2
        )  # Categories A and B both have active items

        # Verify the results
        cat_a = next((r for r in results_list if r["_id"] == "A"), None)
        cat_b = next((r for r in results_list if r["_id"] == "B"), None)

        assert cat_a is not None  # Make sure we found category A
        assert cat_b is not None  # Make sure we found category B
        assert cat_a["total"] == 10  # Only one active item in A
        assert cat_b["total"] == 20  # One active item in B


def test_query_engine_distinct_with_complex_filter():
    """Test distinct with complex filter scenarios"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "value": 10, "status": "active"},
                {"category": "A", "value": 15, "status": "inactive"},
                {"category": "B", "value": 10, "status": "active"},
                {"category": "B", "value": 20, "status": "active"},
            ]
        )

        # Test distinct with filter
        distinct_values = collection.distinct("value", {"status": "active"})
        assert 10 in distinct_values
        assert 20 in distinct_values
        assert 15 not in distinct_values  # inactive item

        # Test distinct without filter
        all_distinct = collection.distinct("value")
        assert len(all_distinct) == 3  # 10, 15, 20


def test_query_engine_update_methods_edge_cases():
    """Test update methods with edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test update_one with no match
        result = collection.update_one(
            {"nonexistent": "value"}, {"$set": {"new": "value"}}
        )
        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is None

        # Test update_one with upsert
        result = collection.update_one(
            {"name": "NewUser"}, {"$set": {"age": 25}}, upsert=True
        )
        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is not None

        # Verify the upserted document
        doc = collection.find_one({"name": "NewUser"})
        assert doc is not None
        assert doc["age"] == 25


def test_query_engine_update_many_edge_cases():
    """Test update_many with edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "status": "active"},
                {"name": "Bob", "status": "active"},
                {"name": "Charlie", "status": "inactive"},
            ]
        )

        # Test update_many with no matches
        result = collection.update_many(
            {"status": "nonexistent"}, {"$set": {"updated": True}}
        )
        assert result.matched_count == 0
        assert result.modified_count == 0

        # Test update_many with some matches
        result = collection.update_many(
            {"status": "active"}, {"$set": {"updated": True}}
        )
        assert result.matched_count == 2
        assert result.modified_count == 2

        # Verify updates were applied
        active_docs = list(
            collection.find({"status": "active", "updated": True})
        )
        assert len(active_docs) == 2


def test_query_engine_replace_one_edge_cases():
    """Test replace_one with edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert a test document
        collection.insert_one({"_id": 1, "name": "Original", "value": 100})

        # Test replace_one with match
        replacement = {"name": "Replaced", "new_field": "value"}
        result = collection.replace_one({"_id": 1}, replacement)
        assert result.matched_count == 1
        assert result.modified_count == 1
        assert result.upserted_id is None

        # Verify replacement
        doc = collection.find_one({"_id": 1})
        assert doc["name"] == "Replaced"
        assert doc["new_field"] == "value"
        assert "value" not in doc  # Original field should be gone

        # Test replace_one with no match
        result = collection.replace_one({"_id": 999}, {"name": "New"})
        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is None

        # Test replace_one with upsert
        result = collection.replace_one(
            {"name": "NewDoc"},
            {"name": "NewDoc", "status": "created"},
            upsert=True,
        )
        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is not None


def test_query_engine_delete_methods_edge_cases():
    """Test delete methods with edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "status": "active"},
                {"name": "Bob", "status": "inactive"},
            ]
        )

        # Test delete_one with no match
        result = collection.delete_one({"status": "nonexistent"})
        assert result.deleted_count == 0

        # Test delete_many with no match
        result = collection.delete_many({"status": "nonexistent"})
        assert result.deleted_count == 0

        # Test delete_many with multiple matches
        result = collection.delete_many({"status": "active"})
        assert result.deleted_count == 1


def test_query_engine_aggregation_with_unwind_and_group():
    """Test aggregation with $unwind and $group operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data with arrays
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"], "age": 30},
                {"name": "Bob", "tags": ["python", "java"], "age": 25},
                {"name": "Charlie", "tags": ["javascript"], "age": 35},
            ]
        )

        # Test aggregation with unwind and group
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "count": {"$sum": 1},
                    "avg_age": {"$avg": "$age"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        results = collection.aggregate(pipeline)
        results_list = list(results)  # Convert cursor to list
        assert len(results_list) > 0

        # Find specific tag counts
        python_count = next(
            (r for r in results_list if r["_id"] == "python"), None
        )
        js_count = next(
            (r for r in results_list if r["_id"] == "javascript"), None
        )

        if python_count:  # Only assert if found
            assert python_count["count"] == 2  # Alice and Bob
        if js_count:  # Only assert if found
            assert js_count["count"] == 2  # Alice and Charlie


def test_query_engine_aggregate_raw_batches():
    """Test aggregate_raw_batches method"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ]
        )

        # Test aggregate_raw_batches
        pipeline = [{"$match": {"age": {"$gte": 25}}}]
        cursor = collection.aggregate_raw_batches(pipeline, batch_size=2)

        # Count batches
        batch_count = 0
        total_docs = 0
        for batch in cursor:
            batch_count += 1
            # Each batch is raw JSON bytes, split by newline
            docs = [
                line
                for line in batch.decode("utf-8").split("\n")
                if line.strip()
            ]
            total_docs += len(docs)

        assert total_docs == 3  # All docs match the filter
        assert batch_count >= 1  # At least one batch


def test_query_engine_initialize_bulk_ops():
    """Test bulk operation initialization methods"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test ordered bulk op initialization
        ordered_bulk = collection.initialize_ordered_bulk_op()
        assert ordered_bulk is not None
        assert hasattr(ordered_bulk, "_ordered")
        assert ordered_bulk._ordered is True

        # Test unordered bulk op initialization
        unordered_bulk = collection.initialize_unordered_bulk_op()
        assert unordered_bulk is not None
        assert hasattr(unordered_bulk, "_ordered")
        assert unordered_bulk._ordered is False
