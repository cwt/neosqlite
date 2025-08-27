# coding: utf-8
from neosqlite import InsertOne, UpdateOne, DeleteOne
from typing import Tuple, Type
import neosqlite
import sqlite3

# Handle both standard sqlite3 and pysqlite3 exceptions
try:
    import pysqlite3.dbapi2 as sqlite3_with_jsonb  # type: ignore

    IntegrityError: Tuple[Type[Exception], ...] = (
        sqlite3.IntegrityError,
        sqlite3_with_jsonb.IntegrityError,
    )
except ImportError:
    IntegrityError = (sqlite3.IntegrityError,)


def test_bulk_write_ordered_parameter(collection):
    """Test bulk_write with ordered parameter."""
    # Test ordered=True (default behavior)
    requests = [
        InsertOne({"name": "Alice", "age": 25}),
        InsertOne({"name": "Bob", "age": 30}),
    ]

    # Test with ordered=True
    result = collection.bulk_write(requests, ordered=True)
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 2

    # Verify documents were inserted
    assert collection.count_documents({}) == 2
    alice = collection.find_one({"name": "Alice"})
    bob = collection.find_one({"name": "Bob"})
    assert alice is not None and alice["age"] == 25
    assert bob is not None and bob["age"] == 30

    # Clear collection for next test
    collection.db.execute(f"DELETE FROM {collection.name}")

    # Test with ordered=False
    result = collection.bulk_write(requests, ordered=False)
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 2

    # Verify documents were inserted
    assert collection.count_documents({}) == 2
    alice = collection.find_one({"name": "Alice"})
    bob = collection.find_one({"name": "Bob"})
    assert alice is not None and alice["age"] == 25
    assert bob is not None and bob["age"] == 30


def test_bulk_write_ordered_vs_unordered_behavior(collection):
    """Test that ordered and unordered parameters are accepted (behavior may be same for now)."""
    # Since our implementation executes operations sequentially anyway,
    # both ordered=True and ordered=False should work the same way
    # This test ensures the parameter is accepted without error

    requests = [
        InsertOne({"test": "ordered"}),
        UpdateOne({"test": "ordered"}, {"$set": {"updated": True}}),
    ]

    # Both should work without error
    result_ordered = collection.bulk_write(requests, ordered=True)
    assert result_ordered.inserted_count == 1
    assert result_ordered.matched_count == 1

    # Clear collection
    collection.db.execute(f"DELETE FROM {collection.name}")

    result_unordered = collection.bulk_write(requests, ordered=False)
    assert result_unordered.inserted_count == 1
    assert result_unordered.matched_count == 1


def test_bulk_write_ordered_with_mixed_operations(collection):
    """Test bulk_write with ordered parameter and mixed operations."""
    # Insert some initial data
    collection.insert_many(
        [{"name": "Charlie", "age": 35}, {"name": "David", "age": 40}]
    )

    requests = [
        InsertOne({"name": "Eve", "age": 28}),
        UpdateOne({"name": "Charlie"}, {"$set": {"age": 36}}),
        DeleteOne({"name": "David"}),
        UpdateOne({"name": "Eve"}, {"$set": {"age": 29}}, upsert=True),
    ]

    # Test with ordered=True
    result = collection.bulk_write(requests, ordered=True)
    assert result.inserted_count == 1
    assert result.matched_count == 2  # Charlie update (1) + Eve update (1)
    assert result.modified_count == 2  # Charlie modified (1) + Eve modified (1)
    assert result.deleted_count == 1
    assert (
        result.upserted_count == 0
    )  # Eve already exists, so it's an update, not an upsert

    # Verify final state
    assert collection.count_documents({}) == 2  # Eve and Charlie remain
    eve = collection.find_one({"name": "Eve"})
    charlie = collection.find_one({"name": "Charlie"})
    assert eve is not None and eve["age"] == 29
    assert charlie is not None and charlie["age"] == 36
    assert collection.find_one({"name": "David"}) is None  # David was deleted
