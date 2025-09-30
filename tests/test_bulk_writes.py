"""
Tests for bulk write operations.
"""

from pytest import raises
from typing import Tuple, Type
import neosqlite
from neosqlite.collection import sqlite3
from neosqlite import InsertOne, UpdateOne, DeleteOne

# Handle both standard sqlite3 and pysqlite3 exceptions
try:
    import pysqlite3.dbapi2 as sqlite3_with_jsonb  # type: ignore

    IntegrityError: Tuple[Type[Exception], ...] = (
        sqlite3.IntegrityError,
        sqlite3_with_jsonb.IntegrityError,
    )
except ImportError:
    IntegrityError = (sqlite3.IntegrityError,)


def test_initialize_ordered_bulk_op(collection):
    """Test initialize_ordered_bulk_op functionality."""
    # Initialize an ordered bulk operation
    bulk_op = collection.initialize_ordered_bulk_op()

    # Add operations to the bulk operation
    bulk_op.insert({"name": "Alice", "age": 25})
    bulk_op.find({"name": "Alice"}).update_one({"$set": {"age": 26}})
    bulk_op.find({"name": "Alice"}).delete_one()

    # Execute the bulk operation
    result = bulk_op.execute()

    # Verify the result
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 1
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.deleted_count == 1

    # Verify that Alice was deleted
    assert collection.count_documents({}) == 0


def test_initialize_unordered_bulk_op(collection):
    """Test initialize_unordered_bulk_op functionality."""
    # Initialize an unordered bulk operation
    bulk_op = collection.initialize_unordered_bulk_op()

    # Add operations to the bulk operation
    bulk_op.insert({"name": "Bob", "age": 30})
    bulk_op.insert({"name": "Charlie", "age": 35})
    bulk_op.find({"name": "Bob"}).update_one({"$set": {"age": 31}})

    # Execute the bulk operation
    result = bulk_op.execute()

    # Verify the result
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 2
    assert result.matched_count == 1
    assert result.modified_count == 1

    # Verify the data
    assert collection.count_documents({}) == 2
    bob = collection.find_one({"name": "Bob"})
    assert bob["age"] == 31


def test_bulk_op_with_upsert(collection):
    """Test bulk operations with upsert."""
    # Test ordered bulk op with upsert
    bulk_op = collection.initialize_ordered_bulk_op()
    bulk_op.find({"name": "David"}).upsert().update_one({"$set": {"age": 40}})
    result = bulk_op.execute()

    assert result.upserted_count == 1
    assert collection.count_documents({}) == 1
    david = collection.find_one({"name": "David"})
    assert david["age"] == 40


def test_empty_bulk_op(collection):
    """Test executing an empty bulk operation."""
    # Test ordered bulk op
    bulk_op = collection.initialize_ordered_bulk_op()
    result = bulk_op.execute()

    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 0
    assert result.matched_count == 0
    assert result.modified_count == 0
    assert result.deleted_count == 0
    assert result.upserted_count == 0


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


def test_bulk_write(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    requests = [
        InsertOne({"a": 4}),
        UpdateOne({"a": 1}, {"$set": {"a": 10}}),
        DeleteOne({"a": 2}),
    ]
    result = collection.bulk_write(requests)
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 1
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.deleted_count == 1
    assert collection.count_documents({}) == 3
    assert collection.find_one({"a": 10}) is not None
    assert collection.find_one({"a": 4}) is not None
    assert collection.find_one({"a": 2}) is None


def test_bulk_write_with_upsert(collection):
    requests = [UpdateOne({"a": 1}, {"$set": {"a": 10}}, upsert=True)]
    result = collection.bulk_write(requests)
    assert result.upserted_count == 1
    assert collection.count_documents({}) == 1


def test_bulk_write_rollback(collection):
    collection.create_index("a", unique=True)
    collection.insert_one({"a": 1})
    requests = [
        InsertOne({"a": 2}),
        InsertOne({"a": 1}),  # This will fail
    ]
    with raises(IntegrityError):
        collection.bulk_write(requests)
    assert collection.count_documents({}) == 1
    assert collection.find_one({"a": 2}) is None


def test_bulk_write_ordered_parameter_from_collection_bulk(collection):
    """Test bulk_write with ordered parameter"""
    # Test with ordered=True (default)
    collection.insert_many([{"a": 1}, {"a": 2}])
    requests = [
        InsertOne({"a": 3}),
        UpdateOne({"a": 1}, {"$set": {"a": 10}}),
        DeleteOne({"a": 2}),
    ]
    result = collection.bulk_write(requests, ordered=True)
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 1
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.deleted_count == 1

    # Verify the operations were executed
    assert collection.count_documents({}) == 2
    assert collection.find_one({"a": 10}) is not None
    assert collection.find_one({"a": 3}) is not None
    assert collection.find_one({"a": 2}) is None

    # Test with ordered=False
    collection.delete_many({})
    collection.insert_many([{"a": 1}, {"a": 2}])
    requests = [
        InsertOne({"a": 4}),
        UpdateOne({"a": 1}, {"$set": {"a": 10}}),
        DeleteOne({"a": 2}),
    ]
    result = collection.bulk_write(requests, ordered=False)
    assert isinstance(result, neosqlite.BulkWriteResult)
    assert result.inserted_count == 1
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.deleted_count == 1

    # Verify the operations were executed
    assert collection.count_documents({}) == 2
    assert collection.find_one({"a": 10}) is not None
    assert collection.find_one({"a": 4}) is not None
    assert collection.find_one({"a": 2}) is None
