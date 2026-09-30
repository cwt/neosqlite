"""
Tests for bulk write operations.
"""

from typing import Tuple, Type

from pytest import raises

import neosqlite
from neosqlite import (
    BulkWriteError,
    DeleteOne,
    InsertOne,
    UpdateOne,
)
from neosqlite.collection import sqlite3

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
    """Test difference between ordered and unordered bulk write error handling."""
    collection.create_index("x", unique=True)
    collection.insert_one({"x": 10})

    # requests:
    # 0: Insert x=1 (success)
    # 1: Insert x=10 (fail: duplicate)
    # 2: Insert x=2 (should not run in ordered=True, should run in ordered=False)
    requests = [
        InsertOne({"x": 1}),
        InsertOne({"x": 10}),
        InsertOne({"x": 2}),
    ]

    # ordered=True: stops after failing at index 1
    with raises(BulkWriteError) as exc_info:
        collection.bulk_write(requests, ordered=True)
    err = exc_info.value.details
    assert err["nInserted"] == 1
    assert len(err["writeErrors"]) == 1
    assert err["writeErrors"][0]["index"] == 1
    assert collection.find_one({"x": 1}) is not None
    assert collection.find_one({"x": 2}) is None

    # Clear inserted documents except initial
    collection.delete_many({"x": {"$ne": 10}})

    # ordered=False: continues after failure at index 1 and executes index 2
    with raises(BulkWriteError) as exc_info:
        collection.bulk_write(requests, ordered=False)
    err = exc_info.value.details
    assert err["nInserted"] == 2
    assert len(err["writeErrors"]) == 1
    assert err["writeErrors"][0]["index"] == 1
    assert collection.find_one({"x": 1}) is not None
    assert collection.find_one({"x": 2}) is not None


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
    assert 0 in result.upserted_ids
    assert collection.count_documents({}) == 1


def test_bulk_write_rollback(collection):
    collection.create_index("a", unique=True)
    collection.insert_one({"a": 1})
    requests = [
        InsertOne({"a": 2}),
        InsertOne({"a": 1}),  # This will fail
    ]
    with raises(BulkWriteError) as exc_info:
        collection.bulk_write(requests)
    assert exc_info.value.details["nInserted"] == 1
    assert len(exc_info.value.details["writeErrors"]) == 1
    assert exc_info.value.details["writeErrors"][0]["index"] == 1
    assert collection.count_documents({}) == 2
    assert collection.find_one({"a": 2}) is not None


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


def test_bulk_operation_executor_ordered_and_unordered(collection):
    collection.create_index("b", unique=True)
    collection.insert_one({"b": 10})

    # Ordered executor
    ordered_op = collection.initialize_ordered_bulk_op()
    ordered_op.insert({"b": 1})
    ordered_op.insert({"b": 10})  # duplicate error
    ordered_op.insert({"b": 2})
    with raises(BulkWriteError) as exc_info:
        ordered_op.execute()
    assert exc_info.value.details["nInserted"] == 1
    assert len(exc_info.value.details["writeErrors"]) == 1
    assert exc_info.value.details["writeErrors"][0]["index"] == 1
    assert collection.find_one({"b": 1}) is not None
    assert collection.find_one({"b": 2}) is None

    collection.delete_many({"b": {"$ne": 10}})

    # Unordered executor
    unordered_op = collection.initialize_unordered_bulk_op()
    unordered_op.insert({"b": 1})
    unordered_op.insert({"b": 10})  # duplicate error
    unordered_op.insert({"b": 2})
    with raises(BulkWriteError) as exc_info:
        unordered_op.execute()
    assert exc_info.value.details["nInserted"] == 2
    assert len(exc_info.value.details["writeErrors"]) == 1
    assert exc_info.value.details["writeErrors"][0]["index"] == 1
    assert collection.find_one({"b": 1}) is not None
    assert collection.find_one({"b": 2}) is not None


def test_bulk_write_pymongo_operations(collection):
    import pymongo.operations as p_ops

    requests = [
        p_ops.InsertOne({"k": 1, "v": "a"}),
        p_ops.InsertOne({"k": 2, "v": "b"}),
        p_ops.UpdateOne({"k": 1}, {"$set": {"v": "updated_a"}}),
        p_ops.UpdateMany(
            {"v": {"$regex": "^updated_"}}, {"$set": {"flag": True}}
        ),
        p_ops.ReplaceOne({"k": 2}, {"k": 2, "v": "replaced_b"}),
        p_ops.DeleteOne({"k": 1}),
        p_ops.DeleteMany({"k": 2}),
    ]
    result = collection.bulk_write(requests)
    assert result.inserted_count == 2
    assert result.matched_count >= 2
    assert result.modified_count >= 2
    assert result.deleted_count == 2
    assert collection.count_documents({}) == 0


def test_bulk_operation_executor_add_pymongo_and_duck_typed(collection):
    import pymongo.operations as p_ops

    executor = collection.initialize_ordered_bulk_op()
    executor.add(p_ops.InsertOne({"c": 1}))
    executor.add(p_ops.UpdateOne({"c": 1}, {"$set": {"c": 2}}))
    executor.add(p_ops.ReplaceOne({"c": 2}, {"c": 3, "name": "three"}))
    executor.add(p_ops.DeleteOne({"c": 3}))
    res = executor.execute()
    assert res.inserted_count == 1
    assert res.matched_count == 2
    assert res.deleted_count == 1
    assert collection.count_documents({}) == 0
