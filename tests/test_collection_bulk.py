# coding: utf-8
from neosqlite import InsertOne, UpdateOne, DeleteOne
from pytest import raises
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


def test_bulk_write_ordered_parameter(collection):
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
