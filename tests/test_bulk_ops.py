# coding: utf-8
import pytest
import neosqlite
from neosqlite import InsertOne, UpdateOne, DeleteOne


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
