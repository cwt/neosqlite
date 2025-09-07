"""
Tests for collection operations.
"""

import pytest
from pytest import raises
from typing import Tuple, Type
from unittest.mock import MagicMock
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

from neosqlite import InsertOne, UpdateOne, DeleteOne
from neosqlite.query_operators import _eq, _gt, _lt, _lte, _in


def test_rename_collection_already_exists():
    """Test renaming a collection to a name that already exists."""
    db = neosqlite.Connection(":memory:")

    # Create two collections
    collection1 = db["test1"]
    collection2 = db["test2"]
    collection1.insert_one({"foo": "bar"})
    collection2.insert_one({"baz": "qux"})

    # Try to rename collection1 to collection2's name
    with pytest.raises(sqlite3.Error):
        collection1.rename("test2")


# Tests for collection operations


def test_create(collection):
    """Test collection creation."""
    row = collection.db.execute(
        "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
        (collection.name,),
    ).fetchone()
    assert row[0] == 1


def test_insert_one(collection):
    """Test inserting a single document."""
    doc = {"foo": "bar"}
    result = collection.insert_one(doc)
    assert isinstance(result, neosqlite.InsertOneResult)
    assert result.inserted_id == 1
    assert doc["_id"] == 1
    found = collection.find_one({"_id": 1})
    assert found["foo"] == "bar"


def test_insert_many(collection):
    """Test inserting multiple documents."""
    docs = [{"foo": "bar"}, {"foo": "baz"}]
    result = collection.insert_many(docs)
    assert isinstance(result, neosqlite.InsertManyResult)
    assert result.inserted_ids == [1, 2]
    assert collection.count_documents({}) == 2


def test_insert_non_dict_raise(collection):
    """Test that inserting non-dict documents raises an exception."""
    doc = "{'foo': 'bar'}"
    with raises(neosqlite.MalformedDocument):
        collection.insert_one(doc)


def test_replace_one(collection):
    """Test replacing a single document."""
    collection.insert_one({"foo": "bar"})
    result = collection.replace_one({"foo": "bar"}, {"foo": "baz"})
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert collection.find_one({"foo": "bar"}) is None
    assert collection.find_one({"foo": "baz"}) is not None


def test_update_one(collection):
    """Test updating a single document."""
    collection.insert_one({"foo": "bar", "count": 1})
    result = collection.update_one(
        {"foo": "bar"}, {"$set": {"foo": "baz"}, "$inc": {"count": 1}}
    )
    assert result.matched_count == 1
    assert result.modified_count == 1
    updated_doc = collection.find_one({"_id": 1})
    assert updated_doc["foo"] == "baz"
    assert updated_doc["count"] == 2


def test_update_many(collection):
    """Test updating multiple documents."""
    collection.insert_many([{"foo": "bar"}, {"foo": "bar"}])
    result = collection.update_many({"foo": "bar"}, {"$set": {"foo": "baz"}})
    assert result.matched_count == 2
    assert result.modified_count == 2
    assert collection.count_documents({"foo": "baz"}) == 2


def test_update_many_fast_path(collection):
    """Test fast path for updating multiple documents."""
    collection.insert_many(
        [
            {"a": 1, "b": 1, "c": 0},
            {"a": 1, "b": 2, "c": 0},
            {"a": 2, "b": 1, "c": 0},
        ]
    )
    result = collection.update_many(
        {"a": 1}, {"$set": {"b": 5}, "$inc": {"c": 1}}
    )
    assert result.matched_count == 2
    assert result.modified_count == 2
    docs = list(collection.find({"a": 1}))
    assert len(docs) == 2
    assert all(doc["b"] == 5 for doc in docs)
    assert all(doc["c"] == 1 for doc in docs)


def test_delete_one(collection):
    """Test deleting a single document."""
    collection.insert_one({"foo": "bar"})
    result = collection.delete_one({"foo": "bar"})
    assert result.deleted_count == 1
    assert collection.count_documents({}) == 0


def test_delete_many(collection):
    """Test deleting multiple documents."""
    collection.insert_many([{"foo": "bar"}, {"foo": "bar"}])
    result = collection.delete_many({"foo": "bar"})
    assert result.deleted_count == 2
    assert collection.count_documents({}) == 0


def test_delete_many_fast_path(collection):
    """Test fast path for deleting multiple documents."""
    collection.insert_many(
        [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1}]
    )
    result = collection.delete_many({"a": 1})
    assert result.deleted_count == 2
    assert collection.count_documents({}) == 1
    assert collection.find_one({})["a"] == 2


def test_transaction(connection):
    """Test transaction handling."""
    collection = connection["foo"]
    try:
        connection.db.execute("BEGIN")
        collection.insert_one({"a": 1})
        collection.insert_one({"a": 2})
        connection.db.rollback()
    except sqlite3.OperationalError:
        pass
    assert collection.count_documents({}) == 0

    connection.db.execute("BEGIN")
    collection.insert_one({"a": 1})
    collection.insert_one({"a": 2})
    connection.db.commit()
    assert collection.count_documents({}) == 2


def test_update_with_push(collection):
    """Test $push operator in updates."""
    collection.insert_one({"a": 1, "items": ["x"]})
    collection.update_one({"a": 1}, {"$push": {"items": "y"}})
    doc = collection.find_one({"a": 1})
    assert doc["items"] == ["x", "y"]


def test_update_with_pull(collection):
    """Test $pull operator in updates."""
    collection.insert_one({"a": 1, "items": ["x", "y", "z", "y"]})
    collection.update_one({"a": 1}, {"$pull": {"items": "y"}})
    doc = collection.find_one({"a": 1})
    assert doc["items"] == ["x", "z"]


def test_update_with_pop_last(collection):
    """Test $pop operator with positive value in updates."""
    collection.insert_one({"a": 1, "items": ["x", "y", "z"]})
    collection.update_one({"a": 1}, {"$pop": {"items": 1}})
    doc = collection.find_one({"a": 1})
    assert doc["items"] == ["x", "y"]


def test_update_with_pop_first(collection):
    """Test $pop operator with negative value in updates."""
    collection.insert_one({"a": 1, "items": ["x", "y", "z"]})
    collection.update_one({"a": 1}, {"$pop": {"items": -1}})
    doc = collection.find_one({"a": 1})
    assert doc["items"] == ["y", "z"]


def test_update_with_rename(collection):
    """Test $rename operator in updates."""
    collection.insert_one({"a": 1, "foo": "bar"})
    collection.update_one({"a": 1}, {"$rename": {"foo": "baz"}})
    doc = collection.find_one({"a": 1})
    assert "foo" not in doc
    assert "baz" in doc
    assert doc["baz"] == "bar"


def test_update_with_mul(collection):
    """Test $mul operator in updates."""
    collection.insert_one({"a": 1, "value": 5})
    collection.update_one({"a": 1}, {"$mul": {"value": 2}})
    doc = collection.find_one({"a": 1})
    assert doc["value"] == 10


def test_update_with_min(collection):
    """Test $min operator in updates."""
    collection.insert_one({"a": 1, "value": 10})
    collection.update_one({"a": 1}, {"$min": {"value": 5}})
    doc = collection.find_one({"a": 1})
    assert doc["value"] == 5
    collection.update_one({"a": 1}, {"$min": {"value": 10}})
    doc = collection.find_one({"a": 1})
    assert doc["value"] == 5


def test_update_with_max(collection):
    """Test $max operator in updates."""
    collection.insert_one({"a": 1, "value": 5})
    collection.update_one({"a": 1}, {"$max": {"value": 10}})
    doc = collection.find_one({"a": 1})
    assert doc["value"] == 10
    collection.update_one({"a": 1}, {"$max": {"value": 5}})
    doc = collection.find_one({"a": 1})
    assert doc["value"] == 10


def test_find_one_and_update(collection):
    """Test find_one_and_update functionality."""
    collection.insert_one({"foo": "bar", "count": 1})
    doc = collection.find_one_and_update(
        {"foo": "bar"}, {"$set": {"foo": "baz"}, "$inc": {"count": 1}}
    )
    assert doc is not None
    updated_doc = collection.find_one({"_id": doc["_id"]})
    assert updated_doc["foo"] == "baz"
    assert updated_doc["count"] == 2


def test_find_one_and_replace(collection):
    """Test find_one_and_replace functionality."""
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_replace({"foo": "bar"}, {"foo": "baz"})
    assert doc is not None
    assert collection.find_one({"foo": "bar"}) is None
    assert collection.find_one({"foo": "baz"}) is not None


def test_find_one_and_delete(collection):
    """Test find_one_and_delete functionality."""
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_delete({"foo": "bar"})
    assert doc is not None
    assert collection.count_documents({}) == 0


def test_count_documents(collection):
    """Test counting documents."""
    collection.insert_many([{}, {}, {}])
    assert collection.count_documents({}) == 3
    assert collection.count_documents({"foo": "bar"}) == 0


def test_estimated_document_count(collection):
    """Test estimated document count."""
    collection.insert_many([{}, {}, {}])
    assert collection.estimated_document_count() == 3


def test_distinct(collection):
    """Test distinct values."""
    collection.insert_many(
        [{"foo": "bar"}, {"foo": "baz"}, {"foo": 10}, {"bar": "foo"}]
    )
    assert set(("bar", "baz", 10)) == collection.distinct("foo")


def test_distinct_nested(collection):
    """Test distinct values with nested keys."""
    collection.insert_many(
        [
            {"a": {"b": 1}},
            {"a": {"b": 2}},
            {"a": {"b": 1}},
            {"a": {"c": 1}},
        ]
    )
    assert {1, 2} == collection.distinct("a.b")


def test_distinct_no_match(collection):
    """Test distinct with no matching documents."""
    collection.insert_many([{"foo": "bar"}])
    assert set() == collection.distinct("nonexistent")


def test_distinct_with_null(collection):
    """Test distinct with null values."""
    collection.insert_many([{"foo": "bar"}, {"foo": None}])
    assert {"bar"} == collection.distinct("foo")


def test_distinct_complex_types(collection):
    """Test distinct with complex types."""
    collection.insert_many(
        [
            {"foo": [1, 2]},
            {"foo": [1, 2]},
            {"foo": [2, 3]},
            {"foo": {"a": 1}},
            {"foo": {"a": 1}},
        ]
    )
    results = collection.distinct("foo")
    assert len(results) == 3
    results = collection.distinct("foo")
    assert len(results) == 3
    assert (1, 2) in results
    assert (2, 3) in results
    assert '{"a": 1}' in results


def test_distinct_with_filter(collection):
    """Test distinct with filter."""
    collection.insert_many(
        [
            {"category": "A", "value": 1},
            {"category": "A", "value": 2},
            {"category": "B", "value": 1},
            {"category": "A", "value": 1},
        ]
    )
    assert {1, 2} == collection.distinct("value", filter={"category": "A"})


def test_distinct_with_filter_no_match(collection):
    """Test distinct with filter that matches no documents."""
    collection.insert_many(
        [
            {"category": "A", "value": 1},
            {"category": "B", "value": 2},
        ]
    )
    assert set() == collection.distinct("value", filter={"category": "C"})


def test_distinct_with_filter_and_nested_key(collection):
    """Test distinct with filter and nested key."""
    collection.insert_many(
        [
            {"group": "X", "data": {"value": 10}},
            {"group": "Y", "data": {"value": 20}},
            {"group": "X", "data": {"value": 10}},
            {"group": "X", "data": {"value": 30}},
        ]
    )
    assert {10, 30} == collection.distinct("data.value", filter={"group": "X"})


def test_rename(collection):
    """Test renaming a collection."""
    collection.insert_one({"foo": "bar"})
    collection.rename("new_collection")
    assert collection.name == "new_collection"
    assert collection.find_one({"foo": "bar"}) is not None


def test_rename_to_existing_collection(collection):
    """Test renaming to an existing collection name."""
    collection.insert_one({"foo": "bar"})
    collection.database.new_collection.insert_one({"baz": "qux"})
    try:
        collection.rename("new_collection")
        assert False, "Should have raised an exception"
    except Exception:
        pass


def test_options(collection):
    """Test collection options."""
    options = collection.options()
    assert options["name"] == collection.name
    assert "columns" in options
    assert "indexes" in options
    assert "count" in options


def test_database_property(collection):
    """Test database property."""
    assert isinstance(collection.database, neosqlite.Connection)
    assert (
        collection.database["some_other_collection"].name
        == "some_other_collection"
    )


def test_find_returns_cursor(collection):
    """Test that find returns a cursor."""
    cursor = collection.find()
    assert isinstance(cursor, neosqlite.Cursor)


def test_find_with_sort(collection):
    """Test find with sorting."""
    collection.insert_many(
        [
            {"a": 1, "b": "c"},
            {"a": 1, "b": "a"},
            {"a": 5, "b": "x"},
            {"a": 3, "b": "x"},
            {"a": 4, "b": "z"},
        ]
    )
    docs = list(collection.find().sort("a", neosqlite.ASCENDING))
    assert [d["a"] for d in docs] == [1, 1, 3, 4, 5]
    docs = list(collection.find().sort("a", neosqlite.DESCENDING))
    assert [d["a"] for d in docs] == [5, 4, 3, 1, 1]
    docs = list(
        collection.find().sort(
            [("a", neosqlite.ASCENDING), ("b", neosqlite.DESCENDING)]
        )
    )
    a_vals = [d["a"] for d in docs]
    b_vals = [d["b"] for d in docs]
    assert a_vals == [1, 1, 3, 4, 5]
    assert b_vals == ["c", "a", "x", "z", "x"]


def test_find_with_skip_and_limit(collection):
    """Test find with skip and limit."""
    collection.insert_many([{"i": i} for i in range(10)])
    docs = list(collection.find().skip(5))
    assert len(docs) == 5
    assert docs[0]["i"] == 5
    docs = list(collection.find().limit(5))
    assert len(docs) == 5
    assert docs[0]["i"] == 0
    docs = list(collection.find().skip(2).limit(3))
    assert len(docs) == 3
    assert [d["i"] for d in docs] == [2, 3, 4]


def test_find_with_sort_on_nested_key(collection):
    """Test find with sorting on nested keys."""
    collection.insert_many(
        [
            {"a": {"b": 5}, "c": "B"},
            {"a": {"b": 9}, "c": "A"},
            {"a": {"b": 7}, "c": "C"},
        ]
    )
    docs = list(collection.find().sort("a.b", neosqlite.ASCENDING))
    assert [d["a"]["b"] for d in docs] == [5, 7, 9]
    docs = list(collection.find().sort("a.b", neosqlite.DESCENDING))
    assert [d["a"]["b"] for d in docs] == [9, 7, 5]


def test_find_one(collection):
    """Test find_one functionality."""
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"
    assert collection.find_one({"foo": "baz"}) is None


def test_find_one_with_projection_inclusion(collection):
    """Test find_one with inclusion projection."""
    collection.insert_one({"foo": "bar", "baz": 42, "qux": [1, 2]})
    doc = collection.find_one({"foo": "bar"}, {"foo": 1, "baz": 1})
    assert doc is not None
    assert "foo" in doc
    assert "baz" in doc
    assert "qux" not in doc
    assert "_id" in doc


def test_find_one_with_projection_exclusion(collection):
    """Test find_one with exclusion projection."""
    collection.insert_one({"foo": "bar", "baz": 42, "qux": [1, 2]})
    doc = collection.find_one({"foo": "bar"}, {"qux": 0, "_id": 0})
    assert doc is not None
    assert "foo" in doc
    assert "baz" in doc
    assert "qux" not in doc
    assert "_id" not in doc


def test_find_one_with_projection_id_only(collection):
    """Test find_one with _id only projection."""
    collection.insert_one({"foo": "bar", "baz": 42})
    doc = collection.find_one({"foo": "bar"}, {"_id": 1})
    assert doc is not None
    assert doc.keys() == {"_id"}


def test_find_with_projection(collection):
    """Test find with projection."""
    collection.insert_many(
        [
            {"a": 1, "b": "c", "d": True},
            {"a": 1, "b": "a", "d": False},
        ]
    )
    docs = list(collection.find(projection={"a": 1, "_id": 0}))
    assert len(docs) == 2
    for doc in docs:
        assert doc.keys() == {"a"}


def test_find_with_in_operator(collection):
    """Test find with $in operator."""
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    docs = list(collection.find({"a": {"$in": [1, 3]}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 3}


def test_find_with_nin_operator(collection):
    """Test find with $nin operator."""
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    docs = list(collection.find({"a": {"$nin": [1, 3]}}))
    assert len(docs) == 1
    assert docs[0]["a"] == 2


def test_find_with_comparison_operators(collection):
    """Test find with comparison operators."""
    collection.insert_many([{"a": 1}, {"a": 5}, {"a": 10}])
    docs = list(collection.find({"a": {"$gt": 3}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {5, 10}
    docs = list(collection.find({"a": {"$gte": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {5, 10}
    docs = list(collection.find({"a": {"$lt": 7}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 5}
    docs = list(collection.find({"a": {"$lte": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 5}
    docs = list(collection.find({"a": {"$ne": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 10}


def test_create_index(collection):
    """Test creating an index."""
    collection.insert_one({"foo": "bar"})
    collection.create_index("foo")
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_index_on_nested_keys(collection):
    """Test creating an index on nested keys."""
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    collection.create_index("foo.bar")
    assert "idx_foo_foo_bar" in collection.list_indexes()


def test_reindex(collection):
    """Test reindexing."""
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.reindex("idx_foo_foo")


def test_insert_auto_index(collection):
    """Test automatic indexing on insert."""
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.insert_one({"foo": "baz"})
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_compound_index(collection):
    """Test creating a compound index."""
    collection.insert_one({"foo": "bar", "far": "boo"})
    collection.create_index(["foo", "far"])
    assert "idx_foo_foo_far" in collection.list_indexes()


def test_create_unique_index_violation(collection):
    """Test creating a unique index violation."""
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    with raises(IntegrityError):
        collection.insert_one({"foo": "bar"})


def test_update_to_break_uniqueness(collection):
    """Test updating to break uniqueness constraint."""
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    res = collection.insert_one({"foo": "baz"})
    with raises(IntegrityError):
        collection.update_one(
            {"_id": res.inserted_id}, {"$set": {"foo": "bar"}}
        )


def test_hint_index(collection):
    """Test using index hints."""
    collection.insert_many(
        [{"foo": "bar", "a": 1}, {"foo": "bar", "a": 2}, {"fox": "baz", "a": 3}]
    )
    collection.create_index("foo")
    docs_with_hint = list(
        collection.find({"foo": "bar", "a": 2}, hint=f"idx_foo_foo")
    )
    assert len(docs_with_hint) == 1
    assert docs_with_hint[0]["a"] == 2


def test_list_indexes(collection):
    """Test listing indexes."""
    collection.create_index("foo")
    indexes = collection.list_indexes()
    assert isinstance(indexes, list)
    assert "idx_foo_foo" in indexes


def test_drop_index(collection):
    """Test dropping an index."""
    collection.create_index("foo")
    collection.drop_index("foo")
    assert "idx_foo_foo" not in collection.list_indexes()


def test_drop_indexes(collection):
    """Test dropping all indexes."""
    collection.create_index("foo")
    collection.create_index("bar")
    collection.drop_indexes()
    assert len(collection.list_indexes()) == 0


def test_bulk_write(collection):
    """Test bulk write operations."""
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
    """Test bulk write with upsert."""
    requests = [UpdateOne({"a": 1}, {"$set": {"a": 10}}, upsert=True)]
    result = collection.bulk_write(requests)
    assert result.upserted_count == 1
    assert collection.count_documents({}) == 1


def test_bulk_write_rollback(collection):
    """Test bulk write rollback on error."""
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
    """Test bulk_write with ordered parameter."""
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


def test_collection_create_with_jsonb_support():
    """Test that Collection.create() uses JSONB when supported."""
    mock_db = MagicMock()

    def execute_side_effect(query, *args, **kwargs):
        if "jsonb(" in query.lower():
            return MagicMock()
        elif "CREATE TABLE" in query:
            mock_db.create_table_query = query
            return MagicMock()
        return MagicMock()

    mock_db.execute.side_effect = execute_side_effect
    collection = neosqlite.Collection(mock_db, "test_collection", create=False)
    collection.create()
    assert "data JSONB" in mock_db.create_table_query
    assert "data TEXT" not in mock_db.create_table_query


def test_collection_create_without_jsonb_support():
    """Test that Collection.create() falls back to TEXT when JSONB is not supported."""
    mock_db = MagicMock()

    def execute_side_effect(query, *args, **kwargs):
        if "jsonb(" in query.lower():
            raise sqlite3.OperationalError("JSONB not supported")
        elif "CREATE TABLE" in query:
            mock_db.create_table_query = query
            return MagicMock()
        return MagicMock()

    mock_db.execute.side_effect = execute_side_effect
    collection = neosqlite.Collection(mock_db, "test_collection", create=False)
    collection.create()
    assert "data TEXT" in mock_db.create_table_query
    assert "data JSONB" not in mock_db.create_table_query


def test_eq_type_error():
    """Test _eq with type error."""
    document = {"foo": 5}
    assert not _eq("foo", "bar", document)


def test_eq_attribute_error():
    """Test _eq with attribute error."""
    document = None
    assert not _eq("foo", "bar", document)


def test_gt_type_error():
    """Test _gt with type error."""
    document = {"foo": "bar"}
    assert not _gt("foo", 5, document)


def test_lt_type_error():
    """Test _lt with type error."""
    document = {"foo": "bar"}
    assert not _lt("foo", 5, document)


def test_lte_type_error():
    """Test _lte with type error."""
    document = {"foo": "bar"}
    assert not _lte("foo", 5, document)


def test_get_operator_fn_improper_op(collection):
    """Test _get_operator_fn with improper operator."""
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._get_operator_fn("foo")


def test_get_operator_fn_valid_op(collection):
    """Test _get_operator_fn with valid operator."""
    assert collection.query_engine.helpers._get_operator_fn("$in") == _in


def test_get_operator_fn_no_op(collection):
    """Test _get_operator_fn with non-existent operator."""
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._get_operator_fn("$foo")


# Query application tests


def test_apply_query_and_type(collection):
    """Test $and query type."""
    query = {"$and": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection.query_engine.helpers._apply_query(
        query, {"foo": "bar", "baz": "qux"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar", "baz": "foo"}
    )


def test_apply_query_or_type(collection):
    """Test $or query type."""
    query = {"$or": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection.query_engine.helpers._apply_query(
        query, {"foo": "bar", "abc": "xyz"}
    )
    assert collection.query_engine.helpers._apply_query(
        query, {"baz": "qux", "abc": "xyz"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"abc": "xyz"}
    )


def test_apply_query_not_type(collection):
    """Test $not query type."""
    query = {"$not": {"foo": "bar"}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": "baz"})
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar"}
    )


def test_apply_query_nor_type(collection):
    """Test $nor query type."""
    query = {"$nor": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection.query_engine.helpers._apply_query(
        query, {"foo": "baz", "baz": "bar"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"baz": "qux"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar", "baz": "qux"}
    )


def test_apply_query_gt_operator(collection):
    """Test $gt operator."""
    query = {"foo": {"$gt": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 10})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 4})


def test_apply_query_gte_operator(collection):
    """Test $gte operator."""
    query = {"foo": {"$gte": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 5})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 4})


def test_apply_query_lt_operator(collection):
    """Test $lt operator."""
    query = {"foo": {"$lt": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 10})


def test_apply_query_lte_operator(collection):
    """Test $lte operator."""
    query = {"foo": {"$lte": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 5})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 10})


def test_apply_query_eq_operator(collection):
    """Test $eq operator."""
    query = {"foo": {"$eq": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 5})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar"}
    )


def test_apply_query_in_operator(collection):
    """Test $in operator."""
    query = {"foo": {"$in": [1, 2, 3]}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 1})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar"}
    )


def test_apply_query_in_operator_raises(collection):
    """Test $in operator with invalid value."""
    query = {"foo": {"$in": 5}}
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._apply_query(query, {"foo": 1})


def test_apply_query_nin_operator(collection):
    """Test $nin operator."""
    query = {"foo": {"$nin": [1, 2, 3]}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert collection.query_engine.helpers._apply_query(query, {"foo": "bar"})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 1})


def test_apply_query_nin_operator_raises(collection):
    """Test $nin operator with invalid value."""
    query = {"foo": {"$nin": 5}}
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._apply_query(query, {"foo": 1})


def test_apply_query_ne_operator(collection):
    """Test $ne operator."""
    query = {"foo": {"$ne": 5}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 1})
    assert collection.query_engine.helpers._apply_query(query, {"foo": "bar"})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 5})


def test_apply_query_all_operator(collection):
    """Test $all operator."""
    query = {"foo": {"$all": [1, 2, 3]}}
    assert collection.query_engine.helpers._apply_query(
        query, {"foo": list(range(10))}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": ["bar", "baz"]}
    )
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 3})


def test_apply_query_all_operator_raises(collection):
    """Test $all operator with invalid value."""
    query = {"foo": {"$all": 3}}
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._apply_query(query, {"foo": "bar"})


def test_apply_query_mod_operator(collection):
    """Test $mod operator."""
    query = {"foo": {"$mod": [2, 0]}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 3})
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bar"}
    )


def test_apply_query_mod_operator_raises(collection):
    """Test $mod operator with invalid value."""
    query = {"foo": {"$mod": 2}}
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._apply_query(query, {"foo": 5})


def test_apply_query_honors_multiple_operators(collection):
    """Test query with multiple operators."""
    query = {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}}
    assert collection.query_engine.helpers._apply_query(query, {"foo": 4})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 3})
    assert not collection.query_engine.helpers._apply_query(query, {"foo": 15})
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "foo"}
    )


def test_apply_query_honors_logical_and_operators(collection):
    """Test query with logical AND operators."""
    query = {
        "bar": "baz",
        "$or": [
            {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}},
            {"foo": {"$gt": 10, "$mod": [2, 1]}},
        ],
    }
    assert collection.query_engine.helpers._apply_query(
        query, {"bar": "baz", "foo": 4}
    )
    assert collection.query_engine.helpers._apply_query(
        query, {"bar": "baz", "foo": 15}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"bar": "baz", "foo": 14}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"bar": "qux", "foo": 4}
    )


def test_apply_query_exists(collection):
    """Test $exists operator."""
    query_exists = {"foo": {"$exists": True}}
    query_not_exists = {"foo": {"$exists": False}}
    assert collection.query_engine.helpers._apply_query(
        query_exists, {"foo": "bar"}
    )
    assert collection.query_engine.helpers._apply_query(
        query_not_exists, {"bar": "baz"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query_exists, {"baz": "bar"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query_not_exists, {"foo": "bar"}
    )


def test_apply_query_exists_raises(collection):
    """Test $exists operator with invalid value."""
    query = {"foo": {"$exists": "foo"}}
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._apply_query(query, {"foo": "bar"})


def test_apply_query_handle_none(collection):
    """Test query handling with None document."""
    query = {"foo": "bar"}
    document = None
    assert not collection.query_engine.helpers._apply_query(query, document)


def test_apply_query_sparse_index(collection):
    """Test query with sparse index."""
    query = {"foo": {"$exists": True}}
    document = {"bar": "baz"}
    assert not collection.query_engine.helpers._apply_query(query, document)


def test_apply_query_with_dot_in_key(collection):
    """Test query with dot in key."""
    query = {"a.b": "some_value"}
    document = {"a.b": "some_value"}
    assert collection.query_engine.helpers._apply_query(query, document)


def test_apply_query_regex(collection):
    """Test $regex operator."""
    query = {"foo": {"$regex": "^bar"}}
    assert collection.query_engine.helpers._apply_query(
        query, {"foo": "barbaz"}
    )
    assert not collection.query_engine.helpers._apply_query(
        query, {"foo": "bazbar"}
    )


def test_apply_query_elem_match(collection):
    """Test $elemMatch operator."""
    query = {"items": {"$elemMatch": {"name": "item1", "value": 5}}}
    doc_match = {
        "items": [{"name": "item1", "value": 5}, {"name": "item2", "value": 10}]
    }
    doc_no_match = {
        "items": [{"name": "item1", "value": 10}, {"name": "item2", "value": 5}]
    }
    assert collection.query_engine.helpers._apply_query(query, doc_match)
    assert not collection.query_engine.helpers._apply_query(query, doc_no_match)


def test_apply_query_size(collection):
    """Test $size operator."""
    query = {"items": {"$size": 2}}
    doc_match = {"items": ["a", "b"]}
    doc_no_match = {"items": ["a", "b", "c"]}
    assert collection.query_engine.helpers._apply_query(query, doc_match)
    assert not collection.query_engine.helpers._apply_query(query, doc_no_match)


"""
Comprehensive tests for update operations.
"""

import pytest
import neosqlite
from neosqlite import MalformedDocument


def test_update_operations_comprehensive(collection):
    """Test comprehensive update operations."""
    # Insert test data
    collection.insert_many(
        [
            {
                "name": "Alice",
                "age": 25,
                "salary": 50000,
                "department": "Engineering",
            },
            {
                "name": "Bob",
                "age": 30,
                "salary": 60000,
                "department": "Marketing",
            },
            {
                "name": "Charlie",
                "age": 35,
                "salary": 70000,
                "department": "Engineering",
            },
        ]
    )

    # Test $inc operator
    result = collection.update_many(
        {"department": "Engineering"}, {"$inc": {"salary": 5000}}
    )
    assert result.matched_count == 2
    assert result.modified_count == 2

    # Verify updates
    alice = collection.find_one({"name": "Alice"})
    assert alice["salary"] == 55000
    charlie = collection.find_one({"name": "Charlie"})
    assert charlie["salary"] == 75000

    # Test $set operator
    result = collection.update_many(
        {"age": {"$gte": 30}}, {"$set": {"senior": True}}
    )
    assert result.matched_count == 2  # Bob and Charlie
    assert result.modified_count == 2

    # Verify updates
    bob = collection.find_one({"name": "Bob"})
    assert bob["senior"] == True
    charlie = collection.find_one({"name": "Charlie"})
    assert charlie["senior"] == True

    # Test $unset operator
    result = collection.update_many(
        {"name": "Alice"}, {"$unset": {"senior": ""}}
    )
    assert result.matched_count == 1
    assert result.modified_count == 1

    # Verify the senior field was removed
    alice = collection.find_one({"name": "Alice"})
    assert "senior" not in alice

    # Test $rename operator
    result = collection.update_many(
        {"department": "Engineering"}, {"$rename": {"salary": "compensation"}}
    )
    assert result.matched_count == 2
    assert result.modified_count == 2

    # Verify the field was renamed
    alice = collection.find_one({"name": "Alice"})
    assert "salary" not in alice
    assert "compensation" in alice
    assert alice["compensation"] == 55000


def test_update_with_complex_queries(collection):
    """Test updates with complex query conditions."""
    # Insert test data
    collection.insert_many(
        [
            {
                "name": "Alice",
                "profile": {"age": 25, "city": "New York"},
                "active": True,
            },
            {
                "name": "Bob",
                "profile": {"age": 30, "city": "Boston"},
                "active": False,
            },
            {
                "name": "Charlie",
                "profile": {"age": 35, "city": "New York"},
                "active": True,
            },
            {
                "name": "David",
                "profile": {"age": 28, "city": "Boston"},
                "active": True,
            },
        ]
    )

    # Update with nested field query
    result = collection.update_many(
        {"profile.city": "New York", "active": True},
        {"$set": {"region": "Northeast"}},
    )
    assert result.matched_count == 2  # Alice and Charlie
    assert result.modified_count == 2

    # Verify updates
    alice = collection.find_one({"name": "Alice"})
    assert alice["region"] == "Northeast"
    charlie = collection.find_one({"name": "Charlie"})
    assert charlie["region"] == "Northeast"

    # Update with array query
    result = collection.update_many(
        {"profile.age": {"$gte": 30}}, {"$push": {"tags": "senior"}}
    )
    assert result.matched_count == 2  # Bob and Charlie
    assert result.modified_count == 2

    # Verify updates
    bob = collection.find_one({"name": "Bob"})
    assert "senior" in bob["tags"]
    charlie = collection.find_one({"name": "Charlie"})
    assert "senior" in charlie["tags"]


def test_replace_one_upsert(collection):
    """Test replace_one with upsert=True."""
    # Replace with upsert when no document matches
    result = collection.replace_one(
        {"name": "Alice"}, {"name": "Alice", "age": 30}, upsert=True
    )

    assert result.matched_count == 0
    assert result.modified_count == 0
    assert result.upserted_id is not None

    # Verify the document was inserted
    alice = collection.find_one({"name": "Alice"})
    assert alice is not None
    assert alice["age"] == 30

    # Replace with upsert when document matches
    result = collection.replace_one(
        {"name": "Alice"},
        {"name": "Alice", "age": 31, "city": "New York"},
        upsert=True,
    )

    assert result.matched_count == 1
    assert result.modified_count == 1
    assert result.upserted_id is None

    # Verify the document was updated
    alice = collection.find_one({"name": "Alice"})
    assert alice is not None
    assert alice["age"] == 31
    assert alice["city"] == "New York"


def test_internal_update_edge_cases(collection):
    """Test edge cases for internal update functionality."""
    # Test with a non-dict document for insert (should raise MalformedDocument)
    with pytest.raises(MalformedDocument):
        collection.query_engine.helpers._internal_insert("not a dict")

    # Test _internal_update with an unsupported operator
    with pytest.raises(
        neosqlite.MalformedQueryException,
        match="Update operator '\\$unsupported' not supported",
    ):
        collection.query_engine.helpers._internal_update(
            1, {"$unsupported": {"field": "value"}}, {"_id": 1}
        )


def test_update_many_fallback(collection):
    """Test update_many with a complex query that requires the fallback path."""
    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
    )

    # Update using a complex query that should trigger the fallback
    result = collection.update_many(
        {"name": {"$regex": "^A"}},  # This should trigger the fallback
        {"$inc": {"age": 1}},
    )

    # Verify the update worked
    alice = collection.find_one({"name": "Alice"})
    assert alice["age"] == 31

    # Note: The exact behavior of $regex depends on the implementation
    # In some cases it might use SQL, in others it might use the fallback path
