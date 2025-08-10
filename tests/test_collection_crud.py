# coding: utf-8
import sqlite3
from pytest import raises
import neosqlite


def test_create(collection):
    # The fixture now handles creation, so this test just checks if it exists.
    row = collection.db.execute(
        "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
        (collection.name,),
    ).fetchone()
    assert row[0] == 1


def test_insert_one(collection):
    doc = {"foo": "bar"}
    result = collection.insert_one(doc)
    assert isinstance(result, neosqlite.InsertOneResult)
    assert result.inserted_id == 1
    assert doc["_id"] == 1
    found = collection.find_one({"_id": 1})
    assert found["foo"] == "bar"


def test_insert_many(collection):
    docs = [{"foo": "bar"}, {"foo": "baz"}]
    result = collection.insert_many(docs)
    assert isinstance(result, neosqlite.InsertManyResult)
    assert result.inserted_ids == [1, 2]
    assert collection.count_documents({}) == 2


def test_insert_non_dict_raise(collection):
    doc = "{'foo': 'bar'}"
    with raises(neosqlite.MalformedDocument):
        collection.insert_one(doc)


def test_replace_one(collection):
    collection.insert_one({"foo": "bar"})
    result = collection.replace_one({"foo": "bar"}, {"foo": "baz"})
    assert result.matched_count == 1
    assert result.modified_count == 1
    assert collection.find_one({"foo": "bar"}) is None
    assert collection.find_one({"foo": "baz"}) is not None


def test_update_one(collection):
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
    collection.insert_many([{"foo": "bar"}, {"foo": "bar"}])
    result = collection.update_many({"foo": "bar"}, {"$set": {"foo": "baz"}})
    assert result.matched_count == 2
    assert result.modified_count == 2
    assert collection.count_documents({"foo": "baz"}) == 2


def test_delete_one(collection):
    collection.insert_one({"foo": "bar"})
    result = collection.delete_one({"foo": "bar"})
    assert result.deleted_count == 1
    assert collection.count_documents({}) == 0


def test_delete_many(collection):
    collection.insert_many([{"foo": "bar"}, {"foo": "bar"}])
    result = collection.delete_many({"foo": "bar"})
    assert result.deleted_count == 2
    assert collection.count_documents({}) == 0


def test_transaction(collection):
    try:
        collection.db.execute("BEGIN")
        collection.insert_one({"a": 1})
        collection.insert_one({"a": 2})
        collection.db.rollback()
    except sqlite3.OperationalError:
        pass  # Some versions of sqlite might complain
    assert collection.count_documents({}) == 0

    collection.db.execute("BEGIN")
    collection.insert_one({"a": 1})
    collection.insert_one({"a": 2})
    collection.db.commit()
    assert collection.count_documents({}) == 2
