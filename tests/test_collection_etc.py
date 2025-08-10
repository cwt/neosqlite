# coding: utf-8
import neosqlite


def test_find_one_and_update(collection):
    collection.insert_one({"foo": "bar", "count": 1})
    doc = collection.find_one_and_update(
        {"foo": "bar"}, {"$set": {"foo": "baz"}, "$inc": {"count": 1}}
    )
    assert doc is not None

    updated_doc = collection.find_one({"_id": doc["_id"]})
    assert updated_doc["foo"] == "baz"
    assert updated_doc["count"] == 2


def test_find_one_and_replace(collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_replace({"foo": "bar"}, {"foo": "baz"})
    assert doc is not None
    assert collection.find_one({"foo": "bar"}) is None
    assert collection.find_one({"foo": "baz"}) is not None


def test_find_one_and_delete(collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_delete({"foo": "bar"})
    assert doc is not None
    assert collection.count_documents({}) == 0


def test_count_documents(collection):
    collection.insert_many([{}, {}, {}])
    assert collection.count_documents({}) == 3
    assert collection.count_documents({"foo": "bar"}) == 0


def test_distinct(collection):
    collection.insert_many(
        [{"foo": "bar"}, {"foo": "baz"}, {"foo": 10}, {"bar": "foo"}]
    )
    assert set(("bar", "baz", 10)) == collection.distinct("foo")
