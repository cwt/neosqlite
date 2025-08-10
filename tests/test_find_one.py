# coding: utf-8
import neosqlite


def test_returns_None_if_collection_is_empty(collection: neosqlite.Collection):
    assert collection.find_one({}) is None


def test_returns_None_if_document_is_not_found(
    collection: neosqlite.Collection,
):
    collection.insert_one({"foo": "bar"})
    assert collection.find_one({"foo": "baz"}) is None


def test_returns_document_if_found(collection: neosqlite.Collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"
