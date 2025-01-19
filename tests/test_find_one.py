# coding: utf-8
import nosqlite

class TestFindOne:
    def test_returns_None_if_collection_does_not_exist(
        self, collection: nosqlite.Collection
    ):
        assert collection.find_one({}) is None

    def test_returns_None_if_document_is_not_found(
        self, collection: nosqlite.Collection
    ):
        collection.create()
        assert collection.find_one({}) is None
