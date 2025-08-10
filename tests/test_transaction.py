# coding: utf-8
import sqlite3
from pytest import raises
import neosqlite


def test_transaction_commit(connection):
    collection = connection["foo"]
    with connection.transaction():
        collection.insert_one({"a": 1})
        collection.insert_one({"a": 2})
    assert collection.count_documents({}) == 2


def test_transaction_rollback(connection):
    collection = connection["foo"]
    with raises(ValueError):
        with connection.transaction():
            collection.insert_one({"a": 1})
            raise ValueError("Something went wrong")
    assert collection.count_documents({}) == 0
