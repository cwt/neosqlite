# coding: utf-8
import sqlite3
from pytest import raises
import nosqlite


def test_apply_query_and_type(collection):
    query = {"$and": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection._apply_query(query, {"foo": "bar", "baz": "qux"})
    assert not collection._apply_query(query, {"foo": "bar", "baz": "foo"})


def test_apply_query_or_type(collection):
    query = {"$or": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection._apply_query(query, {"foo": "bar", "abc": "xyz"})
    assert collection._apply_query(query, {"baz": "qux", "abc": "xyz"})
    assert not collection._apply_query(query, {"abc": "xyz"})


def test_apply_query_not_type(collection):
    query = {"$not": {"foo": "bar"}}
    assert collection._apply_query(query, {"foo": "baz"})
    assert not collection._apply_query(query, {"foo": "bar"})


def test_apply_query_nor_type(collection):
    query = {"$nor": [{"foo": "bar"}, {"baz": "qux"}]}
    assert collection._apply_query(query, {"foo": "baz", "baz": "bar"})
    assert not collection._apply_query(query, {"foo": "bar"})
    assert not collection._apply_query(query, {"baz": "qux"})
    assert not collection._apply_query(query, {"foo": "bar", "baz": "qux"})


def test_apply_query_gt_operator(collection):
    query = {"foo": {"$gt": 5}}
    assert collection._apply_query(query, {"foo": 10})
    assert not collection._apply_query(query, {"foo": 4})


def test_apply_query_gte_operator(collection):
    query = {"foo": {"$gte": 5}}
    assert collection._apply_query(query, {"foo": 5})
    assert not collection._apply_query(query, {"foo": 4})


def test_apply_query_lt_operator(collection):
    query = {"foo": {"$lt": 5}}
    assert collection._apply_query(query, {"foo": 4})
    assert not collection._apply_query(query, {"foo": 10})


def test_apply_query_lte_operator(collection):
    query = {"foo": {"$lte": 5}}
    assert collection._apply_query(query, {"foo": 5})
    assert not collection._apply_query(query, {"foo": 10})


def test_apply_query_eq_operator(collection):
    query = {"foo": {"$eq": 5}}
    assert collection._apply_query(query, {"foo": 5})
    assert not collection._apply_query(query, {"foo": 4})
    assert not collection._apply_query(query, {"foo": "bar"})


def test_apply_query_in_operator(collection):
    query = {"foo": {"$in": [1, 2, 3]}}
    assert collection._apply_query(query, {"foo": 1})
    assert not collection._apply_query(query, {"foo": 4})
    assert not collection._apply_query(query, {"foo": "bar"})


def test_apply_query_in_operator_raises(collection):
    query = {"foo": {"$in": 5}}
    with raises(nosqlite.MalformedQueryException):
        collection._apply_query(query, {"foo": 1})


def test_apply_query_nin_operator(collection):
    query = {"foo": {"$nin": [1, 2, 3]}}
    assert collection._apply_query(query, {"foo": 4})
    assert collection._apply_query(query, {"foo": "bar"})
    assert not collection._apply_query(query, {"foo": 1})


def test_apply_query_nin_operator_raises(collection):
    query = {"foo": {"$nin": 5}}
    with raises(nosqlite.MalformedQueryException):
        collection._apply_query(query, {"foo": 1})


def test_apply_query_ne_operator(collection):
    query = {"foo": {"$ne": 5}}
    assert collection._apply_query(query, {"foo": 1})
    assert collection._apply_query(query, {"foo": "bar"})
    assert not collection._apply_query(query, {"foo": 5})


def test_apply_query_all_operator(collection):
    query = {"foo": {"$all": [1, 2, 3]}}
    assert collection._apply_query(query, {"foo": list(range(10))})
    assert not collection._apply_query(query, {"foo": ["bar", "baz"]})
    assert not collection._apply_query(query, {"foo": 3})


def test_apply_query_all_operator_raises(collection):
    query = {"foo": {"$all": 3}}
    with raises(nosqlite.MalformedQueryException):
        collection._apply_query(query, {"foo": "bar"})


def test_apply_query_mod_operator(collection):
    query = {"foo": {"$mod": [2, 0]}}
    assert collection._apply_query(query, {"foo": 4})
    assert not collection._apply_query(query, {"foo": 3})
    assert not collection._apply_query(query, {"foo": "bar"})


def test_apply_query_mod_operator_raises(collection):
    query = {"foo": {"$mod": 2}}
    with raises(nosqlite.MalformedQueryException):
        collection._apply_query(query, {"foo": 5})


def test_apply_query_honors_multiple_operators(collection):
    query = {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}}
    assert collection._apply_query(query, {"foo": 4})
    assert not collection._apply_query(query, {"foo": 3})
    assert not collection._apply_query(query, {"foo": 15})
    assert not collection._apply_query(query, {"foo": "foo"})


def test_apply_query_honors_logical_and_operators(collection):
    query = {
        "bar": "baz",
        "$or": [
            {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}},
            {"foo": {"$gt": 10, "$mod": [2, 1]}},
        ],
    }
    assert collection._apply_query(query, {"bar": "baz", "foo": 4})
    assert collection._apply_query(query, {"bar": "baz", "foo": 15})
    assert not collection._apply_query(query, {"bar": "baz", "foo": 14})
    assert not collection._apply_query(query, {"bar": "qux", "foo": 4})


def test_apply_query_exists(collection):
    query_exists = {"foo": {"$exists": True}}
    query_not_exists = {"foo": {"$exists": False}}
    assert collection._apply_query(query_exists, {"foo": "bar"})
    assert collection._apply_query(query_not_exists, {"bar": "baz"})
    assert not collection._apply_query(query_exists, {"baz": "bar"})
    assert not collection._apply_query(query_not_exists, {"foo": "bar"})


def test_apply_query_exists_raises(collection):
    query = {"foo": {"$exists": "foo"}}
    with raises(nosqlite.MalformedQueryException):
        collection._apply_query(query, {"foo": "bar"})


def test_apply_query_handle_none(collection):
    query = {"foo": "bar"}
    document = None
    assert not collection._apply_query(query, document)


def test_apply_query_sparse_index(collection):
    query = {"foo": {"$exists": True}}
    document = {"bar": "baz"}
    assert not collection._apply_query(query, document)


def test_apply_query_with_dot_in_key(collection):
    query = {"a.b": "some_value"}
    document = {"a.b": "some_value"}
    assert collection._apply_query(query, document)
