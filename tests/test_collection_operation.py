# coding: utf-8
from neosqlite.query_operators import _eq, _gt, _lt, _lte, _in
from pytest import raises
import neosqlite


def test_eq_type_error():
    document = {"foo": 5}
    assert not _eq("foo", "bar", document)


def test_eq_attribute_error():
    document = None
    assert not _eq("foo", "bar", document)


def test_gt_type_error():
    document = {"foo": "bar"}
    assert not _gt("foo", 5, document)


def test_lt_type_error():
    document = {"foo": "bar"}
    assert not _lt("foo", 5, document)


def test_lte_type_error():
    document = {"foo": "bar"}
    assert not _lte("foo", 5, document)


def test_get_operator_fn_improper_op(collection):
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._get_operator_fn("foo")


def test_get_operator_fn_valid_op(collection):
    assert collection.query_engine.helpers._get_operator_fn("$in") == _in


def test_get_operator_fn_no_op(collection):
    with raises(neosqlite.MalformedQueryException):
        collection.query_engine.helpers._get_operator_fn("$foo")
