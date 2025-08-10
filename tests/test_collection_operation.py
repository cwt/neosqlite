# coding: utf-8
from pytest import raises
import nosqlite


def test_eq_type_error():
    document = {"foo": 5}
    assert not nosqlite._eq("foo", "bar", document)


def test_eq_attribute_error():
    document = None
    assert not nosqlite._eq("foo", "bar", document)


def test_gt_type_error():
    document = {"foo": "bar"}
    assert not nosqlite._gt("foo", 5, document)


def test_lt_type_error():
    document = {"foo": "bar"}
    assert not nosqlite._lt("foo", 5, document)


def test_lte_type_error():
    document = {"foo": "bar"}
    assert not nosqlite._lte("foo", 5, document)


def test_get_operator_fn_improper_op(collection):
    with raises(nosqlite.MalformedQueryException):
        collection._get_operator_fn("foo")


def test_get_operator_fn_valid_op(collection):
    assert collection._get_operator_fn("$in") == nosqlite._in


def test_get_operator_fn_no_op(collection):
    with raises(nosqlite.MalformedQueryException):
        collection._get_operator_fn("$foo")
