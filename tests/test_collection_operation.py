# coding: utf-8
import re
from unittest.mock import Mock, call, patch
from pytest import mark, raises
import sqlite3
import nosqlite


class TestCollection:
    def setup_method(self):
        self.db = sqlite3.connect(":memory:")
        self.collection = nosqlite.Collection(self.db, "foo", create=False)

    def teardown_method(self):
        self.db.close()

    def unformat_sql(self, sql: str) -> str:
        return re.sub(r"[\s]+", " ", sql.strip().replace("\n", ""))

    def test_eq_type_error(self):
        document = {"foo": 5}
        assert not nosqlite._eq("foo", "bar", document)

    def test_eq_attribute_error(self):
        document = None  # This will trigger AttributeError in _eq function
        assert not nosqlite._eq("foo", "bar", document)

    def test_gt_type_error(self):
        document = {"foo": "bar"}
        assert not nosqlite._gt("foo", 5, document)

    def test_lt_type_error(self):
        document = {"foo": "bar"}
        assert not nosqlite._lt("foo", 5, document)

    def test_lte_type_error(self):
        document = {"foo": "bar"}
        assert not nosqlite._lte("foo", 5, document)

    def test_get_operator_fn_improper_op(self):
        with raises(nosqlite.MalformedQueryException):
            self.collection._get_operator_fn("foo")

    def test_get_operator_fn_valid_op(self):
        assert self.collection._get_operator_fn("$in") == nosqlite._in

    def test_get_operator_fn_no_op(self):
        with raises(nosqlite.MalformedQueryException):
            self.collection._get_operator_fn("$foo")

