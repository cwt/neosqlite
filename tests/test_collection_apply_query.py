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

    def test_apply_query_and_type(self):
        query = {"$and": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "bar", "baz": "qux"})
        assert not self.collection._apply_query(
            query, {"foo": "bar", "baz": "foo"}
        )

    def test_apply_query_or_type(self):
        query = {"$or": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "bar", "abc": "xyz"})
        assert self.collection._apply_query(query, {"baz": "qux", "abc": "xyz"})
        assert not self.collection._apply_query(query, {"abc": "xyz"})

    def test_apply_query_not_type(self):
        query = {"$not": {"foo": "bar"}}
        assert self.collection._apply_query(query, {"foo": "baz"})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_nor_type(self):
        query = {"$nor": [{"foo": "bar"}, {"baz": "qux"}]}
        assert self.collection._apply_query(query, {"foo": "baz", "baz": "bar"})
        assert not self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"baz": "qux"})
        assert not self.collection._apply_query(
            query, {"foo": "bar", "baz": "qux"}
        )

    def test_apply_query_gt_operator(self):
        query = {"foo": {"$gt": 5}}
        assert self.collection._apply_query(query, {"foo": 10})
        assert not self.collection._apply_query(query, {"foo": 4})

    def test_apply_query_gte_operator(self):
        query = {"foo": {"$gte": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 4})

    def test_apply_query_lt_operator(self):
        query = {"foo": {"$lt": 5}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 10})

    def test_apply_query_lte_operator(self):
        query = {"foo": {"$lte": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 10})

    def test_apply_query_eq_operator(self):
        query = {"foo": {"$eq": 5}}
        assert self.collection._apply_query(query, {"foo": 5})
        assert not self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_in_operator(self):
        query = {"foo": {"$in": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": 1})
        assert not self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_in_operator_raises(self):
        query = {"foo": {"$in": 5}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_nin_operator(self):
        query = {"foo": {"$nin": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_nin_operator_raises(self):
        query = {"foo": {"$nin": 5}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 1})

    def test_apply_query_ne_operator(self):
        query = {"foo": {"$ne": 5}}
        assert self.collection._apply_query(query, {"foo": 1})
        assert self.collection._apply_query(query, {"foo": "bar"})
        assert not self.collection._apply_query(query, {"foo": 5})

    def test_apply_query_all_operator(self):
        query = {"foo": {"$all": [1, 2, 3]}}
        assert self.collection._apply_query(query, {"foo": list(range(10))})
        assert not self.collection._apply_query(query, {"foo": ["bar", "baz"]})
        assert not self.collection._apply_query(query, {"foo": 3})

    def test_apply_query_all_operator_raises(self):
        query = {"foo": {"$all": 3}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_mod_operator(self):
        query = {"foo": {"$mod": [2, 0]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 3})
        assert not self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_mod_operator_raises(self):
        query = {"foo": {"$mod": 2}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": 5})

    def test_apply_query_honors_multiple_operators(self):
        query = {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}}
        assert self.collection._apply_query(query, {"foo": 4})
        assert not self.collection._apply_query(query, {"foo": 3})
        assert not self.collection._apply_query(query, {"foo": 15})
        assert not self.collection._apply_query(query, {"foo": "foo"})

    def test_apply_query_honors_logical_and_operators(self):
        # 'bar' must be 'baz', and 'foo' must be an even number 0-10
        # or an odd number > 10
        query = {
            "bar": "baz",
            "$or": [
                {"foo": {"$gte": 0, "$lte": 10, "$mod": [2, 0]}},
                {"foo": {"$gt": 10, "$mod": [2, 1]}},
            ],
        }
        assert self.collection._apply_query(query, {"bar": "baz", "foo": 4})
        assert self.collection._apply_query(query, {"bar": "baz", "foo": 15})
        assert not self.collection._apply_query(
            query, {"bar": "baz", "foo": 14}
        )
        assert not self.collection._apply_query(query, {"bar": "qux", "foo": 4})

    def test_apply_query_exists(self):
        query_exists = {"foo": {"$exists": True}}
        query_not_exists = {"foo": {"$exists": False}}
        assert self.collection._apply_query(query_exists, {"foo": "bar"})
        assert self.collection._apply_query(query_not_exists, {"bar": "baz"})
        assert not self.collection._apply_query(query_exists, {"baz": "bar"})
        assert not self.collection._apply_query(
            query_not_exists, {"foo": "bar"}
        )

    def test_apply_query_exists_raises(self):
        query = {"foo": {"$exists": "foo"}}
        with raises(nosqlite.MalformedQueryException):
            self.collection._apply_query(query, {"foo": "bar"})

    def test_apply_query_handle_none(self):
        query = {"foo": "bar"}
        document = None
        assert not self.collection._apply_query(query, document)

    def test_apply_query_sparse_index(self):
        query = {"foo": {"$exists": True}}
        document = {"bar": "baz"}
        assert not self.collection._apply_query(query, document)

    def test_apply_query_with_dot_in_key(self):
        query = {"a.b": "some_value"}
        document = {"a.b": "some_value"}
        assert self.collection._apply_query(query, document)

