"""Unit tests for query operators."""

import pytest
from unittest.mock import patch
from neosqlite.query_operators import (
    _get_nested_field,
    _eq,
    _gt,
    _lt,
    _gte,
    _lte,
    _all,
    _in,
    _ne,
    _nin,
    _mod,
    _exists,
    _regex,
    _elemMatch,
    _apply_query_operators,
    _size,
    _contains,
    _type,
    _bits_all_clear,
    _bits_all_set,
    _bits_any_clear,
    _bits_any_set,
)
from neosqlite.exceptions import MalformedQueryException


def test_get_nested_field():
    doc = {"a": {"b": {"c": 1}}, "d": 2}
    assert _get_nested_field("a.b.c", doc) == 1
    assert _get_nested_field("d", doc) == 2
    assert _get_nested_field("a.b.x", doc) is None
    assert _get_nested_field("a.x.c", doc) is None
    assert _get_nested_field("x", doc) is None
    assert _get_nested_field("d.x", doc) is None  # d is not a dict


def test_comparison_operators_exceptions():
    doc = {"a": "string"}
    assert _gt("a", 10, doc) is False
    assert _lt("a", 10, doc) is False
    assert _gte("a", 10, doc) is False
    assert _lte("a", 10, doc) is False
    assert _eq("a", 10, doc) is False
    with patch(
        "neosqlite.query_operators._get_nested_field", side_effect=TypeError
    ):
        assert _eq("a", 1, {}) is False


def test_ne_operator():
    doc = {"a": {"b": 1}}
    assert _ne("a.b", 1, doc) is False
    assert _ne("a.b", 2, doc) is True
    assert _ne("x", 1, doc) is True


def test_all_operator():
    doc = {"tags": ["a", "b", "c"]}
    assert _all("tags", ["a", "b"], doc) is True
    assert _all("tags", ["a", "d"], doc) is False
    assert _all("nonexistent", ["a"], doc) is False
    assert _all("val", [1], {"val": 1}) is False
    with pytest.raises(
        MalformedQueryException, match="must accept an iterable"
    ):
        _all("tags", 123, doc)
    with patch(
        "neosqlite.query_operators._get_nested_field", return_value=[[1]]
    ):
        assert _all("tags", [1], doc) is False


def test_in_operator():
    doc = {"val": 10, "arr": [1, 2, 3]}
    assert _in("val", [10, 20], doc) is True
    assert _in("val", [1, 2], doc) is False
    assert _in("arr", [2, 4], doc) is True
    assert _in("arr", [4, 5], doc) is False
    assert _in("nonexistent", [1], {}) is False
    with pytest.raises(
        MalformedQueryException, match="must be followed by an array"
    ):
        _in("val", 123, doc)


def test_nin_operator():
    doc = {"val": 10}
    assert _nin("val", [1, 2], doc) is True
    assert _nin("val", [10, 20], doc) is False
    with pytest.raises(
        MalformedQueryException, match="must accept an iterable"
    ):
        _nin("val", 123, doc)


def test_mod_operator():
    doc = {"val": 10, "str_val": "10"}
    assert _mod("val", [3, 1], doc) is True
    assert _mod("val", ["3", "1"], doc) is True
    assert _mod("str_val", [3, 1], doc) is True
    assert _mod("val", [3, 2], doc) is False
    assert _mod("nonexistent", [3, 1], doc) is False
    with pytest.raises(
        MalformedQueryException, match="must accept an iterable"
    ):
        _mod("val", "invalid", doc)
    assert _mod("val", [3, 1], {"val": "not-a-number"}) is False


def test_exists_operator():
    doc = {"a": {"b": 1}, "c": None}
    assert _exists("a.b", True, doc) is True
    assert _exists("a.x", True, doc) is False
    assert _exists("a.x", False, doc) is True
    assert _exists("c", True, doc) is True
    assert _exists("d", True, doc) is False
    assert _exists("a.b.c.d", True, doc) is False
    assert _exists("a.b.c.d", False, doc) is True
    assert _exists("a.b.c", True, {"a": {"b": {}}}) is False
    assert _exists("a.b.c", False, {"a": {"b": {}}}) is True

    with pytest.raises(
        MalformedQueryException, match="must be supplied a boolean"
    ):
        _exists("a", 1, doc)


def test_regex_operator():
    doc = {"name": "Alice"}
    assert _regex("name", "^Al", doc) is True
    assert _regex("name", "ice$", doc) is True
    assert _regex("name", "bob", doc) is False
    assert _regex("nonexistent", ".*", doc) is True
    assert _regex("name", "[", doc) is False
    assert _regex("name", 123, doc) is False

    # Test with options
    assert _regex("name", "alice", doc, options="i") is True
    assert _regex("name", "ALICE", doc, options="i") is True
    assert _regex("name", "^alice", doc, options="i") is True

    # Test multiline
    multi_doc = {"text": "Line 1\nLine 2"}
    assert _regex("text", "^Line 2", multi_doc) is False
    assert _regex("text", "^Line 2", multi_doc, options="m") is True

    # Test dotall
    dotall_doc = {"text": "A\nB"}
    assert _regex("text", "A.B", dotall_doc) is False
    assert _regex("text", "A.B", dotall_doc, options="s") is True

    # Test verbose
    verbose_doc = {"verbose_doc": "Alice"}
    # In verbose mode, whitespace is ignored and # starts a comment
    assert (
        _regex(
            "verbose_doc", " A l i c e # match alice ", verbose_doc, options="x"
        )
        is True
    )


def test_apply_query_operators_with_options():
    # $regex with $options
    assert (
        _apply_query_operators({"$regex": "alice", "$options": "i"}, "Alice")
        is True
    )
    assert (
        _apply_query_operators({"$regex": "bob", "$options": "i"}, "Alice")
        is False
    )

    # $options without $regex should raise MalformedQueryException
    with pytest.raises(
        MalformedQueryException, match="Can't use \\$options without \\$regex"
    ):
        _apply_query_operators({"$options": "i"}, "Alice")


def test_elemMatch_operator():
    doc = {
        "scores": [80, 90, 100],
        "students": [
            {"name": "Alice", "grade": 85},
            {"name": "Bob", "grade": 90},
        ],
    }
    assert _elemMatch("scores", 90, doc) is True
    assert _elemMatch("scores", 70, doc) is False
    assert _elemMatch("scores", {"$gt": 95}, doc) is True
    assert _elemMatch("scores", {"$lt": 50}, doc) is False
    assert _elemMatch("scores", {"$gt": 200}, doc) is False
    assert _elemMatch("students", {"name": "Alice", "grade": 85}, doc) is True
    assert _elemMatch("students", {"name": "Alice", "grade": 90}, doc) is False
    assert _elemMatch("students", {"name": "Bob", "age": 20}, doc) is False
    assert _elemMatch("name", 1, {"name": "test"}) is False


def test_apply_query_operators_edge_cases():
    assert _apply_query_operators({"$nonexistent": 1}, 10) is False
    with patch("neosqlite.query_operators.globals") as mock_globals:
        mock_globals.return_value.get.return_value = "not-callable"
        assert _apply_query_operators({"$gt": 5}, 10) is False


def test_size_operator():
    doc = {"arr": [1, 2, 3]}
    assert _size("arr", 3, doc) is True
    assert _size("arr", 2, doc) is False
    assert _size("not_arr", 0, {"not_arr": 1}) is False


def test_contains_operator():
    doc = {"name": "HelloWorld"}
    assert _contains("name", "hello", doc) is True
    assert _contains("name", "WORLD", doc) is True
    assert _contains("name", "foo", doc) is False
    assert _contains("nonexistent", "bar", doc) is False
    assert _contains("val", "1", {"val": None}) is False
    with patch("neosqlite.query_operators.str", side_effect=TypeError):
        assert _contains("name", "hello", doc) is False


def test_type_operator():
    doc = {"a": 1.5, "b": "str", "c": True}
    assert _type("a", 1, doc) is True
    assert _type("b", 2, doc) is True
    assert _type("c", 8, doc) is True
    assert _type("a", str, doc) is False
    assert _type("a", 999, doc) is False


def test_bits_all_clear():
    doc = {"val": 10}
    assert _bits_all_clear("val", 5, doc) is True
    assert _bits_all_clear("val", 2, doc) is False
    assert _bits_all_clear("val", [0, 2], doc) is True
    assert _bits_all_clear("val", [1], doc) is False
    assert _bits_all_clear("nonexistent", 1, doc) is False
    assert _bits_all_clear("val", 1, {"val": True}) is False
    assert _bits_all_clear("val", "invalid", doc) is False
    assert _bits_all_clear("val", [0, "invalid"], doc) is False
    assert _bits_all_clear("val", object(), doc) is False
    # Trigger line 476-477
    assert _bits_all_clear("val", 1, {"val": "not-a-number"}) is False

    class BitIter:
        def __iter__(self):
            yield 0
            yield 2

    assert _bits_all_clear("val", BitIter(), doc) is True

    class BadIter:
        def __iter__(self):
            yield "not-a-number"

    assert _bits_all_clear("val", BadIter(), doc) is False


def test_bits_all_set():
    doc = {"val": 10}
    assert _bits_all_set("val", 10, doc) is True
    assert _bits_all_set("val", 2, doc) is True
    assert _bits_all_set("val", 8, doc) is True
    assert _bits_all_set("val", 12, doc) is False
    assert _bits_all_set("val", [1, 3], doc) is True
    assert _bits_all_set("val", [1, 2], doc) is False
    assert _bits_all_set("nonexistent", 1, doc) is False
    assert _bits_all_set("val", 1, {"val": True}) is False
    assert _bits_all_set("val", [1, "invalid"], doc) is False
    assert _bits_all_set("val", object(), doc) is False
    # Trigger line 534-535
    assert _bits_all_set("val", 1, {"val": "not-a-number"}) is False

    class BitIter:
        def __iter__(self):
            yield 1
            yield 3

    assert _bits_all_set("val", BitIter(), doc) is True

    class BadIter:
        def __iter__(self):
            yield "not-a-number"

    assert _bits_all_set("val", BadIter(), doc) is False


def test_bits_any_clear():
    doc = {"val": 10}
    assert _bits_any_clear("val", 1, doc) is True
    assert _bits_any_clear("val", 4, doc) is True
    assert _bits_any_clear("val", 2, doc) is False
    assert _bits_any_clear("val", [0, 1], doc) is True
    assert _bits_any_clear("val", [1, 3], doc) is False
    assert _bits_any_clear("nonexistent", 1, doc) is False
    assert _bits_any_clear("val", 1, {"val": True}) is False
    assert _bits_any_clear("val", [0, "invalid"], doc) is False
    assert _bits_any_clear("val", object(), doc) is False
    # Trigger line 587-588
    assert _bits_any_clear("val", 1, {"val": "not-a-number"}) is False

    class BitIter:
        def __iter__(self):
            yield 0
            yield 1

    assert _bits_any_clear("val", BitIter(), doc) is True

    class BadIter:
        def __iter__(self):
            yield "not-a-number"

    assert _bits_any_clear("val", BadIter(), doc) is False


def test_bits_any_set():
    doc = {"val": 10}
    assert _bits_any_set("val", 2, doc) is True
    assert _bits_any_set("val", 8, doc) is True
    assert _bits_any_set("val", 1, doc) is False
    assert _bits_any_set("val", [0, 1], doc) is True
    assert _bits_any_set("val", [0, 2], doc) is False
    assert _bits_any_set("nonexistent", 1, doc) is False
    assert _bits_any_set("val", 1, {"val": True}) is False
    assert _bits_any_set("val", [0, "invalid"], doc) is False
    assert _bits_any_set("val", object(), doc) is False
    # Trigger line 641-642
    assert _bits_any_set("val", 1, {"val": "not-a-number"}) is False

    class BitIter:
        def __iter__(self):
            yield 0
            yield 1

    assert _bits_any_set("val", BitIter(), doc) is True

    class BadIter:
        def __iter__(self):
            yield "not-a-number"

    assert _bits_any_set("val", BadIter(), doc) is False
