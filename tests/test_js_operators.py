"""Tests for JavaScript expression/query operators via the Node worker."""

import shutil

import pytest

import neosqlite
from neosqlite.js_runtime import NodeRuntime
from neosqlite.js_worker import close_worker

_NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")


@pytest.fixture
def supported_node(monkeypatch):
    """Bypass the version gate with the local binary for wiring tests."""
    runtime = NodeRuntime(path=_NODE, version=(24, 22, 3))
    monkeypatch.setattr(
        "neosqlite.js_runtime.find_node_runtime", lambda: runtime
    )
    close_worker()
    yield runtime
    close_worker()


def test_function_aggregation_expression(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "price": 10, "qty": 3},
                {"_id": 2, "price": 5, "qty": 2},
            ]
        )
        results = list(
            coll.aggregate(
                [
                    {
                        "$addFields": {
                            "total": {
                                "$function": {
                                    "body": (
                                        "function(p, q) { return p * q; }"
                                    ),
                                    "args": ["$price", "$qty"],
                                    "lang": "js",
                                }
                            }
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        )
    assert [doc["total"] for doc in results] == [30, 10]


def test_function_with_object_and_array_args(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "items": [1, 2, 3]})
        result = list(
            coll.aggregate(
                [
                    {
                        "$project": {
                            "sum": {
                                "$function": {
                                    "body": (
                                        "function(xs) {"
                                        " return xs.reduce((a,b)=>a+b,0); }"
                                    ),
                                    "args": ["$items"],
                                    "lang": "js",
                                }
                            }
                        }
                    }
                ]
            )
        )[0]
    assert result["sum"] == 6


def test_where_expression_form(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "n": 5},
                {"_id": 2, "n": 20},
            ]
        )
        results = list(coll.find({"$where": "this.n > 10"}))
    assert [doc["_id"] for doc in results] == [2]


def test_where_function_form(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "n": 5},
                {"_id": 2, "n": 20},
            ]
        )
        results = list(
            coll.find({"$where": "function() { return this.n > 10; }"})
        )
    assert [doc["_id"] for doc in results] == [2]


def test_js_error_is_surfaced(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "n": 1})
        with pytest.raises(ValueError, match="JavaScript error"):
            list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "x": {
                                    "$function": {
                                        "body": (
                                            "function(){throw new Error('boom')}"
                                        ),
                                        "args": [],
                                        "lang": "js",
                                    }
                                }
                            }
                        }
                    ]
                )
            )


def test_worker_unit_call(supported_node):
    from neosqlite.js_worker import get_worker

    worker = get_worker(supported_node)
    assert worker is not None
    assert worker.call("function(a,b){return a+b}", [2, 3]) == 5
