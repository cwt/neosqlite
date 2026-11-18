"""Compare server-side JavaScript operators against PyMongo.

Enabled only when a patched, supported Node.js runtime is available. The
NeoSQLite side runs JS in a sandboxed out-of-process worker; MongoDB runs it
with its embedded SpiderMonkey engine. Results are compared as plain values.
"""

from __future__ import annotations

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

_DOCUMENTS = [
    {"_id": 1, "price": 10, "qty": 3, "items": [1, 2, 3]},
    {"_id": 2, "price": 5, "qty": 2, "items": [4, 5]},
    {"_id": 3, "price": 7, "qty": 1, "items": []},
]

_FUNCTION_PIPELINE = [
    {
        "$project": {
            "total": {
                "$function": {
                    "body": "function(p, q) { return p * q; }",
                    "args": ["$price", "$qty"],
                    "lang": "js",
                }
            },
            "sum": {
                "$function": {
                    "body": "function(xs) { return xs.reduce((a,b)=>a+b,0); }",
                    "args": ["$items"],
                    "lang": "js",
                }
            },
        }
    },
    {"$sort": {"_id": 1}},
]


def _summarize(docs: list[dict]) -> list[dict]:
    return [
        {"total": doc.get("total"), "sum": doc.get("sum")}
        for doc in sorted(docs, key=lambda d: d.get("_id", 0))
    ]


def compare_js_operators() -> None:
    """Compare $function and $where against PyMongo when JS is available."""
    print("\n=== JavaScript Operators Comparison ===")

    from neosqlite.js_runtime import find_node_runtime, node_unavailable_reason

    runtime = find_node_runtime()
    skip = None if runtime is not None else node_unavailable_reason()
    if runtime is not None:
        print(f"Node.js runtime: {runtime.version_string} ({runtime.path})")
    else:
        print(f"Node.js runtime unavailable: {skip}")

    neo_function = None
    neo_where_expr = None
    neo_where_func = None
    if runtime is not None:
        with neosqlite.Connection(":memory:") as conn:
            collection = conn.js_docs
            collection.insert_many([dict(doc) for doc in _DOCUMENTS])
            set_accumulation_mode(True)
            start_neo_timing()
            try:
                neo_function = _summarize(
                    list(collection.aggregate(_FUNCTION_PIPELINE))
                )
                neo_where_expr = sorted(
                    doc["_id"]
                    for doc in collection.find({"$where": "this.price > 6"})
                )
                neo_where_func = sorted(
                    doc["_id"]
                    for doc in collection.find(
                        {"$where": "function() { return this.qty >= 2; }"}
                    )
                )
            finally:
                end_neo_timing()

    mongo_function = None
    mongo_where_expr = None
    mongo_where_func = None
    client = get_mongo_client()
    if runtime is not None and client is not None:
        collection = client.test_database.js_docs
        collection.delete_many({})
        collection.insert_many([dict(doc) for doc in _DOCUMENTS])
        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_function = _summarize(
                list(collection.aggregate(_FUNCTION_PIPELINE))
            )
            mongo_where_expr = sorted(
                doc["_id"]
                for doc in collection.find({"$where": "this.price > 6"})
            )
            mongo_where_func = sorted(
                doc["_id"]
                for doc in collection.find(
                    {"$where": "function() { return this.qty >= 2; }"}
                )
            )
        finally:
            end_mongo_timing()
    elif runtime is not None and client is None:
        skip = "MongoDB not available"

    reporter.record_comparison(
        "JavaScript Operators",
        "$function arithmetic + array reduce",
        neo_function,
        mongo_function,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "JavaScript Operators",
        "$where expression",
        neo_where_expr,
        mongo_where_expr,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "JavaScript Operators",
        "$where function",
        neo_where_func,
        mongo_where_func,
        skip_reason=skip,
    )
