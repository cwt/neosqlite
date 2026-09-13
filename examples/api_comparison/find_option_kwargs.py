"""Module for comparing find() option kwargs between NeoSQLite and PyMongo"""

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

_SEED = [{"n": i, "grp": i % 2} for i in range(10)]

_CASES = [
    ("limit", {}, {"limit": 3}, True),
    ("skip", {}, {"skip": 7}, True),
    ("skip+limit", {}, {"skip": 2, "limit": 3}, True),
    ("sort desc+limit", {}, {"sort": [("n", -1)], "limit": 3}, False),
    ("sort+skip+limit", {}, {"sort": [("n", 1)], "skip": 1, "limit": 4}, False),
    (
        "filter+sort+limit",
        {"grp": 0},
        {"sort": [("n", -1)], "limit": 2},
        False,
    ),
    ("limit=0 means no limit", {}, {"limit": 0}, True),
]


def _run_cases(collection):
    results = {}
    for name, flt, kwargs, _ordered in _CASES:
        results[name] = list(collection.find(dict(flt), **dict(kwargs)))
    return results


def compare_find_option_kwargs():
    """Compare find() limit/skip/sort kwargs against PyMongo"""
    print("\n=== find() Option Kwargs Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([dict(doc) for doc in _SEED])

        set_accumulation_mode(True)
        if mongo_available:
            start_neo_timing()
        try:
            neo_results = _run_cases(neo_collection)
        finally:
            if mongo_available:
                end_neo_timing()
        print(
            "NeoSQLite find kwargs: limit, skip, skip+limit, sort, "
            "filter+sort+limit, limit=0"
        )

    mongo_results = {}
    if mongo_available:
        mongo_collection = client.test_database.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([dict(doc) for doc in _SEED])

        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_results = _run_cases(mongo_collection)
        finally:
            end_mongo_timing()
        print("PyMongo find kwargs: all cases completed")

    for name, _flt, _kwargs, ordered in _CASES:
        reporter.record_comparison(
            "find() Option Kwargs",
            f"find {name}",
            neo_results.get(name, "Error: missing NeoSQLite result"),
            mongo_results.get(name),
            ignore_order=not ordered,
            skip_reason=(
                "MongoDB not available" if not mongo_available else None
            ),
        )
