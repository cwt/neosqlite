"""Compare BSON-aware ``_id`` range queries against PyMongo.

MongoDB range comparisons only match values of the operand's BSON type. Both
engines are queried with the same mixed-type ``_id`` collection and the matched
document labels are compared, which verifies the type guards without relying
on engine-specific score or representation details.
"""

from __future__ import annotations

import warnings
from datetime import UTC, datetime

import neosqlite
from neosqlite.objectid import ObjectId as NeoObjectId

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

OID_1 = "507f1f77bcf86cd799439011"
OID_2 = "607f1f77bcf86cd799439012"
DATE_1 = datetime(2024, 1, 1, tzinfo=UTC)
DATE_2 = datetime(2025, 1, 1, tzinfo=UTC)


def _documents(oid_cls) -> list[dict]:
    return [
        {"_id": 1, "label": "int-1"},
        {"_id": 10, "label": "int-10"},
        {"_id": 2.5, "label": "float-2.5"},
        {"_id": "abc", "label": "str-abc"},
        {"_id": "xyz", "label": "str-xyz"},
        {"_id": False, "label": "bool-false"},
        {"_id": True, "label": "bool-true"},
        {"_id": oid_cls(OID_1), "label": "oid-1"},
        {"_id": oid_cls(OID_2), "label": "oid-2"},
        {"_id": DATE_1, "label": "date-2024"},
        {"_id": DATE_2, "label": "date-2025"},
    ]


def _queries(oid_cls) -> list[tuple[str, dict]]:
    return [
        ("numeric_gt_int", {"_id": {"$gt": 1}}),
        ("numeric_lte_float", {"_id": {"$lte": 2.5}}),
        ("string_gt", {"_id": {"$gt": "abc"}}),
        ("string_lt", {"_id": {"$lt": "xyz"}}),
        ("bool_gt_false", {"_id": {"$gt": False}}),
        ("objectid_gte", {"_id": {"$gte": oid_cls(OID_1)}}),
        ("objectid_lt", {"_id": {"$lt": oid_cls(OID_2)}}),
        ("datetime_gt", {"_id": {"$gt": DATE_1}}),
        ("datetime_lte", {"_id": {"$lte": DATE_2}}),
        ("null_gt", {"_id": {"$gt": None}}),
        ("null_gte", {"_id": {"$gte": None}}),
    ]


def _neo_results() -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.id_ranges
        collection.insert_many(_documents(NeoObjectId))
        set_accumulation_mode(True)
        start_neo_timing()
        try:
            for name, query in _queries(NeoObjectId):
                results[name] = sorted(
                    doc["label"] for doc in collection.find(query)
                )
        finally:
            end_neo_timing()
    return results


def _mongo_results(client) -> dict[str, list[str]]:
    from bson.objectid import ObjectId as MongoObjectId

    results: dict[str, list[str]] = {}
    if client is None:
        return results

    collection = client.test_database.id_ranges
    collection.delete_many({})
    collection.insert_many(_documents(MongoObjectId))

    set_accumulation_mode(True)
    start_mongo_timing()
    try:
        for name, query in _queries(MongoObjectId):
            results[name] = sorted(
                doc["label"] for doc in collection.find(query)
            )
    finally:
        end_mongo_timing()
    return results


def compare_id_range_operators() -> None:
    """Compare ``_id`` range semantics between NeoSQLite and PyMongo."""
    print("\n=== _id Range Operators Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo = _neo_results()
    mongo = _mongo_results(client)

    skip = "MongoDB not available" if not mongo_available else None
    for name, _ in _queries(NeoObjectId):
        reporter.record_comparison(
            "_id Range Operators",
            name,
            neo.get(name),
            mongo.get(name),
            skip_reason=skip,
        )
