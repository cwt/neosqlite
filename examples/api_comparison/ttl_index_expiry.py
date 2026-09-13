"""Module for comparing TTL index behavior between NeoSQLite and PyMongo.

MongoDB deletes TTL-expired documents on a ~60s background tick, which is
too coarse for a synchronous comparison suite, so this module compares the
synchronously observable surface: TTL declaration, index_information()
visibility, survival of fresh documents through expiry, and TTL metadata
cleanup on drop_index.
"""

import warnings
from datetime import datetime, timezone

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

_EXPIRE_SECONDS = 43200


def _fresh_dates():
    now = datetime.now(timezone.utc)
    # BSON datetimes carry millisecond precision; truncate to match.
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def _ttl_value(index_info):
    for name, info in index_info.items():
        if name != "_id_" and "expireAfterSeconds" in info:
            return info["expireAfterSeconds"]
    return None


def compare_ttl_index_expiry():
    """Compare TTL index declaration and fresh-document survival"""
    print("\n=== TTL Index Expiry Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    # One shared timestamp so both sides store identical dates.
    now = _fresh_dates()
    seed_docs = [
        {"page": "a", "date": now},
        {"page": "b", "date": now},
    ]

    neo_ttl = None
    neo_remaining = []
    neo_ttl_after_drop = "present"
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([dict(doc) for doc in seed_docs])

        set_accumulation_mode(True)
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.create_index(
                "date", expireAfterSeconds=_EXPIRE_SECONDS
            )
            neo_ttl = _ttl_value(neo_collection.index_information())
            deleted = neo_collection.purge_expired()
            neo_remaining = list(neo_collection.find({}, sort=[("page", 1)]))
            neo_collection.drop_index("date")
            neo_ttl_after_drop = _ttl_value(neo_collection.index_information())
        finally:
            if mongo_available:
                end_neo_timing()
        print(
            f"NeoSQLite TTL: declared, purge deleted {deleted}, "
            f"remaining {len(neo_remaining)}"
        )

    mongo_ttl = None
    mongo_remaining = []
    mongo_ttl_after_drop = "present"
    if mongo_available:
        from datetime import timedelta

        mongo_collection = client.test_database.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([dict(doc) for doc in seed_docs])

        set_accumulation_mode(True)
        start_mongo_timing()
        try:
            mongo_collection.create_index(
                "date", expireAfterSeconds=_EXPIRE_SECONDS
            )
            mongo_ttl = _ttl_value(mongo_collection.index_information())
            cutoff = now - timedelta(seconds=_EXPIRE_SECONDS)
            mongo_collection.delete_many({"date": {"$lt": cutoff}})
            mongo_remaining = list(
                mongo_collection.find({}, sort=[("page", 1)])
            )
            mongo_collection.drop_index("date_1")
            mongo_ttl_after_drop = _ttl_value(
                mongo_collection.index_information()
            )
        finally:
            end_mongo_timing()
        print("PyMongo TTL: all cases completed")

    skip = "MongoDB not available" if not mongo_available else None
    reporter.record_comparison(
        "TTL Index Expiry",
        "TTL declaration accepted",
        "OK" if neo_ttl == _EXPIRE_SECONDS else f"FAIL: {neo_ttl}",
        "OK" if mongo_ttl == _EXPIRE_SECONDS else None,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "TTL Index Expiry",
        "fresh documents survive expiry",
        neo_remaining,
        mongo_remaining if mongo_available else None,
        ignore_order=False,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "TTL Index Expiry",
        "drop_index clears TTL metadata",
        "OK" if neo_ttl_after_drop is None else "FAIL",
        "OK" if mongo_ttl_after_drop is None else None,
        skip_reason=skip,
    )
