"""Query edge cases surfaced by MongoDB differential comparison."""

import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


def test_exact_nested_array_equality_both_tiers():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": 1, "nested": [[1, 2]]},
                {"_id": 2, "nested": [[3]]},
            ]
        )
        for force in (False, True):
            set_force_fallback(force)
            assert [d["_id"] for d in coll.find({"nested": [[1, 2]]})] == [1]
        set_force_fallback(False)


def test_numeric_document_key_query_both_tiers():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": 1, "a": {"1": "doc"}},
                {"_id": 2, "a": {"1": "other"}},
            ]
        )
        for force in (False, True):
            set_force_fallback(force)
            assert [d["_id"] for d in coll.find({"a.1": "doc"})] == [1]
        set_force_fallback(False)


def test_bucket_auto_uses_next_value_as_exclusive_upper_bound():
    """Match MongoDB's $bucketAuto boundaries across all tiers."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": i + 1, "price": price, "tags": [1]}
                for i, price in enumerate([10, 20, 15, 25, 30])
            ]
        )
        expected = [
            {"_id": {"min": 10, "max": 25}, "count": 3},
            {"_id": {"min": 25, "max": 30}, "count": 2},
        ]
        for pipeline in (
            [{"$bucketAuto": {"groupBy": "$price", "buckets": 2}}],
            [
                {"$unwind": "$tags"},
                {"$bucketAuto": {"groupBy": "$price", "buckets": 2}},
            ],
        ):
            for force in (False, True):
                set_force_fallback(force)
                assert list(coll.aggregate(pipeline)) == expected
        set_force_fallback(False)


def test_numeric_array_index_query_both_tiers():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": 1, "arr": ["x", "target"]},
                {"_id": 2, "arr": ["x", "other"]},
            ]
        )
        for force in (False, True):
            set_force_fallback(force)
            assert [d["_id"] for d in coll.find({"arr.1": "target"})] == [1]
        set_force_fallback(False)
