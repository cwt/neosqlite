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
