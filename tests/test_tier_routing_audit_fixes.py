"""Tier-routing guards for operations fixed in the audit round.

Correctness fixes must not silently demote common operations to the Python
tier. Numeric dotted paths are the one deliberate exception: MongoDB's
interpretation depends on the runtime parent type, so they stay in Tier 3.
"""

import neosqlite
from neosqlite.collection.query_helper import set_force_fallback
from neosqlite.objectid import ObjectId


def test_nested_array_and_scalar_queries_stay_in_tier1():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": 1, "nested": [[1, 2]], "v": 10},
                {"_id": 2, "nested": [[3]], "v": 20},
            ]
        )
        set_force_fallback(False)
        assert [d["_id"] for d in coll.find({"nested": [[1, 2]]})] == [1]
        assert coll.query_engine.get_last_tier() == "tier1"
        assert [d["_id"] for d in coll.find({"v": 10})] == [1]
        assert coll.query_engine.get_last_tier() == "tier1"


def test_numeric_paths_are_deliberately_python():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_one({"_id": 1, "a": {"1": "doc"}, "arr": ["x", "target"]})
        set_force_fallback(False)
        assert [d["_id"] for d in coll.find({"a.1": "doc"})] == [1]
        assert coll.query_engine.get_last_tier() == "tier3"
        assert [d["_id"] for d in coll.find({"arr.1": "target"})] == [1]
        assert coll.query_engine.get_last_tier() == "tier3"


def test_root_and_bucket_auto_stay_in_sql_tiers():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_many(
            [
                {"_id": i + 1, "v": v, "tags": [1]}
                for i, v in enumerate([10, 20, 15, 25, 30])
            ]
        )
        set_force_fallback(False)
        for pipeline in (
            [{"$count": "n"}, {"$project": {"r": "$$ROOT"}}],
            [
                {
                    "$densify": {
                        "field": "v",
                        "range": {"step": 1, "bounds": [1, 2]},
                    }
                },
                {"$project": {"r": "$$ROOT"}},
            ],
            [{"$bucketAuto": {"groupBy": "$v", "buckets": 2}}],
        ):
            list(coll.aggregate(pipeline))
            assert coll.query_engine.get_last_tier() == "tier1"

        list(
            coll.aggregate(
                [
                    {"$unwind": "$tags"},
                    {"$bucketAuto": {"groupBy": "$v", "buckets": 2}},
                ]
            )
        )
        assert coll.query_engine.get_last_tier() == "tier2"


def test_graphlookup_objectid_keys_stay_in_tier1():
    root, child, grandchild = ObjectId(), ObjectId(), ObjectId()
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.nodes
        coll.insert_many(
            [
                {"_id": root, "name": "root"},
                {"_id": child, "name": "child", "parent": root},
                {"_id": grandchild, "name": "grandchild", "parent": child},
            ]
        )
        set_force_fallback(False)
        result = list(
            coll.aggregate(
                [
                    {"$match": {"_id": grandchild}},
                    {
                        "$graphLookup": {
                            "from": "nodes",
                            "startWith": "$parent",
                            "connectFromField": "parent",
                            "connectToField": "_id",
                            "as": "ancestors",
                        }
                    },
                ]
            )
        )
        assert coll.query_engine.get_last_tier() == "tier1"
        assert {d["_id"] for d in result[0]["ancestors"]} == {root, child}


def test_normal_updates_stay_in_tier1():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.t
        coll.insert_one({"_id": 1, "v": 1})
        set_force_fallback(False)
        coll.update_one({"_id": 1}, {"$set": {"v": 2}})
        assert coll.query_engine.get_last_tier() == "tier1"
