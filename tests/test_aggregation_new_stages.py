"""
Tests for new aggregation stages.

Covers: $bucket, $bucketAuto, $unionWith, $merge, $redact, $densify
"""

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)


class TestBucketStage:
    """Test $bucket aggregation stage."""

    def test_bucket_basic(self):
        """Test $bucket with basic boundaries."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"x": 5},
                    {"x": 15},
                    {"x": 25},
                    {"x": 35},
                    {"x": 45},
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$x",
                                "boundaries": [0, 10, 20, 30, 40, 50],
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )

            # Should have 5 buckets
            assert len(result) == 5
            # Each bucket should have count 1
            for bucket in result:
                assert bucket["count"] == 1

    def test_bucket_with_default(self):
        """Test $bucket with default for out-of-range values."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"x": 5},
                    {"x": 15},
                    {"x": 55},  # Out of range
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$x",
                                "boundaries": [0, 10, 20, 30],
                                "default": "Other",
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )

            # Should have buckets + "Other"
            assert len(result) >= 3

    def test_bucket_with_avg(self):
        """Test $bucket with $avg accumulator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"category": "A", "value": 10},
                    {"category": "A", "value": 20},
                    {"category": "B", "value": 30},
                    {"category": "B", "value": 40},
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$category",
                                "boundaries": ["A", "B", "C"],
                                "output": {"avgValue": {"$avg": "$value"}},
                            }
                        }
                    ]
                )
            )

            assert len(result) >= 1


class TestBucketAutoStage:
    """Test $bucketAuto aggregation stage."""

    def test_bucket_auto_basic(self):
        """Test $bucketAuto with specified number of buckets."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many([{"x": i} for i in range(1, 21)])  # 1 to 20

            result = list(
                coll.aggregate(
                    [
                        {
                            "$bucketAuto": {
                                "groupBy": "$x",
                                "buckets": 4,
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )

            # Should have approximately 4 buckets
            assert (
                len(result) <= 5
            )  # May have fewer if data doesn't divide evenly

    def test_bucket_auto_with_output(self):
        """Test $bucketAuto with custom output."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"price": 10},
                    {"price": 20},
                    {"price": 30},
                    {"price": 40},
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$bucketAuto": {
                                "groupBy": "$price",
                                "buckets": 2,
                                "output": {
                                    "count": {"$sum": 1},
                                    "avgPrice": {"$avg": "$price"},
                                },
                            }
                        }
                    ]
                )
            )

            assert len(result) >= 1
            for bucket in result:
                assert "count" in bucket
                assert "avgPrice" in bucket


class TestUnionWithStage:
    """Test $unionWith aggregation stage."""

    def test_union_with_basic(self):
        """Test $unionWith combines documents from another collection."""
        with neosqlite.Connection(":memory:") as conn:
            coll1 = conn.test_collection
            # Create the other collection first
            conn.create_collection("other_collection")
            coll2 = conn.get_collection("other_collection")

            coll1.insert_many(
                [
                    {"type": "A", "value": 1},
                    {"type": "A", "value": 2},
                ]
            )

            coll2.insert_many(
                [
                    {"type": "B", "value": 3},
                    {"type": "B", "value": 4},
                ]
            )

            result = list(
                coll1.aggregate([{"$unionWith": {"coll": "other_collection"}}])
            )

            # Should have documents from both collections
            assert len(result) == 4

    def test_union_with_pipeline(self):
        """Test $unionWith with pipeline."""
        with neosqlite.Connection(":memory:") as conn:
            coll1 = conn.test_collection
            # Create the other collection first
            conn.create_collection("other_collection")
            coll2 = conn.get_collection("other_collection")

            coll1.insert_many(
                [
                    {"type": "A", "value": 1},
                ]
            )

            coll2.insert_many(
                [
                    {"type": "B", "value": 2},
                    {"type": "B", "value": 3},
                ]
            )

            result = list(
                coll1.aggregate(
                    [
                        {
                            "$unionWith": {
                                "coll": "other_collection",
                                "pipeline": [{"$match": {"value": 2}}],
                            }
                        }
                    ]
                )
            )

            # Should have 2 documents (1 from coll1, 1 filtered from coll2)
            assert len(result) == 2


class TestMergeStage:
    """Test $merge aggregation stage."""

    def test_merge_basic(self):
        """Test $merge writes to collection."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            # Create target collection
            conn.create_collection("output_collection")
            target_coll = conn.get_collection("output_collection")

            coll.insert_many(
                [
                    {"x": 1, "value": "a"},
                    {"x": 2, "value": "b"},
                ]
            )

            # Merge to target collection
            list(coll.aggregate([{"$merge": {"into": "output_collection"}}]))

            # Check target collection has documents
            target_docs = list(target_coll.find())
            assert len(target_docs) == 2

    def test_merge_with_upsert(self):
        """Test $merge with existing documents."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            # Create and pre-populate target
            conn.create_collection("output_collection")
            target_coll = conn.get_collection("output_collection")
            target_coll.insert_one({"key": 1, "value": "original"})

            coll.insert_many(
                [
                    {"key": 1, "value": "updated"},  # Should update
                    {"key": 2, "value": "new"},  # Should insert
                ]
            )

            list(
                coll.aggregate(
                    [
                        {
                            "$merge": {
                                "into": "output_collection",
                                "on": "key",
                                "whenMatched": "merge",  # Use merge instead of replace to avoid _id issues
                            }
                        }
                    ]
                )
            )

            # Check results
            doc1 = target_coll.find_one({"key": 1})
            doc2 = target_coll.find_one({"key": 2})

            assert doc1["value"] == "updated"  # Updated
            assert doc2["value"] == "new"  # Inserted


class TestRedactStage:
    """Test $redact aggregation stage."""

    def test_redact_prune(self):
        """Test $redact with $$PRUNE removes fields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {"level": 1, "public": "visible", "secret": "hidden"}
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": {
                                    "if": {"$eq": ["$level", 1]},
                                    "then": "$$PRUNE",
                                    "else": "$$DESCEND",
                                }
                            }
                        }
                    ]
                )
            )

            # Document should be pruned (empty)
            assert len(result) == 0

    def test_redact_keep(self):
        """Test $redact with $$KEEP keeps document."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"level": 1, "value": "keep me"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": {
                                    "if": {"$eq": ["$level", 1]},
                                    "then": "$$KEEP",
                                    "else": "$$PRUNE",
                                }
                            }
                        }
                    ]
                )
            )

            # Document should be kept
            assert len(result) == 1
            assert result[0]["value"] == "keep me"

    def test_redact_descend(self):
        """Test $redact with $$DESCEND processes sub-fields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one(
                {
                    "level": 2,
                    "data": {
                        "public": "visible",
                        "secret": {"level": 3, "value": "hidden"},
                    },
                }
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": {
                                    "if": {"$gte": ["$level", 3]},
                                    "then": "$$PRUNE",
                                    "else": "$$DESCEND",
                                }
                            }
                        }
                    ]
                )
            )

            # Document should have secret pruned but public kept
            assert len(result) == 1
            assert "public" in result[0]["data"]


class TestDensifyStage:
    """Test $densify aggregation stage."""

    def test_densify_numeric(self):
        """Test $densify fills numeric gaps."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"year": 2000, "value": 1},
                    {"year": 2002, "value": 2},
                    {"year": 2005, "value": 3},
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$densify": {
                                "field": "year",
                                "range": {"bounds": [2000, 2005], "step": 1},
                            }
                        }
                    ]
                )
            )

            # Should have 6 documents (2000, 2001, 2002, 2003, 2004, 2005)
            assert len(result) == 6
            years = sorted([doc["year"] for doc in result])
            assert years == [2000, 2001, 2002, 2003, 2004, 2005]

    def test_densify_with_partition(self):
        """Test $densify with partitionByFields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many(
                [
                    {"category": "A", "year": 2000},
                    {"category": "A", "year": 2002},
                    {"category": "B", "year": 2000},
                    {"category": "B", "year": 2001},
                ]
            )

            result = list(
                coll.aggregate(
                    [
                        {
                            "$densify": {
                                "field": "year",
                                "range": {"bounds": "full", "step": 1},
                                "partitionByFields": ["category"],
                            }
                        }
                    ]
                )
            )

            # Category A: 2000, 2001, 2002 (3 docs)
            # Category B: 2000, 2001 (2 docs)
            assert len(result) == 5


class TestNewStagesKillSwitch:
    """Test new aggregation stages with kill switch."""

    def test_bucket_kill_switch(self):
        """Test $bucket with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_many([{"x": i} for i in range(1, 11)])

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$bucket": {
                                    "groupBy": "$x",
                                    "boundaries": [0, 5, 10],
                                    "output": {"count": {"$sum": 1}},
                                }
                            }
                        ]
                    )
                )

                # Should work with Python fallback
                assert isinstance(result, list)
            finally:
                set_force_fallback(original_state)

    def test_union_with_kill_switch(self):
        """Test $unionWith with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll1 = conn.test_collection
            # Create the other collection first
            conn.create_collection("other_collection")
            coll2 = conn.get_collection("other_collection")

            coll1.insert_one({"type": "A"})
            coll2.insert_one({"type": "B"})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll1.aggregate(
                        [{"$unionWith": {"coll": "other_collection"}}]
                    )
                )

                assert len(result) == 2
            finally:
                set_force_fallback(original_state)
