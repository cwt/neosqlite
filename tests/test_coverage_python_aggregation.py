"""Comprehensive coverage tests for python_aggregation_engine.py."""

from datetime import datetime, timedelta

import pytest

import neosqlite
from neosqlite.collection.query_engine.python_aggregation_engine import (
    execute_python_aggregation,
)
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.exceptions import MalformedQueryException


@pytest.fixture
def restore_force_fallback():
    original = get_force_fallback()
    try:
        yield
    finally:
        set_force_fallback(original)


class TestPythonAggregationCoreStages:
    """Test error cases and edge branches in core stages."""

    def test_empty_and_unsupported_stage_and_text_score_sort(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.articles
            coll.insert_many(
                [
                    {"title": "python sqlite"},
                    {"title": "python sqlite python database"},
                    {"title": "other topic"},
                ]
            )
            coll.create_index([("title", "text")])
            qe = coll.query_engine

            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{}])

            with pytest.raises(MalformedQueryException, match="not supported"):
                execute_python_aggregation(qe, [{"$notARealStage": {}}])

            res = execute_python_aggregation(
                qe,
                [
                    {"$match": {"$text": {"$search": "python"}}},
                    {"$sort": {"score": {"$meta": "textScore"}}},
                ],
            )
            assert len(res) == 2
            assert "python database" in res[0]["title"]

    def test_replace_root_replace_with_and_unset(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.items
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "sub": {"x": 10, "y": 20},
                        "a": {"b": 1, "c": 2},
                    },
                    {"_id": 2, "sub": "not_a_dict", "a": {"b": 3, "c": 4}},
                ]
            )
            qe = coll.query_engine

            # Valid $replaceRoot preserves original _id when newRoot lacks _id
            res = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {"$replaceRoot": {"newRoot": "$sub"}},
                ],
            )
            assert res == [{"_id": 1, "x": 10, "y": 20}]

            # Dict literal expression in $replaceWith
            res_w = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {"$replaceWith": {"k": 99}},
                ],
            )
            assert res_w == [{"_id": 1, "k": 99}]

            # Invalid $replaceRoot / $replaceWith non-dict result raises
            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$replaceRoot": {}}])

            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(
                    qe,
                    [
                        {"$match": {"_id": 2}},
                        {"$replaceRoot": {"newRoot": "$sub"}},
                    ],
                )

            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$replaceWith": "$sub"}])

            # $replaceRoot expression eval exception falls back to original/dict check
            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(
                    qe,
                    [{"$replaceRoot": {"newRoot": {"$divide": [1, 0]}}}],
                )

            # $unset with string, list, nested dotted paths, invalid spec
            res_u1 = execute_python_aggregation(qe, [{"$unset": "a.b"}])
            assert res_u1[0]["a"] == {"c": 2}

            res_u2 = execute_python_aggregation(
                qe, [{"$unset": ["a.c", "sub", "missing.path"]}]
            )
            assert "sub" not in res_u2[0] and res_u2[0]["a"] == {"b": 1}

            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$unset": 123}])

    def test_unwind_lookup_addfields_and_sample(self):
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.orders
            items = conn.items
            orders.insert_many(
                [
                    {
                        "_id": 1,
                        "groups": [{"codes": ["c1", "c2"]}],
                        "nested": {"arr": []},
                    },
                    {
                        "_id": 2,
                        "groups": [],
                        "other": "scalar",
                    },
                ]
            )
            items.insert_many(
                [
                    {"code": "c1", "price": 5},
                    {"code": "c2", "price": 10},
                ]
            )
            qe = orders.query_engine

            # $unwind invalid specs
            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$unwind": 42}])
            with pytest.raises(KeyError):
                execute_python_aggregation(qe, [{"$unwind": {}}])

            # $unwind preserveNullAndEmptyArrays + includeArrayIndex on nested empty/missing array
            unwound = execute_python_aggregation(
                qe,
                [
                    {
                        "$unwind": {
                            "path": "$nested.arr",
                            "preserveNullAndEmptyArrays": True,
                            "includeArrayIndex": "idx",
                        }
                    }
                ],
            )
            assert len(unwound) == 1
            assert unwound[0]["nested"] == {}
            assert unwound[0]["idx"] is None

            # $lookup with nested list-of-dicts local path
            joined = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {
                        "$lookup": {
                            "from": "items",
                            "localField": "groups.codes",
                            "foreignField": "code",
                            "as": "matched",
                        }
                    },
                ],
            )
            assert len(joined[0]["matched"]) == 2

            # $addFields referencing unknown $$VAR falls back cleanly to None
            added = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {"$addFields": {"unknown_var": "$$NONEXISTENT"}},
                ],
            )
            assert added[0]["unknown_var"] is None

            # $sample valid and negative size error
            sampled = execute_python_aggregation(qe, [{"$sample": {"size": 1}}])
            assert len(sampled) == 1
            with pytest.raises(MalformedQueryException, match="non-negative"):
                execute_python_aggregation(qe, [{"$sample": {"size": -1}}])

    def test_facet_non_force_fallback_and_collstats(
        self, restore_force_fallback
    ):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.metrics
            coll.insert_many([{"x": 1}, {"x": 2}, {"x": 3}])
            set_force_fallback(False)
            qe = coll.query_engine

            # Direct execute_python_aggregation with force_fallback=False hits _run_subpipeline SQL path
            res = execute_python_aggregation(
                qe,
                [
                    {
                        "$facet": {
                            "cnt": [{"$count": "total"}],
                            "filtered": [
                                {"$match": {"x": {"$gte": 2}}},
                                {"$sort": {"x": -1}},
                            ],
                        }
                    }
                ],
            )
            assert res[0]["cnt"] == [{"total": 3}]
            assert [d["x"] for d in res[0]["filtered"]] == [3, 2]

            # $collStats with force_fallback=False uses SQLite table stats
            cs1 = execute_python_aggregation(
                qe, [{"$collStats": {"count": {}}}]
            )
            assert cs1 == [{"count": 3}]

            cs2 = execute_python_aggregation(
                qe, [{"$collStats": {"storageStats": {}}}]
            )
            assert "storageStats" in cs2[0]
            assert cs2[0]["storageStats"]["count"] == 3


class TestPythonAggregationBucketsAndUnions:
    """Test $bucket, $bucketAuto, and $unionWith."""

    def test_bucket_and_bucket_auto_variants(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.sales
            coll.insert_many(
                [
                    {"val": 5, "price": 10},
                    {"val": 15, "price": 20},
                    {"val": 25, "price": 30},
                    {"val": "bad_type", "price": 40},
                    {"val": None, "price": 50},
                ]
            )
            qe = coll.query_engine

            # Empty $bucket spec returns empty list
            assert execute_python_aggregation(qe, [{"$bucket": {}}]) == []

            # Mixed types + default bucket + all accumulators
            buckets = execute_python_aggregation(
                qe,
                [
                    {
                        "$bucket": {
                            "groupBy": "$val",
                            "boundaries": [0, 20, 40],
                            "default": "other",
                            "output": {
                                "count": {"$sum": 1},
                                "totalPrice": {"$sum": "$price"},
                                "avgPrice": {"$avg": "$price"},
                                "minPrice": {"$min": "$price"},
                                "maxPrice": {"$max": "$price"},
                                "countOp": {"$count": {}},
                                "customList": "$price",
                            },
                        }
                    }
                ],
            )
            by_id = {b["_id"]: b for b in buckets}
            assert by_id[0]["count"] == 2
            assert by_id[0]["totalPrice"] == 30
            assert by_id[0]["avgPrice"] == 15.0
            assert by_id[0]["minPrice"] == 10
            assert by_id[0]["maxPrice"] == 20
            assert by_id[0]["countOp"] == 2
            assert by_id["other"]["count"] == 2

            # Bucket without default uses "Other" default bucket
            buckets_no_def = execute_python_aggregation(
                qe,
                [
                    {
                        "$bucket": {
                            "groupBy": "$val",
                            "boundaries": [0, 20],
                        }
                    }
                ],
            )
            assert [b["_id"] for b in buckets_no_def] == [0, "Other"]

            # $bucketAuto empty/invalid specs return []
            assert execute_python_aggregation(qe, [{"$bucketAuto": {}}]) == []
            assert (
                execute_python_aggregation(
                    qe, [{"$bucketAuto": {"groupBy": "$price", "buckets": 0}}]
                )
                == []
            )

            # $bucketAuto empty input returns []
            assert (
                execute_python_aggregation(
                    qe,
                    [
                        {"$match": {"price": 999}},
                        {"$bucketAuto": {"groupBy": "$price", "buckets": 2}},
                    ],
                )
                == []
            )

            # $bucketAuto normal & custom output accumulators + custom dict key sort
            auto_buckets = execute_python_aggregation(
                qe,
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$price",
                            "buckets": 3,
                            "output": {
                                "cnt": {"$sum": 1},
                                "sumP": {"$sum": "$price"},
                                "avgP": {"$avg": "$price"},
                                "minP": {"$min": "$price"},
                                "maxP": {"$max": "$price"},
                                "countOp": {"$count": {}},
                            },
                        }
                    }
                ],
            )
            assert len(auto_buckets) == 3
            assert sum(b["cnt"] for b in auto_buckets) == 5

    def test_union_with_variants(self):
        with neosqlite.Connection(":memory:") as conn:
            c1 = conn.c1
            c2 = conn.c2
            c1.insert_one({"src": "c1", "v": 1})
            c2.insert_many([{"src": "c2", "v": 2}, {"src": "c2", "v": 10}])
            qe = c1.query_engine

            # String shorthand and dict with subpipeline
            u1 = execute_python_aggregation(qe, [{"$unionWith": "c2"}])
            assert len(u1) == 3

            u2 = execute_python_aggregation(
                qe,
                [
                    {
                        "$unionWith": {
                            "coll": "c2",
                            "pipeline": [{"$match": {"v": {"$gte": 5}}}],
                        }
                    }
                ],
            )
            assert [d["v"] for d in u2] == [1, 10]

            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$unionWith": 123}])
            assert (
                len(
                    execute_python_aggregation(
                        qe, [{"$unionWith": {"coll": ""}}]
                    )
                )
                == 1
            )


class TestPythonAggregationMergeOutRedactDensify:
    """Test $merge, $out, $redact, and $densify in Python engine."""

    def test_merge_and_out_modes(self):
        with neosqlite.Connection(":memory:") as conn:
            src = conn.src
            tgt = conn.tgt
            src.insert_many(
                [
                    {"_id": 1, "name": "new1"},
                    {"_id": 2, "name": "new2"},
                ]
            )
            tgt.insert_one({"_id": 1, "name": "old1", "extra": "drop_me"})
            qe = src.query_engine

            # whenMatched="keepExisting", whenNotMatched="discard"
            execute_python_aggregation(
                qe,
                [
                    {
                        "$merge": {
                            "into": {"db": "ignored", "coll": "tgt"},
                            "whenMatched": "keepExisting",
                            "whenNotMatched": "discard",
                        }
                    }
                ],
            )
            assert tgt.find_one({"_id": 1})["name"] == "old1"
            assert tgt.find_one({"_id": 2}) is None

            # whenMatched="merge", whenNotMatched="insert"
            execute_python_aggregation(
                qe,
                [
                    {
                        "$merge": {
                            "into": "tgt",
                            "whenMatched": "merge",
                            "whenNotMatched": "insert",
                        }
                    }
                ],
            )
            d1 = tgt.find_one({"_id": 1})
            assert d1["name"] == "new1" and d1["extra"] == "drop_me"
            assert tgt.find_one({"_id": 2})["name"] == "new2"

            # whenMatched="replace" removes extra existing fields
            execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {"$merge": {"into": "tgt", "whenMatched": "replace"}},
                ],
            )
            d1_rep = tgt.find_one({"_id": 1})
            assert "extra" not in d1_rep and d1_rep["name"] == "new1"

            # whenMatched="fail" and whenNotMatched="fail"
            with pytest.raises(Exception, match="already exists"):
                execute_python_aggregation(
                    qe, [{"$merge": {"into": "tgt", "whenMatched": "fail"}}]
                )
            src.insert_one({"_id": 99, "name": "unmatched"})
            with pytest.raises(Exception, match="no document found"):
                execute_python_aggregation(
                    qe,
                    [
                        {"$match": {"_id": 99}},
                        {"$merge": {"into": "tgt", "whenNotMatched": "fail"}},
                    ],
                )

            # Non-dict/non-str $merge falls back to "merged" collection
            execute_python_aggregation(qe, [{"$merge": 123}])
            assert conn.merged.count_documents({}) >= 1
            with pytest.raises(ValueError, match="empty"):
                execute_python_aggregation(qe, [{"$merge": {"into": ""}}])

            # $out string and dict and error cases
            execute_python_aggregation(
                qe, [{"$match": {"_id": 1}}, {"$out": "out1"}]
            )
            assert [d["_id"] for d in conn.out1.find()] == [1]

            execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 2}},
                    {"$out": {"db": "main", "coll": "out2"}},
                ],
            )
            assert [d["_id"] for d in conn.out2.find()] == [2]

            with pytest.raises(ValueError, match="empty"):
                execute_python_aggregation(qe, [{"$out": ""}])
            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$out": 123}])
            with pytest.raises(MalformedQueryException):
                execute_python_aggregation(qe, [{"$out": {"coll": ""}}])

    def test_redact_nested_lists_and_direct_vars(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            coll.insert_one(
                {
                    "_id": 1,
                    "level": 1,
                    "tags": ["public"],
                    "nested_list": [
                        {"level": 1, "secret": "ok"},
                        {"level": 5, "secret": "hidden"},
                    ],
                    "child": {"level": 5, "val": 99},
                }
            )
            qe = coll.query_engine

            # $$DESCEND into dicts and lists of dicts
            red = execute_python_aggregation(
                qe,
                [
                    {
                        "$redact": {
                            "$cond": {
                                "if": {"$eq": ["$level", 1]},
                                "then": "$$DESCEND",
                                "else": "$$PRUNE",
                            }
                        }
                    }
                ],
            )
            assert len(red) == 1
            assert red[0]["child"] == {}
            assert len(red[0]["nested_list"]) == 2
            assert red[0]["nested_list"][0]["secret"] == "ok"
            assert red[0]["nested_list"][1] == {}

            # Non-dict redact spec falls back to returning doc unchanged
            non_dict = execute_python_aggregation(qe, [{"$redact": "$$KEEP"}])
            assert len(non_dict) == 1 and non_dict[0]["child"]["val"] == 99

            # Top-level $$PRUNE via $cond removes the document
            pruned = execute_python_aggregation(
                qe,
                [
                    {
                        "$redact": {
                            "$cond": {
                                "if": {"$eq": ["$level", 999]},
                                "then": "$$KEEP",
                                "else": "$$PRUNE",
                            }
                        }
                    }
                ],
            )
            assert pruned == []

            # Eval error fallback treats as $$KEEP / fallback without crashing
            err_red = execute_python_aggregation(
                qe, [{"$redact": {"$divide": [1, 0]}}]
            )
            assert len(err_red) == 1

    def test_densify_numeric_partitioned_and_all_date_units(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.series
            base_dt = datetime(2024, 1, 31, 10, 0, 0)
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "cat": {"name": "A"},
                        "seq": 1,
                        "dt": base_dt,
                        "keep": "orig",
                    },
                    {
                        "_id": 2,
                        "cat": {"name": "A"},
                        "seq": 3,
                        "dt": datetime(2024, 3, 1, 10, 0, 0),
                        "keep": "orig",
                    },
                    {
                        "_id": 3,
                        "cat": {"name": "B"},
                        "seq": 10,
                        "dt": base_dt,
                        "keep": "orig",
                    },
                ]
            )
            qe = coll.query_engine

            # Missing field returns original docs unchanged; invalid step raises ValueError
            assert (
                len(
                    execute_python_aggregation(
                        qe, [{"$densify": {"range": {"step": 1}}}]
                    )
                )
                == 3
            )
            with pytest.raises(ValueError):
                execute_python_aggregation(
                    qe,
                    [
                        {
                            "$densify": {
                                "field": "seq",
                                "range": {"step": 0, "bounds": "full"},
                            }
                        }
                    ],
                )

            # Empty input returns []
            assert (
                execute_python_aggregation(
                    qe,
                    [
                        {"$match": {"seq": 999}},
                        {
                            "$densify": {
                                "field": "seq",
                                "range": {"step": 1, "bounds": "full"},
                            }
                        },
                    ],
                )
                == []
            )

            # partitionByFields dotted + output spec
            res_part = execute_python_aggregation(
                qe,
                [
                    {
                        "$densify": {
                            "field": "seq",
                            "partitionByFields": ["cat.name"],
                            "range": {"step": 1, "bounds": "partition"},
                            "output": {"filled": True},
                        }
                    }
                ],
            )
            assert len(res_part) == 4  # added seq=2 in partition A
            added_doc = [d for d in res_part if d.get("seq") == 2][0]
            assert added_doc["cat"] == {"name": "A"}
            assert added_doc["filled"] is True

            # partitionByFields as string shorthand
            res_str_part = execute_python_aggregation(
                qe,
                [
                    {
                        "$densify": {
                            "field": "seq",
                            "partitionByFields": "cat.name",
                            "range": {"step": 1, "bounds": [1, 3]},
                        }
                    }
                ],
            )
            assert len(res_str_part) >= 4

            # All date units with explicit bounds (mid-month avoids 31->28 overflow)
            mid_dt = datetime(2024, 1, 15, 10, 0, 0)
            coll.insert_one({"_id": 4, "dt": mid_dt})
            for unit, delta in [
                ("year", timedelta(days=731)),
                ("month", timedelta(days=65)),
                ("day", timedelta(days=2)),
                ("hour", timedelta(hours=2)),
                ("minute", timedelta(minutes=2)),
                ("second", timedelta(seconds=2)),
            ]:
                out = execute_python_aggregation(
                    qe,
                    [
                        {"$match": {"_id": 4}},
                        {
                            "$densify": {
                                "field": "dt",
                                "range": {
                                    "step": 1,
                                    "unit": unit,
                                    "bounds": [mid_dt, mid_dt + delta],
                                },
                            }
                        },
                    ],
                )
                assert len(out) >= 2

            # Month-end edge (Jan 31 -> Feb 29/Mar) handled without ValueError
            out_eom = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {
                        "$densify": {
                            "field": "dt",
                            "range": {
                                "step": 1,
                                "unit": "month",
                                "bounds": [
                                    base_dt,
                                    datetime(2024, 4, 1, 10, 0),
                                ],
                            },
                        }
                    },
                ],
            )
            assert len(out_eom) >= 1

            # Unknown unit breaks out cleanly without infinite loop
            out_unk = execute_python_aggregation(
                qe,
                [
                    {"$match": {"_id": 1}},
                    {
                        "$densify": {
                            "field": "dt",
                            "range": {
                                "step": 1,
                                "unit": "century",
                                "bounds": [
                                    base_dt,
                                    base_dt + timedelta(days=5),
                                ],
                            },
                        }
                    },
                ],
            )
            assert len(out_unk) == 1
