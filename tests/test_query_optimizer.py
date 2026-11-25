"""Regression tests for query_optimizer barrier safety and SQL aggregation pipeline assembly (#196)."""

import neosqlite


class TestQueryOptimizerBarriers:
    """Verify $match pushdown and index reordering respect pipeline barriers."""

    def test_match_not_pushed_across_barriers(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.sales
            coll.create_index("_id")
            coll.create_index("total")
            coll.create_index("cat")
            qh = coll.query_engine.helpers

            barrier_pipelines = [
                # $group computes fields
                [
                    {"$group": {"_id": "$cat", "total": {"$sum": "$amt"}}},
                    {"$match": {"total": {"$gt": 10}}},
                ],
                # $limit changes window of rows
                [
                    {"$sort": {"amt": -1}},
                    {"$limit": 1},
                    {"$match": {"_id": 3}},
                ],
                # $skip changes window of rows
                [
                    {"$sort": {"amt": -1}},
                    {"$skip": 1},
                    {"$match": {"cat": "A"}},
                ],
                # $project reshapes documents
                [
                    {"$project": {"total": "$amt"}},
                    {"$match": {"total": {"$gt": 5}}},
                ],
                # $lookup generates `as` field
                [
                    {
                        "$lookup": {
                            "from": "other",
                            "localField": "k",
                            "foreignField": "k",
                            "as": "joined",
                        }
                    },
                    {"$match": {"joined.v": 1}},
                ],
                # $unwind affects unwound array path and includeArrayIndex
                [
                    {"$unwind": "$tags"},
                    {"$match": {"tags": "a"}},
                ],
                [
                    {"$unwind": {"path": "$tags", "includeArrayIndex": "idx"}},
                    {"$match": {"idx": 0}},
                ],
                # $addFields / $set overwrites matched field
                [
                    {"$addFields": {"total": {"$multiply": ["$a", 2]}}},
                    {"$match": {"total": {"$gt": 10}}},
                ],
                [
                    {"$set": {"cat": "B"}},
                    {"$match": {"cat": "B"}},
                ],
            ]

            for pipeline in barrier_pipelines:
                assert (
                    qh._optimize_match_pushdown(pipeline) == pipeline
                ), pipeline
                assert (
                    qh._reorder_pipeline_for_indexes(pipeline) == pipeline
                ), pipeline

    def test_match_pushed_when_independent_of_stage(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.sales
            coll.create_index("cat")
            qh = coll.query_engine.helpers

            # Independent of $unwind / $lookup / $addFields / $sort
            safe_unwind = [
                {"$unwind": "$tags"},
                {"$match": {"cat": "A"}},
            ]
            assert qh._optimize_match_pushdown(safe_unwind) == [
                {"$match": {"cat": "A"}},
                {"$unwind": "$tags"},
            ]
            assert qh._reorder_pipeline_for_indexes(safe_unwind) == [
                {"$match": {"cat": "A"}},
                {"$unwind": "$tags"},
            ]

            safe_lookup = [
                {
                    "$lookup": {
                        "from": "other",
                        "localField": "k",
                        "foreignField": "k",
                        "as": "joined",
                    }
                },
                {"$match": {"cat": "A"}},
            ]
            assert qh._optimize_match_pushdown(safe_lookup) == [
                {"$match": {"cat": "A"}},
                safe_lookup[0],
            ]

            safe_addfields = [
                {"$addFields": {"doubled": {"$multiply": ["$amt", 2]}}},
                {"$match": {"cat": "A"}},
            ]
            assert qh._optimize_match_pushdown(safe_addfields) == [
                {"$match": {"cat": "A"}},
                safe_addfields[0],
            ]

    def test_build_aggregation_query_combines_matches_and_respects_order(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            coll.insert_many(
                [
                    {"_id": 1, "a": 1, "b": 1},
                    {"_id": 2, "a": 1, "b": 2},
                    {"_id": 3, "a": 2, "b": 1},
                ]
            )
            qh = coll.query_engine.helpers

            built = qh._build_aggregation_query(
                [
                    {"$match": {"a": 1}},
                    {"$match": {"b": 2}},
                ]
            )
            assert built is not None
            sql, params, _ = built
            assert sql.count("AND") >= 1
            rows = conn.db.execute(sql, params).fetchall()
            assert len(rows) == 1

            # Non-commutative ordering must decline legacy single-query builder
            assert (
                qh._build_aggregation_query(
                    [{"$limit": 2}, {"$sort": {"a": 1}}]
                )
                is None
            )
            assert (
                qh._build_aggregation_query([{"$limit": 2}, {"$skip": 1}])
                is None
            )
            assert (
                qh._build_aggregation_query(
                    [
                        {"$group": {"_id": "$a", "c": {"$sum": 1}}},
                        {"$match": {"c": {"$gt": 0}}},
                    ]
                )
                is None
            )
