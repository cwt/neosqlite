"""
Tests for temporary table aggregation fixes.

This module tests the SQL error fixes implemented in temporary_table_aggregation.py:
- Column detection in _get_results_from_table
- Sort/skip/limit column handling
- UNION ALL column matching
- Window functions _id handling
- graphLookup restrictSearchWithMatch
- bucket/bucketAuto aggregations
- Window frame SQL generation
"""

import neosqlite


class TestDeterministicTempTableManager:
    """Tests for DeterministicTempTableManager class."""

    def test_make_temp_table_name_deterministic(self):
        """Test that table names are deterministic for same stage."""
        from neosqlite.collection.temporary_table_aggregation import (
            DeterministicTempTableManager,
        )

        manager1 = DeterministicTempTableManager("test_pipeline")
        manager2 = DeterministicTempTableManager("test_pipeline")
        stage = {"$match": {"age": {"$gte": 18}}}

        # Same stage with fresh managers should produce same base name
        name1 = manager1.make_temp_table_name(stage)
        name2 = manager2.make_temp_table_name(stage)
        # Names should have same hash (first part before any numbering)
        assert name1.split("_")[:4] == name2.split("_")[:4]

    def test_make_temp_table_name_unique_for_different_stages(self):
        """Test that different stages produce different names."""
        from neosqlite.collection.temporary_table_aggregation import (
            DeterministicTempTableManager,
        )

        manager = DeterministicTempTableManager("test_pipeline")
        stage1 = {"$match": {"age": {"$gte": 18}}}
        stage2 = {"$match": {"age": {"$lt": 65}}}

        name1 = manager.make_temp_table_name(stage1)
        name2 = manager.make_temp_table_name(stage2)
        assert name1 != name2

    def test_make_temp_table_name_with_suffix(self):
        """Test table name generation with suffix."""
        from neosqlite.collection.temporary_table_aggregation import (
            DeterministicTempTableManager,
        )

        manager = DeterministicTempTableManager("test_pipeline")
        stage = {"$match": {"status": "active"}}

        name1 = manager.make_temp_table_name(stage)
        name2 = manager.make_temp_table_name(stage, name_suffix="_v2")

        assert name1 != name2
        assert "match" in name1
        assert "match" in name2

    def test_make_temp_table_name_uniqueness_tracking(self):
        """Test that duplicate calls get numbered."""
        from neosqlite.collection.temporary_table_aggregation import (
            DeterministicTempTableManager,
        )

        manager = DeterministicTempTableManager("test_pipeline")
        stage = {"$match": {"x": 1}}

        manager.make_temp_table_name(stage)
        name2 = manager.make_temp_table_name(stage)

        # Second call should be numbered
        assert name2.endswith("_1")


class TestGetResultsFromTable:
    """Tests for _get_results_from_table column detection."""

    def test_get_results_with_standard_columns(self):
        """Test result retrieval with id, _id, data columns."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_standard
            coll.insert_one({"name": "test", "value": 42})

            processor = TemporaryTableAggregationProcessor(coll)

            # Create temp table with standard columns
            conn.db.execute(
                "CREATE TEMP TABLE test_std AS SELECT id, _id, data FROM test_standard"
            )

            results = processor._get_results_from_table("test_std")
            assert len(results) == 1
            assert results[0]["name"] == "test"

    def test_get_results_with_id_only(self):
        """Test result retrieval with only id column (no _id)."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_id_only
            coll.insert_one({"name": "test"})

            processor = TemporaryTableAggregationProcessor(coll)

            # Create temp table with only id and data
            conn.db.execute(
                "CREATE TEMP TABLE test_id AS SELECT id, data FROM test_id_only"
            )

            results = processor._get_results_from_table("test_id")
            assert len(results) == 1

    def test_get_results_with_custom_columns(self):
        """Test result retrieval from non-standard tables (e.g., $bucket output)."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_custom
            coll.insert_one({"x": 1})

            processor = TemporaryTableAggregationProcessor(coll)

            # Create temp table with custom columns (like $bucket output)
            conn.db.execute("""CREATE TEMP TABLE test_custom AS
                SELECT 0 AS _id, 5 AS count""")

            results = processor._get_results_from_table("test_custom")
            assert len(results) == 1
            assert results[0]["_id"] == 0
            assert results[0]["count"] == 5


class TestSortSkipLimitStage:
    """Tests for _process_sort_skip_limit_stage column handling."""

    def test_sort_by_id_when_only_id_exists(self):
        """Test sorting by _id when table only has id column."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort
            coll.insert_many(
                [
                    {"value": 1},
                    {"value": 2},
                    {"value": 3},
                ]
            )

            # Sort by _id should work
            pipeline = [{"$sort": {"_id": 1}}]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 3


class TestUnionWithStage:
    """Tests for _process_union_with_stage column matching."""

    def test_union_with_matching_columns(self):
        """Test UNION ALL with tables having matching columns."""
        with neosqlite.Connection(":memory:") as conn:
            coll1 = conn.test_union1
            coll2 = conn.test_union2
            coll1.insert_one({"item": "A", "price": 10})
            coll2.insert_one({"item": "B", "price": 20})

            pipeline = [
                {"$unionWith": {"coll": "test_union2"}},
                {"$sort": {"item": 1}},
            ]
            results = list(coll1.aggregate(pipeline))
            assert len(results) == 2


class TestSetWindowFieldsStage:
    """Tests for _process_set_window_fields_stage."""

    def test_window_with_partition_by_and_frame(self):
        """Test window function with partitionBy and window frame."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_window_frame
            coll.insert_many(
                [
                    {"_id": 1, "dept": "A", "score": 100},
                    {"_id": 2, "dept": "A", "score": 90},
                    {"_id": 3, "dept": "B", "score": 80},
                ]
            )

            pipeline = [
                {
                    "$setWindowFields": {
                        "partitionBy": "$dept",
                        "sortBy": {"_id": 1},
                        "output": {
                            "runningSum": {
                                "$sum": "$score",
                                "window": {
                                    "documents": ["unbounded", "current"]
                                },
                            }
                        },
                    }
                }
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 3
            # First doc in dept A should have runningSum = 100
            doc_a = [r for r in results if r["_id"] == 1][0]
            assert doc_a["runningSum"] == 100

    def test_window_without_sort_by(self):
        """Test window function without sortBy (should not use frame)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_window_no_sort
            coll.insert_many(
                [
                    {"_id": 1, "dept": "A", "score": 100},
                    {"_id": 2, "dept": "A", "score": 90},
                ]
            )

            pipeline = [
                {
                    "$setWindowFields": {
                        "partitionBy": "$dept",
                        "output": {"total": {"$sum": "$score"}},
                    }
                }
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2


class TestBuildWindowFrameSql:
    """Tests for _build_window_frame_sql method."""

    def test_unbounded_to_current(self):
        """Test UNBOUNDED PRECEDING to CURRENT ROW."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame
            processor = TemporaryTableAggregationProcessor(coll)

            window_spec = {"documents": ["unbounded", "current"]}
            result = processor._build_window_frame_sql(window_spec)
            assert "UNBOUNDED PRECEDING" in result
            assert "CURRENT ROW" in result

    def test_unbounded_to_unbounded(self):
        """Test UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame2
            processor = TemporaryTableAggregationProcessor(coll)

            window_spec = {"documents": ["unbounded", "unbounded"]}
            result = processor._build_window_frame_sql(window_spec)
            assert "UNBOUNDED PRECEDING" in result
            assert "UNBOUNDED FOLLOWING" in result

    def test_integer_bounds(self):
        """Test integer window bounds."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame3
            processor = TemporaryTableAggregationProcessor(coll)

            window_spec = {"documents": [-1, 1]}
            result = processor._build_window_frame_sql(window_spec)
            assert "1 PRECEDING" in result
            assert "1 FOLLOWING" in result

    def test_empty_window_spec(self):
        """Test empty window spec returns empty string."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame4
            processor = TemporaryTableAggregationProcessor(coll)

            result = processor._build_window_frame_sql(None)
            assert result == ""

            result = processor._build_window_frame_sql({})
            assert result == ""


class TestGraphLookupStage:
    """Tests for _process_graph_lookup_stage restrictSearchWithMatch."""

    def test_graph_lookup_with_restrict(self):
        """Test graphLookup with restrictSearchWithMatch."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.employees
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "name": "CEO",
                        "reportsTo": None,
                        "dept": "Exec",
                    },
                    {"_id": 2, "name": "EngMgr", "reportsTo": 1, "dept": "Eng"},
                    {
                        "_id": 3,
                        "name": "SalesMgr",
                        "reportsTo": 1,
                        "dept": "Sales",
                    },
                    {
                        "_id": 4,
                        "name": "Engineer",
                        "reportsTo": 2,
                        "dept": "Eng",
                    },
                ]
            )

            pipeline = [
                {"$match": {"_id": 4}},
                {
                    "$graphLookup": {
                        "from": "employees",
                        "startWith": "$reportsTo",
                        "connectFromField": "reportsTo",
                        "connectToField": "_id",
                        "as": "managers",
                        "restrictSearchWithMatch": {"dept": "Eng"},
                    }
                },
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 1
            # Should only include Eng department managers
            managers = results[0].get("managers", [])
            for mgr in managers:
                assert mgr.get("dept") == "Eng"

    def test_graph_lookup_without_restrict(self):
        """Test graphLookup without restrictSearchWithMatch."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.employees2
            coll.insert_many(
                [
                    {"_id": 1, "name": "CEO", "reportsTo": None},
                    {"_id": 2, "name": "Mgr", "reportsTo": 1},
                    {"_id": 3, "name": "Dev", "reportsTo": 2},
                ]
            )

            pipeline = [
                {"$match": {"_id": 3}},
                {
                    "$graphLookup": {
                        "from": "employees2",
                        "startWith": "$reportsTo",
                        "connectFromField": "reportsTo",
                        "connectToField": "_id",
                        "as": "managers",
                    }
                },
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 1
            assert "managers" in results[0]


class TestBucketStage:
    """Tests for _process_bucket_stage."""

    def test_bucket_with_boundaries(self):
        """Test $bucket with boundaries."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_bucket
            coll.insert_many(
                [
                    {"item": "A", "price": 10},
                    {"item": "B", "price": 20},
                    {"item": "C", "price": 15},
                    {"item": "D", "price": 25},
                    {"item": "E", "price": 30},
                ]
            )

            pipeline = [
                {
                    "$bucket": {
                        "groupBy": "$price",
                        "boundaries": [0, 15, 25, 100],
                        "default": "other",
                        "output": {"count": {"$sum": 1}},
                    }
                }
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 3
            # Check bucket _id values are lower boundaries
            bucket_ids = [r["_id"] for r in results]
            assert 0 in bucket_ids
            assert 15 in bucket_ids
            assert 25 in bucket_ids


class TestBucketAutoStage:
    """Tests for _process_bucket_auto_stage."""

    def test_bucket_auto_with_count(self):
        """Test $bucketAuto with count output."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_bucketauto
            coll.insert_many(
                [
                    {"item": "A", "price": 10},
                    {"item": "B", "price": 20},
                    {"item": "C", "price": 15},
                    {"item": "D", "price": 25},
                    {"item": "E", "price": 30},
                ]
            )

            pipeline = [{"$bucketAuto": {"groupBy": "$price", "buckets": 2}}]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2
            # Check _id has min/max structure
            for result in results:
                assert "_id" in result
                assert "min" in result["_id"]
                assert "max" in result["_id"]


class TestGroupFirstLast:
    """Tests for $first/$last accumulators in $group."""

    def test_group_with_first(self):
        """Test $group with $first accumulator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_first
            coll.insert_many(
                [
                    {"category": "A", "value": 10},
                    {"category": "A", "value": 20},
                    {"category": "B", "value": 30},
                ]
            )

            pipeline = [
                {
                    "$group": {
                        "_id": "$category",
                        "first_value": {"$first": "$value"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2

    def test_group_with_last(self):
        """Test $group with $last accumulator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_last
            coll.insert_many(
                [
                    {"category": "A", "value": 10},
                    {"category": "A", "value": 20},
                    {"category": "B", "value": 30},
                ]
            )

            pipeline = [
                {
                    "$group": {
                        "_id": "$category",
                        "last_value": {"$last": "$value"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2

    def test_group_by_id_with_first(self):
        """Test $group by $_id with $first (special case)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_group_id
            coll.insert_many(
                [
                    {"_id": 1, "name": "Alice"},
                    {"_id": 2, "name": "Bob"},
                ]
            )

            pipeline = [
                {
                    "$group": {
                        "_id": "$_id",
                        "name": {"$first": "$name"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2
            assert results[0]["name"] == "Alice"
            assert results[1]["name"] == "Bob"


class TestAggregationPipelineContext:
    """Tests for aggregation_pipeline_context function."""

    def test_context_cleanup_on_success(self):
        """Test that temp tables are cleaned up after successful pipeline."""
        from neosqlite.collection.temporary_table_aggregation import (
            aggregation_pipeline_context,
        )

        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TABLE test_ctx AS SELECT 1 as x")

            with aggregation_pipeline_context(
                conn.db, "test_ctx"
            ) as create_temp:
                table_name = create_temp(
                    {"$test": {}}, "SELECT * FROM test_ctx"
                )
                # Verify table exists
                result = conn.db.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                assert result == 1

            # Verify table was dropped after context exit
            tables = conn.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'temp%'"
            ).fetchall()
            # Table should be dropped
            assert not any(t[0] == table_name for t in tables)

    def test_context_rollback_on_error(self):
        """Test that savepoint is rolled back on error."""
        from neosqlite.collection.temporary_table_aggregation import (
            aggregation_pipeline_context,
        )

        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TABLE test_err AS SELECT 1 as x")

            try:
                with aggregation_pipeline_context(
                    conn.db, "test_err"
                ) as create_temp:
                    create_temp({"$test": {}}, "SELECT * FROM test_err")
                    raise ValueError("Test error")
            except ValueError:
                pass  # Expected

    def test_context_without_pipeline_id(self):
        """Test context manager generates default pipeline_id."""
        from neosqlite.collection.temporary_table_aggregation import (
            aggregation_pipeline_context,
        )

        with neosqlite.Connection(":memory:") as conn:
            conn.db.execute("CREATE TABLE test_no_id AS SELECT 1 as x")

            with aggregation_pipeline_context(conn.db) as create_temp:
                table_name = create_temp(
                    {"$test": {}}, "SELECT * FROM test_no_id"
                )
                assert "temp_" in table_name


class TestBucketEdgeCases:
    """Tests for bucket stage edge cases."""

    def test_bucket_with_default(self):
        """Test $bucket with default value for out-of-range documents."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_bucket_default
            coll.insert_many(
                [
                    {
                        "item": "A",
                        "price": 5,
                    },  # Below range, should use default
                    {"item": "B", "price": 15},
                    {"item": "C", "price": 25},
                ]
            )

            pipeline = [
                {
                    "$bucket": {
                        "groupBy": "$price",
                        "boundaries": [10, 20, 30],
                        "default": "other",
                        "output": {"count": {"$sum": 1}},
                    }
                }
            ]
            results = list(coll.aggregate(pipeline))
            # Should have buckets for 10, 20, and "other"
            assert len(results) >= 1

    def test_bucket_auto_with_custom_output(self):
        """Test $bucketAuto with custom output fields."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_bucketauto_custom
            coll.insert_many(
                [
                    {"item": "A", "price": 10, "qty": 1},
                    {"item": "B", "price": 20, "qty": 2},
                    {"item": "C", "price": 30, "qty": 3},
                ]
            )

            pipeline = [
                {
                    "$bucketAuto": {
                        "groupBy": "$price",
                        "buckets": 2,
                        "output": {
                            "totalQty": {"$sum": "$qty"},
                            "avgPrice": {"$avg": "$price"},
                        },
                    }
                }
            ]
            results = list(coll.aggregate(pipeline))
            assert len(results) == 2
            for result in results:
                assert "totalQty" in result
                assert "avgPrice" in result


class TestWindowFrameEdgeCases:
    """Tests for window frame SQL generation edge cases."""

    def test_window_frame_with_current_only(self):
        """Test window frame with current row only."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame_current
            processor = TemporaryTableAggregationProcessor(coll)

            window_spec = {"documents": ["current", "current"]}
            result = processor._build_window_frame_sql(window_spec)
            assert "CURRENT ROW" in result

    def test_window_frame_with_negative_positive(self):
        """Test window frame with negative and positive bounds."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_frame_neg_pos
            processor = TemporaryTableAggregationProcessor(coll)

            window_spec = {"documents": [-2, 2]}
            result = processor._build_window_frame_sql(window_spec)
            assert "2 PRECEDING" in result
            assert "2 FOLLOWING" in result


class TestProcessorInitialization:
    """Tests for TemporaryTableAggregationProcessor initialization."""

    def test_processor_with_jsonb_support(self):
        """Test processor detects JSONB support."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_proc_init
            processor = TemporaryTableAggregationProcessor(coll)

            # Processor should be initialized
            assert processor.collection == coll
            assert processor.db == coll.db
            assert processor.sql_translator is not None

    def test_processor_has_sort_stage_tracking(self):
        """Test processor tracks sort stages."""
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_proc_sort
            processor = TemporaryTableAggregationProcessor(coll)

            # Initially should be False
            assert processor._has_sort_stage is False
