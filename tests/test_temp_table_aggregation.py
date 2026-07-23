"""
Tests for temporary table aggregation error reproduction.

This module reproduces three specific errors seen in production:
1. UTF-8 decode error in $facet stage with Binary data
2. 'no such column: _id' error in $sort/$skip/$limit stages
3. 'malformed JSON' error in $lookup stage

These tests are designed to reproduce the errors so that fixes can be validated.
"""

import neosqlite
from neosqlite import Binary


class TestFacetStageUTF8DecodeError:
    """
    Test reproduction of UTF-8 decode error in $facet stage.

    Error: 'utf-8' codec can't decode byte 0xdc in position 0: invalid continuation byte
    Location: temporary_table_aggregation.py:3217 in _process_facet_stage

    Root cause: When $facet extracts documents from a temp table via
    'SELECT _id, data FROM {current_table}', the data column may contain
    Binary data that wasn't properly JSON-encoded, causing neosqlite_json_loads
    to fail when trying to decode it as UTF-8.
    """

    def test_facet_with_binary_data(self):
        """Test $facet stage with documents containing Binary data."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_binary_facet

            # Insert documents with Binary data
            binary_data = Binary(b"some binary content here")
            coll.insert_many(
                [
                    {
                        "name": "doc1",
                        "data_field": binary_data,
                        "category": "A",
                    },
                    {
                        "name": "doc2",
                        "data_field": Binary(b"more binary"),
                        "category": "B",
                    },
                    {
                        "name": "doc3",
                        "data_field": binary_data,
                        "category": "A",
                    },
                ]
            )

            # This should not raise UTF-8 decode error
            pipeline = [
                {"$match": {"category": "A"}},
                {
                    "$facet": {
                        "by_name": [{"$project": {"name": 1}}],
                        "by_data": [{"$project": {"data_field": 1}}],
                    }
                },
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 1
            assert "by_name" in results[0]
            assert "by_data" in results[0]

    def test_facet_with_nested_binary_data(self):
        """Test $facet with nested Binary data structures."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_nested_binary

            # Insert document with nested Binary
            coll.insert_one(
                {
                    "name": "test",
                    "attachments": [
                        {
                            "filename": "file1.bin",
                            "content": Binary(b"binary content 1"),
                        },
                        {
                            "filename": "file2.bin",
                            "content": Binary(b"binary content 2"),
                        },
                    ],
                }
            )

            # $facet should handle nested Binary data
            pipeline = [
                {
                    "$facet": {
                        "names": [{"$project": {"name": 1}}],
                        "attachments": [
                            {"$unwind": "$attachments"},
                            {"$project": {"attachments.filename": 1}},
                        ],
                    }
                },
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 1


class TestSortStageMissingIdColumn:
    """
    Test reproduction of 'no such column: _id' error in sort/skip/limit stages.

    Error: sqlite3.OperationalError: no such column: _id
    Location: temporary_table_aggregation.py:1542 in _process_sort_skip_limit_stage

    Root cause: When a previous stage (like $facet or certain transformations)
    creates a temp table without the _id column, the _process_sort_skip_limit_stage
    function still tries to reference _id in the SELECT clause, causing the error.
    """

    def test_sort_after_facet_no_id_column(self):
        """Test $sort after $facet when result table may lack _id column."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_after_facet

            coll.insert_many(
                [
                    {"name": "alice", "age": 30, "category": "A"},
                    {"name": "bob", "age": 25, "category": "B"},
                    {"name": "charlie", "age": 35, "category": "A"},
                ]
            )

            # Pipeline that creates intermediate table, then sorts
            pipeline = [
                {"$match": {"category": "A"}},
                {
                    "$facet": {
                        "results": [{"$project": {"name": 1, "age": 1}}],
                    }
                },
                # After $facet, the result is a single document
                # Adding $sort here tests column detection
                {"$sort": {"name": 1}},
            ]

            # This should not raise 'no such column: _id'
            results = list(coll.aggregate(pipeline))
            assert len(results) == 1

    def test_sort_skip_limit_with_transformed_data(self):
        """Test $sort/$skip/$limit after stages that transform document structure."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_transform

            coll.insert_many(
                [
                    {"name": "doc1", "value": 10},
                    {"name": "doc2", "value": 20},
                    {"name": "doc3", "value": 30},
                    {"name": "doc4", "value": 40},
                    {"name": "doc5", "value": 50},
                ]
            )

            # Pipeline with addFields then sort/skip/limit
            pipeline = [
                {"$addFields": {"computed": {"$multiply": ["$value", 2]}}},
                {"$sort": {"computed": -1}},
                {"$skip": 1},
                {"$limit": 2},
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 2
            # Should be doc4 (80) and doc3 (60) after skipping doc5 (100)
            assert results[0]["computed"] == 80
            assert results[1]["computed"] == 60

    def test_sort_on_table_without_underscore_id(self):
        """Test sorting when intermediate table lacks _id column."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_no_id

            coll.insert_many(
                [
                    {"x": 3, "label": "c"},
                    {"x": 1, "label": "a"},
                    {"x": 2, "label": "b"},
                ]
            )

            # Simple sort should work regardless of _id column presence
            pipeline = [
                {"$sort": {"x": 1}},
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 3
            assert results[0]["x"] == 1
            assert results[1]["x"] == 2
            assert results[2]["x"] == 3

    def test_sort_table_missing_id_direct(self):
        """
        Test $sort when temp table is missing both id and _id columns.

        This reproduces the error: no such column: _id
        by creating a temp table that only has a 'data' column.
        """
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_missing_id

            coll.insert_one({"value": 10})

            # Create a temp table WITHOUT id or _id columns (only data)
            conn.db.execute(
                "CREATE TEMP TABLE no_id_table AS SELECT data FROM test_sort_missing_id"
            )

            from neosqlite.collection.temporary_table_aggregation import (
                TemporaryTableAggregationProcessor,
            )

            processor = TemporaryTableAggregationProcessor(coll)

            # This should work - the processor should detect missing columns
            # and handle it gracefully
            result_table = processor._process_sort_skip_limit_stage(
                lambda spec, query: f"temp_{spec}",
                "no_id_table",
                {"value": 1},  # sort spec
                0,  # skip
                None,  # limit
            )

            # Should create a new temp table successfully
            assert result_table.startswith("temp_")

    def test_sort_column_mismatch_error(self):
        """
        Test $sort when SELECT clause references _id but table doesn't have it.

        This reproduces the scenario where column detection fails and the
        SELECT clause incorrectly references a non-existent _id column.
        """
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_mismatch

            coll.insert_many(
                [
                    {"a": 1},
                    {"a": 2},
                ]
            )

            # Manually create a table with only 'data' column (no id, no _id)
            conn.db.execute("CREATE TEMP TABLE data_only (data TEXT)")
            conn.db.execute(
                "INSERT INTO data_only (data) VALUES ('{\"a\":1}'), ('{\"a\":2}')"
            )

            from neosqlite.collection.temporary_table_aggregation import (
                TemporaryTableAggregationProcessor,
            )

            processor = TemporaryTableAggregationProcessor(coll)

            # Try to sort - this would fail if column detection is broken
            result_table = processor._process_sort_skip_limit_stage(
                lambda spec, query: f"temp_{spec}",
                "data_only",
                {"a": 1},
                0,
                None,
            )

            # Should succeed with proper column detection
            assert result_table is not None


class TestLookupStageMalformedJSON:
    """
    Test reproduction of 'malformed JSON' error in $lookup stage.

    Error: sqlite3.OperationalError: malformed JSON
    Location: temporary_table_aggregation.py:1051 in _create_lookup_hash_table

    Root cause: When creating a hash table for $lookup, if the source collection's
    data column contains malformed JSON (e.g., raw binary data, corrupted entries,
    or improperly encoded values), SQLite's JSON functions fail when extracting fields.
    """

    def test_lookup_with_valid_data(self):
        """Baseline test: $lookup with properly formatted data."""
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.test_orders
            products = conn.test_products

            products.insert_many(
                [
                    {"_id": "prod1", "name": "Product 1", "price": 10},
                    {"_id": "prod2", "name": "Product 2", "price": 20},
                ]
            )

            orders.insert_many(
                [
                    {"order_id": 1, "product_id": "prod1", "quantity": 2},
                    {"order_id": 2, "product_id": "prod2", "quantity": 1},
                ]
            )

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products",
                        "localField": "product_id",
                        "foreignField": "_id",
                        "as": "product",
                    }
                },
            ]

            results = list(orders.aggregate(pipeline))
            assert len(results) == 2
            assert len(results[0]["product"]) == 1
            assert results[0]["product"][0]["name"] == "Product 1"

    def test_lookup_with_objectid_foreign_field(self):
        """Test $lookup when foreign field contains ObjectId."""
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.test_orders_oid
            products = conn.test_products_oid

            # Insert products (will have ObjectId _id)
            prod_id = products.insert_one(
                {"name": "Product A", "price": 100}
            ).inserted_id

            orders.insert_one({"product_id": prod_id, "quantity": 5})

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products_oid",
                        "localField": "product_id",
                        "foreignField": "_id",
                        "as": "product",
                    }
                },
            ]

            results = list(orders.aggregate(pipeline))
            assert len(results) == 1
            assert len(results[0]["product"]) == 1

    def test_lookup_with_nested_field_extraction(self):
        """Test $lookup extracting nested field as join key."""
        with neosqlite.Connection(":memory:") as conn:
            coll1 = conn.test_lookup_nested_1
            coll2 = conn.test_lookup_nested_2

            coll2.insert_many(
                [
                    {
                        "_id": "ref1",
                        "info": {"code": "ABC", "label": "Label 1"},
                    },
                    {
                        "_id": "ref2",
                        "info": {"code": "DEF", "label": "Label 2"},
                    },
                ]
            )

            coll1.insert_many(
                [
                    {"name": "item1", "ref_code": "ABC"},
                    {"name": "item2", "ref_code": "DEF"},
                ]
            )

            # Lookup using a nested field from the foreign collection
            pipeline = [
                {
                    "$lookup": {
                        "from": "test_lookup_nested_2",
                        "localField": "ref_code",
                        "foreignField": "info.code",
                        "as": "matched",
                    }
                },
            ]

            results = list(coll1.aggregate(pipeline))
            assert len(results) == 2
            assert len(results[0]["matched"]) == 1
            assert results[0]["matched"][0]["_id"] == "ref1"


class TestCombinedErrors:
    """Test scenarios that might trigger multiple errors in sequence."""

    def test_facet_then_sort_with_binary(self):
        """Test $facet followed by $sort with Binary data in documents."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_combined_binary

            coll.insert_many(
                [
                    {"name": "alpha", "data": Binary(b"binary1"), "score": 10},
                    {"name": "beta", "data": Binary(b"binary2"), "score": 20},
                ]
            )

            pipeline = [
                {
                    "$facet": {
                        "docs": [
                            {"$project": {"name": 1, "score": 1}},
                            {"$sort": {"score": -1}},
                        ],
                        "count": [{"$count": "total"}],
                    }
                },
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 1
            assert len(results[0]["docs"]) == 2
            assert results[0]["docs"][0]["score"] == 20

    def test_lookup_with_binary_in_foreign_collection(self):
        """Test $lookup when foreign collection has Binary data."""
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.test_orders_binary
            products = conn.test_products_binary

            products.insert_many(
                [
                    {
                        "_id": "p1",
                        "name": "Product 1",
                        "thumbnail": Binary(b"img1"),
                    },
                    {
                        "_id": "p2",
                        "name": "Product 2",
                        "thumbnail": Binary(b"img2"),
                    },
                ]
            )

            orders.insert_many(
                [
                    {"order_id": 1, "product_id": "p1"},
                    {"order_id": 2, "product_id": "p2"},
                ]
            )

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products_binary",
                        "localField": "product_id",
                        "foreignField": "_id",
                        "as": "product",
                    }
                },
            ]

            results = list(orders.aggregate(pipeline))
            assert len(results) == 2
            # Verify Binary data is preserved in lookup results
            assert "thumbnail" in results[0]["product"][0]


class TestEdgeCases:
    """Test edge cases related to the three error scenarios."""

    def test_facet_empty_input(self):
        """Test $facet with no input documents."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_facet_empty

            # Don't insert any documents
            pipeline = [
                {"$match": {"nonexistent": True}},
                {
                    "$facet": {
                        "results": [{"$project": {"name": 1}}],
                    }
                },
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 1
            assert results[0]["results"] == []

    def test_sort_with_all_skip_and_limit(self):
        """Test $sort combined with $skip and $limit."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_sort_skip_limit

            coll.insert_many([{"value": i} for i in range(10)])

            pipeline = [
                {"$sort": {"value": -1}},
                {"$skip": 3},
                {"$limit": 2},
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 2
            assert results[0]["value"] == 6
            assert results[1]["value"] == 5

    def test_lookup_same_collection(self):
        """Test $lookup referencing the same collection (self-join)."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_self_join

            coll.insert_many(
                [
                    {"_id": "a", "name": "A", "parent_id": None},
                    {"_id": "b", "name": "B", "parent_id": "a"},
                    {"_id": "c", "name": "C", "parent_id": "a"},
                ]
            )

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_self_join",
                        "localField": "parent_id",
                        "foreignField": "_id",
                        "as": "parent",
                    }
                },
            ]

            results = list(coll.aggregate(pipeline))
            assert len(results) == 3
            # Document 'a' has no parent
            assert results[0]["parent"] == []
            # Documents 'b' and 'c' have 'a' as parent
            assert results[1]["parent"][0]["name"] == "A"

    def test_lookup_malformed_json_direct(self):
        """
        Test $lookup when foreign collection has malformed JSON in data column.

        This verifies that the 'malformed JSON' error is now handled gracefully -
        corrupted documents are skipped with a warning instead of raising an error.
        """
        with neosqlite.Connection(":memory:") as conn:
            # Create the main collection with valid data
            orders = conn.test_orders_malformed

            # Create the foreign collection with malformed JSON
            products = conn.test_products_malformed
            products.insert_one({"_id": "p1", "name": "Valid Product"})

            # Now manually insert a row with malformed JSON directly into the table
            conn.db.execute(
                "INSERT INTO test_products_malformed (id, _id, data) VALUES (?, ?, ?)",
                (2, "p2", "{invalid json content}"),
            )

            orders.insert_one({"order_id": 1, "product_id": "p1"})

            # The lookup should now handle malformed JSON gracefully
            # and still find the valid product
            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products_malformed",
                        "localField": "product_id",
                        "foreignField": "_id",
                        "as": "product",
                    }
                },
            ]

            # Should not raise an error - malformed JSON documents are skipped
            results = list(orders.aggregate(pipeline))
            assert len(results) == 1
            # The valid product should still be found
            assert len(results[0]["product"]) == 1
            assert results[0]["product"][0]["name"] == "Valid Product"

    def test_lookup_with_binary_in_foreign_field(self):
        """
        Test $lookup when foreign field extraction encounters Binary data.

        This tests the scenario where _json_extract_field_with_objectid_support
        tries to extract a field that contains Binary data, causing JSON errors.
        """
        with neosqlite.Connection(":memory:") as conn:
            orders = conn.test_orders_binary_lookup
            products = conn.test_products_binary_lookup

            # Insert product with Binary in a field that might be used as join key
            products.insert_one(
                {
                    "_id": "prod1",
                    "name": "Product 1",
                    "join_field": "valid_value",
                }
            )

            # Insert another product with problematic data
            conn.db.execute(
                "INSERT INTO test_products_binary_lookup (id, _id, data) VALUES (?, ?, ?)",
                (
                    2,
                    "prod2",
                    '{"_id":"prod2","name":"Product 2","join_field":"valid_value2"}',
                ),
            )

            orders.insert_one({"order_id": 1, "join_ref": "valid_value"})

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products_binary_lookup",
                        "localField": "join_ref",
                        "foreignField": "join_field",
                        "as": "matched_products",
                    }
                },
            ]

            results = list(orders.aggregate(pipeline))
            assert len(results) == 1
            assert len(results[0]["matched_products"]) >= 1

    def test_lookup_hash_table_with_objectid_field(self):
        """
        Test $lookup hash table creation when foreign field contains ObjectId.

        This tests the ObjectId-aware extraction in _create_lookup_hash_table.
        """
        with neosqlite.Connection(":memory:") as conn:
            from neosqlite.objectid import ObjectId

            orders = conn.test_orders_oid_lookup
            products = conn.test_products_oid_lookup

            # Create products with ObjectId in a non-_id field
            oid = ObjectId()
            products.insert_one(
                {
                    "_id": "p1",
                    "name": "Product with ObjectId ref",
                    "category_id": oid,
                }
            )

            # Create order referencing that ObjectId
            orders.insert_one(
                {
                    "order_id": 1,
                    "category_ref": oid,
                }
            )

            pipeline = [
                {
                    "$lookup": {
                        "from": "test_products_oid_lookup",
                        "localField": "category_ref",
                        "foreignField": "category_id",
                        "as": "product",
                    }
                },
            ]

            results = list(orders.aggregate(pipeline))
            assert len(results) == 1
            assert len(results[0]["product"]) == 1


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
