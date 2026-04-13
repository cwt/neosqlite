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
