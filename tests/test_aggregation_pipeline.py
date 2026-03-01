"""
Consolidated tests for aggregation pipeline functionality.
"""

import math
import pytest
import neosqlite
from neosqlite import Connection
from neosqlite.collection.aggregation_cursor import (
    AggregationCursor,
    QUEZ_AVAILABLE,
)
from neosqlite.collection.temporary_table_aggregation import (
    execute_2nd_tier_aggregation,
    can_process_with_temporary_tables,
    TemporaryTableAggregationProcessor,
)


# ================================
# Core Aggregation Tests
# ================================


def test_aggregate_match(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    pipeline = [{"$match": {"a": {"$gt": 1}}}]
    result = collection.aggregate(pipeline)
    # Verify that result is a cursor-like object (new behavior)
    assert hasattr(result, "__iter__")
    assert hasattr(result, "__next__")
    # Convert to list to check contents
    result_list = list(result)
    assert len(result_list) == 2
    assert {doc["a"] for doc in result_list} == {2, 3}


def test_aggregate_sort(collection):
    collection.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
    pipeline = [{"$sort": {"a": neosqlite.ASCENDING}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert [doc["a"] for doc in result_list] == [1, 2, 3]


def test_aggregate_skip_limit(collection):
    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$sort": {"a": 1}}, {"$skip": 2}, {"$limit": 3}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 3
    assert [doc["a"] for doc in result_list] == [2, 3, 4]


def test_aggregate_project(collection):
    collection.insert_many([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    pipeline = [{"$project": {"a": 1, "_id": 0}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert all("b" not in doc for doc in result_list)
    assert all("_id" not in doc for doc in result_list)


def test_aggregate_group(collection):
    collection.insert_many(
        [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
            {"store": "A", "price": 30},
        ]
    )
    pipeline = [
        {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
        {"$sort": {"_id": 1}},
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0] == {"_id": "A", "total": 40}
    assert result_list[1] == {"_id": "B", "total": 20}


def test_aggregate_group_accumulators(collection):
    collection.insert_many(
        [
            {"item": "A", "price": 10, "quantity": 2},
            {"item": "B", "price": 20, "quantity": 1},
            {"item": "A", "price": 30, "quantity": 5},
            {"item": "B", "price": 10, "quantity": 2},
        ]
    )
    pipeline = [
        {
            "$group": {
                "_id": "$item",
                "total_quantity": {"$sum": "$quantity"},
                "avg_price": {"$avg": "$price"},
                "min_price": {"$min": "$price"},
                "max_price": {"$max": "$price"},
                "prices": {"$push": "$price"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0] == {
        "_id": "A",
        "total_quantity": 7,
        "avg_price": 20.0,
        "min_price": 10,
        "max_price": 30,
        "prices": [10, 30],
    }
    assert result_list[1] == {
        "_id": "B",
        "total_quantity": 3,
        "avg_price": 15.0,
        "min_price": 10,
        "max_price": 20,
        "prices": [20, 10],
    }


def test_aggregate_unwind(collection):
    collection.insert_one({"_id": 1, "item": "A", "sizes": ["S", "M", "L"]})
    pipeline = [{"$unwind": "$sizes"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 3
    assert {doc["sizes"] for doc in result_list} == {"S", "M", "L"}
    assert all(doc["item"] == "A" for doc in result_list)


def test_aggregate_fast_path(collection):
    collection.insert_many(
        [
            {"a": 1, "b": 10},
            {"a": 2, "b": 20},
            {"a": 3, "b": 30},
            {"a": 4, "b": 40},
        ]
    )
    pipeline = [
        {"$match": {"a": {"$gt": 1}}},
        {"$sort": {"b": neosqlite.DESCENDING}},
        {"$skip": 1},
        {"$limit": 1},
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0]["a"] == 3


def test_aggregate_count(collection):
    """Test $count aggregation stage."""
    # Test basic count
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    pipeline = [{"$count": "total"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0] == {"total": 3}

    # Test count with match filter
    pipeline = [{"$match": {"a": {"$gt": 1}}}, {"$count": "filtered_count"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0] == {"filtered_count": 2}

    # Test count with empty result
    pipeline = [{"$match": {"a": {"$gt": 10}}}, {"$count": "zero_count"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0] == {"zero_count": 0}

    # Test count after group (fallback to Python len, since $group not in temp tables)
    collection.insert_many([{"store": "A"}, {"store": "B"}, {"store": "A"}])
    pipeline = [{"$group": {"_id": "$store"}}, {"$count": "store_count"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0] == {
        "store_count": 3
    }  # Three groups: None (from previous docs), A, B


def test_aggregate_sample(collection):
    """Test $sample aggregation stage."""
    # Test basic sample
    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$sample": {"size": 3}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 3
    # Check that all sampled docs are from the original
    original_a = set(range(10))
    result_a = {doc["a"] for doc in result_list}
    assert result_a.issubset(original_a)

    # Test sample with size larger than available docs
    pipeline = [{"$sample": {"size": 20}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 10

    # Test sample with match before
    pipeline = [{"$match": {"a": {"$lt": 5}}}, {"$sample": {"size": 2}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    result_a = {doc["a"] for doc in result_list}
    assert result_a.issubset({0, 1, 2, 3, 4})

    # Test sample with size 0
    pipeline = [{"$sample": {"size": 0}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 0


def test_aggregate_unset(collection):
    """Test $unset aggregation stage."""
    # Test unset single field
    collection.insert_many([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
    pipeline = [{"$unset": "b"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    for doc in result_list:
        assert "a" in doc
        assert "c" in doc
        assert "b" not in doc

    # Test unset multiple fields
    pipeline = [{"$unset": ["a", "c"]}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    for doc in result_list:
        assert "b" in doc
        assert "a" not in doc
        assert "c" not in doc

    # Test unset nested field
    collection.delete_many({})
    collection.insert_many(
        [{"nested": {"x": 1, "y": 2}}, {"nested": {"x": 3, "y": 4}}]
    )
    pipeline = [{"$unset": "nested.x"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0]["nested"] == {"y": 2}
    assert result_list[1]["nested"] == {"y": 4}

    # Test unset non-existent field (should not error)
    pipeline = [{"$unset": "nonexistent"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2  # Original docs


def test_aggregate_facet(collection):
    """Test $facet aggregation stage."""
    collection.insert_many(
        [{"a": 1, "b": 1}, {"a": 2, "b": 2}, {"a": 3, "b": 3}]
    )
    pipeline = [
        {
            "$facet": {
                "even": [{"$match": {"a": {"$mod": [2, 0]}}}],
                "odd": [{"$match": {"a": {"$mod": [2, 1]}}}],
                "count": [{"$count": "total"}],
            }
        }
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    facet_result = result_list[0]
    assert "even" in facet_result
    assert "odd" in facet_result
    assert "count" in facet_result
    assert len(facet_result["even"]) == 1  # a=2
    assert len(facet_result["odd"]) == 2  # a=1,3
    assert facet_result["count"] == [{"total": 3}]


def test_aggregation_cursor_api():
    """Test that AggregationCursor implements the PyMongo API correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25, "city": "New York"},
                {"name": "Bob", "age": 30, "city": "San Francisco"},
                {"name": "Charlie", "age": 35, "city": "New York"},
            ]
        )

        # Test aggregation
        pipeline = [{"$match": {"age": {"$gte": 30}}}, {"$sort": {"name": 1}}]

        cursor = collection.aggregate(pipeline)

        # Test that cursor implements the iterator protocol
        assert hasattr(cursor, "__iter__")
        assert hasattr(cursor, "__next__")

        # Test that cursor has len method
        assert hasattr(cursor, "__len__")
        assert len(cursor) == 2

        # Test that cursor has getitem method
        assert hasattr(cursor, "__getitem__")
        first_doc = cursor[0]
        assert first_doc["name"] == "Bob"

        # Test iteration
        docs = list(cursor)
        assert len(docs) == 2
        assert docs[0]["name"] == "Bob"
        assert docs[1]["name"] == "Charlie"

        # Test that we can iterate multiple times
        docs2 = list(cursor)
        assert len(docs2) == 2
        assert docs2[0]["name"] == "Bob"
        assert docs2[1]["name"] == "Charlie"

        # Test sort method
        cursor2 = collection.aggregate(pipeline)
        cursor2.sort(key=lambda x: x["name"], reverse=True)
        docs3 = list(cursor2)
        assert len(docs3) == 2
        assert docs3[0]["name"] == "Charlie"
        assert docs3[1]["name"] == "Bob"

        # Test to_list method
        cursor3 = collection.aggregate(pipeline)
        docs4 = cursor3.to_list()
        assert len(docs4) == 2
        assert docs4[0]["name"] == "Bob"
        assert docs4[1]["name"] == "Charlie"


class TestAggregationCursor:
    """Test cases for the AggregationCursor class."""

    def test_init(self, collection):
        """Test AggregationCursor initialization."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        assert cursor.collection == collection
        assert cursor.pipeline == pipeline
        assert cursor._results is None
        assert cursor._position == 0
        assert cursor._executed is False
        assert cursor._batch_size == 1000
        assert cursor._memory_threshold == 100 * 1024 * 1024
        assert cursor._use_quez is False

    def test_iter(self, collection):
        """Test AggregationCursor iteration."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return the cursor itself
        iter_result = iter(cursor)
        assert iter_result is cursor

        # Should have executed the pipeline
        assert cursor._executed is True

    def test_next(self, collection):
        """Test AggregationCursor next method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Get first result
        result1 = next(cursor)
        assert result1["a"] in [2, 3]

        # Get second result
        result2 = next(cursor)
        assert result2["a"] in [2, 3]
        assert result1["a"] != result2["a"]

        # Should raise StopIteration when no more results
        with pytest.raises(StopIteration):
            next(cursor)

    def test_len(self, collection):
        """Test AggregationCursor len method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return the count of results
        assert len(cursor) == 2

    def test_getitem(self, collection):
        """Test AggregationCursor getitem method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$sort": {"a": 1}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should be able to access items by index
        assert cursor[0]["a"] == 1
        assert cursor[1]["a"] == 2
        assert cursor[2]["a"] == 3

        # Should raise IndexError for invalid index
        with pytest.raises(IndexError):
            _ = cursor[10]

    def test_sort(self, collection):
        """Test AggregationCursor sort method."""
        collection.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
        pipeline = [{"$match": {"a": {"$gte": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should be able to sort results
        cursor.sort(key=lambda doc: doc["a"])
        results = list(cursor)
        assert [doc["a"] for doc in results] == [1, 2, 3]

        # Should return self for chaining
        result = cursor.sort(key=lambda doc: doc["a"])
        assert result is cursor

    def test_to_list(self, collection):
        """Test AggregationCursor to_list method."""
        collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
        pipeline = [{"$match": {"a": {"$gt": 1}}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return all results as a list
        results = cursor.to_list()
        assert len(results) == 2
        assert all(doc["a"] in [2, 3] for doc in results)

    def test_batch_size(self, collection):
        """Test AggregationCursor batch_size method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set batch size and return self for chaining
        result = cursor.batch_size(500)
        assert result is cursor
        assert cursor._batch_size == 500

    def test_max_await_time_ms(self, collection):
        """Test AggregationCursor max_await_time_ms method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set max await time and return self for chaining
        result = cursor.max_await_time_ms(5000)
        assert result is cursor

    def test_use_quez(self, collection):
        """Test AggregationCursor use_quez method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should set use_quez flag and return self for chaining
        result = cursor.use_quez(True)
        assert result is cursor
        # Note: Actual quez availability depends on system setup

    def test_get_quez_stats(self, collection):
        """Test AggregationCursor get_quez_stats method."""
        pipeline = [{"$match": {"status": "active"}}]
        cursor = AggregationCursor(collection, pipeline)

        # Should return None when quez is not being used
        stats = cursor.get_quez_stats()
        assert stats is None


def test_aggregation_cursor_integration():
    """Integration test for AggregationCursor with actual data."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        test_docs = [
            {"name": "Alice", "age": 30, "department": "Engineering"},
            {"name": "Bob", "age": 25, "department": "Marketing"},
            {"name": "Charlie", "age": 35, "department": "Engineering"},
            {"name": "Diana", "age": 28, "department": "Sales"},
        ]
        collection.insert_many(test_docs)

        # Test a simple aggregation pipeline
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$sort": {"age": 1}},
        ]

        cursor = collection.aggregate(pipeline)

        # Verify it's an AggregationCursor
        assert isinstance(cursor, AggregationCursor)

        # Test iteration
        results = list(cursor)
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"

        # Test len
        cursor2 = collection.aggregate(pipeline)
        assert len(cursor2) == 2

        # Test to_list
        cursor3 = collection.aggregate(pipeline)
        results_list = cursor3.to_list()
        assert len(results_list) == 2
        assert all(isinstance(doc, dict) for doc in results_list)


def test_basic_aggregation_coverage():
    """Test basic aggregation scenarios to maximize code coverage."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert diverse test data
        test_documents = [
            {
                "name": "Alice",
                "age": 30,
                "department": "Engineering",
                "salary": 75000,
                "skills": ["python", "javascript"],
                "active": True,
            },
            {
                "name": "Bob",
                "age": 25,
                "department": "Marketing",
                "salary": 60000,
                "skills": ["design", "copywriting"],
                "active": True,
            },
            {
                "name": "Charlie",
                "age": 35,
                "department": "Engineering",
                "salary": 85000,
                "skills": ["python", "go", "rust"],
                "active": False,
            },
            {
                "name": "Diana",
                "age": 28,
                "department": "Engineering",
                "salary": 70000,
                "skills": ["javascript", "typescript"],
                "active": True,
            },
            {
                "name": "Eve",
                "age": 32,
                "department": "Marketing",
                "salary": 65000,
                "skills": ["analytics", "design"],
                "active": True,
            },
        ]

        collection.insert_many(test_documents)

        # Test 1: Simple $match
        results = list(
            collection.aggregate([{"$match": {"department": "Engineering"}}])
        )
        assert len(results) == 3

        # Test 2: $match with operators
        results = list(
            collection.aggregate([{"$match": {"age": {"$gte": 30}}}])
        )
        assert len(results) == 3

        # Test 3: $match with $in
        results = list(
            collection.aggregate(
                [
                    {
                        "$match": {
                            "department": {"$in": ["Engineering", "Marketing"]}
                        }
                    }
                ]
            )
        )
        assert len(results) == 5

        # Test 4: $sort
        results = list(collection.aggregate([{"$sort": {"age": -1}}]))
        assert len(results) == 5
        # First result should be oldest person
        assert results[0]["age"] == 35

        # Test 5: $limit
        results = list(
            collection.aggregate([{"$sort": {"age": 1}}, {"$limit": 2}])
        )
        assert len(results) == 2
        assert results[0]["age"] == 25
        assert results[1]["age"] == 28

        # Test 6: Complex $match with logical operators
        results = list(
            collection.aggregate(
                [
                    {
                        "$match": {
                            "$and": [
                                {"age": {"$gte": 25}},
                                {
                                    "$or": [
                                        {"department": "Engineering"},
                                        {"salary": {"$lt": 65000}},
                                    ]
                                },
                            ]
                        }
                    }
                ]
            )
        )
        assert len(results) >= 1

        # Test 7: $unwind simple
        results = list(collection.aggregate([{"$unwind": "$skills"}]))
        # Should have more results due to unwinding (5 docs * avg ~2.5 skills each)
        assert len(results) > 5

        # Test 8: $group simple
        results = list(
            collection.aggregate(
                [{"$group": {"_id": "$department", "count": {"$sum": 1}}}]
            )
        )
        assert len(results) == 2  # Engineering and Marketing
        dept_counts = {doc["_id"]: doc["count"] for doc in results}
        assert dept_counts["Engineering"] == 3
        assert dept_counts["Marketing"] == 2


def test_unwind_then_group_coverage():
    """Test $unwind followed by $group to hit the specific optimization path."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data that should trigger the specific $unwind+$group optimization
        collection.insert_many(
            [
                {"category": "tech", "items": ["laptop", "phone"]},
                {"category": "tech", "items": ["laptop", "tablet"]},
                {"category": "home", "items": ["sofa", "table", "laptop"]},
            ]
        )

        # This specific pattern should trigger the optimization path at lines 1388-1515:
        # $unwind followed immediately by $group with simple string field references
        pipeline = [
            {"$unwind": "$items"},  # String syntax with $ prefix
            {
                "$group": {
                    "_id": "$items",  # String syntax with $ prefix - this should trigger the path
                    "count": {"$sum": 1},
                }
            },
        ]

        # Execute the pipeline - this should use the SQL optimization path
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) >= 3  # At least laptop, phone, tablet, sofa, table
        item_counts = {doc["_id"]: doc["count"] for doc in results}
        # Laptop appears in all 3 documents
        assert item_counts["laptop"] == 3


# Additional tests for comprehensive coverage


def test_aggregation_comprehensive_group_coverage(collection):
    """Test comprehensive group operation coverage."""
    collection.insert_many(
        [
            {
                "category": "A",
                "price": 100,
                "quantity": 2,
                "tags": ["red", "small"],
            },
            {
                "category": "A",
                "price": 200,
                "quantity": 1,
                "tags": ["blue", "large"],
            },
            {
                "category": "B",
                "price": 150,
                "quantity": 3,
                "tags": ["red", "medium"],
            },
            {
                "category": "B",
                "price": 300,
                "quantity": 1,
                "tags": ["green", "large"],
            },
        ]
    )

    # Test simple group with basic accumulators (should work in SQL)
    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "avg_price": {"$avg": "$price"},
                "max_quantity": {"$max": "$quantity"},
                "min_price": {"$min": "$price"},
                "count": {"$sum": 1},
                "all_prices": {"$push": "$price"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    assert len(result_list) == 2

    # Verify category A results
    category_a = next((doc for doc in result_list if doc["_id"] == "A"), None)
    assert category_a is not None
    assert category_a["avg_price"] == 150.0  # (100+200)/2
    assert category_a["max_quantity"] == 2  # max of [2, 1]
    assert category_a["min_price"] == 100  # min of [100, 200]
    assert category_a["count"] == 2
    assert set(category_a["all_prices"]) == {100, 200}

    # Verify category B results
    category_b = next((doc for doc in result_list if doc["_id"] == "B"), None)
    assert category_b is not None
    assert category_b["avg_price"] == 225.0  # (150+300)/2
    assert category_b["max_quantity"] == 3  # max of [3, 1]
    assert category_b["min_price"] == 150  # min of [150, 300]
    assert category_b["count"] == 2
    assert set(category_b["all_prices"]) == {150, 300}


def test_aggregation_temporary_table_benefits(collection):
    """Test temporary table aggregation benefits."""
    # Insert a larger dataset to test performance benefits
    docs = []
    for i in range(1000):
        docs.append(
            {
                "category": f"Category_{i % 10}",
                "value": i,
                "tags": [f"tag_{j}" for j in range(i % 5)],
            }
        )

    collection.insert_many(docs)

    # Test aggregation that can benefit from temporary tables
    pipeline = [
        {"$match": {"value": {"$gte": 500}}},
        {"$unwind": "$tags"},
        {
            "$group": {
                "_id": "$category",
                "total_value": {"$sum": "$value"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"total_value": -1}},
        {"$limit": 5},
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    assert len(result_list) <= 5
    # Verify results are sorted by total_value in descending order
    if len(result_list) > 1:
        for i in range(len(result_list) - 1):
            assert (
                result_list[i]["total_value"]
                >= result_list[i + 1]["total_value"]
            )


def test_aggregation_pipeline_reordering_enhancement(collection):
    """Test pipeline reordering enhancements."""
    collection.insert_many(
        [
            {"name": "Alice", "age": 25, "city": "New York", "active": True},
            {"name": "Bob", "age": 30, "city": "Boston", "active": False},
            {"name": "Charlie", "age": 35, "city": "New York", "active": True},
            {"name": "David", "age": 28, "city": "Boston", "active": True},
        ]
    )

    # Test pipeline with match, sort, skip, limit that can be reordered
    pipeline = [
        {"$match": {"active": True}},
        {"$sort": {"age": 1}},
        {"$skip": 1},
        {"$limit": 2},
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    assert len(result_list) == 2
    # Should have Charlie (35) and David (28) after skipping Alice (25)
    # But wait, we're sorting by age ascending, so:
    # Active users: Alice (25), Charlie (35), David (28)
    # After sort: Alice (25), David (28), Charlie (35)
    # After skip 1: David (28), Charlie (35)
    # After limit 2: David (28), Charlie (35)
    assert result_list[0]["name"] == "David"
    assert result_list[1]["name"] == "Charlie"


def test_aggregation_unwind_advanced_options_comprehensive(collection):
    """Test comprehensive unwind advanced options."""
    collection.insert_many(
        [
            {"_id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"_id": 2, "name": "Bob", "scores": []},  # Empty array
            {"_id": 3, "name": "Charlie", "scores": None},  # Null value
            {"_id": 4, "name": "David"},  # No scores field
        ]
    )

    # Test unwind with all advanced options
    pipeline = [
        {
            "$unwind": {
                "path": "$scores",
                "includeArrayIndex": "scoreIndex",
                "preserveNullAndEmptyArrays": True,
            }
        }
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have 5 documents:
    # 3 for Alice's scores, 1 for Bob (null due to empty array),
    # 1 for Charlie (null due to null value), 1 for David (null due to missing field)
    assert len(result_list) == 5

    # Verify Alice's scores with indices
    alice_scores = [doc for doc in result_list if doc["name"] == "Alice"]
    assert len(alice_scores) == 3
    alice_scores.sort(key=lambda x: x["scoreIndex"])
    assert (
        alice_scores[0]["scores"] == 85 and alice_scores[0]["scoreIndex"] == 0
    )
    assert (
        alice_scores[1]["scores"] == 90 and alice_scores[1]["scoreIndex"] == 1
    )
    assert (
        alice_scores[2]["scores"] == 78 and alice_scores[2]["scoreIndex"] == 2
    )

    # Verify null handling
    null_scores = [doc for doc in result_list if doc.get("scores") is None]
    assert len(null_scores) == 2
    for doc in null_scores:
        assert doc["scoreIndex"] is None


def test_aggregation_fallback_kill_switch(collection):
    """Test aggregation with fallback kill switch."""
    collection.insert_many(
        [
            {"name": "Alice", "data": {"nested": {"value": 1}}},
            {"name": "Bob", "data": {"nested": {"value": 2}}},
        ]
    )

    # Test a pipeline that might trigger fallback
    # Use simple projection instead of computed fields
    pipeline = [
        {
            "$match": {"data.nested.value": {"$gt": 0}}
        },  # This might use fallback
        {"$project": {"name": 1, "_id": 0}},  # Simple field selection
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    assert len(result_list) == 2
    # Verify projection worked
    names = {doc["name"] for doc in result_list}
    assert names == {"Alice", "Bob"}

    # Verify that only the selected fields are present
    for doc in result_list:
        assert "name" in doc
        assert "_id" not in doc  # Excluded


# Additional tests for AggregationCursor to improve coverage


def test_aggregation_cursor_initialization(collection):
    """Test AggregationCursor initialization."""
    pipeline = [{"$match": {"status": "active"}}]
    cursor = AggregationCursor(collection, pipeline)
    assert cursor.collection == collection
    assert cursor.pipeline == pipeline
    assert cursor._results is None
    assert cursor._position == 0
    assert not cursor._executed
    assert cursor._batch_size == 1000
    assert cursor._memory_threshold == 100 * 1024 * 1024
    assert not cursor._use_quez


def test_aggregation_cursor_len_and_getitem(collection):
    """Test __len__ and __getitem__ methods."""
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    pipeline = [{"$sort": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    assert len(cursor) == 3
    assert cursor[0]["a"] == 1
    assert cursor[1]["a"] == 2
    assert cursor[2]["a"] == 3
    with pytest.raises(IndexError):
        _ = cursor[3]


def test_aggregation_cursor_methods_chaining(collection):
    """Test method chaining for cursor configuration."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    pipeline = [{"$match": {"status": "active"}}]
    cursor = AggregationCursor(collection, pipeline)

    # Test batch_size
    result = cursor.batch_size(500)
    assert result is cursor
    assert cursor._batch_size == 500

    # Test max_await_time_ms
    result = cursor.max_await_time_ms(5000)
    assert result is cursor

    # Test use_quez
    result = cursor.use_quez(True)
    assert result is cursor
    assert cursor._use_quez


def test_aggregation_cursor_get_quez_stats(collection):
    """Test get_quez_stats method."""
    pipeline = [{"$match": {"status": "active"}}]
    cursor = AggregationCursor(collection, pipeline)
    assert cursor.get_quez_stats() is None


def test_aggregation_cursor_iteration(collection):
    """Test iteration over the cursor."""
    collection.insert_many([{"a": 1}, {"a": 2}])
    pipeline = [{"$sort": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # First iteration
    results1 = list(cursor)
    assert len(results1) == 2
    assert results1[0]["a"] == 1
    assert results1[1]["a"] == 2

    # Second iteration should be possible and yield same results
    results2 = list(cursor)
    assert len(results2) == 2
    assert results2[0]["a"] == 1
    assert results2[1]["a"] == 2


def test_aggregation_cursor_quez_functionality(collection):
    """Test AggregationCursor with quez functionality."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(20)])
    pipeline = [{"$match": {"a": {"$gte": 5}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez
    cursor.use_quez(True)

    # Should be able to iterate
    results = list(cursor)
    assert len(results) == 15  # Values 5-19
    assert all(5 <= doc["a"] <= 19 for doc in results)

    # Test get_quez_stats (should be None since quez wasn't actually used due to small result size)
    cursor.get_quez_stats()
    # Stats could be None or have data depending on implementation details
    # We're primarily testing that the method doesn't crash


def test_aggregation_cursor_quez_stats_when_used(collection):
    """Test get_quez_stats when quez is actually used."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    # Insert many documents to trigger quez usage
    collection.insert_many([{"a": i, "data": "x" * 100} for i in range(1000)])
    pipeline = [{"$match": {"a": {"$gte": 500}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set a low memory threshold to force quez usage
    cursor.use_quez(True)
    cursor._memory_threshold = 1000  # Very low threshold

    # Execute the cursor
    results = list(cursor)
    assert len(results) == 500  # Values 500-999

    # Test get_quez_stats
    stats = cursor.get_quez_stats()
    # Should return a dict with stats or None
    assert stats is None or isinstance(stats, dict)


def test_aggregation_cursor_len_with_quez(collection):
    """Test __len__ method with quez."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$match": {"a": {"$gte": 3}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set low threshold
    cursor.use_quez(True)
    cursor._memory_threshold = 1000

    # Test len
    length = len(cursor)
    assert length == 7  # Values 3-9


def test_aggregation_cursor_next_with_empty_results(collection):
    """Test __next__ method error handling with empty results."""
    collection.insert_many([{"a": 1}, {"a": 2}])
    pipeline = [{"$match": {"a": {"$gt": 10}}}]  # No matches
    cursor = AggregationCursor(collection, pipeline)

    # Should raise StopIteration immediately
    with pytest.raises(StopIteration):
        next(cursor)


def test_aggregation_cursor_sort_with_quez_raises_error(collection):
    """Test that sort raises NotImplementedError with quez."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$match": {"a": {"$gte": 5}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set low threshold
    cursor.use_quez(True)
    cursor._memory_threshold = 1000

    # Should raise NotImplementedError when trying to sort
    with pytest.raises(
        NotImplementedError, match="Sorting not supported with quez"
    ):
        cursor.sort(key=lambda x: x["a"])


def test_aggregation_cursor_getitem_with_quez_raises_error(collection):
    """Test that __getitem__ raises NotImplementedError with quez."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$match": {"a": {"$gte": 5}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set low threshold
    cursor.use_quez(True)
    cursor._memory_threshold = 1000

    # Should raise NotImplementedError when trying to index
    with pytest.raises(
        NotImplementedError, match="Indexing not supported with quez"
    ):
        _ = cursor[0]


def test_aggregation_cursor_to_list_with_quez(collection):
    """Test to_list method with quez."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(20)])
    pipeline = [{"$match": {"a": {"$gte": 15}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set low threshold
    cursor.use_quez(True)
    cursor._memory_threshold = 1000

    # Should be able to convert to list
    results = cursor.to_list()
    assert len(results) == 5  # Values 15-19
    assert all(15 <= doc["a"] <= 19 for doc in results)


def test_aggregation_cursor_sort_with_none_results(collection):
    """Test sort method when results is None."""
    pipeline = [{"$match": {"nonexistent": "value"}}]
    cursor = AggregationCursor(collection, pipeline)

    # Should return self without error
    result = cursor.sort(key=lambda x: x.get("a", 0))
    assert result is cursor


def test_aggregation_cursor_next_after_exhaustion(collection):
    """Test __next__ method after all results are exhausted."""
    collection.insert_many([{"a": 1}])
    pipeline = [{"$match": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # Get the first (and only) result
    result = next(cursor)
    assert result["a"] == 1

    # Next call should raise StopIteration
    with pytest.raises(StopIteration):
        next(cursor)


def test_aggregation_cursor_quez_get_exception_handling(
    monkeypatch, collection
):
    """Test exception handling in CompressedQueue get() method."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    # Import CompressedQueue for mocking

    # Create a mock queue class
    class MockQueue:
        def get(self, block=False):
            raise Exception("Queue error")

        @property
        def empty(self):
            return False

    collection.insert_many([{"a": i} for i in range(5)])
    pipeline = [{"$match": {"a": {"$gte": 2}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Force the cursor to use our mock queue
    cursor.use_quez(True)
    cursor._results = MockQueue()
    cursor._executed = True

    # Should raise StopIteration when get() fails
    with pytest.raises(StopIteration):
        next(cursor)


def test_aggregation_cursor_quez_to_list_exception_handling(
    monkeypatch, collection
):
    """Test exception handling in to_list with CompressedQueue."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    # Create a mock queue class
    class MockQueue:
        def get(self, block=False):
            raise Exception("Queue error")

        @property
        def empty(self):
            return False

    collection.insert_many([{"a": i} for i in range(5)])
    pipeline = [{"$match": {"a": {"$gte": 2}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Force the cursor to use our mock queue
    cursor.use_quez(True)
    cursor._results = MockQueue()
    cursor._executed = True

    # Should return empty list when get() fails
    results = cursor.to_list()
    assert results == []


def test_aggregation_cursor_len_none_results(collection):
    """Test __len__ method when results is None."""
    pipeline = [{"$match": {"nonexistent": "value"}}]
    cursor = AggregationCursor(collection, pipeline)

    # Should return 0 without error
    length = len(cursor)
    assert length == 0


def test_aggregation_cursor_getitem_none_results(collection):
    """Test __getitem__ method when results is None."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"nonexistent": "value"}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to None to test the specific code path
    cursor._results = None
    cursor._executed = True

    # Should raise IndexError with specific message
    with pytest.raises(IndexError, match="Cursor has no results"):
        _ = cursor[0]


def test_aggregation_cursor_getitem_unsupported_type_results(collection):
    """Test __getitem__ method when results is an unsupported type."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to an unsupported type to test the specific code path
    cursor._results = "unsupported_type"
    cursor._executed = True

    # Should raise IndexError with specific message
    with pytest.raises(IndexError, match="Cursor has no results"):
        _ = cursor[0]


def test_aggregation_cursor_sort_unsupported_type_results(collection):
    """Test sort method when results is an unsupported type."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to an unsupported type to test the specific code path
    cursor._results = "unsupported_type"
    cursor._executed = True

    # Should return self without error
    result = cursor.sort(key=lambda x: x.get("a", 0))
    assert result is cursor


def test_aggregation_cursor_len_unsupported_type_results(collection):
    """Test __len__ method when results is an unsupported type."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to an unsupported type to test the specific code path
    cursor._results = "unsupported_type"
    cursor._executed = True

    # Should return 0 without error
    length = len(cursor)
    assert length == 0


def test_aggregation_cursor_to_list_unsupported_type_results(collection):
    """Test to_list method when results is an unsupported type."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"a": 1}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to an unsupported type to test the specific code path
    cursor._results = "unsupported_type"
    cursor._executed = True

    # Should return empty list without error
    results = cursor.to_list()
    assert results == []


def test_aggregation_cursor_to_list_none_results(collection):
    """Test to_list method when results is None."""
    # Create a cursor but don't execute it yet
    pipeline = [{"$match": {"nonexistent": "value"}}]
    cursor = AggregationCursor(collection, pipeline)

    # Manually set results to None to test the specific code path
    cursor._results = None
    cursor._executed = True

    # Should return empty list without error
    results = cursor.to_list()
    assert results == []


def test_aggregation_cursor_import_error_path():
    """Test the import error path for quez."""
    # This test is more for coverage of the import try/except block
    # We can't easily test the ImportError path without manipulating the import system
    # But we can at least verify that QUEZ_AVAILABLE is properly set
    from neosqlite.collection.aggregation_cursor import (
        QUEZ_AVAILABLE,
        CompressedQueue,
    )

    # If we get here, the import either succeeded or failed gracefully
    assert isinstance(QUEZ_AVAILABLE, bool)
    if QUEZ_AVAILABLE:
        # quez is available, so CompressedQueue should be the actual class
        assert CompressedQueue is not None
    else:
        # quez is not available, so CompressedQueue should be None
        assert CompressedQueue is None


def test_aggregation_cursor_next_method(collection):
    """Test the next() method."""
    collection.insert_many([{"a": 1}, {"a": 2}])
    pipeline = [{"$match": {"a": {"$gte": 1}}}]
    cursor = AggregationCursor(collection, pipeline)

    # next() should work the same as __next__()
    result1 = cursor.next()
    result2 = cursor.next()

    assert result1["a"] == 1
    assert result2["a"] == 2

    # Should raise StopIteration when exhausted
    with pytest.raises(StopIteration):
        cursor.next()


def test_aggregation_cursor_execute_with_constraints(monkeypatch, collection):
    """Test _execute method with aggregate_with_constraints path."""
    # Skip if quez is not available
    if not QUEZ_AVAILABLE:
        pytest.skip("Quez not available")

    collection.insert_many([{"a": i} for i in range(100)])
    pipeline = [{"$match": {"a": {"$gte": 50}}}]
    cursor = AggregationCursor(collection, pipeline)

    # Enable quez and set low threshold to force constraint path
    cursor.use_quez(True)
    cursor._memory_threshold = 1000  # Low threshold

    # Mock the estimate to return a large value to trigger constraints path
    def mock_estimate_result_size(*args, **kwargs):
        return 200 * 1024 * 1024  # 200MB

    monkeypatch.setattr(
        cursor, "_estimate_result_size", mock_estimate_result_size
    )

    # Mock aggregate_with_constraints to return a simple list
    mock_results = [{"a": i} for i in range(50, 100)]

    # Track if the method was called
    called_with_args = []

    def mock_aggregate_with_constraints(*args, **kwargs):
        called_with_args.append((args, kwargs))
        return mock_results

    monkeypatch.setattr(
        collection.query_engine,
        "aggregate_with_constraints",
        mock_aggregate_with_constraints,
    )

    # Execute the cursor
    results = list(cursor)

    # Verify that aggregate_with_constraints was called
    assert len(called_with_args) == 1
    args, kwargs = called_with_args[0]
    assert args[0] == pipeline
    assert kwargs.get("batch_size") == 1000
    assert kwargs.get("memory_constrained")

    # Verify results
    assert len(results) == 50
    assert all(50 <= doc["a"] <= 99 for doc in results)


# ================================
# Group Operations Tests
# ================================


def test_group_stage_with_first_accumulator():
    """Test $group stage with $first accumulator to get first value in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "B", "value": 20, "name": "fourth"},
            {"category": "B", "value": 40, "name": "fifth"},
        ]
        collection.insert_many(docs)

        # Test group with $first
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first value 10 (first document)
        assert {"_id": "A", "first_value": 10} in result
        # Category B should have first value 20 (first document)
        assert {"_id": "B", "first_value": 20} in result


def test_group_stage_with_last_accumulator():
    """Test $group stage with $last accumulator to get last value in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "B", "value": 20, "name": "fourth"},
            {"category": "B", "value": 40, "name": "fifth"},
        ]
        collection.insert_many(docs)

        # Test group with $last
        pipeline = [
            {"$group": {"_id": "$category", "last_value": {"$last": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have last value 50 (last document)
        assert {"_id": "A", "last_value": 50} in result
        # Category B should have last value 40 (last document)
        assert {"_id": "B", "last_value": 40} in result


def test_group_stage_with_push_accumulator():
    """Test $group stage with $push accumulator to build arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 30},
            {"category": "B", "value": 40},
            {"category": "A", "value": 50},
        ]
        collection.insert_many(docs)

        # Test group with $push
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have items [10, 30, 50] in order
        a_result = next((doc for doc in result if doc["_id"] == "A"), None)
        assert a_result is not None
        assert a_result["items"] == [10, 30, 50]

        # Category B should have items [20, 40] in order
        b_result = next((doc for doc in result if doc["_id"] == "B"), None)
        assert b_result is not None
        assert b_result["items"] == [20, 40]


def test_group_stage_with_add_to_set_accumulator():
    """Test $group stage with $addToSet accumulator to build unique value arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set"]

        # Insert test data with duplicate values
        docs = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 30},
            {"category": "B", "value": 20},  # Duplicate
            {"category": "A", "value": 10},  # Duplicate
            {"category": "A", "value": 50},
        ]
        collection.insert_many(docs)

        # Test group with $addToSet
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "unique_items": {"$addToSet": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have unique items [10, 30, 50] (order may vary)
        a_result = next((doc for doc in result if doc["_id"] == "A"), None)
        assert a_result is not None
        assert set(a_result["unique_items"]) == {10, 30, 50}

        # Category B should have unique items [20] (duplicates removed)
        b_result = next((doc for doc in result if doc["_id"] == "B"), None)
        assert b_result is not None
        assert set(b_result["unique_items"]) == {20}


def test_group_stage_with_first_n_accumulator():
    """Test $group stage with $firstN accumulator to get first N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $firstN (get first 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_values": {"$firstN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first 2 values [10, 30]
        a_result = next((doc for doc in result if doc["_id"] == "A"), None)
        assert a_result is not None
        assert a_result["first_values"] == [10, 30]

        # Category B should have first 2 values [20, 40]
        b_result = next((doc for doc in result if doc["_id"] == "B"), None)
        assert b_result is not None
        assert b_result["first_values"] == [20, 40]


def test_group_stage_with_last_n_accumulator():
    """Test $group stage with $lastN accumulator to get last N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $lastN (get last 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "last_values": {"$lastN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have last 2 values [50, 70]
        a_result = next((doc for doc in result if doc["_id"] == "A"), None)
        assert a_result is not None
        assert a_result["last_values"] == [50, 70]

        # Category B should have last 2 values [20, 40]
        b_result = next((doc for doc in result if doc["_id"] == "B"), None)
        assert b_result is not None
        assert b_result["last_values"] == [20, 40]


def test_group_stage_with_top_accumulator():
    """Test $group stage with $first accumulator as a substitute for $top."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first"]

        # Insert test data
        docs = [
            {"category": "A", "score": 80, "name": "Alice"},
            {"category": "A", "score": 90, "name": "Bob"},
            {"category": "A", "score": 70, "name": "Charlie"},
            {"category": "B", "score": 85, "name": "David"},
            {"category": "B", "score": 95, "name": "Eve"},
        ]
        collection.insert_many(docs)

        # Test group with $first (get first document in group after sorting)
        # Since documents are inserted in order and we sort by category,
        # we should get the first document for each category
        pipeline = [
            {
                "$sort": {"category": 1, "name": 1}
            },  # Sort to ensure consistent order
            {
                "$group": {
                    "_id": "$category",
                    "first_student": {"$first": "$name"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)
        result_list = list(result)

        assert len(result_list) == 2
        # Should get first student alphabetically for each category
        a_result = next((doc for doc in result_list if doc["_id"] == "A"), None)
        assert a_result is not None
        assert a_result["first_student"] == "Alice"  # First alphabetically

        b_result = next((doc for doc in result_list if doc["_id"] == "B"), None)
        assert b_result is not None
        assert b_result["first_student"] == "David"  # First alphabetically


def test_group_stage_with_bottom_accumulator():
    """Test $group stage with $last accumulator as a substitute for $bottom."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last"]

        # Insert test data
        docs = [
            {"category": "A", "score": 80, "name": "Alice"},
            {"category": "A", "score": 90, "name": "Bob"},
            {"category": "A", "score": 70, "name": "Charlie"},
            {"category": "B", "score": 85, "name": "David"},
            {"category": "B", "score": 95, "name": "Eve"},
        ]
        collection.insert_many(docs)

        # Test group with $last (get last document in group after sorting)
        # Since documents are inserted in order and we sort by category,
        # we should get the last document for each category
        pipeline = [
            {
                "$sort": {"category": 1, "name": 1}
            },  # Sort to ensure consistent order
            {
                "$group": {
                    "_id": "$category",
                    "last_student": {"$last": "$name"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)
        result_list = list(result)

        assert len(result_list) == 2
        # Should get last student alphabetically for each category
        a_result = next((doc for doc in result_list if doc["_id"] == "A"), None)
        assert a_result is not None
        assert a_result["last_student"] == "Charlie"  # Last alphabetically

        b_result = next((doc for doc in result_list if doc["_id"] == "B"), None)
        assert b_result is not None
        assert b_result["last_student"] == "Eve"  # Last alphabetically


def test_group_stage_with_std_dev_pop_accumulator():
    """Test $group stage with $stdDevPop accumulator for population standard deviation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_pop"]

        # Insert test data
        docs = [
            {"category": "A", "value": 2},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 5},
            {"category": "A", "value": 5},
            {"category": "A", "value": 7},
            {"category": "A", "value": 9},
            {"category": "B", "value": 1},
            {"category": "B", "value": 3},
            {"category": "B", "value": 5},
            {"category": "B", "value": 7},
            {"category": "B", "value": 9},
        ]
        collection.insert_many(docs)

        # Test group with $stdDevPop
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev": {"$stdDevPop": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Verify standard deviation calculations
        for doc in result:
            if doc["_id"] == "A":
                # Population std dev for [2,4,4,4,5,5,7,9] ≈ 2.0
                assert abs(doc["std_dev"] - 2.0) < 0.01
            elif doc["_id"] == "B":
                # Population std dev for [1,3,5,7,9] = 2.828...
                assert abs(doc["std_dev"] - math.sqrt(8)) < 0.01


def test_group_stage_with_std_dev_samp_accumulator():
    """Test $group stage with $stdDevSamp accumulator for sample standard deviation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_samp"]

        # Insert test data
        docs = [
            {"category": "A", "value": 2},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 5},
            {"category": "A", "value": 5},
            {"category": "A", "value": 7},
            {"category": "A", "value": 9},
            {"category": "B", "value": 1},
            {"category": "B", "value": 3},
            {"category": "B", "value": 5},
            {"category": "B", "value": 7},
            {"category": "B", "value": 9},
        ]
        collection.insert_many(docs)

        # Test group with $stdDevSamp
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev": {"$stdDevSamp": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Verify sample standard deviation calculations
        for doc in result:
            if doc["_id"] == "A":
                # Sample std dev for [2,4,4,4,5,5,7,9] ≈ 2.138
                assert abs(doc["std_dev"] - 2.138) < 0.01
            elif doc["_id"] == "B":
                # Sample std dev for [1,3,5,7,9] ≈ 3.162
                assert abs(doc["std_dev"] - math.sqrt(10)) < 0.01


def test_group_stage_with_merge_objects_accumulator():
    """Test $group stage with $mergeObjects accumulator to merge documents in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_merge_objects"]

        # Insert test data
        docs = [
            {"category": "A", "data1": {"x": 1}, "data2": {"y": 2}},
            {"category": "A", "data1": {"x": 3}, "data2": {"z": 4}},
            {"category": "B", "data1": {"x": 5}, "data2": {"y": 6}},
            {"category": "B", "data1": {"x": 7}, "data2": {"z": 8}},
        ]
        collection.insert_many(docs)

        # Test group with $mergeObjects on data1 field
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "merged_data": {"$mergeObjects": "$data1"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have merged data from data1 fields
        assert {
            "_id": "A",
            "merged_data": {"x": 3},
        } in result  # Last value wins
        # Category B should have merged data from data1 fields
        assert {
            "_id": "B",
            "merged_data": {"x": 7},
        } in result  # Last value wins


def test_group_stage_with_first_and_last_combined():
    """Test $group stage with $first and $last accumulators combined"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_last_combined"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "timestamp": 1},
            {"category": "A", "value": 30, "timestamp": 2},
            {"category": "A", "value": 50, "timestamp": 3},
            {"category": "B", "value": 20, "timestamp": 1},
            {"category": "B", "value": 40, "timestamp": 2},
        ]
        collection.insert_many(docs)

        # Test group with $first and $last combined
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                    "last_value": {"$last": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first=10 and last=50
        assert {"_id": "A", "first_value": 10, "last_value": 50} in result
        # Category B should have first=20 and last=40
        assert {"_id": "B", "first_value": 20, "last_value": 40} in result


def test_group_stage_with_std_dev_edge_cases():
    """Test $group stage with $stdDevPop and $stdDevSamp edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_edge_cases"]

        # Test with single value (std dev should be 0)
        docs = [
            {"category": "A", "value": 5},
            {"category": "B", "value": 10},
            {"category": "B", "value": 10},
        ]
        collection.insert_many(docs)

        # Test group with both std dev accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev_pop": {"$stdDevPop": "$value"},
                    "std_dev_samp": {"$stdDevSamp": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A has only one value, so std dev should be 0
        assert {"_id": "A", "std_dev_pop": 0.0, "std_dev_samp": 0.0} in result
        # Category B has two identical values, so std dev should be 0
        assert result[1]["_id"] == "B"
        assert result[1]["std_dev_pop"] == 0.0
        # For sample std dev with n=2, it should also be 0 when values are identical
        assert result[1]["std_dev_samp"] == 0.0


def test_group_stage_with_merge_objects_complex():
    """Test $group stage with $mergeObjects with more complex nested objects"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_merge_objects_complex"]

        # Insert test data with nested objects
        docs = [
            {"category": "A", "config": {"database": "mysql", "port": 3306}},
            {"category": "A", "config": {"cache": "redis", "port": 6379}},
            {
                "category": "A",
                "config": {"database": "postgresql", "ssl": True},
            },
        ]
        collection.insert_many(docs)

        # Test group with $mergeObjects
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "merged_config": {"$mergeObjects": "$config"},
                }
            },
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 1
        # The merged config should contain all keys, with later values overwriting earlier ones
        expected_config = {
            "database": "postgresql",
            "port": 6379,
            "cache": "redis",
            "ssl": True,
        }
        assert result[0]["merged_config"] == expected_config


def test_group_stage_with_push_accumulator_alt():
    """Test $group stage with $push accumulator to build arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 30},
            {"category": "B", "value": 40},
            {"category": "A", "value": 50},
        ]
        collection.insert_many(docs)

        # Test group with $push
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have items [10, 30, 50] in order
        assert {"_id": "A", "items": [10, 30, 50]} in result
        # Category B should have items [20, 40] in order
        assert {"_id": "B", "items": [20, 40]} in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_accumulator_alt():
    """Test $group stage with $addToSet accumulator to build unique value arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set"]

        # Insert test data with duplicate values
        docs = [
            {"category": "A", "tag": "python"},
            {"category": "B", "tag": "java"},
            {"category": "A", "tag": "javascript"},
            {"category": "B", "tag": "python"},
            {"category": "A", "tag": "python"},  # Duplicate
            {"category": "B", "tag": "java"},  # Duplicate
            {"category": "A", "tag": "go"},
        ]
        collection.insert_many(docs)

        # Test group with $addToSet
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$addToSet": "$tag"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check that each category has the correct unique tags
        for doc in result:
            if doc["_id"] == "A":
                # Should have python, javascript, go (order may vary due to set behavior)
                assert len(doc["items"]) == 3
                assert set(doc["items"]) == {"python", "javascript", "go"}
            elif doc["_id"] == "B":
                # Should have java, python (order may vary due to set behavior)
                assert len(doc["items"]) == 2
                assert set(doc["items"]) == {"java", "python"}

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_push_and_other_accumulators():
    """Test $group stage with $push accumulator combined with other accumulators"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push_combined"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "B", "value": 20, "name": "second"},
            {"category": "A", "value": 30, "name": "third"},
            {"category": "B", "value": 40, "name": "fourth"},
        ]
        collection.insert_many(docs)

        # Test group with $push and $sum
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "names": {"$push": "$name"},
                    "total": {"$sum": "$value"},
                    "count": {"$count": {}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have names ["first", "third"], total 40, count 2
        assert {
            "_id": "A",
            "names": ["first", "third"],
            "total": 40,
            "count": 2,
        } in result
        # Category B should have names ["second", "fourth"], total 60, count 2
        assert {
            "_id": "B",
            "names": ["second", "fourth"],
            "total": 60,
            "count": 2,
        } in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_and_other_accumulators():
    """Test $group stage with $addToSet accumulator combined with other accumulators"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set_combined"]

        # Insert test data with duplicate values
        docs = [
            {"category": "A", "tag": "python", "value": 10},
            {"category": "B", "tag": "java", "value": 20},
            {"category": "A", "tag": "javascript", "value": 30},
            {"category": "B", "tag": "python", "value": 40},
            {"category": "A", "tag": "python", "value": 50},  # Duplicate tag
        ]
        collection.insert_many(docs)

        # Test group with $addToSet and $sum
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "items": {"$addToSet": "$tag"},
                    "total": {"$sum": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check results
        for doc in result:
            if doc["_id"] == "A":
                # Should have unique tags and sum of values
                assert len(doc["items"]) == 2  # python, javascript
                assert set(doc["items"]) == {"python", "javascript"}
                assert doc["total"] == 90  # 10 + 30 + 50
            elif doc["_id"] == "B":
                # Should have unique tags and sum of values
                assert len(doc["items"]) == 2  # java, python
                assert set(doc["items"]) == {"java", "python"}
                assert doc["total"] == 60  # 20 + 40

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_push_null_values():
    """Test $group stage with $push accumulator handling null values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push_null"]

        # Insert test data with null values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": None},
            {"category": "A"},  # Missing value field
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $push including null values
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [10, None, None] (missing field becomes None)
        assert {"_id": "A", "items": [10, None, None]} in result
        # Category B should have [20]
        assert {"_id": "B", "items": [20]} in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_null_values():
    """Test $group stage with $addToSet accumulator handling null values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set_null"]

        # Insert test data with duplicate null values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": None},
            {"category": "A"},  # Missing value field
            {"category": "A", "value": 10},  # Duplicate
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $addToSet including null values
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$addToSet": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check that each category has the correct unique values (including null)
        for doc in result:
            if doc["_id"] == "A":
                # Should have 10, None (duplicates removed, nulls treated as equal)
                assert len(doc["items"]) == 2
                assert 10 in doc["items"]
                assert None in doc["items"]
            elif doc["_id"] == "B":
                # Should have 20
                assert doc["items"] == [20]

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_first_n_accumulator_alt():
    """Test $group stage with $firstN accumulator to get first N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $firstN (get first 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_values": {"$firstN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first 2 values [10, 30]
        assert {"_id": "A", "first_values": [10, 30]} in result
        # Category B should have first 2 values [20, 40]
        assert {"_id": "B", "first_values": [20, 40]} in result


def test_group_stage_with_last_n_accumulator_alt():
    """Test $group stage with $lastN accumulator to get last N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $lastN (get last 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "last_values": {"$lastN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have last 2 values [50, 70]
        assert {"_id": "A", "last_values": [50, 70]} in result
        # Category B should have last 2 values [20, 40]
        assert {"_id": "B", "last_values": [20, 40]} in result


def test_group_stage_with_min_n_accumulator():
    """Test $group stage with $minN accumulator to get N minimum values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_min_n"]

        # Insert test data
        docs = [
            {"category": "A", "value": 70},
            {"category": "A", "value": 30},
            {"category": "A", "value": 50},
            {"category": "A", "value": 10},
            {"category": "B", "value": 40},
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $minN (get 2 minimum values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "min_values": {"$minN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have 2 minimum values [10, 30]
        assert {"_id": "A", "min_values": [10, 30]} in result
        # Category B should have 2 minimum values [20, 40]
        assert {"_id": "B", "min_values": [20, 40]} in result


def test_group_stage_with_max_n_accumulator():
    """Test $group stage with $maxN accumulator to get N maximum values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_max_n"]

        # Insert test data
        docs = [
            {"category": "A", "value": 70},
            {"category": "A", "value": 30},
            {"category": "A", "value": 50},
            {"category": "A", "value": 10},
            {"category": "B", "value": 40},
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $maxN (get 2 maximum values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "max_values": {"$maxN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have 2 maximum values [70, 50]
        assert {"_id": "A", "max_values": [70, 50]} in result
        # Category B should have 2 maximum values [40, 20]
        assert {"_id": "B", "max_values": [40, 20]} in result


def test_group_stage_with_first_n_edge_cases():
    """Test $group stage with $firstN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_n_edge_cases"]

        # Test with fewer documents than n
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $firstN where n=5 but only 2/1 documents exist
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_values": {"$firstN": {"input": "$value", "n": 5}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have all values [10, 20]
        assert {"_id": "A", "first_values": [10, 20]} in result
        # Category B should have all values [30]
        assert {"_id": "B", "first_values": [30]} in result


def test_group_stage_with_last_n_edge_cases():
    """Test $group stage with $lastN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last_n_edge_cases"]

        # Test with fewer documents than n
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $lastN where n=5 but only 2/1 documents exist
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "last_values": {"$lastN": {"input": "$value", "n": 5}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have all values [10, 20]
        assert {"_id": "A", "last_values": [10, 20]} in result
        # Category B should have all values [30]
        assert {"_id": "B", "last_values": [30]} in result


def test_group_stage_with_min_n_edge_cases():
    """Test $group stage with $minN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_min_n_edge_cases"]

        # Test with duplicate values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $minN
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "min_values": {"$minN": {"input": "$value", "n": 3}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [10, 10, 20]
        assert {"_id": "A", "min_values": [10, 10, 20]} in result
        # Category B should have [30]
        assert {"_id": "B", "min_values": [30]} in result


def test_group_stage_with_max_n_edge_cases():
    """Test $group stage with $maxN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_max_n_edge_cases"]

        # Test with duplicate values
        docs = [
            {"category": "A", "value": 20},
            {"category": "A", "value": 20},
            {"category": "A", "value": 10},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $maxN
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "max_values": {"$maxN": {"input": "$value", "n": 3}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [20, 20, 10]
        assert {"_id": "A", "max_values": [20, 20, 10]} in result
        # Category B should have [30]
        assert {"_id": "B", "max_values": [30]} in result


def test_group_stage_with_multiple_accumulators():
    """Test $group stage with multiple accumulator functions."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_multi"]

        # Insert test data
        docs = [
            {"item": "A", "price": 10, "quantity": 2},
            {"item": "B", "price": 20, "quantity": 1},
            {"item": "A", "price": 30, "quantity": 5},
            {"item": "B", "price": 10, "quantity": 2},
        ]
        collection.insert_many(docs)

        # Test group with multiple accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$item",
                    "total_quantity": {"$sum": "$quantity"},
                    "avg_price": {"$avg": "$price"},
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        assert result[0] == {
            "_id": "A",
            "total_quantity": 7,
            "avg_price": 20.0,
            "min_price": 10,
            "max_price": 30,
        }
        assert result[1] == {
            "_id": "B",
            "total_quantity": 3,
            "avg_price": 15.0,
            "min_price": 10,
            "max_price": 20,
        }


def test_group_stage_with_count():
    """Test $group stage with $count accumulator."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_count"]

        # Insert test data
        docs = [
            {"category": "A", "value": 1},
            {"category": "B", "value": 2},
            {"category": "A", "value": 3},
            {"category": "A", "value": 4},
        ]
        collection.insert_many(docs)

        # Test group with count
        pipeline = [
            {"$group": {"_id": "$category", "count": {"$count": {}}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        assert result[0] == {"_id": "A", "count": 3}
        assert result[1] == {"_id": "B", "count": 1}


def test_group_stage_with_match():
    """Test $group stage combined with $match stage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_match"]

        # Insert test data
        docs = [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
            {"store": "A", "price": 30},
            {"store": "C", "price": 40},
        ]
        collection.insert_many(docs)

        # Test group with match
        pipeline = [
            {"$match": {"price": {"$gte": 20}}},
            {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 3
        # Only stores with price >= 20
        assert {"_id": "A", "total": 30} in result  # Only the 30 priced item
        assert {"_id": "B", "total": 20} in result
        assert {"_id": "C", "total": 40} in result


def test_group_stage_fallback_to_python():
    """Test that complex group operations still fallback to Python."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_fallback"]

        # Insert test data
        docs = [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
        ]
        collection.insert_many(docs)

        # Test complex grouping that should fallback to Python - using _id: None
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$price"}}}]

        # This should fallback to Python processing but still work
        result = collection.aggregate(pipeline)

        # Should get one document with the total of all prices
        assert len(result) == 1
        assert result[0]["total"] == 30


# ================================
# $addFields Tests
# ================================


def test_add_fields_python_fallback():
    """Test $addFields functionality with Python fallback (when temp tables fail)."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        query_engine = collection.query_engine

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $addFields with a pipeline that includes an unsupported stage
        # This should force fallback to Python implementation

        # This should raise an exception because $out is not supported
        # But let's test with a supported pipeline first

        # Test with only $addFields - this should work with temp tables
        simple_pipeline = [
            {"$addFields": {"userName": "$name", "userAge": "$age"}}
        ]

        # Use the integration function which will try temp tables first
        try:
            results = execute_2nd_tier_aggregation(
                query_engine, simple_pipeline
            )
            # Verify results
            assert len(results) == 3
            for doc in results:
                assert "userName" in doc
                assert "userAge" in doc
                assert doc["userName"] == doc["name"]
                assert doc["userAge"] == doc["age"]
        except Exception as e:
            pytest.fail(f"Integration failed: {e}")


def test_add_fields_with_unsupported_stage_fallback():
    """Test that pipelines with $addFields and unsupported stages fall back properly."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        query_engine = collection.query_engine

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $addFields in Python fallback by using execute_2nd_tier_aggregation directly
        # First verify that a simple $addFields works (should use temp tables)
        pipeline = [{"$addFields": {"userName": "$name"}}]

        # This should work with temp tables
        results = execute_2nd_tier_aggregation(query_engine, pipeline)
        assert len(results) == 3
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]


def test_add_fields_basic():
    """Test basic $addFields functionality with temporary table aggregation."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test simple field copying with $addFields
        pipeline = [{"$addFields": {"userName": "$name", "userAge": "$age"}}]

        # Check if pipeline can be processed with temporary tables
        assert can_process_with_temporary_tables(pipeline)

        # Process the pipeline
        processor = TemporaryTableAggregationProcessor(collection)
        results = processor.process_pipeline(pipeline)

        # Verify results
        assert len(results) == 3
        for doc in results:
            assert "userName" in doc
            assert "userAge" in doc
            assert doc["userName"] == doc["name"]
            assert doc["userAge"] == doc["age"]


def test_add_fields_with_match():
    """Test $addFields combined with $match using temporary table aggregation."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $match followed by $addFields
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$addFields": {"dept": "$department"}},
        ]

        # Check if pipeline can be processed with temporary tables
        assert can_process_with_temporary_tables(pipeline)

        # Process the pipeline
        processor = TemporaryTableAggregationProcessor(collection)
        results = processor.process_pipeline(pipeline)

        # Verify results
        assert len(results) == 2
        for doc in results:
            assert doc["department"] == "Engineering"
            assert "dept" in doc
            assert doc["dept"] == "Engineering"


# ================================
# Pipeline Reordering Tests
# ================================


def test_pipeline_reordering_optimization():
    """Test that pipeline stages are reordered for better performance"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Test pipeline reordering - match should be moved to the front
        pipeline = [
            {"$unwind": "$tags"},  # Expensive operation first
            {
                "$match": {"category": "Category2", "status": "active"}
            },  # Match should be moved to front
            {"$limit": 10},
        ]

        # The optimization should reorder this to put match first
        result = collection.aggregate(pipeline)

        # Should still work correctly
        assert len(result) <= 10

        # All documents should match the criteria
        categories = [doc["category"] for doc in result]
        statuses = [doc["status"] for doc in result]
        assert all(cat == "Category2" for cat in categories)
        assert all(status == "active" for status in statuses)


def test_cost_based_pipeline_selection():
    """Test that cost estimation is used to select optimal pipeline execution"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {"name": f"User{i}", "category": f"Category{i % 3}", "value": i}
            )
        collection.insert_many(docs)

        # Create index on category
        collection.create_index("category")

        # Test a pipeline where reordering should be beneficial
        pipeline = [
            {"$sort": {"value": -1}},  # Sort expensive operation
            {
                "$match": {"category": "Category1"}
            },  # Indexed match - should be moved to front
            {"$limit": 5},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 5

        # All should match category
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category1" for cat in categories)

        # Should be sorted by value descending
        values = [doc["value"] for doc in result]
        assert values == sorted(values, reverse=True)


def test_match_pushdown_optimization():
    """Test that match stages are pushed down for early filtering"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with nested arrays
        docs = []
        for i in range(30):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "items": [{"id": j, "value": i * 10 + j} for j in range(5)],
                }
            )
        collection.insert_many(docs)

        # Create index on category
        collection.create_index("category")

        # Test pipeline with match after expensive unwind
        pipeline = [
            {"$unwind": "$items"},  # Expensive unwind operation
            {"$match": {"category": "Category2"}},  # Should be pushed to front
            {"$sort": {"items.value": 1}},
            {"$limit": 10},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 10

        # All should match category
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category2" for cat in categories)


def test_pipeline_reordering_with_complex_logical_operators():
    """Test pipeline reordering with complex logical operators in match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Test pipeline with complex logical operators in match
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$match": {
                    "$or": [
                        {"category": "Category2"},
                        {"status": "active", "score": {"$gt": 50}},
                    ]
                }
            },
            {"$limit": 10},
        ]

        # The optimization should reorder this to put match first
        result = collection.aggregate(pipeline)

        # Should still work correctly
        assert len(result) <= 10

        # All documents should match the criteria
        for doc in result:
            category_match = doc["category"] == "Category2"
            status_score_match = doc["status"] == "active" and doc["score"] > 50
            assert category_match or status_score_match


def test_pipeline_reordering_with_and_operators():
    """Test pipeline reordering with $and operators in match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 3}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "value": i * 2,
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("score")

        # Test pipeline with $and operators
        pipeline = [
            {"$sort": {"value": -1}},
            {
                "$match": {
                    "$and": [
                        {"category": "Category1"},
                        {"status": "active"},
                        {"score": {"$gte": 20}},
                    ]
                }
            },
            {"$limit": 5},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 5

        # All should match all criteria
        for doc in result:
            assert doc["category"] == "Category1"
            assert doc["status"] == "active"
            assert doc["score"] >= 20

        # Should be sorted by value descending
        values = [doc["value"] for doc in result]
        assert values == sorted(values, reverse=True)


def test_cost_based_optimization_favors_early_filtering():
    """Test that cost-based optimization favors pipelines with early filtering"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Create test pipelines
        # Pipeline 1: Expensive operation first, then match
        pipeline1 = [
            {"$unwind": "$items"},  # Expensive
            {"$match": {"category": "A"}},  # Should be moved to front
            {"$sort": {"value": 1}},
            {"$limit": 10},
        ]

        # Pipeline 2: Match first, then expensive operation
        pipeline2 = [
            {"$match": {"category": "A"}},  # Indexed match at front
            {"$unwind": "$items"},  # Expensive but processes fewer docs
            {"$sort": {"value": 1}},
            {"$limit": 10},
        ]

        # Create indexes to make the cost estimation realistic
        collection.create_index("category")

        # Estimate costs
        cost1 = query_helper._estimate_pipeline_cost(pipeline1)
        cost2 = query_helper._estimate_pipeline_cost(pipeline2)

        # Pipeline 2 should have lower cost because it filters early
        assert cost2 < cost1


def test_no_reordering_when_no_indexes():
    """Test that pipelines are not reordered when no indexes are available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Pipeline with match on non-indexed field
        pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"description": "test"}},  # Non-indexed field
            {"$limit": 5},
        ]

        # Reorder the pipeline
        reordered = query_helper._reorder_pipeline_for_indexes(pipeline)

        # Should remain unchanged since no indexes are used
        assert reordered == pipeline


def test_mixed_indexed_and_non_indexed_fields():
    """Test pipeline reordering with mixed indexed and non-indexed fields"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(30):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "description": f"Description {i}",
                    "items": [{"id": j, "value": i * 10 + j} for j in range(5)],
                }
            )
        collection.insert_many(docs)

        # Create index only on category
        collection.create_index("category")

        # Pipeline with match on both indexed and non-indexed fields
        pipeline = [
            {"$unwind": "$items"},
            {"$match": {"category": "Category2", "description": "test"}},
            {"$sort": {"items.value": 1}},
            {"$limit": 10},
        ]

        # The optimization should still move the match to the front
        # even though it contains both indexed and non-indexed fields
        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 10

        # All should match the category (indexed field)
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category2" for cat in categories)


def test_pipeline_with_multiple_match_stages():
    """Test pipeline reordering with multiple match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Insert test data
        docs = []
        for i in range(40):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "status": "active" if i % 3 == 0 else "inactive",
                    "priority": "high" if i % 5 == 0 else "low",
                    "score": i,
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("priority")

        # Pipeline with multiple match stages
        pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"category": "Category1"}},  # Indexed
            {"$sort": {"score": -1}},
            {"$match": {"status": "active"}},  # Indexed but later in pipeline
            {"$limit": 8},
        ]

        # Reorder the pipeline
        reordered = query_helper._reorder_pipeline_for_indexes(pipeline)

        # Both match stages should be moved to the front
        # The order might vary but both should be before unwind
        [stage for stage in reordered if "$match" in stage]
        [stage for stage in reordered if "$unwind" in stage]

        # All match stages should come before unwind stages
        first_unwind_index = next(
            i for i, stage in enumerate(reordered) if "$unwind" in stage
        )
        last_match_index = max(
            i for i, stage in enumerate(reordered) if "$match" in stage
        )

        assert last_match_index < first_unwind_index


def test_complex_pipeline_cost_estimation():
    """Test cost estimation for complex pipelines with multiple operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Complex pipeline with multiple operations
        complex_pipeline = [
            {"$unwind": "$items"},
            {"$unwind": "$items.subitems"},
            {"$match": {"category": "A", "status": "active"}},
            {"$group": {"_id": "$items.id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]

        # Simple pipeline with early filtering
        optimized_pipeline = [
            {"$match": {"category": "A", "status": "active"}},
            {"$unwind": "$items"},
            {"$unwind": "$items.subitems"},
            {"$group": {"_id": "$items.id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]

        # Estimate costs
        complex_cost = query_helper._estimate_pipeline_cost(complex_pipeline)
        optimized_cost = query_helper._estimate_pipeline_cost(
            optimized_pipeline
        )

        # The optimized pipeline should have lower cost
        assert optimized_cost < complex_cost


# ================================
# Aggregation Expression Tests (Phase 2)
# ================================


def test_multi_stage_pipeline_with_expressions():
    """Test multi-stage pipeline with aggregation expressions."""
    from neosqlite.collection.query_helper import set_force_fallback

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_multi"]
        collection.insert_many(
            [
                {"_id": 1, "category": "A", "price": 100, "quantity": 5},
                {"_id": 2, "category": "A", "price": 50, "quantity": 10},
                {"_id": 3, "category": "B", "price": 75, "quantity": 8},
            ]
        )

        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "revenue": {"$multiply": ["$price", "$quantity"]}
                    }
                },
                {"$match": {"revenue": {"$gte": 500}}},
                {
                    "$group": {
                        "_id": "$category",
                        "total_revenue": {"$sum": "$revenue"},
                    }
                },
                {"$sort": {"total_revenue": -1}},
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 2
            # Category A: 500 + 500 = 1000
            # Category B: 600
            assert results[0]["_id"] == "A"
            assert results[0]["total_revenue"] == 1000
            assert results[1]["_id"] == "B"
            assert results[1]["total_revenue"] == 600
        finally:
            set_force_fallback(False)


def test_match_with_expr_in_aggregation():
    """Test $match with $expr in aggregation pipeline."""
    from neosqlite.collection.query_helper import set_force_fallback

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_match_expr"]
        collection.insert_many(
            [
                {"_id": 1, "name": "Alice", "salary": 50000},
                {"_id": 2, "name": "Bob", "salary": 45000},
                {"_id": 3, "name": "Charlie", "salary": 60000},
            ]
        )

        set_force_fallback(True)
        try:
            pipeline = [{"$match": {"$expr": {"$gt": ["$salary", 50000]}}}]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 1
            assert results[0]["name"] == "Charlie"
        finally:
            set_force_fallback(False)


def test_facet_with_expressions_in_subpipeline():
    """Test $facet with expressions in sub-pipeline."""
    from neosqlite.collection.query_helper import set_force_fallback

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_facet"]
        collection.insert_many(
            [
                {"_id": 1, "category": "A", "price": 100, "quantity": 5},
                {"_id": 2, "category": "A", "price": 50, "quantity": 10},
                {"_id": 3, "category": "B", "price": 75, "quantity": 8},
                {"_id": 4, "category": "B", "price": 25, "quantity": 20},
            ]
        )

        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$facet": {
                        "by_category": [
                            {
                                "$group": {
                                    "_id": "$category",
                                    "total_revenue": {
                                        "$sum": {
                                            "$multiply": ["$price", "$quantity"]
                                        }
                                    },
                                }
                            }
                        ],
                        "high_value": [
                            {
                                "$addFields": {
                                    "revenue": {
                                        "$multiply": ["$price", "$quantity"]
                                    }
                                }
                            },
                            {"$match": {"revenue": {"$gte": 500}}},
                        ],
                    }
                }
            ]
            results = list(collection.aggregate(pipeline))

            assert len(results) == 1
            facet_result = results[0]

            # Check by_category facet
            assert "by_category" in facet_result
            assert len(facet_result["by_category"]) == 2

            # Check high_value facet
            assert "high_value" in facet_result
            # Should have documents with revenue >= 500
            for doc in facet_result["high_value"]:
                assert doc["revenue"] >= 500
        finally:
            set_force_fallback(False)


def test_aggregate_pymongo_compatibility_params(collection):
    """Test that aggregate() accepts PyMongo-compatible parameters.

    These parameters are accepted for API compatibility but don't affect
    NeoSQLite's behavior.
    """
    collection.insert_many([{"value": i} for i in range(10)])

    # Test allowDiskUse parameter (accepted but ignored)
    cursor = collection.aggregate(
        [{"$match": {"value": {"$gte": 5}}}], allowDiskUse=True
    )
    results = list(cursor)
    assert len(results) == 5

    # Test allowDiskUse=False (also accepted)
    cursor = collection.aggregate(
        [{"$match": {"value": {"$gte": 5}}}], allowDiskUse=False
    )
    results = list(cursor)
    assert len(results) == 5

    # Test batchSize parameter (accepted but ignored)
    cursor = collection.aggregate(
        [{"$match": {"value": {"$gte": 5}}}], batchSize=3
    )
    results = list(cursor)
    assert len(results) == 5

    # Test both parameters together
    cursor = collection.aggregate(
        [{"$match": {"value": {"$gte": 5}}}], allowDiskUse=True, batchSize=5
    )
    results = list(cursor)
    assert len(results) == 5

    # Test that method chaining still works
    cursor = collection.aggregate(
        [{"$match": {"value": {"$gte": 5}}}]
    ).allow_disk_use(True)
    results = list(cursor)
    assert len(results) == 5
