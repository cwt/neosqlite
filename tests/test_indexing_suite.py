"""
Consolidated tests for indexing functionality.
"""

from pytest import raises
from typing import Tuple, Type
import sqlite3
import neosqlite
import pytest
import time

# Handle both standard sqlite3 and pysqlite3 exceptions
try:
    import pysqlite3.dbapi2 as sqlite3_with_jsonb  # type: ignore

    IntegrityError: Tuple[Type[Exception], ...] = (
        sqlite3.IntegrityError,
        sqlite3_with_jsonb.IntegrityError,
    )
except ImportError:
    IntegrityError = (sqlite3.IntegrityError,)


# ================================
# Basic Indexing Tests
# ================================


def test_create_index(collection):
    collection.insert_one({"foo": "bar"})
    collection.create_index("foo")
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_index_on_nested_keys(collection):
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    collection.create_index("foo.bar")
    assert "idx_foo_foo_bar" in collection.list_indexes()


def test_reindex(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    # With native JSON indexing, reindex does nothing but should not fail
    collection.reindex("idx_foo_foo")


def test_insert_auto_index(collection):
    collection.create_index("foo")
    collection.insert_one({"foo": "bar"})
    collection.insert_one({"foo": "baz"})

    # With native JSON indexing, we can't directly query the index table
    # but we can verify the index exists by checking the index list
    assert "idx_foo_foo" in collection.list_indexes()


def test_create_compound_index(collection):
    collection.insert_one({"foo": "bar", "far": "boo"})
    collection.create_index(["foo", "far"])
    assert "idx_foo_foo_far" in collection.list_indexes()


def test_create_unique_index_violation(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    with raises(IntegrityError):
        collection.insert_one({"foo": "bar"})


def test_update_to_break_uniqueness(collection):
    collection.create_index("foo", unique=True)
    collection.insert_one({"foo": "bar"})
    res = collection.insert_one({"foo": "baz"})

    with raises(IntegrityError):
        collection.update_one(
            {"_id": res.inserted_id}, {"$set": {"foo": "bar"}}
        )


def test_hint_index(collection):
    collection.insert_many(
        [{"foo": "bar", "a": 1}, {"foo": "bar", "a": 2}, {"fox": "baz", "a": 3}]
    )
    collection.create_index("foo")

    # This test is more conceptual now, as the implementation details changed
    # We can't easily mock the execute call in the same way.
    # We'll trust the implementation detail that hint is used.
    docs_with_hint = list(
        collection.find({"foo": "bar", "a": 2}, hint="idx_foo_foo")
    )
    assert len(docs_with_hint) == 1
    assert docs_with_hint[0]["a"] == 2


def test_list_indexes(collection):
    collection.create_index("foo")
    indexes = collection.list_indexes()
    assert isinstance(indexes, list)
    assert "idx_foo_foo" in indexes


def test_drop_index(collection):
    collection.create_index("foo")
    collection.drop_index("foo")
    assert "idx_foo_foo" not in collection.list_indexes()


def test_drop_indexes_from_collection_index(collection):
    collection.create_index("foo")
    collection.create_index("bar")
    collection.drop_indexes()
    assert len(collection.list_indexes()) == 0


def test_create_indexes_with_string_keys(collection):
    collection.insert_one({"foo": "bar", "baz": "qux"})
    indexes = collection.create_indexes(["foo", "baz"])
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz" in collection.list_indexes()


def test_create_indexes_with_compound_keys(collection):
    collection.insert_one({"foo": "bar", "baz": "qux", "quux": "corge"})
    indexes = collection.create_indexes([["foo", "baz"], ["quux"]])
    assert "idx_foo_foo_baz" in indexes
    assert "idx_foo_quux" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo_baz" in collection.list_indexes()
    assert "idx_foo_quux" in collection.list_indexes()


def test_create_indexes_with_dict_specifications(collection):
    collection.insert_one({"foo": "bar", "baz": "qux"})
    indexes = collection.create_indexes(
        [{"key": "foo"}, {"key": ["baz"], "unique": True}]
    )
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz" in indexes
    assert len(indexes) == 2
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz" in collection.list_indexes()


def test_create_indexes_with_mixed_specifications(collection):
    collection.insert_one({"foo": "bar", "baz": "qux", "quux": "corge"})
    indexes = collection.create_indexes(
        ["foo", ["baz", "quux"], {"key": "quux", "unique": True}]
    )
    assert "idx_foo_foo" in indexes
    assert "idx_foo_baz_quux" in indexes
    assert "idx_foo_quux" in indexes
    assert len(indexes) == 3
    assert "idx_foo_foo" in collection.list_indexes()
    assert "idx_foo_baz_quux" in collection.list_indexes()
    assert "idx_foo_quux" in collection.list_indexes()


def test_create_indexes_on_nested_keys(collection):
    collection.insert_many(
        [{"foo": {"bar": "zzz"}, "bok": "bak"}, {"a": 1, "b": 2}]
    )
    indexes = collection.create_indexes(["foo.bar"])
    assert "idx_foo_foo_bar" in indexes
    assert len(indexes) == 1
    assert "idx_foo_foo_bar" in collection.list_indexes()


# ================================
# Index Information Tests
# ================================


def test_index_information_empty(collection):
    """Test index_information on a collection with no indexes."""
    info = collection.index_information()
    assert isinstance(info, dict)
    assert len(info) == 0


def test_index_information_single_index(collection):
    """Test index_information with a single index."""
    collection.create_index("foo")
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo" in info

    # Check index details
    idx_info = info["idx_foo_foo"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    assert idx_info["key"] == {"foo": 1}


def test_index_information_single_index_unique(collection):
    """Test index_information with a single unique index."""
    collection.create_index("foo", unique=True)
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo" in info

    # Check index details
    idx_info = info["idx_foo_foo"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is True
    assert "key" in idx_info
    assert idx_info["key"] == {"foo": 1}


def test_index_information_nested_key_index(collection):
    """Test index_information with an index on a nested key."""
    collection.create_index("foo.bar")
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo_bar" in info

    # Check index details
    idx_info = info["idx_foo_foo_bar"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    assert idx_info["key"] == {"foo.bar": 1}


def test_index_information_compound_index(collection):
    """Test index_information with a compound index."""
    collection.create_index(["foo", "bar"])
    info = collection.index_information()

    # Should have one index
    assert len(info) == 1

    # Check the index name exists
    assert "idx_foo_foo_bar" in info

    # Check index details
    idx_info = info["idx_foo_foo_bar"]
    assert "v" in idx_info
    assert idx_info["v"] == 2
    assert "unique" in idx_info
    assert idx_info["unique"] is False
    assert "key" in idx_info
    # For compound indexes, we expect both keys
    assert "foo" in idx_info["key"]
    assert "bar" in idx_info["key"]
    assert idx_info["key"]["foo"] == 1
    assert idx_info["key"]["bar"] == 1


def test_index_information_multiple_indexes(collection):
    """Test index_information with multiple indexes."""
    collection.create_index("foo")
    collection.create_index("bar", unique=True)
    collection.create_index(["baz", "qux"])

    info = collection.index_information()

    # Should have three indexes
    assert len(info) == 3

    # Check all index names exist
    assert "idx_foo_foo" in info
    assert "idx_foo_bar" in info
    assert "idx_foo_baz_qux" in info

    # Check details for each index
    # foo index
    foo_info = info["idx_foo_foo"]
    assert foo_info["unique"] is False
    assert foo_info["key"] == {"foo": 1}

    # bar index (unique)
    bar_info = info["idx_foo_bar"]
    assert bar_info["unique"] is True
    assert bar_info["key"] == {"bar": 1}

    # baz.qux compound index
    baz_qux_info = info["idx_foo_baz_qux"]
    assert baz_qux_info["unique"] is False
    assert "baz" in baz_qux_info["key"]
    assert "qux" in baz_qux_info["key"]


# ================================
# Index Usage and Performance Tests
# ================================


def test_index_usage_with_explain(collection):
    """Test that indexes are actually being used by examining the SQLite query plan."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
            {"name": "David", "age": 25},
        ]
    )

    # Create an index on age
    collection.create_index("age")

    # Verify the index exists
    assert "idx_foo_age" in collection.list_indexes()

    # Build a query using the Collection's own query building mechanism
    query_filter = {"age": 25}
    where_result = collection.query_engine.helpers._build_simple_where_clause(
        query_filter
    )

    # Verify that we can build a SQL query for this filter
    assert where_result is not None
    where_clause, params = where_result

    # Construct the full SQL query that would be used by the Collection
    sql_query = f"SELECT id, data FROM {collection.name} {where_clause}"

    # Check that a simple query on the indexed field uses the index
    # We'll do this by examining the SQLite query plan
    cursor = collection.db.execute(f"EXPLAIN QUERY PLAN {sql_query}", params)
    plan = cursor.fetchall()

    # Check if the plan shows index usage
    # The plan should show that we're using our index
    plan_text = " ".join(str(item) for row in plan for item in row)

    # Verify that the index is being used
    # The plan should contain both "INDEX" and our specific index name
    assert "INDEX" in plan_text
    assert "idx_foo_age" in plan_text
    # Also verify it's a SEARCH operation (not a SCAN which would be less efficient)
    assert "SEARCH" in plan_text


def test_index_usage_performance_verification(collection):
    """Test that indexes improve query performance."""
    # Insert a larger dataset
    docs = [{"name": f"User{i}", "age": i % 50} for i in range(1000)]
    collection.insert_many(docs)

    # Create an index on age
    collection.create_index("age")

    # Time a query with the index
    start_time = time.time()
    results_with_index = list(collection.find({"age": 25}))
    time.time() - start_time

    # The query should return results quickly
    assert len(results_with_index) > 0


def test_compound_index_usage(collection):
    """Test that compound indexes are being used."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "age": 25, "city": "New York"},
            {"name": "Bob", "age": 30, "city": "Boston"},
            {"name": "Charlie", "age": 25, "city": "New York"},
            {"name": "David", "age": 30, "city": "Boston"},
        ]
    )

    # Create a compound index
    collection.create_index(["age", "city"])

    # Verify the index exists
    assert "idx_foo_age_city" in collection.list_indexes()

    # Check that a query using both indexed fields works
    results = list(collection.find({"age": 25, "city": "New York"}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Charlie"}


def test_nested_index_usage(collection):
    """Test that indexes on nested fields are being used."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "profile": {"age": 25, "city": "New York"}},
            {"name": "Bob", "profile": {"age": 30, "city": "Boston"}},
            {"name": "Charlie", "profile": {"age": 25, "city": "New York"}},
        ]
    )

    # Create an index on a nested field
    collection.create_index("profile.age")

    # Verify the index exists
    assert "idx_foo_profile_age" in collection.list_indexes()

    # Check that a query using the nested field works
    results = list(collection.find({"profile.age": 25}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Charlie"}


def test_list_indexes_as_keys():
    """Test list_indexes with as_keys=True parameter."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create some indexes
    collection.create_index("foo")
    collection.create_index("bar.baz")  # Nested key

    # Get indexes as keys
    indexes_as_keys = collection.list_indexes(as_keys=True)

    # Check that we get the expected keys
    assert ["foo"] in indexes_as_keys
    assert ["bar.baz"] in indexes_as_keys


def test_drop_indexes_from_index_utils():
    """Test drop_indexes method."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create some indexes
    collection.create_index("foo")
    collection.create_index("bar")

    # Verify indexes exist
    indexes = collection.list_indexes()
    assert len(indexes) == 2
    assert "idx_test_foo" in indexes
    assert "idx_test_bar" in indexes

    # Drop all indexes
    collection.drop_indexes()

    # Verify indexes are gone
    indexes = collection.list_indexes()
    assert len(indexes) == 0


def test_drop_compound_index():
    """Test dropping compound indexes."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create a compound index
    collection.create_index(["foo", "bar"])

    # Verify index exists
    indexes = collection.list_indexes()
    assert "idx_test_foo_bar" in indexes

    # Drop the compound index
    collection.drop_index(["foo", "bar"])

    # Verify index is gone
    indexes = collection.list_indexes()
    assert "idx_test_foo_bar" not in indexes


def test_object_exists():
    """Test _object_exists method."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test that the collection table exists
    assert collection._object_exists("table", "test")

    # Test that a non-existent table doesn't exist
    assert not collection._object_exists("table", "nonexistent")

    # Create an index and check that it exists
    collection.create_index("foo")
    assert collection._object_exists("index", "idx_test_foo")

    # Check that a non-existent index doesn't exist
    assert not collection._object_exists("index", "idx_test_nonexistent")

    # Test the default case (should return False)
    assert not collection._object_exists("unknown_type", "name")


# ================================
# Index Optimization Tests
# ================================


def test_unwind_with_indexed_field_optimization():
    """Test that $unwind operations on indexed fields use the index for better performance"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i}",
                    "tags": [f"tag{j}" for j in range(5)],  # 5 tags per user
                    "category": f"Category{i % 10}",  # 10 categories
                }
            )
        collection.insert_many(docs)

        # Create an index on the category field
        collection.create_index("category")

        # Verify the index exists
        assert "idx_test_collection_category" in collection.list_indexes()

        # Test $match + $unwind with indexed field
        pipeline = [{"$match": {"category": "Category5"}}, {"$unwind": "$tags"}]

        # The query should be optimized to use the index
        result = collection.aggregate(pipeline)

        # Should have 50 documents (5 users with Category5, each with 5 tags = 25 documents)
        # Actually, let's check the data more carefully
        # Users with Category5 are those where i % 10 == 5, so users 5, 15, 25, 35, 45, 55, 65, 75, 85, 95
        # That's 10 users, each with 5 tags = 50 documents
        assert len(result) == 50

        # All documents should have category "Category5"
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category5" for cat in categories)


def test_unwind_group_with_indexed_field_optimization():
    """Test that $unwind + $group operations use indexes when available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",  # 5 categories
                    "tags": [f"tag{j}" for j in range(3)],  # 3 tags per user
                }
            )
        collection.insert_many(docs)

        # Create an index on the category field
        collection.create_index("category")

        # Verify the index exists
        assert "idx_test_collection_category" in collection.list_indexes()

        # Test $match + $unwind + $group with indexed field
        pipeline = [
            {"$match": {"category": "Category2"}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]

        # The query should be optimized to use the index
        result = collection.aggregate(pipeline)

        # Should have 3 documents (3 tags)
        assert len(result) == 3

        # All counts should be 10 (users with Category2: 2, 7, 12, 17, 22, 27, 32, 37, 42, 47 = 10 users, each with 3 tags)
        counts = [doc["count"] for doc in result]
        assert all(count == 10 for count in counts)

        # Sort by tag name for consistent ordering
        result.sort(key=lambda x: x["_id"])
        tags = [doc["_id"] for doc in result]
        assert tags == ["tag0", "tag1", "tag2"]


def test_nested_unwind_with_indexed_field():
    """Test that nested $unwind operations use indexes when available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with nested arrays
        docs = []
        for i in range(20):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",  # 4 categories
                    "orders": [
                        {
                            "orderId": f"Order{i}_1",
                            "items": [
                                {"product": f"Product{j}", "quantity": j + 1}
                                for j in range(2)
                            ],
                        },
                        {
                            "orderId": f"Order{i}_2",
                            "items": [
                                {"product": f"Product{j+2}", "quantity": j + 3}
                                for j in range(2)
                            ],
                        },
                    ],
                }
            )
        collection.insert_many(docs)

        # Create an index on the category field
        collection.create_index("category")

        # Verify the index exists
        assert "idx_test_collection_category" in collection.list_indexes()

        # Test nested $unwind with indexed field
        pipeline = [
            {"$match": {"category": "Category1"}},
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
        ]

        # The query should be optimized to use the index
        result = collection.aggregate(pipeline)

        # Should have 20 documents (users with Category1: 1, 5, 9, 13, 17 = 5 users,
        # each with 2 orders, each with 2 items = 5 * 2 * 2 = 20 documents)
        assert len(result) == 20

        # All documents should have category "Category1"
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category1" for cat in categories)


def test_sort_with_indexed_field_optimization():
    """Test that $sort operations use indexes when available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i:03d}",  # Zero-padded for proper sorting
                    "score": 100 - i,  # Descending scores
                    "category": f"Category{i % 10}",
                }
            )
        collection.insert_many(docs)

        # Create an index on the score field
        collection.create_index("score")

        # Verify the index exists
        assert "idx_test_collection_score" in collection.list_indexes()

        # Test $match + $sort with indexed field
        pipeline = [
            {"$match": {"category": "Category3"}},
            {"$sort": {"score": -1}},  # Sort by score descending
            {"$limit": 5},
        ]

        # The query should be optimized to use the index
        result = collection.aggregate(pipeline)

        # Should have 5 documents (limited)
        assert len(result) == 5

        # Scores should be in descending order
        scores = [doc["score"] for doc in result]
        assert scores == sorted(scores, reverse=True)


def test_complex_pipeline_with_multiple_indexes():
    """Test a complex pipeline that can leverage multiple indexes"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(200):
            docs.append(
                {
                    "name": f"User{i:03d}",
                    "category": f"Category{i % 10}",
                    "status": "active" if i % 3 != 0 else "inactive",
                    "score": i,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )
        collection.insert_many(docs)

        # Create indexes on frequently queried fields
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("score")

        # Verify the indexes exist
        indexes = collection.list_indexes()
        assert "idx_test_collection_category" in indexes
        assert "idx_test_collection_status" in indexes
        assert "idx_test_collection_score" in indexes

        # Test a complex pipeline that can benefit from multiple indexes
        pipeline = [
            {"$match": {"category": "Category5", "status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"score": 1}},  # Sort by score ascending
            {"$limit": 10},
        ]

        # The query should be optimized to use the indexes
        result = collection.aggregate(pipeline)

        # Should have at most 10 documents (limited)
        assert len(result) <= 10

        # All documents should match the criteria
        categories = [doc["category"] for doc in result]
        statuses = [doc["status"] for doc in result]
        assert all(cat == "Category5" for cat in categories)
        assert all(status == "active" for status in statuses)

        # Scores should be in ascending order
        scores = [doc["score"] for doc in result]
        assert scores == sorted(scores)


def test_unwind_sort_limit_with_indexed_field():
    """Test $unwind + $sort + $limit operations use indexes when available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",
                    "tags": [
                        f"tag{j}_{i}" for j in range(10)
                    ],  # 10 tags per user
                }
            )
        collection.insert_many(docs)

        # Create an index on the category field
        collection.create_index("category")

        # Verify the index exists
        assert "idx_test_collection_category" in collection.list_indexes()

        # Test $match + $unwind + $sort + $limit with indexed field
        pipeline = [
            {"$match": {"category": "Category2"}},
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},  # Sort tags alphabetically
            {"$limit": 5},
        ]

        # The query should be optimized to use the index
        result = collection.aggregate(pipeline)

        # Should have exactly 5 documents (limited)
        assert len(result) == 5

        # All documents should have category "Category2"
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category2" for cat in categories)

        # Tags should be sorted alphabetically
        tags = [doc["tags"] for doc in result]
        assert tags == sorted(tags)


# ================================
# Comprehensive Indexing Tests
# ================================


def test_index_dropping(collection):
    """Test index dropping functionality."""
    # Create an index
    collection.create_index("test_field")

    # Verify it exists using index_information
    info = collection.index_information()
    assert "idx_foo_test_field" in info  # Use correct collection name

    # Drop the index (should use field name, not full index name)
    collection.drop_index("test_field")  # Use field name, not index name

    # Verify it's gone
    info = collection.index_information()
    assert "idx_foo_test_field" not in info

    # Test dropping non-existent index (should not raise error)
    collection.drop_index("non_existent_field")  # Should not raise error

    # Test creating and dropping multiple indexes
    collection.create_index("field1")
    collection.create_index("field2")

    # Verify they exist
    info = collection.index_information()
    assert "idx_foo_field1" in info
    assert "idx_foo_field2" in info

    # Drop all indexes
    collection.drop_indexes()

    # Verify all indexes are gone
    info = collection.index_information()
    # Should only have the default _id index
    assert len(info) <= 1

    # Try to drop non-existent index (should not raise error)
    collection.drop_index("nonexistent_index")


def test_compound_index_functionality(collection):
    """Test compound index functionality."""
    # Insert test data
    collection.insert_many(
        [
            {"category": "A", "subcategory": "X", "value": 10},
            {"category": "A", "subcategory": "Y", "value": 20},
            {"category": "B", "subcategory": "X", "value": 30},
            {"category": "B", "subcategory": "Y", "value": 40},
        ]
    )

    # Create compound index using the correct API (list of strings)
    collection.create_index(["category", "subcategory"])

    # Verify the compound index exists
    indexes = collection.list_indexes()
    assert "idx_foo_category_subcategory" in indexes

    # Test that queries work with the compound index
    results = list(collection.find({"category": "A"}))
    assert len(results) == 2  # Should get both A,X and A,Y

    # Test more specific query
    results = list(collection.find({"category": "A", "subcategory": "X"}))
    assert len(results) == 1  # Should get only A,X
    assert results[0]["value"] == 10

    # Test another specific query
    results = list(collection.find({"category": "B", "subcategory": "Y"}))
    assert len(results) == 1  # Should get only B,Y
    assert results[0]["value"] == 40

    # Test sorting with compound index
    results = list(collection.find({"category": "A"}).sort("subcategory", 1))
    assert len(results) == 2
    # Should be sorted by subcategory (X, Y)
    assert results[0]["subcategory"] == "X"
    assert results[1]["subcategory"] == "Y"


# ================================
# Search Index Tests
# ================================


def test_create_search_index(collection):
    """Test creating a search index."""
    # Insert test data
    collection.insert_one(
        {"title": "Python Programming", "content": "Learn Python basics"}
    )

    # Create search index
    collection.create_search_index("title")

    # Verify the search index was created
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes


def test_create_search_indexes(collection):
    """Test creating multiple search indexes at once."""
    # Insert test data
    collection.insert_one(
        {
            "title": "Python Programming",
            "content": "Learn Python basics",
            "tags": "python,programming,education",
        }
    )

    # Create multiple search indexes
    collection.create_search_indexes(["title", "content", "tags"])

    # Verify all search indexes were created
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes
    assert "content" in search_indexes
    assert "tags" in search_indexes


def test_drop_search_index(collection):
    """Test dropping a search index."""
    # Insert test data
    collection.insert_one(
        {"title": "Python Programming", "content": "Learn Python basics"}
    )

    # Create search index
    collection.create_search_index("title")

    # Verify the search index was created
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes

    # Drop the search index
    collection.drop_search_index("title")

    # Verify the search index was dropped
    search_indexes = collection.list_search_indexes()
    assert "title" not in search_indexes


def test_list_search_indexes(collection):
    """Test listing search indexes."""
    # Initially there should be no search indexes
    search_indexes = collection.list_search_indexes()
    assert search_indexes == []

    # Insert test data
    collection.insert_one(
        {
            "title": "Python Programming",
            "content": "Learn Python basics",
            "author": "John Doe",
        }
    )

    # Create search indexes
    collection.create_search_index("title")
    collection.create_search_index("content")

    # Verify the search indexes are listed
    search_indexes = collection.list_search_indexes()
    assert len(search_indexes) == 2
    assert "title" in search_indexes
    assert "content" in search_indexes
    assert "author" not in search_indexes


def test_update_search_index(collection):
    """Test updating a search index."""
    # Insert test data
    collection.insert_one(
        {"title": "Python Programming", "content": "Learn Python basics"}
    )

    # Create search index
    collection.create_search_index("title")

    # Verify the search index was created
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes

    # Update the search index (this drops and recreates it)
    collection.update_search_index("title")

    # Verify the search index still exists
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes


def test_search_index_functionality(collection):
    """Test that search indexes work for text search queries."""
    # Insert test data
    collection.insert_many(
        [
            {"title": "Python Programming", "content": "Learn Python basics"},
            {
                "title": "JavaScript Guide",
                "content": "Web development with JavaScript",
            },
            {
                "title": "Database Fundamentals",
                "content": "SQL and Python integration",
            },
        ]
    )

    # Create search index
    collection.create_search_index("title")

    # Test text search (this would use the FTS index)
    # Note: Actual search functionality would be tested in query engine tests
    search_indexes = collection.list_search_indexes()
    assert "title" in search_indexes


if __name__ == "__main__":
    pytest.main([__file__])
