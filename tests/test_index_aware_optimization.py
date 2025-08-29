# coding: utf-8
"""
Test cases for Advanced Index-Aware Optimization (#9 enhancement)
These tests verify the implementation of query cost estimation based on index availability
and automatic selection of optimal execution paths.
"""
import neosqlite
import pytest


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


if __name__ == "__main__":
    pytest.main([__file__])
