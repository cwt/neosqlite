# coding: utf-8
"""
Performance test for the new $unwind + $sort + $limit implementation with json_each()
"""
import neosqlite
import time
import pytest


def test_unwind_sort_limit_performance():
    """Demonstrate performance improvement with SQL-based $unwind + $sort + $limit"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert many documents with arrays
        docs = []
        for i in range(1000):
            docs.append(
                {
                    "id": i,
                    "name": f"User{i}",
                    "tags": [f"tag{j}" for j in range(10)],  # 10 tags per user
                    "score": 100 - (i % 50),  # Scores from 50 to 100
                }
            )

        collection.insert_many(docs)

        # Time the new SQL-based implementation
        start = time.time()
        pipeline = [
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$limit": 100},
        ]
        result_sql = collection.aggregate(pipeline)
        sql_time = time.time() - start

        # Verify we got the expected results
        assert len(result_sql) == 100  # Limited to 100

        # Test with $match + $unwind + $sort + $limit combination
        start = time.time()
        pipeline = [
            {"$match": {"id": {"$lt": 500}}},  # First 500 users
            {"$unwind": "$tags"},
            {"$sort": {"score": -1}},  # Sort by score descending
            {"$limit": 50},
        ]
        result_combined = collection.aggregate(pipeline)
        combined_time = time.time() - start

        # Verify we got the expected results
        assert len(result_combined) == 50  # Limited to 50

        print(f"SQL-based $unwind+$sort+$limit time: {sql_time:.4f}s")
        print(
            f"SQL-based $match+$unwind+$sort+$limit time: {combined_time:.4f}s"
        )

        # The implementation should be significantly faster than the old Python-based approach
        # for large datasets, though in this simple test the difference might be minimal


if __name__ == "__main__":
    pytest.main([__file__])
