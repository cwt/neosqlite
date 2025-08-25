# coding: utf-8
"""
Performance test for the new $unwind implementation with json_each()
"""
import neosqlite
import time
import pytest


def test_unwind_performance_improvement():
    """Demonstrate performance improvement with SQL-based $unwind"""
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
                }
            )

        collection.insert_many(docs)

        # Time the new SQL-based implementation
        start = time.time()
        pipeline = [{"$unwind": "$tags"}]
        result_sql = collection.aggregate(pipeline)
        sql_time = time.time() - start

        # Verify we got the expected results
        assert len(result_sql) == 10000  # 1000 docs * 10 tags each

        # Test with $match + $unwind combination
        start = time.time()
        pipeline = [
            {"$match": {"id": {"$lt": 500}}},  # First 500 users
            {"$unwind": "$tags"},
        ]
        result_combined = collection.aggregate(pipeline)
        combined_time = time.time() - start

        # Verify we got the expected results
        assert len(result_combined) == 5000  # 500 docs * 10 tags each

        print(f"SQL-based $unwind time: {sql_time:.4f}s")
        print(f"SQL-based $match+$unwind time: {combined_time:.4f}s")

        # The implementation should be significantly faster than the old Python-based approach
        # for large datasets, though in this simple test the difference might be minimal


def test_unwind_correctness():
    """Verify that the SQL-based implementation produces correct results"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "hobbies": ["reading", "swimming"]},
                {"name": "Bob", "hobbies": ["gaming", "cooking", "hiking"]},
                {"name": "Charlie", "hobbies": ["painting"]},
            ]
        )

        # Test simple unwind
        pipeline = [{"$unwind": "$hobbies"}]
        result = collection.aggregate(pipeline)

        # Expected results
        expected = [
            {"name": "Alice", "hobbies": "reading", "_id": 1},
            {"name": "Alice", "hobbies": "swimming", "_id": 1},
            {"name": "Bob", "hobbies": "gaming", "_id": 2},
            {"name": "Bob", "hobbies": "cooking", "_id": 2},
            {"name": "Bob", "hobbies": "hiking", "_id": 2},
            {"name": "Charlie", "hobbies": "painting", "_id": 3},
        ]

        # Sort both results by name and hobbies for comparison
        result_sorted = sorted(result, key=lambda x: (x["name"], x["hobbies"]))
        expected_sorted = sorted(
            expected, key=lambda x: (x["name"], x["hobbies"])
        )

        # Compare the results (ignoring the exact _id values for now)
        assert len(result_sorted) == len(expected_sorted)

        for i, (res, exp) in enumerate(zip(result_sorted, expected_sorted)):
            assert res["name"] == exp["name"]
            assert res["hobbies"] == exp["hobbies"]


def test_unwind_edge_cases():
    """Test edge cases for the SQL-based $unwind implementation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert documents with edge cases
        collection.insert_many(
            [
                {"name": "Alice", "hobbies": []},  # Empty array
                {"name": "Bob", "hobbies": ["single"]},  # Single element
                {"name": "Charlie"},  # Missing field
                {"name": "David", "hobbies": ["a", "b", "c"]},  # Normal case
            ]
        )

        # Test unwind on documents with various array states
        pipeline = [{"$unwind": "$hobbies"}]
        result = collection.aggregate(pipeline)

        # Should only get results for documents with non-empty arrays
        assert len(result) == 4  # 1 from Bob + 3 from David

        hobbies = [doc["hobbies"] for doc in result]
        expected_hobbies = ["single", "a", "b", "c"]
        assert sorted(hobbies) == sorted(expected_hobbies)

        # Users with empty arrays or missing fields should not appear in results


if __name__ == "__main__":
    pytest.main([__file__])
