# coding: utf-8
"""
Test cases for the force fallback kill switch functionality
"""
import neosqlite
import time


def test_force_fallback_flag():
    """Test that the force fallback flag can be set and retrieved"""
    # Initially should be False
    assert neosqlite.collection.query_helper.get_force_fallback() is False

    # Set to True
    neosqlite.collection.query_helper.set_force_fallback(True)
    assert neosqlite.collection.query_helper.get_force_fallback() is True

    # Set back to False
    neosqlite.collection.query_helper.set_force_fallback(False)
    assert neosqlite.collection.query_helper.get_force_fallback() is False


def test_force_fallback_with_unwind_only():
    """Test force fallback with simple unwind operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"]},
                {"name": "Bob", "tags": ["java", "python"]},
            ]
        )

        # Test normal operation (should use SQL optimization)
        neosqlite.collection.query_helper.set_force_fallback(False)
        pipeline = [{"$unwind": "$tags"}]

        result_normal = collection.aggregate(pipeline)

        # Test with forced fallback
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = collection.aggregate(pipeline)

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)

        # Extract tags for easier comparison
        normal_tags = sorted([doc["tags"] for doc in result_normal])
        fallback_tags = sorted([doc["tags"] for doc in result_fallback])

        assert normal_tags == fallback_tags
        assert normal_tags == ["java", "javascript", "python", "python"]


def test_force_fallback_with_match():
    """Test force fallback with match operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25, "city": "New York"},
                {"name": "Bob", "age": 30, "city": "London"},
                {"name": "Charlie", "age": 35, "city": "New York"},
            ]
        )

        pipeline = [{"$match": {"city": "New York"}}]

        # Test normal operation
        neosqlite.collection.query_helper.set_force_fallback(False)
        result_normal = collection.aggregate(pipeline)

        # Test with forced fallback
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = collection.aggregate(pipeline)

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)
        assert len(result_normal) == 2

        # Extract names for easier comparison
        normal_names = sorted([doc["name"] for doc in result_normal])
        fallback_names = sorted([doc["name"] for doc in result_fallback])

        assert normal_names == fallback_names
        assert normal_names == ["Alice", "Charlie"]


def test_force_fallback_with_advanced_unwind():
    """Test force fallback with advanced unwind features"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "hobbies": ["reading", "swimming", "coding"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "hobbies": [],  # Empty array
                },
            ]
        )

        # Pipeline with advanced unwind options (normally forces fallback)
        pipeline_advanced = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "includeArrayIndex": "hobbyIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]

        # Pipeline with simple unwind (can be optimized)
        pipeline_simple = [{"$unwind": "$hobbies"}]

        # Test with force fallback disabled - advanced should still use fallback
        neosqlite.collection.query_helper.set_force_fallback(False)
        result_advanced = collection.aggregate(pipeline_advanced)
        result_simple = collection.aggregate(pipeline_simple)

        # Test with force fallback enabled - both should use fallback
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_advanced_forced = collection.aggregate(pipeline_advanced)
        result_simple_forced = collection.aggregate(pipeline_simple)

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Results should be consistent
        assert len(result_advanced) == len(result_advanced_forced)
        assert len(result_simple) == len(result_simple_forced)

        # Advanced should have more results due to preserveNullAndEmptyArrays
        assert len(result_advanced) > len(result_simple)


def test_force_fallback_with_lookup():
    """Test force fallback with lookup operations"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        users = conn["users"]
        orders = conn["orders"]

        # Insert test data
        users.insert_many(
            [
                {"_id": 1, "name": "Alice"},
                {"_id": 2, "name": "Bob"},
            ]
        )

        orders.insert_many(
            [
                {"userId": 1, "product": "Book"},
                {"userId": 1, "product": "Pen"},
                {"userId": 2, "product": "Notebook"},
            ]
        )

        # Pipeline with $lookup (can be optimized when it's the last stage)
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            }
        ]

        # Test normal operation
        neosqlite.collection.query_helper.set_force_fallback(False)
        result_normal = users.aggregate(pipeline)

        # Test with forced fallback
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = users.aggregate(pipeline)

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)

        # Check that both have the same user orders
        result_normal.sort(key=lambda x: x["_id"])
        result_fallback.sort(key=lambda x: x["_id"])

        for normal_doc, fallback_doc in zip(result_normal, result_fallback):
            assert normal_doc["_id"] == fallback_doc["_id"]
            assert normal_doc["name"] == fallback_doc["name"]
            assert len(normal_doc["userOrders"]) == len(
                fallback_doc["userOrders"]
            )


def test_benchmark_simple_operations():
    """Benchmark test comparing optimized vs fallback performance for simple operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert larger test data set for meaningful benchmarking
        test_data = [
            {"category": f"Cat{i % 5}", "value": i} for i in range(1000)
        ]
        collection.insert_many(test_data)

        pipeline = [{"$match": {"category": "Cat0"}}]

        # Test optimized path
        neosqlite.collection.query_helper.set_force_fallback(False)
        start_time = time.time()
        result_optimized = collection.aggregate(pipeline)
        optimized_time = time.time() - start_time

        # Test fallback path
        neosqlite.collection.query_helper.set_force_fallback(True)
        start_time = time.time()
        result_fallback = collection.aggregate(pipeline)
        fallback_time = time.time() - start_time

        # Reset flag
        neosqlite.collection.query_helper.set_force_fallback(False)

        # Results should be identical
        assert len(result_optimized) == len(result_fallback)

        # Print benchmark results (for informational purposes)
        print(f"Optimized time: {optimized_time:.6f}s")
        print(f"Fallback time: {fallback_time:.6f}s")
        if optimized_time > 0:
            print(f"Performance ratio: {fallback_time/optimized_time:.2f}x")


if __name__ == "__main__":
    test_force_fallback_flag()
    test_force_fallback_with_unwind_only()
    test_force_fallback_with_match()
    test_force_fallback_with_advanced_unwind()
    test_force_fallback_with_lookup()
    test_benchmark_simple_operations()
    print("All force fallback tests passed!")
