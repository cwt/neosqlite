import neosqlite
import pytest


def test_quez_memory_constrained_processing():
    """Test quez memory-constrained processing functionality."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        test_docs = [
            {"name": f"User {i}", "age": 20 + i, "department": f"Dept{i % 3}"}
            for i in range(100)
        ]
        collection.insert_many(test_docs)

        # Test normal aggregation
        pipeline = [{"$match": {"age": {"$gte": 25}}}, {"$limit": 10}]
        cursor = collection.aggregate(pipeline)

        # Verify cursor works normally
        results = list(cursor)
        assert len(results) == 10
        assert all(doc["age"] >= 25 for doc in results)

        # Test quez-enabled aggregation (if quez is available)
        try:
            from quez import CompressedQueue
            
            cursor2 = collection.aggregate(pipeline)
            cursor2.use_quez(True)  # Enable quez processing

            # Verify quez processing works
            results2 = list(cursor2)
            assert len(results2) == 10
            assert all(doc["age"] >= 25 for doc in results2)
            
            # Results should be the same
            assert results == results2
            
        except ImportError:
            # quez not available, skip test
            pytest.skip("quez library not available")


def test_quez_cursor_methods():
    """Test that quez-enabled cursor supports required methods."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        collection.insert_many([{"value": i} for i in range(10)])

        try:
            from quez import CompressedQueue
            
            cursor = collection.aggregate([{"$limit": 5}])
            cursor.use_quez(True)
            
            # Test that we can iterate
            results = list(cursor)
            assert len(results) == 5
            
            # Test that we can call to_list again (should be empty since we consumed all)
            results2 = cursor.to_list()
            assert len(results2) == 0
            
        except ImportError:
            pytest.skip("quez library not available")


if __name__ == "__main__":
    test_quez_memory_constrained_processing()
    test_quez_cursor_methods()
    print("All quez integration tests passed!")