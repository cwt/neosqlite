# coding: utf-8
"""
Test for the new $unwind implementation with json_each()
"""
import neosqlite
import pytest


def test_unwind_with_json_each_integration():
    """Test the new $unwind implementation using json_each()"""
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

        # Test $unwind as the first stage
        pipeline = [{"$unwind": "$hobbies"}]
        result = collection.aggregate(pipeline)

        # Should have 6 documents (2+3+1 hobbies)
        assert len(result) == 6

        # Check that each document has the unwound field
        hobbies = [doc["hobbies"] for doc in result]
        expected_hobbies = [
            "reading",
            "swimming",
            "gaming",
            "cooking",
            "hiking",
            "painting",
        ]
        assert sorted(hobbies) == sorted(expected_hobbies)

        # Check that original fields are preserved
        names = [doc["name"] for doc in result]
        # Alice appears 2 times, Bob 3 times, Charlie 1 time
        assert names.count("Alice") == 2
        assert names.count("Bob") == 3
        assert names.count("Charlie") == 1


def test_unwind_with_match_integration():
    """Test $unwind combined with $match"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "status": "active",
                    "hobbies": ["reading", "swimming"],
                },
                {
                    "name": "Bob",
                    "status": "inactive",
                    "hobbies": ["gaming", "cooking"],
                },
                {
                    "name": "Charlie",
                    "status": "active",
                    "hobbies": ["painting"],
                },
            ]
        )

        # Test $unwind after $match
        pipeline = [{"$match": {"status": "active"}}, {"$unwind": "$hobbies"}]
        result = collection.aggregate(pipeline)

        # Should have 3 documents (2+1 hobbies from active users)
        assert len(result) == 3

        # Check that only active users are in the results
        statuses = [doc["status"] for doc in result]
        assert all(status == "active" for status in statuses)

        # Check the unwound hobbies
        hobbies = [doc["hobbies"] for doc in result]
        expected_hobbies = ["reading", "swimming", "painting"]
        assert sorted(hobbies) == sorted(expected_hobbies)


if __name__ == "__main__":
    pytest.main([__file__])
