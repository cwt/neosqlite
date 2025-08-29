# coding: utf-8
"""
Test to verify that the Python fallback produces the same results as the SQL optimized path
"""
import neosqlite
import copy


def test_unwind_with_advanced_options_fallback():
    """Test that $unwind with advanced options (fallback) produces consistent results"""
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
                {
                    "_id": 3,
                    "name": "Charlie",
                    "hobbies": None,  # Null value
                },
                {
                    "_id": 4,
                    "name": "David",
                    # No hobbies field
                },
            ]
        )

        # Pipeline with advanced unwind options (forces Python fallback)
        pipeline_with_advanced = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "includeArrayIndex": "hobbyIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]

        # Pipeline with simple unwind (can use SQL optimization)
        pipeline_simple = [{"$unwind": "$hobbies"}]

        # Get results from both approaches
        result_advanced = collection.aggregate(pipeline_with_advanced)
        result_simple = collection.aggregate(pipeline_simple)

        # The simple pipeline should have fewer results since it doesn't preserve null/empty
        assert len(result_simple) == 3  # Only Alice's hobbies
        assert (
            len(result_advanced) == 5
        )  # Alice's hobbies + Bob and Charlie preserved

        # Check simple results
        simple_hobbies = {doc["hobbies"] for doc in result_simple}
        assert simple_hobbies == {"reading", "swimming", "coding"}

        # Check advanced results
        alice_advanced = [
            doc for doc in result_advanced if doc["name"] == "Alice"
        ]
        assert len(alice_advanced) == 3
        alice_hobbies = {doc["hobbies"] for doc in alice_advanced}
        assert alice_hobbies == {"reading", "swimming", "coding"}

        # Check that indices are present for Alice's documents
        alice_indices = {doc["hobbyIndex"] for doc in alice_advanced}
        assert alice_indices == {0, 1, 2}

        # Check preserved documents
        bob_advanced = [doc for doc in result_advanced if doc["name"] == "Bob"]
        charlie_advanced = [
            doc for doc in result_advanced if doc["name"] == "Charlie"
        ]

        assert len(bob_advanced) == 1
        assert len(charlie_advanced) == 1

        # Bob should have hobbies as None and index as None
        assert bob_advanced[0].get("hobbies") is None
        assert bob_advanced[0]["hobbyIndex"] is None

        # Charlie should have hobbies as None and index as None
        assert charlie_advanced[0].get("hobbies") is None
        assert charlie_advanced[0]["hobbyIndex"] is None


def test_lookup_with_subsequent_stages_fallback():
    """Test that $lookup with subsequent stages uses fallback and produces correct results"""
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
        pipeline_optimized = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            }
        ]

        # Pipeline with $lookup followed by other stages (forces fallback)
        pipeline_fallback = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            },
            {"$match": {"name": "Alice"}},  # This forces fallback
        ]

        # Get results from both approaches
        result_optimized = users.aggregate(pipeline_optimized)
        result_fallback = users.aggregate(pipeline_fallback)

        # Optimized result should have all users with their orders
        assert len(result_optimized) == 2
        alice_optimized = [
            doc for doc in result_optimized if doc["name"] == "Alice"
        ][0]
        bob_optimized = [
            doc for doc in result_optimized if doc["name"] == "Bob"
        ][0]
        assert len(alice_optimized["userOrders"]) == 2
        assert len(bob_optimized["userOrders"]) == 1

        # Fallback result should only have Alice (due to $match)
        assert len(result_fallback) == 1
        assert result_fallback[0]["name"] == "Alice"
        assert len(result_fallback[0]["userOrders"]) == 2


def test_group_push_addtoset_consistency():
    """Test that $group with $push and $addToSet produces consistent results"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "name": "Item1",
                    "tags": ["red", "blue", "red"],
                },
                {"category": "A", "name": "Item2", "tags": ["blue", "green"]},
                {"category": "B", "name": "Item3", "tags": ["red", "yellow"]},
            ]
        )

        # Pipeline that can be optimized ($unwind + $group with $push/$addToSet)
        pipeline_optimized = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$category",
                    "allTags": {"$push": "$tags"},  # Preserves duplicates
                    "uniqueTags": {"$addToSet": "$tags"},  # Removes duplicates
                }
            },
        ]

        # Get results from optimized approach
        result_optimized = collection.aggregate(pipeline_optimized)

        # Verify optimized results
        assert len(result_optimized) == 2
        result_optimized.sort(key=lambda x: x["_id"])

        # Check category A
        assert result_optimized[0]["_id"] == "A"
        # allTags should have duplicates: red, blue, red, blue, green
        assert len(result_optimized[0]["allTags"]) == 5
        assert sorted(result_optimized[0]["allTags"]) == [
            "blue",
            "blue",
            "green",
            "red",
            "red",
        ]
        # uniqueTags should have no duplicates: red, blue, green
        assert len(result_optimized[0]["uniqueTags"]) == 3
        assert sorted(result_optimized[0]["uniqueTags"]) == [
            "blue",
            "green",
            "red",
        ]

        # Check category B
        assert result_optimized[1]["_id"] == "B"
        # allTags should have: red, yellow
        assert len(result_optimized[1]["allTags"]) == 2
        assert sorted(result_optimized[1]["allTags"]) == ["red", "yellow"]
        # uniqueTags should have: red, yellow
        assert len(result_optimized[1]["uniqueTags"]) == 2
        assert sorted(result_optimized[1]["uniqueTags"]) == ["red", "yellow"]


def test_verify_fallback_paths():
    """Test that we can verify fallback paths are being used by checking internal behavior"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"]},
                {"name": "Bob", "tags": ["java", "python"]},
            ]
        )

        # Test a pipeline that forces fallback due to advanced unwind options
        pipeline_fallback = [
            {
                "$unwind": {
                    "path": "$tags",
                    "includeArrayIndex": "tagIndex",  # This forces fallback
                }
            }
        ]

        # Test a pipeline that can be optimized
        pipeline_optimized = [{"$unwind": "$tags"}]

        # Both should produce the same core results (ignoring the extra fields)
        result_fallback = collection.aggregate(pipeline_fallback)
        result_optimized = collection.aggregate(pipeline_optimized)

        # Extract just the core data for comparison
        fallback_core = [(doc["name"], doc["tags"]) for doc in result_fallback]
        optimized_core = [
            (doc["name"], doc["tags"]) for doc in result_optimized
        ]

        # Should have same core data
        assert sorted(fallback_core) == sorted(optimized_core)

        # But fallback should have the extra index field
        assert all("tagIndex" in doc for doc in result_fallback)
        assert all("tagIndex" not in doc for doc in result_optimized)


if __name__ == "__main__":
    test_unwind_with_advanced_options_fallback()
    test_lookup_with_subsequent_stages_fallback()
    test_group_push_addtoset_consistency()
    test_verify_fallback_paths()
    print("All fallback consistency tests passed!")
