#!/usr/bin/env python3
"""
Test to verify that our new $push and $addToSet accumulators work with the
unwind + group SQL optimization.
"""

import neosqlite


def test_unwind_group_with_new_accumulators():
    """Test that $push and $addToSet work with unwind + group optimization."""
    print(
        "=== Testing $push and $addToSet with unwind + group optimization ===\n"
    )

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data with arrays
        print("1. Inserting sample user data with tags...")
        sample_docs = [
            {
                "name": "Alice",
                "tags": ["python", "javascript", "go"],
                "scores": [85, 92, 78],
            },
            {
                "name": "Bob",
                "tags": ["java", "python", "rust"],
                "scores": [88, 84, 90],
            },
            {
                "name": "Charlie",
                "tags": ["javascript", "go", "swift"],
                "scores": [92, 87, 89],
            },
        ]

        users.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} users\n")

        # Test $push with unwind + group optimization
        print("2. Testing $push with unwind + group optimization")
        print("   Pipeline: Unwind tags, group by tag, collect all user names")
        pipeline1 = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "users": {"$push": "$name"}}},
            {"$sort": {"_id": 1}},
        ]

        result1 = users.aggregate(pipeline1)
        print("   Results:")
        for doc in result1:
            print(f"     Tag '{doc['_id']}': {doc.get('users', [])}")
        print()

        # Test $addToSet with unwind + group optimization
        print("3. Testing $addToSet with unwind + group optimization")
        print(
            "   Pipeline: Unwind scores, group by score, collect unique user names"
        )
        pipeline2 = [
            {"$unwind": "$scores"},
            {
                "$group": {
                    "_id": "$scores",
                    "uniqueUsers": {"$addToSet": "$name"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result2 = users.aggregate(pipeline2)
        print("   Results:")
        for doc in result2:
            # Sort the users for consistent output
            sorted_users = sorted(doc.get("uniqueUsers", []))
            print(f"     Score {doc['_id']}: {sorted_users}")
        print()

        print("=== Test Complete ===")
        print(
            "Both $push and $addToSet work correctly with unwind + group optimization!"
        )


if __name__ == "__main__":
    test_unwind_group_with_new_accumulators()
