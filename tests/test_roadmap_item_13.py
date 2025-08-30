# coding: utf-8
"""
Test for the exact use case from roadmap item #13.
"""

import neosqlite


def test_roadmap_item_13_exact():
    """Test the exact use case from roadmap item #13."""
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    collection = conn.articles

    # Insert documents with comments
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "comments": [
                "Great performance on this product",
                "Good design but slow performance",
                "Overall satisfied with performance",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "comments": [
                "Excellent performance work",
                "Some performance issues in testing",
                "Performance could be better",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "author": "Charlie",
            "comments": [
                "Average quality product",
                "Standard features available",
                "Basic implementation done",
            ],
        }
    )

    # Create FTS index on comments
    collection.create_index("comments", fts=True)

    # Test the exact roadmap use case: unwind comments, search for "performance", group by author
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$group": {"_id": "$author", "commentCount": {"$sum": 1}}},
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results:")
    for result in results:
        print(f"  {result}")

    assert len(results) == 2  # Alice and Bob have comments with "performance"

    # Verify counts
    result_dict = {doc["_id"]: doc["commentCount"] for doc in results}
    assert result_dict["Alice"] == 3  # 3 comments with "performance"
    assert result_dict["Bob"] == 3  # 3 comments with "performance"


if __name__ == "__main__":
    test_roadmap_item_13_exact()
    print("Test passed!")
