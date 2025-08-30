# coding: utf-8
"""
Simple test for unwind + text search without projections.
"""

import neosqlite


def test_simple_unwind_text():
    """Test simple unwind + text search without projections."""
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    collection = conn.test_collection

    # Insert documents with string arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "comments": [
                "Great performance",
                "Good design",
                "Needs improvement",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Bob",
            "comments": ["Excellent work", "Performance issues", "Well done"],
        }
    )

    # Test case: Unwind and search for "performance" (case insensitive)
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results:")
    for result in results:
        print(f"  {result}")

    assert (
        len(results) == 2
    )  # Should find "Great performance" and "Performance issues"

    # Verify the content
    comments = [doc["comments"] for doc in results]
    assert "Great performance" in comments
    assert "Performance issues" in comments


if __name__ == "__main__":
    test_simple_unwind_text()
    print("Test passed!")
