# coding: utf-8
"""
Debug the hybrid approach.
"""

import neosqlite


def debug_hybrid_approach():
    """Debug the hybrid approach."""
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    collection = conn.test_collection

    # Insert documents with object arrays
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "posts": [
                {
                    "title": "Python Performance Tips",
                    "content": "How to optimize Python code for better performance",
                },
                {
                    "title": "Database Design",
                    "content": "Best practices for database design",
                },
            ],
        }
    )

    # Don't create FTS index for now
    # collection.create_index("posts.content", fts=True)

    # Test with a simple search term that's definitely in the data
    pipeline = [
        {"$unwind": "$posts"},
        {
            "$match": {"$text": {"$search": "Python"}}
        },  # Search for "Python" which is in the title
        {
            "$project": {
                "author": 1,
                "title": "$posts.title",
                "content": "$posts.content",
            }
        },
    ]

    print("Pipeline:", pipeline)

    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results:")
    for i, result in enumerate(results):
        print(f"  Result {i}: {result}")

    # Let's also test without projection to see what we get
    print("\n--- Without projection ---")
    pipeline_no_project = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results_no_project = list(collection.aggregate(pipeline_no_project))
    print(f"Found {len(results_no_project)} results without projection:")
    for i, result in enumerate(results_no_project):
        print(f"  Result {i}: {result}")


if __name__ == "__main__":
    debug_hybrid_approach()
