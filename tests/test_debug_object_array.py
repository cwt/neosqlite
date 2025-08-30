# coding: utf-8
"""
Debug test for object array text search.
"""

import neosqlite


def test_debug_object_array():
    """Debug object array text search."""
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
                    "content": "How to optimize Python code",
                },
                {
                    "title": "Database Design",
                    "content": "Best practices for database design",
                },
                {
                    "title": "Web Development",
                    "content": "Modern web development techniques",
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "posts": [
                {
                    "title": "JavaScript Performance",
                    "content": "Optimizing JavaScript applications",
                },
                {
                    "title": "Mobile Development",
                    "content": "Building mobile apps with React Native",
                },
                {
                    "title": "Backend Systems",
                    "content": "Designing scalable backend systems",
                },
            ],
        }
    )

    # Create FTS index on nested content field
    collection.create_index("posts.content", fts=True)

    # Check what FTS tables exist
    cursor = collection.db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%fts%'"
    )
    fts_tables = cursor.fetchall()
    print("FTS tables:", fts_tables)

    # Check the FTS table content
    for table in fts_tables:
        table_name = table[0]
        print(f"Content of {table_name}:")
        try:
            cursor = collection.db.execute(
                f"SELECT rowid, posts_content FROM {table_name}"
            )
            rows = cursor.fetchall()
            for row in rows:
                print("  ", row)
        except Exception as e:
            print(f"  Error querying table: {e}")
            # Try to see the table structure
            try:
                cursor = collection.db.execute(
                    f"PRAGMA table_info({table_name})"
                )
                rows = cursor.fetchall()
                print("  Table structure:", rows)
            except Exception as e2:
                print(f"  Error getting table info: {e2}")

    # Try a simple text search
    results = list(collection.find({"$text": {"$search": "performance"}}))
    print("Direct text search results:", len(results))
    for result in results:
        print("  ", result)

    # Try the aggregation
    pipeline = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    print("Aggregation results:", len(results))
    for result in results:
        print("  ", result)


if __name__ == "__main__":
    test_debug_object_array()
