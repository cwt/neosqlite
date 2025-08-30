# coding: utf-8
"""
Demonstrate hybrid approach: SQLite preprocessing + Python postprocessing.
"""

import neosqlite


def demonstrate_hybrid_approach():
    """Show how we can use SQLite to preprocess data for Python."""
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    collection = conn.articles

    # Insert sample documents with object arrays
    collection.insert_many(
        [
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
                        "content": "Best practices for database design and performance",
                    },
                ],
            },
            {
                "_id": 2,
                "author": "Bob",
                "posts": [
                    {
                        "title": "JavaScript Performance",
                        "content": "Optimizing JavaScript applications performance",
                    },
                    {
                        "title": "Mobile Development",
                        "content": "Building mobile apps with React Native",
                    },
                ],
            },
        ]
    )

    # Create FTS index
    collection.create_index("posts.content", fts=True)

    print("=== Current Approach (Full Python Fallback) ===")
    # This would currently fall back to Python entirely
    pipeline = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
        {
            "$project": {
                "author": 1,
                "title": "$posts.title",
                "content": "$posts.content",
            }
        },
    ]

    # Let's see what a hybrid approach could look like

    print("\n=== Proposed Hybrid Approach ===")
    print("1. SQLite does the unwinding and basic field extraction:")

    # What SQLite could return to make Python processing easier
    hybrid_sql = """
    SELECT tc.id,
           json_extract(tc.data, '$.author') as author,
           json_extract(je.value, '$.title') as post_title,
           json_extract(je.value, '$.content') as post_content
    FROM articles tc,
         json_each(json_extract(tc.data, '$.posts')) as je
    WHERE lower(json_extract(je.value, '$.content')) LIKE '%performance%'
    """

    cursor = collection.db.execute(hybrid_sql)
    results = cursor.fetchall()
    print(f"   SQLite returns {len(results)} simplified rows:")
    for row in results:
        print(
            f"     ID: {row[0]}, Author: {row[1]}, Title: {row[2]}, Content: {row[3]}"
        )

    print("\n2. Python only needs to:")
    print("   - Apply projection (select which fields to include)")
    print("   - Format results as documents")
    print("   - Handle any complex logic")

    # Simulate what Python would do with this preprocessed data
    processed_results = []
    for row in results:
        # Apply projection logic in Python (much simpler!)
        projected_doc = {
            "_id": row[0],
            "author": row[1],
            "title": row[2],  # from $posts.title
            "content": row[3],  # from $posts.content
        }
        processed_results.append(projected_doc)

    print(f"\n3. Final results ({len(processed_results)} documents):")
    for doc in processed_results:
        print(f"     {doc}")

    print("\n=== Benefits of Hybrid Approach ===")
    print("✓ SQLite handles expensive operations (unwinding, filtering)")
    print("✓ Python only handles lightweight post-processing")
    print("✓ Much faster than pure Python array unwinding")
    print("✓ Maintains full PyMongo API compatibility")
    print("✓ Can be implemented as an enhancement to current system")


def demonstrate_current_vs_hybrid():
    """Compare current fallback vs proposed hybrid approach."""
    print("\n=== Performance Comparison ===")

    print("Current Fallback (Pure Python):")
    print("  1. Python loads entire document")
    print("  2. Python unwinds arrays manually")
    print("  3. Python searches through all elements")
    print("  4. Python applies all pipeline stages")
    print("  ⏱️  Slow for large arrays/documents")

    print("\nProposed Hybrid Approach:")
    print("  1. SQLite unwinds arrays using json_each()")
    print("  2. SQLite applies basic filtering/text search")
    print("  3. SQLite extracts only needed fields")
    print("  4. Python applies projection and complex logic")
    print("  ⚡ Much faster - SQLite does the heavy lifting")


if __name__ == "__main__":
    demonstrate_hybrid_approach()
    demonstrate_current_vs_hybrid()
    print("\nHybrid approach demonstration completed!")
