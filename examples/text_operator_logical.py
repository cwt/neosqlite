#!/usr/bin/env python3
"""
Example demonstrating the $text operator with logical operators in neosqlite.

This example shows how to use the $text operator with logical operators like $and, $or, $not, and $nor
for complex text search queries.
"""

import neosqlite


def main():
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    articles = conn.articles

    # Insert sample documents
    articles.insert_many(
        [
            {
                "title": "Python Programming Guide",
                "content": "Python is a high-level programming language that is widely used for web development, data analysis, and machine learning.",
                "category": "programming",
                "tags": ["python", "beginner", "tutorial"],
            },
            {
                "title": "JavaScript Fundamentals",
                "content": "JavaScript is a versatile programming language that runs in web browsers and can also be used for server-side development with Node.js.",
                "category": "web",
                "tags": ["javascript", "web", "nodejs"],
            },
            {
                "title": "Introduction to Machine Learning",
                "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms that can learn from data and make predictions.",
                "category": "ai",
                "tags": ["ml", "ai", "data"],
            },
            {
                "title": "Web Development with HTML and CSS",
                "content": "HTML and CSS are the foundational technologies for building web pages and web applications.",
                "category": "web",
                "tags": ["html", "css", "web"],
            },
            {
                "title": "Database Design Principles",
                "content": "Good database design is crucial for efficient data storage and retrieval in applications.",
                "category": "database",
                "tags": ["database", "design", "sql"],
            },
        ]
    )

    # Create FTS index on content field for efficient text search
    articles.create_index("content", fts=True)
    print("✓ Created FTS index on 'content' field")

    print("\n" + "=" * 60)
    print("DEMONSTRATION: $text operator with logical operators")
    print("=" * 60)

    # Example 1: $text with $and operator
    print("\n1. $text with $and operator")
    print("   Find articles about 'python' AND in 'programming' category:")
    results = list(
        articles.find(
            {
                "$and": [
                    {"$text": {"$search": "python"}},
                    {"category": "programming"},
                ]
            }
        )
    )
    for doc in results:
        print(f"   • {doc['title']}")

    # Example 2: $text with $or operator
    print("\n2. $text with $or operator")
    print("   Find articles about 'python' OR 'javascript':")
    results = list(
        articles.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )
    for doc in results:
        print(f"   • {doc['title']}")

    # Example 3: $text with $not operator
    print("\n3. $text with $not operator")
    print("   Find articles that do NOT contain 'web':")
    results = list(articles.find({"$not": {"$text": {"$search": "web"}}}))
    for doc in results:
        print(f"   • {doc['title']}")

    # Example 4: $text with $nor operator
    print("\n4. $text with $nor operator")
    print("   Find articles that do NOT contain 'python' NOR 'javascript':")
    results = list(
        articles.find(
            {
                "$nor": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )
    for doc in results:
        print(f"   • {doc['title']}")

    # Example 5: Complex nested logical operators
    print("\n5. Complex nested logical operators")
    print("   Find articles where:")
    print("   - Content contains 'web' OR category is 'ai'")
    print("   - AND tags contain 'web' or 'ai':")
    results = list(
        articles.find(
            {
                "$and": [
                    {
                        "$or": [
                            {"$text": {"$search": "web"}},
                            {"category": "ai"},
                        ]
                    },
                    {
                        "$or": [
                            {"tags": {"$in": ["web"]}},
                            {"tags": {"$in": ["ai"]}},
                        ]
                    },
                ]
            }
        )
    )
    for doc in results:
        print(f"   • {doc['title']} (category: {doc['category']})")

    # Example 6: Mixed conditions with $text and other operators
    print("\n6. Mixed conditions with $text and other field operators")
    print("   Find articles where:")
    print("   - Content contains 'python' OR")
    print("   - Content contains 'javascript' AND version >= 2020:")
    # Note: This example shows the concept, though we don't have a version field
    # In a real scenario, you would have such fields
    results = list(
        articles.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {
                        "$and": [
                            {"$text": {"$search": "javascript"}},
                            {"category": "web"},
                        ]
                    },
                ]
            }
        )
    )
    for doc in results:
        print(f"   • {doc['title']}")

    print("\n" + "=" * 60)
    print("HOW IT WORKS:")
    print("=" * 60)
    print(
        "• The $text operator leverages SQLite's FTS5 for efficient text search"
    )
    print(
        "• When combined with logical operators, neosqlite uses a hybrid approach:"
    )
    print(
        "  - Individual conditions are processed efficiently (SQL for FTS, Python for others)"
    )
    print("  - Results are combined using set operations in Python")
    print(
        "• This provides both performance and flexibility for complex queries"
    )
    print("• Case-insensitive search is supported automatically")

    print("\n" + "=" * 60)
    print("BENEFITS:")
    print("=" * 60)
    print("✓ Efficient text search using FTS5 when indexes exist")
    print("✓ Flexible combination with any logical operators")
    print("✓ Fallback to Python processing when needed")
    print("✓ Full compatibility with PyMongo API")
    print("✓ Automatic case-insensitive matching")

    print("\nDemonstration completed successfully!")


if __name__ == "__main__":
    main()
