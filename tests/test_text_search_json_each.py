# coding: utf-8
"""
Tests for text search integration with json_each() enhancement.

This test module verifies the implementation of text search operations
that work with array unwinding operations, as specified in roadmap item #13.
"""

import pytest
import warnings


def test_text_search_on_unwound_simple_strings(collection):
    """Test text search on simple string array elements after $unwind."""
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

    collection.insert_one(
        {
            "_id": 3,
            "name": "Charlie",
            "comments": [
                "Average quality",
                "Standard features",
                "Basic implementation",
            ],
        }
    )

    # Test case 1: Unwind and search for "performance" (case insensitive)
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert (
        len(results) == 2
    )  # Should find "Great performance" and "Performance issues"

    # Verify the content
    comments = [doc["comments"] for doc in results]
    assert "Great performance" in comments
    assert "Performance issues" in comments

    # Verify the parent document info is preserved
    names = [doc["name"] for doc in results]
    assert "Alice" in names
    assert "Bob" in names


@pytest.mark.xfail(
    reason="Object array with FTS index not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_objects_basic(collection):
    """Test text search on object arrays without projections (basic case)."""
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

    # Test: Unwind posts and search for "performance" in content (without projection)
    pipeline = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Should find posts with "performance" in content

    # Verify the content contains the search term
    for doc in results:
        # The unwound post should be in the 'posts' field
        assert "posts" in doc
        post_content = doc["posts"]["content"]
        assert "performance" in post_content.lower()


def test_text_search_on_unwound_with_grouping(collection):
    """Test the full use case from the roadmap: unwind + text search + group."""
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

    # Test the roadmap use case: unwind comments, search for "performance", group by author
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$group": {"_id": "$author", "commentCount": {"$sum": 1}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Alice and Bob have comments with "performance"

    # Verify counts
    result_dict = {doc["_id"]: doc["commentCount"] for doc in results}
    assert result_dict["Alice"] == 3  # 3 comments with "performance"
    assert result_dict["Bob"] == 3  # 3 comments with "performance"


def test_text_search_on_unwound_nested_arrays_basic(collection):
    """Test text search on nested array structures (basic case without projection)."""
    # Insert documents with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Tech Reviewer",
            "reviews": [
                {
                    "product": "Laptop",
                    "comments": [
                        "Excellent performance for the price",
                        "Battery life could be better",
                        "Good design and build quality",
                    ],
                },
                {
                    "product": "Phone",
                    "comments": [
                        "Amazing camera performance",
                        "Smooth performance overall",
                        "Average battery performance",
                    ],
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Gadget Expert",
            "reviews": [
                {
                    "product": "Tablet",
                    "comments": [
                        "Decent performance for basic tasks",
                        "Poor performance under load",
                        "Battery performance is acceptable",
                    ],
                }
            ],
        }
    )

    # Create FTS index on nested comments
    collection.create_index("reviews.comments", fts=True)

    # Test: Unwind reviews, then unwind comments, then search for "performance" (without projection)
    pipeline = [
        {"$unwind": "$reviews"},
        {"$unwind": "$reviews.comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) >= 5  # Should find multiple comments with "performance"

    # Verify some content is found
    for doc in results:
        assert "reviews" in doc
        assert "comments" in doc["reviews"]
        assert "performance" in doc["reviews"]["comments"].lower()


def test_text_search_on_unwound_with_sort_and_limit(collection):
    """Test text search on unwound arrays with sorting and limiting."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "category": "Tech",
            "tags": ["performance", "python", "database", "web", "mobile"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "category": "Science",
            "tags": [
                "research",
                "data",
                "analysis",
                "performance",
                "statistics",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "category": "Art",
            "tags": [
                "design",
                "creativity",
                "performance",
                "visual",
                "digital",
            ],
        }
    )

    # Create FTS index on tags
    collection.create_index("tags", fts=True)

    # Test: Unwind tags, search for "performance", sort by category, limit to 2
    pipeline = [
        {"$unwind": "$tags"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$sort": {"category": 1}},
        {"$limit": 2},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2

    # Results should be sorted by category
    categories = [doc["category"] for doc in results]
    assert categories == sorted(categories)


def test_text_search_on_unwound_no_matches(collection):
    """Test text search on unwound arrays when no matches are found."""
    # Insert documents
    collection.insert_one(
        {"_id": 1, "name": "Alice", "items": ["apple", "banana", "cherry"]}
    )

    collection.insert_one(
        {"_id": 2, "name": "Bob", "items": ["dog", "elephant", "frog"]}
    )

    # Create FTS index
    collection.create_index("items", fts=True)

    # Test: Search for non-existent term
    pipeline = [
        {"$unwind": "$items"},
        {"$match": {"$text": {"$search": "nonexistent"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 0


def test_text_search_on_unwound_python_fallback(collection):
    """Test text search on unwound arrays with Python fallback when no FTS index exists."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "reviews": [
                "Great performance product",
                "Good quality item",
                "Fast delivery service",
            ],
        }
    )

    # No FTS index created - should use Python fallback

    # Test: Unwind and search (should work with Python fallback)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        pipeline = [
            {"$unwind": "$reviews"},
            {"$match": {"$text": {"$search": "performance"}}},
        ]

        results = list(collection.aggregate(pipeline))
        # Should find the review with "performance"
        assert len(results) == 1
        assert "performance" in results[0]["reviews"].lower()


def test_text_search_on_unwound_mixed_with_regular_match(collection):
    """Test text search on unwound arrays mixed with regular field matching."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "status": "active",
            "comments": ["Great performance", "Good features", "Nice design"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "status": "inactive",
            "comments": ["Performance issues", "Good support", "Fast updates"],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "author": "Charlie",
            "status": "active",
            "comments": [
                "Average quality",
                "Standard features",
                "Basic design",
            ],
        }
    )

    # Create FTS index
    collection.create_index("comments", fts=True)

    # Test: Unwind comments, search for "performance", and filter by status
    pipeline = [
        {"$unwind": "$comments"},
        {
            "$match": {
                "$and": [
                    {"$text": {"$search": "performance"}},
                    {"status": "active"},
                ]
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    # Should only find Alice's comments since Bob is inactive
    assert len(results) == 1
    assert results[0]["author"] == "Alice"
    assert "performance" in results[0]["comments"].lower()


# Tests that currently fall back to Python implementation due to projection complexity
# These are documented limitations of the current implementation


@pytest.mark.xfail(
    reason="Projection support not yet implemented - falls back to Python. Hybrid approach possible as future enhancement."
)
def test_text_search_on_unwound_objects_with_fts(collection):
    """Test text search on object arrays with FTS index and projection."""
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

    # Test: Unwind posts and search for "performance" in content
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

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Should find posts with "performance" in content

    # Verify the content
    contents = [doc["content"] for doc in results]
    assert "How to optimize Python code" in contents
    assert "Optimizing JavaScript applications" in contents


@pytest.mark.xfail(
    reason="Nested array projection support not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_nested_arrays(collection):
    """Test text search on nested array structures with projection."""
    # Insert documents with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Tech Reviewer",
            "reviews": [
                {
                    "product": "Laptop",
                    "comments": [
                        "Excellent performance for the price",
                        "Battery life could be better",
                        "Good design and build quality",
                    ],
                },
                {
                    "product": "Phone",
                    "comments": [
                        "Amazing camera performance",
                        "Smooth performance overall",
                        "Average battery performance",
                    ],
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Gadget Expert",
            "reviews": [
                {
                    "product": "Tablet",
                    "comments": [
                        "Decent performance for basic tasks",
                        "Poor performance under load",
                        "Battery performance is acceptable",
                    ],
                }
            ],
        }
    )

    # Create FTS index on nested comments
    collection.create_index("reviews.comments", fts=True)

    # Test: Unwind reviews, then unwind comments, then search for "performance"
    pipeline = [
        {"$unwind": "$reviews"},
        {"$unwind": "$reviews.comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {
            "$project": {
                "reviewer": "$name",
                "product": "$reviews.product",
                "comment": "$reviews.comments",
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) >= 5  # Should find multiple comments with "performance"

    # Verify some content is found
    comments = [doc["comment"] for doc in results]
    assert any("performance" in comment.lower() for comment in comments)


@pytest.mark.xfail(
    reason="Multiple term search not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_with_multiple_matches(collection):
    """Test text search that matches multiple terms within unwound elements."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Developer",
            "snippets": [
                "High performance code with clean design",
                "Database performance optimization techniques",
                "Web application performance tuning",
                "Mobile performance best practices",
            ],
        }
    )

    # Create FTS index
    collection.create_index("snippets", fts=True)

    # Test: Search for multiple terms
    pipeline = [
        {"$unwind": "$snippets"},
        {"$match": {"$text": {"$search": "performance database"}}},
    ]

    results = list(collection.aggregate(pipeline))
    # Should find snippets containing both terms
    assert len(results) >= 1

    # Verify content contains both terms
    snippets = [doc["snippets"] for doc in results]
    found_performance = False
    found_database = False
    for snippet in snippets:
        if "performance" in snippet.lower():
            found_performance = True
        if "database" in snippet.lower():
            found_database = True
    assert found_performance and found_database


@pytest.mark.xfail(
    reason="FTS index integration with unwound elements not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_case_insensitive(collection):
    """Test that text search on unwound arrays is case insensitive."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "notes": ["PERFORMANCE Review", "Quality Check", "Design Analysis"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "notes": ["performance testing", "Code Review", "System Analysis"],
        }
    )

    # Create FTS index
    collection.create_index("notes", fts=True)

    # Test: Search with different case
    pipeline = [
        {"$unwind": "$notes"},
        {"$match": {"$text": {"$search": "PERFORMANCE"}}},  # Uppercase search
    ]

    results = list(collection.aggregate(pipeline))
    assert (
        len(results) == 2
    )  # Should find both "PERFORMANCE Review" and "performance testing"

    # Verify content
    notes = [doc["notes"] for doc in results]
    assert "PERFORMANCE Review" in notes
    assert "performance testing" in notes


if __name__ == "__main__":
    pytest.main([__file__])
