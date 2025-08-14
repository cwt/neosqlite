# coding: utf-8
"""
Tests for the $text operator with logical operators ($and, $or, $not, $nor)
"""
import pytest
import neosqlite


def test_text_with_and_operator(collection):
    """Test $text operator with $and operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Python Web Development",
                "content": "Python is great for web development",
                "category": "web",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $and
    results = list(
        collection.find(
            {"$and": [{"$text": {"$search": "python"}}, {"category": "web"}]}
        )
    )

    assert len(results) == 1
    assert results[0]["title"] == "Python Web Development"


def test_text_with_or_operator(collection):
    """Test $text operator with $or operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
                "category": "ai",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $or
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles


def test_text_with_not_operator(collection):
    """Test $text operator with $not operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $not
    results = list(collection.find({"$not": {"$text": {"$search": "web"}}}))

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Machine Learning" in titles


def test_text_with_nor_operator(collection):
    """Test $text operator with $nor operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $nor
    results = list(
        collection.find(
            {
                "$nor": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )

    assert len(results) == 1
    assert results[0]["title"] == "Machine Learning"


def test_complex_nested_logical_operators_with_text(collection):
    """Test complex nested logical operators with $text"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
                "category": "ai",
            },
            {
                "title": "Web Development",
                "content": "HTML, CSS, and JavaScript for web development",
                "category": "web",
            },
            {
                "title": "Data Science",
                "content": "Python is widely used in data science",
                "category": "data",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test complex nested logical operators
    # Find documents where:
    # 1. Content contains "web" OR category is "ai"
    # 2. AND title contains "JavaScript" OR title contains "Machine"
    results = list(
        collection.find(
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
                            {"title": {"$regex": "JavaScript"}},
                            {"title": {"$regex": "Machine"}},
                        ]
                    },
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "JavaScript Basics" in titles
    assert "Machine Learning" in titles


def test_text_with_mixed_conditions(collection):
    """Test $text operator with mixed conditions in logical operators"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "version": 3.9,
            },
            {
                "title": "JavaScript Guide",
                "content": "JavaScript is used for web development",
                "version": 2021,
            },
            {
                "title": "Python Tips",
                "content": "Python tips and tricks for developers",
                "version": 3.8,
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with mixed conditions
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {
                        "$and": [
                            {"$text": {"$search": "javascript"}},
                            {"version": {"$gte": 2020}},
                        ]
                    },
                ]
            }
        )
    )

    # Should find 3 documents:
    # 1. "Python Programming" (matches $text: "python")
    # 2. "Python Tips" (matches $text: "python")
    # 3. "JavaScript Guide" (matches both $text: "javascript" and version >= 2020)
    assert len(results) == 3
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Python Tips" in titles
    assert "JavaScript Guide" in titles


def test_text_logical_operators_case_insensitive(collection):
    """Test that $text with logical operators is case insensitive"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a HIGH-LEVEL programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for WEB development",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test case insensitive search with $or
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "HIGH"}},  # Uppercase
                    {"$text": {"$search": "web"}},  # Lowercase
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles


def test_text_with_logical_operators_no_matches(collection):
    """Test $text with logical operators when there are no matches"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test with no matches
    results = list(
        collection.find(
            {
                "$and": [
                    {"$text": {"$search": "python"}},
                    {
                        "category": "web"
                    },  # This won't match any Python documents
                ]
            }
        )
    )

    assert len(results) == 0


def test_text_with_logical_operators_fallback(collection):
    """Test $text with logical operators when FTS is not available (fallback to Python)"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
        ]
    )

    # Don't create FTS index to test Python fallback

    # Test with Python fallback
    results = list(
        collection.find(
            {"$or": [{"$text": {"$search": "python"}}, {"category": "web"}]}
        )
    )

    # Should find both documents - one through text search fallback, one through category match
    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles
