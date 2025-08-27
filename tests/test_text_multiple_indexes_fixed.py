# coding: utf-8
"""
Tests for text search with multiple FTS indexes - verifying the fix
"""
import pytest


def test_text_search_multiple_indexes_fixed_behavior(collection):
    """Test that text search now works across multiple FTS indexes"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should find documents 1 and 3 (both contain "Python")
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 2 documents that contain "Python" in either title or subtitle
    assert len(results) == 2

    # Get the titles to verify we found the right documents
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Database Fundamentals" in titles


def test_text_search_multiple_indexes_with_nested_fields(collection):
    """Test text search with multiple FTS indexes including nested fields"""
    # Insert documents with nested fields
    collection.insert_one(
        {
            "title": "Python Guide",
            "metadata": {
                "description": "Learn Python programming",
                "category": "programming",
            },
        }
    )
    collection.insert_one(
        {
            "title": "JavaScript Basics",
            "metadata": {
                "description": "Web development tutorial",
                "category": "web",
            },
        }
    )
    collection.insert_one(
        {
            "title": "Advanced Python",
            "metadata": {
                "description": "Advanced Python techniques",
                "category": "programming",
            },
        }
    )

    # Create FTS indexes on title and nested field
    collection.create_index("title", fts=True)
    collection.create_index("metadata.description", fts=True)

    # Search for "Python" - should find documents 1 and 3
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 2 documents that contain "Python"
    assert len(results) == 2

    # Get the titles to verify we found the right documents
    titles = [doc["title"] for doc in results]
    assert "Python Guide" in titles
    assert "Advanced Python" in titles


def test_text_search_single_index_still_works(collection):
    """Test that text search still works with only one FTS index"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )

    # Create FTS index on only one field
    collection.create_index("title", fts=True)

    # Search for "Python" - should find document 1
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 1 document that contains "Python" in title
    assert len(results) == 1
    assert results[0]["title"] == "Python Programming"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
