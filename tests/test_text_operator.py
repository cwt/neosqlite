# coding: utf-8
"""
Tests for the $text operator and FTS functionality
"""
import pytest
import warnings
import neosqlite


def test_create_fts_index_and_text_search(collection):
    """Test creating FTS index and performing text search"""
    # Insert documents with text content
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})
    collection.insert_one(
        {"title": "Python programming", "content": "is fun and powerful"}
    )

    # Create FTS index on content field
    collection.create_index("content", fts=True)

    # Perform text search using $text operator
    results = list(collection.find({"$text": {"$search": "fun"}}))
    assert len(results) == 1
    assert results[0]["content"] == "is fun and powerful"

    # Search for another term
    results = list(collection.find({"$text": {"$search": "dolor"}}))
    assert len(results) == 1
    assert results[0]["content"] == "dolor sit amet"


def test_text_operator_python_fallback(collection):
    """Test $text operator with Python fallback when no FTS index exists"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search without FTS index should fall back to Python implementation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "jumps"}}))
        # Should find the document with simple substring matching
        assert len(results) == 1
        assert results[0]["content"] == "jumps over the lazy dog"


def test_text_operator_multiple_words_python_fallback(collection):
    """Test $text operator with multiple words in Python fallback"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search for multiple words without FTS index
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "quick brown"}}))
        # Should find the document with simple substring matching
        assert len(results) == 1
        assert results[0]["title"] == "The quick brown fox"


def test_text_operator_with_multiple_words_and_fts(collection):
    """Test $text operator with multiple words and FTS index"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})
    collection.insert_one(
        {"title": "Python tutorial", "content": "learn programming with Python"}
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for multiple words with FTS
    results = list(collection.find({"$text": {"$search": "lazy dog"}}))
    assert len(results) == 1
    assert results[0]["content"] == "jumps over the lazy dog"


def test_text_operator_no_matches_with_fts(collection):
    """Test $text operator with no matches when FTS index exists"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for term that doesn't exist
    results = list(collection.find({"$text": {"$search": "nonexistent"}}))
    assert len(results) == 0


def test_text_operator_no_matches_python_fallback(collection):
    """Test $text operator with no matches in Python fallback"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search for term that doesn't exist without FTS index
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "nonexistent"}}))
        assert len(results) == 0


def test_text_search_with_nested_fields(collection):
    """Test text search with nested fields"""
    # Insert documents with nested fields
    collection.insert_one(
        {
            "title": "Document 1",
            "metadata": {
                "description": "This is a sample document for testing"
            },
        }
    )
    collection.insert_one(
        {
            "title": "Document 2",
            "metadata": {
                "description": "Another document with different content"
            },
        }
    )

    # Create FTS index on nested field
    collection.create_index("metadata.description", fts=True)

    # Search in nested field
    results = list(collection.find({"$text": {"$search": "sample"}}))
    assert len(results) == 1
    assert results[0]["title"] == "Document 1"


def test_text_search_case_insensitive(collection):
    """Test that text search is case insensitive"""
    # Insert documents
    collection.insert_one({"content": "PYTHON Programming"})
    collection.insert_one({"content": "java programming"})

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search with different case
    results = list(collection.find({"$text": {"$search": "python"}}))
    assert len(results) == 1
    assert results[0]["content"] == "PYTHON Programming"

    # Search with different case
    results = list(collection.find({"$text": {"$search": "PROGRAMMING"}}))
    assert len(results) == 2  # Both documents should match


def test_invalid_text_query_formats(collection):
    """Test handling of invalid $text query formats"""
    # Insert a document
    collection.insert_one({"content": "test content"})

    # Test invalid $text format - missing $search
    results = list(collection.find({"$text": {"$invalid": "test"}}))
    # Should fall back to Python and find no matches
    assert len(results) == 0

    # Test invalid $text format - non-string search term
    results = list(collection.find({"$text": {"$search": 123}}))
    # Should fall back to Python and find no matches
    assert len(results) == 0


def test_text_search_with_fts_triggers(collection):
    """Test that FTS indexes are updated via triggers when documents are modified"""
    # Insert documents
    result = collection.insert_one({"content": "original text"})
    doc_id = result.inserted_id

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for original text
    results = list(collection.find({"$text": {"$search": "original"}}))
    assert len(results) == 1

    # Update the document
    collection.update_one(
        {"_id": doc_id}, {"$set": {"content": "updated text"}}
    )

    # Search for updated text
    results = list(collection.find({"$text": {"$search": "updated"}}))
    assert len(results) == 1
    assert results[0]["_id"] == doc_id

    # Search for original text (should not find anything)
    results = list(collection.find({"$text": {"$search": "original"}}))
    assert len(results) == 0

    # Delete the document
    collection.delete_one({"_id": doc_id})

    # Search for updated text (should not find anything)
    results = list(collection.find({"$text": {"$search": "updated"}}))
    assert len(results) == 0
