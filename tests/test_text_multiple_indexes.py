# coding: utf-8
"""
Tests for text search with multiple FTS indexes
"""
import neosqlite


def test_text_search_multiple_indexes_current_behavior(collection, capsys):
    """Test current behavior with multiple FTS indexes - this should demonstrate the limitation"""
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

    # With current implementation, this might only find one document
    # because it only searches the first FTS table it finds
    print(f"Found {len(results)} documents")
    for doc in results:
        print(f"  - {doc['title']}: {doc['subtitle']}")

    # Capture the output
    captured = capsys.readouterr()
    print(captured.out)


def test_text_search_multiple_indexes_expected_behavior(collection):
    """Test expected behavior with multiple FTS indexes - should find all matching documents"""
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

    # Verify the subtitles also contain "Python"
    subtitles = [doc["subtitle"] for doc in results]
    for subtitle in subtitles:
        assert "Python" in subtitle


if __name__ == "__main__":
    # Run the tests manually for debugging
    import tempfile
    import os

    # Create a temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Create connection and collection
        conn = neosqlite.Connection(db_path)
        collection = conn.test_collection

        print("Testing current behavior:")
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

        # With current implementation, this might only find one document
        # because it only searches the first FTS table it finds
        print(f"Found {len(results)} documents")
        for doc in results:
            print(f"  - {doc['title']}: {doc['subtitle']}")

    finally:
        # Clean up
        conn.close()
        os.unlink(db_path)
