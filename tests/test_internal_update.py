# coding: utf-8
import neosqlite


def test_internal_update_sql_path():
    """Test _internal_update with SQL-based path."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "age": 30, "score": 100}
    ).inserted_id

    # Update using SQL-based path (simple operations)
    original_doc = collection.find_one({"_id": doc_id})
    result = collection._internal_update(
        doc_id,
        {"$set": {"name": "Bob"}, "$inc": {"age": 5}, "$mul": {"score": 1.1}},
        original_doc,
    )

    # Verify the update worked
    assert result["name"] == "Bob"
    assert result["age"] == 35
    assert result["score"] == 110  # 100 * 1.1


def test_internal_update_python_path():
    """Test _internal_update with Python-based path for complex operations."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "items": [1, 2, 3]}
    ).inserted_id

    # Update using Python-based path (complex operations)
    original_doc = collection.find_one({"_id": doc_id})
    result = collection._internal_update(
        doc_id, {"$push": {"items": 4}, "$set": {"name": "Bob"}}, original_doc
    )

    # Verify the update worked
    assert result["name"] == "Bob"
    assert result["items"] == [1, 2, 3, 4]


def test_internal_update_mixed_operations():
    """Test _internal_update with mixed operations (falls back to Python)."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert a document
    doc_id = collection.insert_one(
        {"name": "Alice", "age": 30, "items": [1, 2, 3]}
    ).inserted_id

    # Update with mixed operations (should fall back to Python)
    original_doc = collection.find_one({"_id": doc_id})
    result = collection._internal_update(
        doc_id,
        {
            "$set": {"name": "Bob"},
            "$inc": {"age": 5},
            "$push": {"items": 4},  # This requires Python fallback
        },
        original_doc,
    )

    # Verify the update worked
    assert result["name"] == "Bob"
    assert result["age"] == 35
    assert result["items"] == [1, 2, 3, 4]
