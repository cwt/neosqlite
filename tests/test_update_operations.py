# coding: utf-8
import neosqlite


def test_update_many_mul():
    """Test update_many with $mul operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "price": 100},
            {"name": "Bob", "price": 200},
            {"name": "Charlie", "price": 150},
        ]
    )

    # Update using $mul
    result = collection.update_many({"name": "Alice"}, {"$mul": {"price": 1.1}})

    # Verify the update worked
    alice = collection.find_one({"name": "Alice"})
    assert alice["price"] == 110  # 100 * 1.1
    assert result.matched_count == 1
    assert result.modified_count == 1


def test_update_many_min():
    """Test update_many with $min operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "score": 80},
            {"name": "Bob", "score": 90},
            {"name": "Charlie", "score": 70},
        ]
    )

    # Update using $min to set a maximum score of 85
    result = collection.update_many({"name": "Alice"}, {"$min": {"score": 85}})

    # Verify the update worked (80 < 85, so no change)
    alice = collection.find_one({"name": "Alice"})
    assert alice["score"] == 80
    assert result.matched_count == 1
    assert result.modified_count == 1

    # Try with a lower value
    result = collection.update_many({"name": "Bob"}, {"$min": {"score": 85}})

    # Verify the update worked (90 > 85, so score becomes 85)
    bob = collection.find_one({"name": "Bob"})
    assert bob["score"] == 85


def test_update_many_max():
    """Test update_many with $max operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "score": 80},
            {"name": "Bob", "score": 90},
            {"name": "Charlie", "score": 70},
        ]
    )

    # Update using $max to set a minimum score of 85
    result = collection.update_many({"name": "Alice"}, {"$max": {"score": 85}})

    # Verify the update worked (80 < 85, so score becomes 85)
    alice = collection.find_one({"name": "Alice"})
    assert alice["score"] == 85
    assert result.matched_count == 1
    assert result.modified_count == 1

    # Try with a higher value
    result = collection.update_many({"name": "Bob"}, {"$max": {"score": 85}})

    # Verify the update worked (90 > 85, so no change)
    bob = collection.find_one({"name": "Bob"})
    assert bob["score"] == 90


def test_update_many_unset():
    """Test update_many with $unset operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "age": 30, "temp": "value"},
            {"name": "Bob", "age": 25, "temp": "value"},
        ]
    )

    # Update using $unset to remove the temp field
    result = collection.update_many({"name": "Alice"}, {"$unset": {"temp": ""}})

    # Verify the update worked
    alice = collection.find_one({"name": "Alice"})
    assert "temp" not in alice
    assert "age" in alice
    assert alice["age"] == 30
    assert result.matched_count == 1
    assert result.modified_count == 1


def test_update_many_combined():
    """Test update_many with combined operators."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Insert some documents
    collection.insert_many(
        [
            {"name": "Alice", "price": 100, "score": 80},
            {"name": "Bob", "price": 200, "score": 90},
        ]
    )

    # Update using multiple operators
    result = collection.update_many(
        {"name": "Alice"}, {"$mul": {"price": 1.1}, "$max": {"score": 85}}
    )

    # Verify the update worked
    alice = collection.find_one({"name": "Alice"})
    assert alice["price"] == 110  # 100 * 1.1
    assert alice["score"] == 85  # max(80, 85)
    assert result.matched_count == 1
    assert result.modified_count == 1


def test_upsert_min_max_non_existent_field():
    """Test $min and $max operators on non-existent fields during an upsert."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Upsert with $min on a non-existent document
    collection.update_one(
        {"name": "Frank"}, {"$min": {"score": 50}}, upsert=True
    )
    frank = collection.find_one({"name": "Frank"})
    assert frank is not None
    assert "score" in frank
    assert frank["score"] == 50

    # Upsert with $max on a non-existent document
    collection.update_one(
        {"name": "Grace"}, {"$max": {"score": 60}}, upsert=True
    )
    grace = collection.find_one({"name": "Grace"})
    assert grace is not None
    assert "score" in grace
    assert grace["score"] == 60
