# coding: utf-8
import neosqlite


def test_list_indexes_as_keys():
    """Test list_indexes with as_keys=True parameter."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create some indexes
    collection.create_index("foo")
    collection.create_index("bar.baz")  # Nested key

    # Get indexes as keys
    indexes_as_keys = collection.list_indexes(as_keys=True)

    # Check that we get the expected keys
    assert ["foo"] in indexes_as_keys
    assert ["bar.baz"] in indexes_as_keys


def test_drop_indexes():
    """Test drop_indexes method."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create some indexes
    collection.create_index("foo")
    collection.create_index("bar")

    # Verify indexes exist
    indexes = collection.list_indexes()
    assert len(indexes) == 2
    assert "idx_test_foo" in indexes
    assert "idx_test_bar" in indexes

    # Drop all indexes
    collection.drop_indexes()

    # Verify indexes are gone
    indexes = collection.list_indexes()
    assert len(indexes) == 0


def test_drop_compound_index():
    """Test dropping compound indexes."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Create a compound index
    collection.create_index(["foo", "bar"])

    # Verify index exists
    indexes = collection.list_indexes()
    assert "idx_test_foo_bar" in indexes

    # Drop the compound index
    collection.drop_index(["foo", "bar"])

    # Verify index is gone
    indexes = collection.list_indexes()
    assert "idx_test_foo_bar" not in indexes


def test_object_exists():
    """Test _object_exists method."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test that the collection table exists
    assert collection._object_exists("table", "test")

    # Test that a non-existent table doesn't exist
    assert not collection._object_exists("table", "nonexistent")

    # Create an index and check that it exists
    collection.create_index("foo")
    assert collection._object_exists("index", "idx_test_foo")

    # Check that a non-existent index doesn't exist
    assert not collection._object_exists("index", "idx_test_nonexistent")

    # Test the default case (should return False)
    assert not collection._object_exists("unknown_type", "name")
