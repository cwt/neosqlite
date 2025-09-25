"""
Test suite for ObjectId functionality in NeoSQLite.
"""

from neosqlite.objectid import ObjectId
import time


def test_objectid_creation():
    """Test creating ObjectId instances."""
    # Test creating a new ObjectId
    oid1 = ObjectId()
    assert isinstance(oid1, ObjectId)
    assert len(oid1.hex) == 24
    assert len(oid1.binary) == 12

    # Test creating ObjectId from hex string
    hex_str = oid1.hex
    oid2 = ObjectId(hex_str)
    assert oid2 == oid1
    assert oid2.hex == hex_str

    # Test creating ObjectId from bytes
    binary_data = oid1.binary
    oid3 = ObjectId(binary_data)
    assert oid3 == oid1
    assert oid3.binary == binary_data

    # Test creating ObjectId from another ObjectId
    oid4 = ObjectId(oid1)
    assert oid4 == oid1


def test_objectid_equality():
    """Test ObjectId equality comparisons."""
    oid1 = ObjectId()
    oid2 = ObjectId(oid1)
    oid3 = ObjectId()

    assert oid1 == oid2
    assert oid1 != oid3
    assert oid1 == oid1.hex
    assert oid1 == oid1.binary


def test_objectid_string_representations():
    """Test string representations of ObjectId."""
    oid = ObjectId()
    assert str(oid) == oid.hex
    assert repr(oid).startswith("ObjectId(")


def test_objectid_timestamp():
    """Test timestamp extraction from ObjectId."""
    before = int(time.time())
    oid = ObjectId()
    after = int(time.time())

    timestamp = oid.generation_time()
    assert before <= timestamp <= after


def test_objectid_uniqueness():
    """Test that ObjectIds are unique."""
    ids = set()
    for _ in range(100):
        ids.add(ObjectId())

    assert len(ids) == 100


def test_objectid_validation():
    """Test ObjectId validation."""
    # Valid hex string
    assert ObjectId.is_valid("507f1f77bcf86cd799439011")

    # Invalid hex string (wrong length)
    assert not ObjectId.is_valid("507f1f77bcf86cd79943901")  # 23 chars
    assert not ObjectId.is_valid("507f1f77bcf86cd7994390111")  # 25 chars

    # Valid bytes
    assert ObjectId.is_valid(b"\x00" * 12)

    # Invalid bytes (wrong length)
    assert not ObjectId.is_valid(b"\x00" * 11)
    assert not ObjectId.is_valid(b"\x00" * 13)

    # Valid ObjectId instance
    oid = ObjectId()
    assert ObjectId.is_valid(oid)

    # Invalid values
    assert not ObjectId.is_valid(123)
    assert not ObjectId.is_valid("invalid_hex")
    assert not ObjectId.is_valid([])


def test_objectid_json_serialization():
    """Test ObjectId JSON serialization."""
    oid = ObjectId()
    encoded = oid.encode_for_storage()

    assert encoded["__neosqlite_objectid__"] is True
    assert encoded["id"] == oid.hex

    # Test decoding
    decoded = ObjectId.decode_from_storage(encoded)
    assert decoded == oid
    assert decoded.hex == oid.hex


def test_objectid_storage_integration():
    """Test ObjectId storage and retrieval in a NeoSQLite collection."""
    from neosqlite import Connection
    import tempfile
    import os

    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
        tmp_path = tmp.name

    try:
        # Connect to the database
        db = Connection(tmp_path)
        collection = db["test_collection"]

        # Test inserting a document with an ObjectId _id
        oid = ObjectId()
        result = collection.insert_one(
            {"_id": oid, "name": "test_doc", "value": 42}
        )
        # The inserted_id should be the auto-increment integer ID, not the ObjectId
        assert isinstance(result.inserted_id, int)

        # Retrieve the document by ObjectId
        retrieved = collection.find_one({"_id": oid})
        assert retrieved is not None
        assert retrieved["_id"] == oid
        assert retrieved["name"] == "test_doc"
        assert retrieved["value"] == 42

        # Retrieve the document by the integer ID returned from insert
        retrieved_by_int = collection.find_one({"_id": result.inserted_id})
        assert retrieved_by_int is not None
        assert retrieved_by_int["_id"] == oid
        assert retrieved_by_int["name"] == "test_doc"
        assert retrieved_by_int["value"] == 42

        # Test inserting a document without _id (should generate ObjectId)
        result2 = collection.insert_one({"name": "auto_id_doc", "value": 123})
        assert result2.inserted_id is not None
        assert isinstance(result2.inserted_id, int)  # Auto-increment ID

        retrieved2 = collection.find_one({"_id": result2.inserted_id})
        assert retrieved2 is not None
        assert isinstance(retrieved2["_id"], ObjectId)  # Generated ObjectId
        assert retrieved2["name"] == "auto_id_doc"
        assert retrieved2["value"] == 123

        # Test querying by _id - query by integer ID should return document with ObjectId _id
        retrieved3 = collection.find_one({"_id": result2.inserted_id})
        assert retrieved3 is not None
        # The document should have an ObjectId in the _id field, not the integer ID
        assert isinstance(retrieved3["_id"], ObjectId)
        # But the ObjectId in the document should match what was generated for this record
        # We can check that querying by that ObjectId also works
        retrieved_by_oid = collection.find_one({"_id": retrieved3["_id"]})
        assert retrieved_by_oid is not None
        assert (
            retrieved_by_oid["_id"] == retrieved3["_id"]
        )  # Both should be the same ObjectId

        # Test querying with hex string of a manually inserted ObjectId
        oid_manual = ObjectId()
        collection.insert_one(
            {"_id": oid_manual, "name": "manual_oid", "value": 999}
        )
        retrieved_manual = collection.find_one({"_id": str(oid_manual)})
        assert retrieved_manual is not None
        assert retrieved_manual["_id"] == oid_manual

        # Test find with _id filter
        cursor = collection.find({"_id": oid})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["_id"] == oid

    finally:
        # Clean up the temporary database file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_objectid_backward_compatibility():
    """Test that ObjectId works with existing collections without _id column."""
    from neosqlite import Connection
    import tempfile
    import os

    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
        tmp_path = tmp.name

    try:
        # Connect to the database
        db = Connection(tmp_path)
        collection = db["test_collection"]

        # Insert some documents the old way (without _id column initially)
        doc1 = {"name": "doc1", "value": 100}
        result1 = collection.insert_one(doc1)
        old_style_id = result1.inserted_id

        # Verify that it works with integer IDs for backward compatibility
        retrieved_by_id = collection.find_one({"_id": old_style_id})
        assert retrieved_by_id is not None
        # When a document is inserted without specifying _id, the _id field will be an ObjectId
        # but when we query by the integer id, it should still find the document
        # For backward compatibility, when no _id column exists initially, the integer ID
        # should be used as the _id until the table is updated
        assert retrieved_by_id["name"] == "doc1"

        # Now insert with ObjectId
        oid = ObjectId()
        result2 = collection.insert_one(
            {"_id": oid, "name": "doc2", "value": 200}
        )
        assert isinstance(result2.inserted_id, int)  # Should be integer ID

        # Test that both work in the same collection
        retrieved_old = collection.find_one({"_id": old_style_id})
        assert retrieved_old is not None
        # For documents inserted without explicit _id, the _id field will be auto-generated ObjectId
        # but the integer ID in the 'id' column should still work for retrieval
        assert retrieved_old["name"] == "doc1"

        retrieved_new = collection.find_one({"_id": oid})
        assert retrieved_new is not None
        assert retrieved_new["_id"] == oid

    finally:
        # Clean up the temporary database file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
