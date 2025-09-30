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

    # Valid integer values (within range 0 to 0xFFFFFFFF)
    assert ObjectId.is_valid(
        123
    )  # According to MongoDB spec, integers are allowed as timestamps
    assert ObjectId.is_valid(0)
    assert ObjectId.is_valid(0xFFFFFFFF)

    # Invalid values
    assert not ObjectId.is_valid(-1)  # Negative integer
    assert not ObjectId.is_valid(
        0x100000000
    )  # Too large integer (> 0xFFFFFFFF)
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


def test_objectid_index_usage():
    """Test that the _id index speeds up searches by ObjectId."""
    from neosqlite import Connection

    # Use in-memory database
    db = Connection(":memory:")
    collection = db["test_collection"]

    # Insert multiple documents to make index performance meaningful
    oids = []
    for i in range(
        100
    ):  # Insert more documents to better test index performance
        oid = ObjectId()
        oids.append(oid)
        collection.insert_one({"_id": oid, "name": f"doc_{i}", "value": i * 10})

    # Test that searching by ObjectId works correctly
    test_oid = oids[50]  # Pick one of the inserted ObjectIds

    # Verify that the lookup works correctly
    retrieved = collection.find_one({"_id": test_oid})
    assert retrieved is not None
    assert retrieved["_id"] == test_oid
    assert retrieved["name"] == "doc_50"

    # Test with hex string as well
    retrieved_by_hex = collection.find_one({"_id": str(test_oid)})
    assert retrieved_by_hex is not None
    assert retrieved_by_hex["_id"] == test_oid

    # Verify the index exists in the database
    index_cursor = collection.db.execute(
        f"SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{collection.name}'"
    )
    index_names = [row[0] for row in index_cursor.fetchall()]
    expected_index_name = f"idx_{collection.name}_id"

    # Check if our expected index exists
    assert (
        expected_index_name in index_names
    ), f"The _id index {expected_index_name} should exist in the database"

    # Check the query plan to verify the index is actually being used
    explain_cursor = collection.db.execute(
        f"EXPLAIN QUERY PLAN SELECT id, _id, data FROM {collection.name} WHERE _id = ?",
        (str(test_oid),),
    )
    query_plan_rows = explain_cursor.fetchall()

    # Check that the query plan includes the index name, indicating it's being used
    query_plan_str = " ".join([str(row) for row in query_plan_rows]).lower()

    # The plan should indicate that the index was used (different SQLite versions may use different terms)
    # Common indicators: 'search', 'idx_', the specific index name, or 'using index'
    index_used = any(
        indicator in query_plan_str
        for indicator in [
            expected_index_name.lower(),
            "search",
            "eqp",
            "using index",
            "idx_",
        ]
    )

    assert (
        index_used
    ), f"The query plan should indicate that the index was used: {query_plan_str}"

    # Verify that searching with a non-existent ObjectId returns None
    fake_oid = ObjectId()
    not_found = collection.find_one({"_id": fake_oid})
    assert not_found is None

    # Test that multiple rapid searches by ObjectId work efficiently
    for i in [10, 25, 75, 99]:
        oid = oids[i]
        retrieved_doc = collection.find_one({"_id": oid})
        assert retrieved_doc is not None
        assert retrieved_doc["_id"] == oid
        assert retrieved_doc["name"] == f"doc_{i}"
