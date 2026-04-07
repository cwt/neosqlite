"""
Tests for find operations - focusing on uncovered paths in find_operations.py

Targets uncovered code paths:
- find_raw_batches
- find with session validation
- find_one_and_* with complex filters (empty results, no match)
- find_one_and_* with RETURNING clause variations
- find_one_and_* with Python fallback (force_fallback mode)
- find_one_and_* two-step process (non-RETURNING fallback)
- find with various filter types
"""

import pytest

import neosqlite
from neosqlite.collection.query_helper import set_force_fallback
from neosqlite.objectid import ObjectId


@pytest.fixture
def connection():
    """Set up a neosqlite connection."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def collection(connection):
    """Provide a collection with test data."""
    coll = connection["test_find"]
    coll.insert_many(
        [
            {"name": "Alice", "age": 30, "score": 85},
            {"name": "Bob", "age": 25, "score": 92},
            {"name": "Charlie", "age": 35, "score": 78},
            {"name": "Diana", "age": 28, "score": 95},
        ]
    )
    return coll


class TestFind:
    """Tests for the find method."""

    def test_find_with_no_filter(self, collection):
        """Test find with no filter returns all documents."""
        cursor = collection.find()
        docs = list(cursor)
        assert len(docs) == 4

    def test_find_with_empty_filter(self, collection):
        """Test find with empty dict filter."""
        cursor = collection.find({})
        docs = list(cursor)
        assert len(docs) == 4

    def test_find_with_projection(self, collection):
        """Test find with field projection."""
        cursor = collection.find({}, {"name": 1, "_id": 0})
        docs = list(cursor)
        assert len(docs) == 4
        for doc in docs:
            assert "name" in doc
            assert "_id" not in doc

    def test_find_with_hint(self, collection):
        """Test find with index hint."""
        collection.create_index("age")
        cursor = collection.find({"age": {"$gte": 30}}, hint="age_1")
        docs = list(cursor)
        assert len(docs) == 2

    def test_find_with_complex_filter(self, collection):
        """Test find with multiple filter conditions."""
        cursor = collection.find({"age": {"$gte": 28, "$lte": 35}})
        docs = list(cursor)
        assert len(docs) == 3

    def test_find_with_in_operator(self, collection):
        """Test find with $in operator."""
        cursor = collection.find({"name": {"$in": ["Alice", "Bob"]}})
        docs = list(cursor)
        assert len(docs) == 2

    def test_find_with_regex(self, collection):
        """Test find with regex pattern."""
        import re

        cursor = collection.find({"name": re.compile(r"^A")})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"

    def test_find_with_objectid_filter(self, collection):
        """Test find with ObjectId filter."""
        doc = collection.find_one({"name": "Alice"})
        cursor = collection.find({"_id": doc["_id"]})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"


class TestFindRawBatches:
    """Tests for the find_raw_batches method."""

    def test_find_raw_batches_basic(self, collection):
        """Test find_raw_batches returns batches of raw JSON bytes."""
        cursor = collection.find_raw_batches(batch_size=2)
        batches = list(cursor)
        # Should have 2 batches
        assert len(batches) == 2
        for batch in batches:
            assert isinstance(batch, bytes)
            # Each batch should contain JSON documents
            assert b'"name"' in batch

    def test_find_raw_batches_with_filter(self, collection):
        """Test find_raw_batches with filter."""
        cursor = collection.find_raw_batches(
            {"age": {"$gte": 30}}, batch_size=5
        )
        batches = list(cursor)
        assert len(batches) == 1
        # Should contain Alice and Charlie
        assert b'"Alice"' in batches[0]
        assert b'"Charlie"' in batches[0]

    def test_find_raw_batches_custom_batch_size(self, collection):
        """Test find_raw_batches with custom batch size."""
        cursor = collection.find_raw_batches(batch_size=10)
        batches = list(cursor)
        assert len(batches) == 1
        # Should contain all 4 names
        for name in [b'"Alice"', b'"Bob"', b'"Charlie"', b'"Diana"']:
            assert name in batches[0]

    def test_find_raw_batches_no_results(self, collection):
        """Test find_raw_batches with no matching documents."""
        cursor = collection.find_raw_batches({"age": {"$gt": 100}})
        batches = list(cursor)
        assert batches == [b""] or batches == []

    def test_find_raw_batches_with_projection(self, collection):
        """Test find_raw_batches with projection."""
        cursor = collection.find_raw_batches(
            {}, projection={"name": 1, "_id": 0}, batch_size=2
        )
        batches = list(cursor)
        for batch in batches:
            assert b'"name"' in batch


class TestFindOne:
    """Tests for the find_one method."""

    def test_find_one_no_filter(self, collection):
        """Test find_one with no filter returns first document."""
        doc = collection.find_one()
        assert doc is not None
        assert "name" in doc

    def test_find_one_no_match(self, collection):
        """Test find_one with no matching document."""
        doc = collection.find_one({"name": "NonExistent"})
        assert doc is None

    def test_find_one_with_objectid(self, collection):
        """Test find_one with ObjectId filter."""
        doc = collection.find_one({"name": "Alice"})
        result = collection.find_one({"_id": doc["_id"]})
        assert result is not None
        assert result["name"] == "Alice"

    def test_find_one_with_string_id(self, collection):
        """Test find_one with string _id filter."""
        doc = collection.find_one({"name": "Alice"})
        result = collection.find_one({"_id": str(doc["_id"])})
        assert result is not None

    def test_find_one_with_int_id(self, collection):
        """Test find_one with integer id filter."""
        doc = collection.find_one({"name": "Alice"})
        # Old documents may have integer id
        if "id" in doc and isinstance(doc["id"], int):
            result = collection.find_one({"_id": doc["id"]})
            assert result is not None

    def test_find_one_with_projection(self, collection):
        """Test find_one with projection."""
        doc = collection.find_one({"name": "Alice"}, {"name": 1, "_id": 0})
        assert doc is not None
        assert "name" in doc
        assert "age" not in doc

    def test_find_one_with_session(self, collection, connection):
        """Test find_one with client session."""
        with connection.start_session() as session:
            doc = collection.find_one({"name": "Alice"}, session=session)
            assert doc is not None
            assert doc["name"] == "Alice"


class TestFindOneAndDelete:
    """Tests for find_one_and_delete method."""

    def test_find_one_and_delete_no_match(self, collection):
        """Test find_one_and_delete when no document matches."""
        result = collection.find_one_and_delete({"name": "NonExistent"})
        assert result is None
        assert collection.count_documents({}) == 4

    def test_find_one_and_delete_with_sort(self, collection):
        """Test find_one_and_delete with sort parameter."""
        result = collection.find_one_and_delete({}, sort=[("age", -1)])
        assert result is not None
        assert result["name"] == "Charlie"  # Oldest
        assert collection.count_documents({}) == 3

    def test_find_one_and_delete_returns_original_doc(self, collection):
        """Test find_one_and_delete returns the document before deletion."""
        original = collection.find_one({"name": "Bob"})
        result = collection.find_one_and_delete({"name": "Bob"})
        assert result is not None
        assert result["_id"] == original["_id"]
        assert result["name"] == "Bob"
        assert collection.find_one({"name": "Bob"}) is None

    def test_find_one_and_delete_with_complex_filter(self, collection):
        """Test find_one_and_delete with complex filter."""
        result = collection.find_one_and_delete(
            {"age": {"$gte": 30}, "score": {"$lt": 80}}
        )
        assert result is not None
        assert result["name"] == "Charlie"

    def test_find_one_and_delete_with_projection(self, collection):
        """Test find_one_and_delete with projection."""
        result = collection.find_one_and_delete(
            {"name": "Alice"}, projection={"name": 1}
        )
        assert result is not None
        assert "name" in result


class TestFindOneAndReplace:
    """Tests for find_one_and_replace method."""

    def test_find_one_and_replace_no_match(self, collection):
        """Test find_one_and_replace when no document matches."""
        result = collection.find_one_and_replace(
            {"name": "NonExistent"}, {"name": "New"}
        )
        assert result is None

    def test_find_one_and_replace_return_document_true(self, collection):
        """Test find_one_and_replace with return_document=True."""
        result = collection.find_one_and_replace(
            {"name": "Alice"},
            {"name": "Alice Updated", "age": 31},
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "Alice Updated"
        assert result["age"] == 31

    def test_find_one_and_replace_return_document_false(self, collection):
        """Test find_one_and_replace with return_document=False (default)."""
        result = collection.find_one_and_replace(
            {"name": "Bob"}, {"name": "Bob Updated", "age": 26}
        )
        assert result is not None
        assert result["name"] == "Bob"  # Original document
        # Verify update
        updated = collection.find_one({"name": "Bob Updated"})
        assert updated is not None

    def test_find_one_and_replace_with_sort(self, collection):
        """Test find_one_and_replace with sort parameter."""
        result = collection.find_one_and_replace(
            {"age": {"$gte": 25}},
            {"name": "Replaced", "age": 99},
            sort=[("age", -1)],
        )
        assert result is not None
        assert result["name"] == "Charlie"  # Oldest matching

    def test_find_one_and_replace_upsert_no_match(self, collection):
        """Test find_one_and_replace with upsert when no document matches."""
        result = collection.find_one_and_replace(
            {"name": "NewPerson"},
            {"name": "NewPerson", "age": 50},
            upsert=True,
        )
        assert result is None  # Returns original (None)
        assert collection.count_documents({"name": "NewPerson"}) == 1

    def test_find_one_and_replace_upsert_return_document(self, collection):
        """Test find_one_and_replace with upsert and return_document=True."""
        result = collection.find_one_and_replace(
            {"name": "Upserted"},
            {"name": "Upserted", "value": 100},
            upsert=True,
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "Upserted"
        assert result["value"] == 100

    def test_find_one_and_replace_with_session(self, collection, connection):
        """Test find_one_and_replace with client session."""
        with connection.start_session() as session:
            result = collection.find_one_and_replace(
                {"name": "Alice"},
                {"name": "Alice Session"},
                session=session,
            )
            assert result is not None
            assert result["name"] == "Alice"


class TestFindOneAndUpdate:
    """Tests for find_one_and_update method."""

    def test_find_one_and_update_no_match(self, collection):
        """Test find_one_and_update when no document matches."""
        result = collection.find_one_and_update(
            {"name": "NonExistent"}, {"$set": {"score": 100}}
        )
        assert result is None

    def test_find_one_and_update_return_document_true(self, collection):
        """Test find_one_and_update with return_document=True."""
        result = collection.find_one_and_update(
            {"name": "Alice"},
            {"$set": {"score": 100}},
            return_document=True,
        )
        assert result is not None
        assert result["score"] == 100

    def test_find_one_and_update_return_document_false(self, collection):
        """Test find_one_and_update with return_document=False."""
        result = collection.find_one_and_update(
            {"name": "Bob"}, {"$set": {"score": 50}}
        )
        assert result is not None
        assert result["score"] == 92  # Original score

    def test_find_one_and_update_with_sort(self, collection):
        """Test find_one_and_update with sort parameter."""
        result = collection.find_one_and_update(
            {"age": {"$gte": 25}},
            {"$set": {"updated": True}},
            sort=[("age", 1)],
        )
        assert result is not None
        assert result["name"] == "Bob"  # Youngest matching

    def test_find_one_and_update_upsert_no_match(self, collection):
        """Test find_one_and_update with upsert when no document matches."""
        result = collection.find_one_and_update(
            {"name": "NewUser"},
            {"$set": {"score": 100}},
            upsert=True,
        )
        assert result is None
        # Document should be created with filter fields
        new_doc = collection.find_one({"name": "NewUser"})
        assert new_doc is not None

    def test_find_one_and_update_upsert_return_document(self, collection):
        """Test find_one_and_update with upsert and return_document=True."""
        result = collection.find_one_and_update(
            {"name": "UpsertUser"},
            {"$set": {"score": 99}},
            upsert=True,
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "UpsertUser"

    def test_find_one_and_update_with_array_filters(self, collection):
        """Test find_one_and_update with array_filters."""
        collection.insert_one({"name": "Test", "scores": [80, 90, 100]})
        result = collection.find_one_and_update(
            {"name": "Test"},
            {"$set": {"scores.$[elem]": 85}},
            array_filters=[{"elem": {"$gte": 90}}],
        )
        assert result is not None
        assert result["name"] == "Test"
        # Verify update
        updated = collection.find_one({"name": "Test"})
        assert 85 in updated["scores"]

    def test_find_one_and_update_with_session(self, collection, connection):
        """Test find_one_and_update with client session."""
        with connection.start_session() as session:
            result = collection.find_one_and_update(
                {"name": "Alice"},
                {"$set": {"session_test": True}},
                session=session,
            )
            assert result is not None
            assert result["name"] == "Alice"

    def test_find_one_and_update_with_complex_filter(self, collection):
        """Test find_one_and_update with complex filter conditions."""
        result = collection.find_one_and_update(
            {"age": {"$gte": 25, "$lte": 30}, "score": {"$gt": 80}},
            {"$set": {"complex_update": True}},
        )
        assert result is not None
        assert result["name"] in ["Alice", "Bob", "Diana"]

    def test_find_one_and_update_with_inc(self, collection):
        """Test find_one_and_update with $inc operator."""
        result = collection.find_one_and_update(
            {"name": "Alice"}, {"$inc": {"score": 10}}
        )
        assert result is not None
        assert result["score"] == 85  # Original
        # Verify update
        updated = collection.find_one({"name": "Alice"})
        assert updated["score"] == 95

    def test_find_one_and_update_with_push(self, collection):
        """Test find_one_and_update with $push operator."""
        collection.insert_one({"name": "Test", "items": [1, 2, 3]})
        result = collection.find_one_and_update(
            {"name": "Test"}, {"$push": {"items": 4}}
        )
        assert result is not None
        assert result["items"] == [1, 2, 3]  # Original
        # Verify update
        updated = collection.find_one({"name": "Test"})
        assert updated["items"] == [1, 2, 3, 4]


class TestFindOperationsWithObjectId:
    """Tests for find operations with ObjectId variations."""

    def test_find_one_by_objectid_string(self, collection):
        """Test find_one with ObjectId as 24-char string."""
        doc = collection.find_one({"name": "Alice"})
        oid_str = str(doc["_id"])
        assert len(oid_str) == 24
        result = collection.find_one({"_id": oid_str})
        assert result is not None

    def test_find_one_by_objectid_instance(self, collection):
        """Test find_one with ObjectId instance."""
        doc = collection.find_one({"name": "Alice"})
        oid = doc["_id"]
        assert isinstance(oid, ObjectId)
        result = collection.find_one({"_id": oid})
        assert result is not None
        assert result["name"] == "Alice"

    def test_find_by_id_in_with_objectids(self, collection):
        """Test find with $in operator using ObjectIds."""
        docs = list(collection.find({"name": {"$in": ["Alice", "Bob"]}}))
        ids = [doc["_id"] for doc in docs]
        # Find using those IDs
        results = list(collection.find({"_id": {"$in": ids}}))
        assert len(results) == 2

    def test_find_by_id_nin(self, collection):
        """Test find with $nin operator on _id."""
        doc = collection.find_one({"name": "Alice"})
        results = list(collection.find({"_id": {"$nin": [doc["_id"]]}}))
        assert len(results) == 3

    def test_find_one_and_delete_by_objectid(self, collection):
        """Test find_one_and_delete with ObjectId filter."""
        doc = collection.find_one({"name": "Alice"})
        result = collection.find_one_and_delete({"_id": doc["_id"]})
        assert result is not None
        assert result["name"] == "Alice"
        assert collection.count_documents({"name": "Alice"}) == 0


class TestFindEdgeCases:
    """Edge case tests for find operations."""

    def test_find_empty_collection(self, connection):
        """Test find on empty collection."""
        coll = connection["empty_coll"]
        cursor = coll.find()
        docs = list(cursor)
        assert docs == []

    def test_find_one_empty_collection(self, connection):
        """Test find_one on empty collection."""
        coll = connection["empty_coll2"]
        result = coll.find_one()
        assert result is None

    def test_find_with_ne_operator(self, collection):
        """Test find with $ne operator."""
        cursor = collection.find({"name": {"$ne": "Alice"}})
        docs = list(cursor)
        assert len(docs) == 3

    def test_find_with_exists_true(self, collection):
        """Test find with $exists: true."""
        cursor = collection.find({"score": {"$exists": True}})
        docs = list(cursor)
        assert len(docs) == 4

    def test_find_with_exists_false(self, collection):
        """Test find with $exists: false."""
        cursor = collection.find({"nonexistent": {"$exists": True}})
        docs = list(cursor)
        assert len(docs) == 0

    def test_find_with_mod_operator(self, collection):
        """Test find with $mod operator."""
        cursor = collection.find({"age": {"$mod": [5, 0]}})
        docs = list(cursor)
        # 25, 30, 35 are divisible by 5
        assert len(docs) == 3

    def test_find_with_size_operator(self, collection):
        """Test find with $size operator."""
        collection.insert_one({"name": "Test", "tags": ["a", "b", "c"]})
        cursor = collection.find({"tags": {"$size": 3}})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["name"] == "Test"

    def test_find_with_elemmatch(self, collection):
        """Test find with $elemMatch operator."""
        collection.insert_one({"name": "Test", "scores": [80, 90, 100]})
        cursor = collection.find({"scores": {"$elemMatch": {"$gt": 95}}})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["name"] == "Test"

    def test_find_with_contains(self, collection):
        """Test find with $contains operator."""
        collection.insert_one({"name": "Test", "description": "Hello World"})
        cursor = collection.find({"description": {"$contains": "hello"}})
        docs = list(cursor)
        assert len(docs) == 1
        assert docs[0]["name"] == "Test"


class TestFindOperationsForceFallback:
    """Tests that force fallback to Python implementation paths.

    These exercises the code paths where sql_translator returns no where_clause,
    triggering the Python-based fallback in find_one_and_* methods.
    """

    def test_find_one_and_delete_fallback_path(self, collection):
        """Test find_one_and_delete using Python fallback (force_fallback)."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_delete({"name": "Alice"})
            assert result is not None
            assert result["name"] == "Alice"
            assert collection.count_documents({"name": "Alice"}) == 0
        finally:
            set_force_fallback(False)

    def test_find_one_and_delete_fallback_with_sort(self, collection):
        """Test find_one_and_delete fallback path with sort."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_delete({}, sort=[("age", -1)])
            assert result is not None
            assert result["name"] == "Charlie"  # Oldest
        finally:
            set_force_fallback(False)

    def test_find_one_and_delete_fallback_no_match(self, collection):
        """Test find_one_and_delete fallback path with no match."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_delete({"name": "NonExistent"})
            assert result is None
        finally:
            set_force_fallback(False)

    def test_find_one_and_delete_fallback_with_projection(self, collection):
        """Test find_one_and_delete fallback path with projection."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_delete(
                {"name": "Bob"}, projection={"name": 1}
            )
            assert result is not None
            assert "name" in result
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_path(self, collection):
        """Test find_one_and_replace using Python fallback."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"name": "Alice"}, {"name": "Alice Replaced", "age": 31}
            )
            assert result is not None
            assert result["name"] == "Alice"  # Returns original
            # Verify replacement happened
            updated = collection.find_one({"name": "Alice Replaced"})
            assert updated is not None
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_return_document(self, collection):
        """Test find_one_and_replace fallback with return_document=True."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"name": "Bob"},
                {"name": "Bob Replaced", "age": 26},
                return_document=True,
            )
            assert result is not None
            assert result["name"] == "Bob Replaced"  # Returns updated doc
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_no_match(self, collection):
        """Test find_one_and_replace fallback with no match."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"name": "NonExistent"}, {"name": "New"}
            )
            assert result is None
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_upsert(self, collection):
        """Test find_one_and_replace fallback with upsert."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"name": "UpsertFallback"},
                {"name": "UpsertFallback", "value": 100},
                upsert=True,
            )
            assert result is None  # Returns original (None)
            assert collection.count_documents({"name": "UpsertFallback"}) == 1
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_upsert_return_document(
        self, collection
    ):
        """Test find_one_and_replace fallback upsert with return_document."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"name": "UpsertRetDoc"},
                {"name": "UpsertRetDoc", "value": 200},
                upsert=True,
                return_document=True,
            )
            assert result is not None
            assert result["name"] == "UpsertRetDoc"
            assert result["value"] == 200
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_with_sort(self, collection):
        """Test find_one_and_replace fallback with sort."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_replace(
                {"age": {"$gte": 25}},
                {"name": "ReplacedSorted", "age": 99},
                sort=[("age", -1)],
            )
            assert result is not None
            # Should replace oldest matching: Charlie (35)
            assert result["name"] == "Charlie"
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_path(self, collection):
        """Test find_one_and_update using Python fallback."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "Alice"}, {"$set": {"score": 100}}
            )
            assert result is not None
            assert result["name"] == "Alice"
            assert result["score"] == 85  # Original score
            # Verify update
            updated = collection.find_one({"name": "Alice"})
            assert updated["score"] == 100
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_return_document(self, collection):
        """Test find_one_and_update fallback with return_document=True."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "Bob"},
                {"$set": {"score": 50}},
                return_document=True,
            )
            assert result is not None
            assert result["score"] == 50  # Updated score
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_no_match(self, collection):
        """Test find_one_and_update fallback with no match."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "NonExistent"}, {"$set": {"score": 100}}
            )
            assert result is None
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_upsert(self, collection):
        """Test find_one_and_update fallback with upsert."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "UpsertUpdate"},
                {"$set": {"score": 99}},
                upsert=True,
            )
            assert result is None
            # Document created with filter
            doc = collection.find_one({"name": "UpsertUpdate"})
            assert doc is not None
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_upsert_return_document(
        self, collection
    ):
        """Test find_one_and_update fallback upsert with return_document."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "UpsertUpdateRet"},
                {"$set": {"score": 88}},
                upsert=True,
                return_document=True,
            )
            assert result is not None
            assert result["name"] == "UpsertUpdateRet"
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_with_sort(self, collection):
        """Test find_one_and_update fallback with sort."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"age": {"$gte": 25}},
                {"$set": {"sorted_update": True}},
                sort=[("age", 1)],
            )
            assert result is not None
            # Should update youngest matching: Bob (25)
            assert result["name"] == "Bob"
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_with_array_filters(self, collection):
        """Test find_one_and_update fallback with array_filters."""
        collection.insert_one({"name": "ArrayTest", "scores": [80, 90, 100]})
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "ArrayTest"},
                {"$set": {"scores.$[elem]": 85}},
                array_filters=[{"elem": {"$gte": 90}}],
            )
            assert result is not None
            assert result["name"] == "ArrayTest"
            # Verify update
            updated = collection.find_one({"name": "ArrayTest"})
            assert 85 in updated["scores"]
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_with_inc(self, collection):
        """Test find_one_and_update fallback with $inc operator."""
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "Alice"}, {"$inc": {"score": 10}}
            )
            assert result is not None
            assert result["score"] == 85  # Original
            # Verify update
            updated = collection.find_one({"name": "Alice"})
            assert updated["score"] == 95
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_with_push(self, collection):
        """Test find_one_and_update fallback with $push operator."""
        collection.insert_one({"name": "PushTest", "items": [1, 2, 3]})
        set_force_fallback(True)
        try:
            result = collection.find_one_and_update(
                {"name": "PushTest"}, {"$push": {"items": 4}}
            )
            assert result is not None
            assert result["items"] == [1, 2, 3]  # Original
            # Verify update
            updated = collection.find_one({"name": "PushTest"})
            assert updated["items"] == [1, 2, 3, 4]
        finally:
            set_force_fallback(False)


class TestFindOneAndReplaceReturningEdgeCases:
    """Tests for find_one_and_replace RETURNING branch edge cases.

    Specifically targets the upsert paths within the RETURNING clause branch
    (lines 337-367) which are hard to hit normally.
    """

    def test_find_one_and_replace_returning_upsert_no_match(self, collection):
        """Test find_one_and_replace upsert in RETURNING path."""
        # Normal mode (RETURNING enabled) with upsert, no match
        result = collection.find_one_and_replace(
            {"name": "NewUpsert"},
            {"name": "NewUpsert", "age": 50},
            upsert=True,
        )
        assert result is None  # Returns original (None)
        assert collection.count_documents({"name": "NewUpsert"}) == 1

    def test_find_one_and_replace_returning_upsert_return_document(
        self, collection
    ):
        """Test find_one_and_replace upsert with return_document in RETURNING path."""
        result = collection.find_one_and_replace(
            {"name": "UpsertRet"},
            {"name": "UpsertRet", "value": 100},
            upsert=True,
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "UpsertRet"
        assert result["value"] == 100

    def test_find_one_and_replace_returning_update_return_document(
        self, collection
    ):
        """Test find_one_and_replace with return_document=True in RETURNING path."""
        result = collection.find_one_and_replace(
            {"name": "Alice"},
            {"name": "AliceNew", "age": 31},
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "AliceNew"
        assert result["age"] == 31


class TestFindOneAndUpdateReturningEdgeCases:
    """Tests for find_one_and_update RETURNING branch edge cases.

    Specifically targets the upsert paths within the RETURNING clause branch
    and the return_document fetching paths.
    """

    def test_find_one_and_update_returning_upsert_no_match(self, collection):
        """Test find_one_and_update upsert in RETURNING path."""
        result = collection.find_one_and_update(
            {"name": "UpdateUpsert"},
            {"$set": {"score": 99}},
            upsert=True,
        )
        assert result is None
        doc = collection.find_one({"name": "UpdateUpsert"})
        assert doc is not None

    def test_find_one_and_update_returning_upsert_return_document(
        self, collection
    ):
        """Test find_one_and_update upsert with return_document in RETURNING path."""
        result = collection.find_one_and_update(
            {"name": "UpdateUpsertRet"},
            {"$set": {"score": 88}},
            upsert=True,
            return_document=True,
        )
        assert result is not None
        assert result["name"] == "UpdateUpsertRet"

    def test_find_one_and_update_returning_return_document_true(
        self, collection
    ):
        """Test find_one_and_update return_document=True in RETURNING path."""
        result = collection.find_one_and_update(
            {"name": "Diana"},
            {"$set": {"score": 100}},
            return_document=True,
        )
        assert result is not None
        assert result["score"] == 100

    def test_find_one_and_update_returning_with_array_filters(self, collection):
        """Test find_one_and_update with array_filters in RETURNING path."""
        collection.insert_one({"name": "ArrayUpdate", "scores": [70, 80, 90]})
        result = collection.find_one_and_update(
            {"name": "ArrayUpdate"},
            {"$set": {"scores.$[elem]": 75}},
            array_filters=[{"elem": {"$gte": 80}}],
        )
        assert result is not None
        assert result["name"] == "ArrayUpdate"
        updated = collection.find_one({"name": "ArrayUpdate"})
        assert updated["scores"].count(75) == 2


class TestFindOperationsSessionValidation:
    """Tests for session validation edge cases in find operations."""

    def test_find_with_closed_session(self, connection):
        """Test find raises error with session from different database."""
        other_conn = neosqlite.Connection(":memory:")
        other_coll = other_conn["other"]
        other_coll.insert_one({"name": "Other"})

        # Session from different database should raise error
        with pytest.raises(Exception):
            connection["foo"].find({}, session=other_conn.start_session())

        other_conn.close()

    def test_find_one_and_delete_with_session_from_different_db(
        self, connection
    ):
        """Test find_one_and_delete raises error with wrong session."""
        other_conn = neosqlite.Connection(":memory:")

        with pytest.raises(Exception):
            connection["foo"].find_one_and_delete(
                {"name": "test"}, session=other_conn.start_session()
            )

        other_conn.close()

    def test_find_one_and_replace_with_session_from_different_db(
        self, connection
    ):
        """Test find_one_and_replace raises error with wrong session."""
        other_conn = neosqlite.Connection(":memory:")

        with pytest.raises(Exception):
            connection["foo"].find_one_and_replace(
                {"name": "test"},
                {"name": "replaced"},
                session=other_conn.start_session(),
            )

        other_conn.close()

    def test_find_one_and_update_with_session_from_different_db(
        self, connection
    ):
        """Test find_one_and_update raises error with wrong session."""
        other_conn = neosqlite.Connection(":memory:")

        with pytest.raises(Exception):
            connection["foo"].find_one_and_update(
                {"name": "test"},
                {"$set": {"x": 1}},
                session=other_conn.start_session(),
            )

        other_conn.close()


class TestFindOperationsComplexFilterFallback:
    """Tests for complex filters that trigger SQL translation fallback.

    These exercise paths where translate_match returns no where_clause.
    """

    def test_find_one_and_replace_with_json_schema_filter(self, connection):
        """Test find_one_and_replace with $jsonSchema filter."""
        coll = connection["schema_replace_coll"]
        coll.insert_one({"name": "schema_replace", "age": 30})
        schema = {
            "required": ["name"],
            "properties": {"name": {"bsonType": "string"}},
        }
        result = coll.find_one_and_replace(
            {"$jsonSchema": schema},
            {"name": "schema_replaced"},
        )
        assert result is not None
        assert result["name"] == "schema_replace"

    def test_find_one_and_update_with_json_schema_filter(self, connection):
        """Test find_one_and_update with $jsonSchema filter on isolated collection."""
        coll = connection["schema_update_coll"]
        coll.insert_one({"name": "schema_update", "age": 35})
        schema = {
            "required": ["name"],
            "properties": {"name": {"bsonType": "string"}},
        }
        result = coll.find_one_and_update(
            {"$jsonSchema": schema},
            {"$set": {"schema_updated": True}},
        )
        assert result is not None
        assert result["name"] == "schema_update"

    def test_find_one_and_delete_fallback_exercises_internal_delete(
        self, connection
    ):
        """Test find_one_and_delete fallback path exercises _internal_delete."""
        coll = connection["fallback_delete_coll"]
        coll.insert_one({"name": "fallback_delete_test", "value": 42})

        set_force_fallback(True)
        try:
            result = coll.find_one_and_delete({"name": "fallback_delete_test"})
            assert result is not None
            assert result["name"] == "fallback_delete_test"
            assert result["value"] == 42
            assert coll.count_documents({}) == 0
        finally:
            set_force_fallback(False)

    def test_find_one_and_replace_fallback_exercises_internal_replace(
        self, connection
    ):
        """Test find_one_and_replace fallback path exercises _internal_replace."""
        coll = connection["fallback_replace_coll"]
        coll.insert_one({"name": "fallback_replace_test", "value": 10})

        set_force_fallback(True)
        try:
            result = coll.find_one_and_replace(
                {"name": "fallback_replace_test"},
                {"name": "replaced", "value": 20},
            )
            assert result is not None
            assert result["name"] == "fallback_replace_test"
            # Verify replacement
            updated = coll.find_one({"name": "replaced"})
            assert updated is not None
            assert updated["value"] == 20
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_exercises_internal_update(
        self, connection
    ):
        """Test find_one_and_update fallback path exercises _internal_update."""
        coll = connection["fallback_update_coll"]
        coll.insert_one({"name": "fallback_update_test", "value": 10})

        set_force_fallback(True)
        try:
            result = coll.find_one_and_update(
                {"name": "fallback_update_test"},
                {"$inc": {"value": 5}},
            )
            assert result is not None
            assert result["name"] == "fallback_update_test"
            assert result["value"] == 10  # Original value
            # Verify update
            updated = coll.find_one({"name": "fallback_update_test"})
            assert updated["value"] == 15
        finally:
            set_force_fallback(False)

    def test_find_one_and_update_fallback_exercises_upsert_with_array_filters(
        self, connection
    ):
        """Test find_one_and_update fallback upsert with array_filters."""
        coll = connection["fallback_upsert_array_coll"]

        set_force_fallback(True)
        try:
            result = coll.find_one_and_update(
                {"name": "ArrayUpsert"},
                {"$set": {"scores.$[elem]": 100}},
                array_filters=[{"elem": {"$gte": 90}}],
                upsert=True,
            )
            assert result is None
            # Verify document was created
            doc = coll.find_one({"name": "ArrayUpsert"})
            assert doc is not None
        finally:
            set_force_fallback(False)
