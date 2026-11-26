"""
Tests for the watch() method and ChangeStream functionality
"""

import json
import time
from datetime import UTC

import pytest


def test_watch_basic_functionality(collection):
    """Test basic watch functionality"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert a document
    result = collection.insert_one({"name": "Alice", "age": 30})
    doc_id = result.inserted_id

    # Get the change
    change = next(change_stream)

    # Verify change structure
    assert "_id" in change
    assert "operationType" in change
    assert change["operationType"] == "insert"
    assert "clusterTime" in change
    assert "ns" in change
    assert change["ns"]["db"] == "default"
    assert change["ns"]["coll"] == collection.name
    assert "documentKey" in change
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_with_full_document(collection):
    """Test watch with fullDocument lookup"""
    # Start watching with fullDocument lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Insert a document
    result = collection.insert_one({"name": "Bob", "age": 25})
    doc_id = result.inserted_id

    # Get the change
    change = next(change_stream)

    # Verify full document is included
    assert "fullDocument" in change
    full_doc = change["fullDocument"]
    assert full_doc["name"] == "Bob"
    assert full_doc["age"] == 25
    assert full_doc["_id"] == doc_id

    change_stream.close()


def test_watch_update_operations(collection):
    """Test watching update operations"""
    # Insert a document first
    result = collection.insert_one({"name": "Charlie", "age": 35})
    doc_id = result.inserted_id

    # Start watching for changes
    change_stream = collection.watch()

    # Update the document
    collection.update_one({"_id": doc_id}, {"$set": {"age": 36}})

    # Get the change
    change = next(change_stream)

    # Verify update change
    assert change["operationType"] == "update"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_delete_operations(collection):
    """Test watching delete operations"""
    # Insert a document first
    result = collection.insert_one({"name": "David", "age": 40})
    doc_id = result.inserted_id

    # Start watching for changes
    change_stream = collection.watch()

    # Delete the document
    collection.delete_one({"_id": doc_id})

    # Get the change
    change = next(change_stream)

    # Verify delete change
    assert change["operationType"] == "delete"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_multiple_operations(collection):
    """Test watching multiple operations in sequence"""
    # Start watching for changes
    change_stream = collection.watch()

    # Insert documents
    result1 = collection.insert_one({"name": "Eve", "age": 28})
    result2 = collection.insert_one({"name": "Frank", "age": 32})

    # Update a document
    collection.update_one({"_id": result1.inserted_id}, {"$set": {"age": 29}})

    # Delete a document
    collection.delete_one({"_id": result2.inserted_id})

    # Collect all changes
    changes = []
    for i in range(4):  # Expecting 4 changes
        try:
            change = next(change_stream)
            changes.append(change)
        except StopIteration:
            break

    # Verify we got all changes
    assert len(changes) == 4

    # Verify operation types
    operation_types = [change["operationType"] for change in changes]
    assert "insert" in operation_types
    assert "update" in operation_types
    assert "delete" in operation_types

    change_stream.close()


def test_watch_context_manager(collection):
    """Test ChangeStream as a context manager"""
    with collection.watch() as change_stream:
        # Insert a document
        collection.insert_one({"name": "Grace", "age": 33})

        # Get the change
        change = next(change_stream)

        # Verify change
        assert change["operationType"] == "insert"

    # The change stream should be closed after exiting the context


def test_watch_timeout_mechanism(collection):
    """Test the timeout mechanism"""
    # Start watching with a short timeout
    change_stream = collection.watch(max_await_time_ms=100)

    # Try to get a change when there are none
    start_time = time.time()
    with pytest.raises(StopIteration):
        next(change_stream)

    elapsed = time.time() - start_time
    # Should timeout in about 0.1 seconds
    assert elapsed < 1.0

    change_stream.close()


def test_watch_close_functionality(collection):
    """Test that closing the change stream works"""
    # Start watching
    change_stream = collection.watch()

    # Close the stream
    change_stream.close()

    # Trying to get a change should raise StopIteration
    with pytest.raises(StopIteration):
        next(change_stream)


def test_watch_batch_size(collection):
    """Test the batch_size parameter"""
    # Start watching with batch size of 1
    change_stream = collection.watch(batch_size=1)

    # Insert documents
    collection.insert_one({"name": "Henry", "age": 45})
    collection.insert_one({"name": "Ivy", "age": 27})

    # Get changes one by one (due to batch_size=1)
    change1 = next(change_stream)
    change2 = next(change_stream)

    # Verify both changes
    assert change1["operationType"] == "insert"
    assert change2["operationType"] == "insert"

    change_stream.close()


def test_watch_with_no_changes(collection):
    """Test watch when there are no changes"""
    # Start watching
    change_stream = collection.watch(max_await_time_ms=100)

    # Immediately try to get a change (should timeout)
    with pytest.raises(StopIteration):
        next(change_stream)

    change_stream.close()


def test_watch_different_collections(connection):
    """Test that watch only captures changes from the specific collection"""
    # Get two different collections
    collection1 = connection["collection1"]
    collection2 = connection["collection2"]

    # Start watching collection1
    change_stream = collection1.watch()

    # Insert into collection2 (should not trigger change in collection1 stream)
    collection2.insert_one({"name": "Jack", "age": 50})

    # Insert into collection1 (should trigger change)
    result = collection1.insert_one({"name": "Kate", "age": 29})
    doc_id = result.inserted_id

    # Get the change (should be from collection1 only)
    change = next(change_stream)

    # Verify it's from collection1
    assert change["operationType"] == "insert"
    assert change["ns"]["coll"] == "collection1"
    assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_with_binary_data(collection):
    """Test watch with binary data that may contain non-UTF-8 bytes"""
    from neosqlite.binary import Binary

    # Start watching with fullDocument lookup
    change_stream = collection.watch(full_document="updateLookup")

    # Insert a document with binary data containing non-UTF-8 bytes
    # This simulates the bug where bytes like 0xcc can't be decoded as UTF-8
    binary_data = Binary(b"\x00\x01\x02\xcc\xfe\xff\x00\x01\x02\x03")
    result = collection.insert_one({"name": "BinaryTest", "data": binary_data})
    doc_id = result.inserted_id

    # Get the change - should not raise UnicodeDecodeError
    change = next(change_stream)

    # Verify basic change structure
    assert change["operationType"] == "insert"
    assert change["documentKey"]["_id"] == doc_id

    # Full document should be included
    assert "fullDocument" in change
    assert change["fullDocument"]["name"] == "BinaryTest"

    change_stream.close()


def test_watch_with_various_binary_subtypes(collection):
    """Test watch with different binary subtypes"""
    from neosqlite.binary import Binary

    # Start watching
    change_stream = collection.watch(full_document="updateLookup")

    # Test with different binary subtypes
    test_cases = [
        ("generic", Binary(b"\x00\xcc\xfe\xff", subtype=0)),
        ("function", Binary(b"\x00\xcc\xfe\xff", subtype=1)),
        ("uuid", Binary(b"\x00\xcc\xfe\xff" * 4, subtype=3)),
        ("md5", Binary(b"\x00\xcc\xfe\xff" * 4, subtype=5)),
    ]

    for name, binary_data in test_cases:
        result = collection.insert_one({"type": name, "binary": binary_data})
        doc_id = result.inserted_id

        # Should not raise UnicodeDecodeError
        change = next(change_stream)
        assert change["operationType"] == "insert"
        assert change["documentKey"]["_id"] == doc_id

    change_stream.close()


def test_watch_update_with_binary_data(collection):
    """Test watching update operations with binary data"""
    from neosqlite.binary import Binary

    # Insert a document first
    result = collection.insert_one({"name": "UpdateTest", "value": 1})
    doc_id = result.inserted_id

    # Start watching
    change_stream = collection.watch(full_document="updateLookup")

    # Update with binary data containing non-UTF-8 bytes
    binary_data = Binary(b"\xcc\xdd\xee\xff\x00\x11\x22\x33")
    collection.update_one(
        {"_id": doc_id}, {"$set": {"data": binary_data, "value": 2}}
    )

    # Get the change - should not raise UnicodeDecodeError
    change = next(change_stream)

    # Verify update change
    assert change["operationType"] == "update"
    assert change["documentKey"]["_id"] == doc_id
    assert "fullDocument" in change
    assert change["fullDocument"]["value"] == 2

    change_stream.close()


def test_changestream_iter(collection):
    """Test __iter__ returns the stream itself."""
    with collection.watch() as stream:
        assert iter(stream) is stream


def test_changestream_closed_next(collection):
    """Test __next__ raises StopIteration when closed."""
    stream = collection.watch()
    stream.close()
    with pytest.raises(StopIteration, match="Change stream is closed"):
        next(stream)


def test_next_document_id_value_not_objectid(collection):
    """Test __next__ when document_id_value is not a valid ObjectId hex."""
    stream = collection.watch()

    # Manually insert a record with a non-ObjectId string as document_id_value
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_id_value) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, "not-an-objectid"),
    )

    change = next(stream)
    assert change["documentKey"]["_id"] == "not-an-objectid"
    stream.close()


def test_next_document_data_bytes_decode_fail(collection):
    """Test __next__ when document_data bytes fail to decode as UTF-8."""
    stream = collection.watch()

    # \xcc is invalid UTF-8
    invalid_utf8 = b"\xcc\xdd\xee"
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, invalid_utf8),
    )

    change = next(stream)
    # Should fall back to document_id (1) because decoding failed
    assert change["documentKey"]["_id"] == 1
    stream.close()


def test_next_document_data_json_decode_fail(collection):
    """Test __next__ when document_data is invalid JSON."""
    stream = collection.watch()

    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, "{invalid-json}"),
    )

    change = next(stream)
    # Should fall back to database lookup or document_id
    assert change["documentKey"]["_id"] == 1
    stream.close()


def test_next_document_data_no_id_in_json(collection):
    """Test __next__ when document_data is JSON but missing _id."""
    stream = collection.watch()

    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 1, json.dumps({"name": "test"})),
    )

    change = next(stream)
    # Should fall back to database lookup or document_id
    assert change["documentKey"]["_id"] == 1
    stream.close()


def test_next_no_json_data_fallback(collection):
    """Test fallback when document_data is None."""
    stream = collection.watch()

    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "insert", 999, None),
    )

    change = next(stream)
    # Should fall back to document_id 999
    assert change["documentKey"]["_id"] == 999
    stream.close()


def test_updatelookup_decode_fail(collection):
    """Test updateLookup when document_data bytes fail to decode."""
    stream = collection.watch(
        full_document="updateLookup", max_await_time_ms=500
    )

    # Insert invalid record that should be skipped (causing a continue in the loop)
    invalid_utf8 = b"\xcc\xdd\xee"
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, invalid_utf8),
    )

    # After one invalid record, it should keep polling until timeout
    with pytest.raises(StopIteration, match="Change stream timeout exceeded"):
        next(stream)

    stream.close()


def test_updatelookup_json_fail(collection):
    """Test updateLookup when JSON parsing fails (should just pass through)."""
    stream = collection.watch(full_document="updateLookup")

    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, "{invalid-json}"),
    )

    change = next(stream)
    assert "fullDocument" not in change
    stream.close()


def test_updatelookup_non_objectid_value(collection):
    """Test updateLookup fallback with non-ObjectId document_id_value when _id absent in JSON."""
    stream = collection.watch(full_document="updateLookup")

    doc_data = json.dumps({"name": "test"})
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data, document_id_value) VALUES (?, ?, ?, ?, ?)",
        (collection.name, "update", 1, doc_data, "string-id"),
    )

    change = next(stream)
    assert change["fullDocument"]["_id"] == "string-id"
    stream.close()


def test_updatelookup_no_id_in_json_fallback(collection):
    """Test updateLookup fallback when _id is missing in JSON and no document_id_value."""
    stream = collection.watch(full_document="updateLookup")

    doc_data = json.dumps({"name": "test"})
    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (collection.name, "update", 1, doc_data),
    )

    change = next(stream)
    assert change["fullDocument"]["_id"] == 1
    stream.close()


def test_next_document_data_has_id_in_json(collection):
    """Test __next__ when document_data is JSON and has _id."""
    stream = collection.watch()

    collection.db.execute(
        "INSERT INTO _neosqlite_changestream (collection_name, operation, document_id, document_data) VALUES (?, ?, ?, ?)",
        (
            collection.name,
            "insert",
            1,
            json.dumps({"_id": "custom-id", "name": "test"}),
        ),
    )

    change = next(stream)
    assert change["documentKey"]["_id"] == "custom-id"
    stream.close()


def test_close_idempotent(collection):
    """Test that close() can be called multiple times."""
    stream = collection.watch()
    stream.close()
    assert stream._closed is True
    # Should not raise any error
    stream.close()
    assert stream._closed is True


def test_close_after_changelog_table_dropped_no_warning(collection, caplog):
    """Closing a watcher after dropDatabase is silent and safe.

    ``dropDatabase`` drops the internal ``_neosqlite_changestream`` table
    while watchers may still be registered on the connection. Cleanup must
    tolerate a missing changelog table instead of logging a WARNING (#NX).
    """
    import logging

    stream = collection.watch()
    # Simulate dropDatabase removing the internal changelog table.
    collection.db.execute("DROP TABLE IF EXISTS _neosqlite_changestream")
    collection.db.commit()

    with caplog.at_level(logging.WARNING, logger="neosqlite.changestream"):
        stream.close()

    assert stream._closed is True
    assert "Error during ChangeStream cleanup" not in caplog.text


def test_changestream_ensures_id_column_exists():
    """Test that ChangeStream ensures _id column exists before creating triggers.

    If a collection was created without an _id column (e.g., legacy schema),
    ChangeStream should add it before setting up triggers that reference
    NEW._id / OLD._id, preventing trigger runtime failures.
    """
    import neosqlite

    db = neosqlite.Connection(":memory:")
    # Create a table without _id column (simulate legacy schema)
    db.db.execute(
        "CREATE TABLE legacy_coll (id INTEGER PRIMARY KEY, data TEXT)"
    )
    # Manually register the collection (bypassing Collection.create())
    coll = neosqlite.Collection(db.db, "legacy_coll", create=False, database=db)
    # Verify _id column doesn't exist yet
    cursor = db.db.execute(
        "SELECT name FROM pragma_table_info('legacy_coll') WHERE name='_id'"
    )
    assert cursor.fetchone() is None
    # Creating a ChangeStream should add the _id column
    stream = coll.watch()
    # Now the _id column should exist
    cursor = db.db.execute(
        "SELECT name FROM pragma_table_info('legacy_coll') WHERE name='_id'"
    )
    assert cursor.fetchone() is not None
    # Insert should work (trigger references NEW._id which now exists)
    coll.insert_one({"foo": "bar"})
    change = next(stream)
    assert change["operationType"] == "insert"
    stream.close()
    db.close()


def test_changestream_last_id_updated_after_commit(collection):
    """Test that _last_id is only updated after successful commit.

    This ensures a crash between DELETE and COMMIT does not advance
    _last_id beyond the actually-committed data, preventing skipped
    change events on recovery.
    """
    stream = collection.watch()
    # Insert a document to generate a change event
    collection.insert_one({"test": 1})
    last_id_before = stream._last_id
    # Get the next change (which commits internally)
    change = next(stream)
    # _last_id should have advanced after the successful commit
    assert stream._last_id > last_id_before
    # Verify the change is valid
    assert change["operationType"] == "insert"
    stream.close()


def test_changestream_preserves_id_types_across_insert_update_delete(
    connection,
):
    """Verify int, float, 24-hex str, Binary, and ObjectId _id types round-trip cleanly."""
    from neosqlite.binary import Binary
    from neosqlite.objectid import ObjectId

    coll = connection.id_types_test
    oid_val = ObjectId()
    hex_str_val = "507f1f77bcf86cd799439011"
    bin_val = Binary(b"0123456789ab")
    ids = [1, 2.5, hex_str_val, bin_val, oid_val]

    with coll.watch(
        full_document="updateLookup", max_await_time_ms=500
    ) as stream:
        for idx, doc_id in enumerate(ids):
            coll.insert_one({"_id": doc_id, "step": idx})
            ev_ins = next(stream)
            assert ev_ins["operationType"] == "insert"
            assert ev_ins["documentKey"]["_id"] == doc_id
            assert type(ev_ins["documentKey"]["_id"]) is type(doc_id)
            assert ev_ins["fullDocument"]["_id"] == doc_id
            assert type(ev_ins["fullDocument"]["_id"]) is type(doc_id)

            coll.update_one({"_id": doc_id}, {"$set": {"step": idx + 10}})
            ev_upd = next(stream)
            assert ev_upd["operationType"] == "update"
            assert ev_upd["documentKey"]["_id"] == doc_id
            assert type(ev_upd["documentKey"]["_id"]) is type(doc_id)
            assert ev_upd["fullDocument"]["_id"] == doc_id
            assert type(ev_upd["fullDocument"]["_id"]) is type(doc_id)

            coll.delete_one({"_id": doc_id})
            ev_del = next(stream)
            assert ev_del["operationType"] == "delete"
            assert ev_del["documentKey"]["_id"] == doc_id
            assert type(ev_del["documentKey"]["_id"]) is type(doc_id)
            assert ev_del["fullDocument"]["_id"] == doc_id
            assert type(ev_del["fullDocument"]["_id"]) is type(doc_id)


def test_changestream_decodes_bson_types_in_full_document(connection):
    """Verify datetime, ObjectId, and Binary fields decode in fullDocument via neosqlite_json_loads."""
    from datetime import datetime

    from neosqlite.binary import Binary
    from neosqlite.objectid import ObjectId

    coll = connection.bson_full_doc
    run_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    owner = ObjectId()
    payload = Binary(b"\xde\xad\xbe\xef")

    with coll.watch(
        full_document="updateLookup", max_await_time_ms=500
    ) as stream:
        coll.insert_one(
            {
                "_id": 1,
                "next_run": run_at,
                "owner": owner,
                "nested": {"sig": payload},
            }
        )
        ev = next(stream)
        assert ev["documentKey"]["_id"] == 1
        assert type(ev["documentKey"]["_id"]) is int
        full_doc = ev["fullDocument"]
        assert isinstance(full_doc["next_run"], datetime)
        assert full_doc["next_run"] == run_at
        assert isinstance(full_doc["owner"], ObjectId)
        assert full_doc["owner"] == owner
        assert isinstance(full_doc["nested"]["sig"], Binary)
        assert full_doc["nested"]["sig"] == payload


def test_changestream_mixed_type_match_does_not_wedge(connection):
    """Verify mixed-type $gt/$lte in $match return False safely without wedging _last_id."""
    coll = connection.mixed_match

    with coll.watch(
        pipeline=[{"$match": {"fullDocument.score": {"$gt": 0}}}],
        full_document="updateLookup",
        max_await_time_ms=100,
    ) as stream:
        coll.insert_one({"_id": 1, "score": "not-a-number"})
        coll.insert_one({"_id": 2, "score": True})
        coll.insert_one({"_id": 3, "score": 25})

        ev = next(stream)
        assert ev["documentKey"]["_id"] == 3
        assert ev["fullDocument"]["score"] == 25

        with pytest.raises(StopIteration, match="timeout exceeded"):
            next(stream)


def test_changestream_prune_consumed_retains_resume_rows(
    connection, monkeypatch
):
    """Verify _prune_consumed honors _RESUME_RETENTION_ROWS for concurrent streams and bounded pruning."""
    from neosqlite import changestream as cs_mod

    monkeypatch.setattr(cs_mod, "_RESUME_RETENTION_ROWS", 3)
    coll = connection.prune_retention

    s1 = coll.watch(max_await_time_ms=100)
    s2 = coll.watch(max_await_time_ms=100)
    tokens = []
    try:
        for i in range(6):
            coll.insert_one({"seq": i})
            ev1 = next(s1)
            next(s2)
            tokens.append(ev1["_id"])

        row_count = connection.db.execute(
            "SELECT COUNT(*) FROM _neosqlite_changestream WHERE collection_name = ?",
            (coll.name,),
        ).fetchone()[0]
        assert row_count == 3

        # Replaying after the 3rd-to-last retained token must still work while live
        with coll.watch(
            resume_after=tokens[3], max_await_time_ms=100
        ) as replayed:
            ev_r1 = next(replayed)
            ev_r2 = next(replayed)
            assert ev_r1["_id"] == tokens[4]
            assert ev_r2["_id"] == tokens[5]
    finally:
        s1.close()
        s2.close()


def test_changestream_registry_cleanup_and_thread_safety():
    """Verify _stream_registry releases closed connections and handles concurrent access."""
    import concurrent.futures

    import neosqlite
    from neosqlite.changestream import _stream_registry

    snapshot_before = set(_stream_registry.keys())

    # 1. Explicit stream close clears registry entry when last stream closes
    conn1 = neosqlite.Connection(":memory:")
    with conn1.items.watch(max_await_time_ms=20) as cs:
        assert id(conn1.db) in _stream_registry
        conn1.items.insert_one({"x": 1})
        next(cs)
    assert id(conn1.db) not in _stream_registry
    conn1.close()

    # 2. Connection.close() clears registry even if stream was left unclosed
    conn2 = neosqlite.Connection(":memory:")
    abandoned = conn2.items.watch(max_await_time_ms=20)
    assert id(conn2.db) in _stream_registry
    conn2.close()
    assert id(conn2.db) not in _stream_registry
    assert abandoned is not None

    # 3. Concurrent stream open/close across connections stays thread-safe
    def open_and_close_stream(idx: int) -> int:
        local_conn = neosqlite.Connection(":memory:")
        try:
            with local_conn[f"t_{idx}"].watch(max_await_time_ms=20) as st:
                local_conn[f"t_{idx}"].insert_one({"idx": idx})
                return next(st)["operationType"] == "insert"
        finally:
            local_conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(open_and_close_stream, range(12)))
    assert all(results)
    assert set(_stream_registry.keys()) <= snapshot_before


def test_changestream_max_await_time_ms_zero_and_short_timeout(connection):
    """Verify max_await_time_ms=0 is non-blocking and short timeouts finish without 100ms sleep."""
    import time

    coll = connection.await_timing

    # 0ms timeout with no queued events returns StopIteration immediately
    with coll.watch(max_await_time_ms=0) as zero_stream:
        start = time.monotonic()
        with pytest.raises(StopIteration, match="timeout exceeded"):
            next(zero_stream)
        elapsed_empty = time.monotonic() - start
        assert elapsed_empty < 0.05

        # 0ms timeout returns an already-queued matching event cleanly, then stops
        coll.insert_one({"ready": True})
        ev = next(zero_stream)
        assert ev["operationType"] == "insert"
        with pytest.raises(StopIteration, match="timeout exceeded"):
            next(zero_stream)

    # 30ms timeout must not sleep a fixed 100ms
    with coll.watch(max_await_time_ms=30) as short_stream:
        start = time.monotonic()
        with pytest.raises(StopIteration, match="timeout exceeded"):
            next(short_stream)
        elapsed_short = time.monotonic() - start
        assert elapsed_short < 0.09


def test_changestream_match_operators_are_bson_strict():
    """$eq/$ne/$in/$nin must not conflate booleans with integers."""
    from neosqlite.changestream import _match_operators

    assert _match_operators(True, {"$eq": 1}) is False
    assert _match_operators(True, {"$eq": True}) is True
    assert _match_operators(1, {"$ne": True}) is True
    assert _match_operators(False, {"$in": [0, 1]}) is False
    assert _match_operators(True, {"$in": [0, True]}) is True
    assert _match_operators(False, {"$nin": [0, 1]}) is True
