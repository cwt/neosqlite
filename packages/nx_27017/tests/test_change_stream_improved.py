import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


def test_change_stream_lifecycle_and_getmore(handler):
    # 1. Start change stream
    stream_msg = {
        "request_id": 1,
        "sections": [
            (
                "body",
                {
                    "aggregate": "users",
                    "pipeline": [{"$changeStream": {}}],
                    "$db": "test",
                },
            )
        ],
    }
    req_id, res = handler.handle_command(stream_msg)
    assert res["ok"] == 1
    cursor_id = res["cursor"]["id"]
    assert cursor_id > 0
    assert res["cursor"]["firstBatch"] == []

    # 2. Insert document
    insert_msg = {
        "request_id": 2,
        "sections": [
            ("body", {"insert": "users", "$db": "test"}),
            ("payload", {"documents": [{"_id": 1, "name": "Alice"}]}),
        ],
    }
    req_id, insert_res = handler.handle_insert(insert_msg)
    assert insert_res["ok"] == 1

    # 3. Call getMore to retrieve change events
    getmore_msg = {
        "request_id": 3,
        "sections": [
            (
                "body",
                {
                    "getMore": cursor_id,
                    "collection": "users",
                    "$db": "test",
                },
            )
        ],
    }
    req_id, getmore_res = handler.handle_command(getmore_msg)
    assert getmore_res["ok"] == 1
    cursor = getmore_res["cursor"]
    assert cursor["id"] == cursor_id
    events = cursor["nextBatch"]
    assert len(events) == 1
    assert events[0]["operationType"] == "insert"
    assert events[0]["fullDocument"]["name"] == "Alice"
    assert cursor["postBatchResumeToken"] is not None

    # 4. Try updating the document
    update_msg = {
        "request_id": 4,
        "sections": [
            (
                "body",
                {
                    "update": "users",
                    "updates": [
                        {
                            "q": {"_id": 1},
                            "u": {"$set": {"name": "Bob"}},
                        }
                    ],
                    "$db": "test",
                },
            )
        ],
    }
    req_id, update_res = handler.handle_command(update_msg)
    assert update_res["ok"] == 1

    # 5. Call getMore to retrieve update event
    req_id, getmore_res2 = handler.handle_command(getmore_msg)
    assert getmore_res2["ok"] == 1
    events2 = getmore_res2["cursor"]["nextBatch"]
    assert len(events2) == 1
    assert events2[0]["operationType"] == "update"
    assert events2[0]["fullDocument"]["name"] == "Bob"

    # 6. Delete document
    delete_msg = {
        "request_id": 5,
        "sections": [
            (
                "body",
                {
                    "delete": "users",
                    "deletes": [{"q": {"_id": 1}}],
                    "$db": "test",
                },
            )
        ],
    }
    req_id, delete_res = handler.handle_command(delete_msg)
    assert delete_res["ok"] == 1

    # 7. Call getMore to retrieve delete event
    req_id, getmore_res3 = handler.handle_command(getmore_msg)
    assert getmore_res3["ok"] == 1
    events3 = getmore_res3["cursor"]["nextBatch"]
    assert len(events3) == 1
    assert events3[0]["operationType"] == "delete"
    assert getmore_res3["cursor"]["postBatchResumeToken"] == events3[0]["_id"]


def test_change_stream_manager_pull_and_cleanup(tmp_path):
    """Trigger-backed tracking: writes via NeoSQLite appear in pull()."""
    from nx_27017.changestream import ChangeStreamManager

    from neosqlite import Connection

    conn = Connection(str(tmp_path / "cs.db"))
    try:
        manager = ChangeStreamManager()
        stream = manager.create_stream(
            "test_coll",
            [{"$changeStream": {}}],
            db_name="mydb",
            get_collection=lambda: conn["test_coll"],
        )
        assert manager.get_stream(stream._id) is stream

        conn["test_coll"].insert_one({"_id": 1, "val": "abc"})
        batch, token = manager.pull_stream(stream)
        assert len(batch) == 1
        assert batch[0]["operationType"] == "insert"
        assert batch[0]["fullDocument"]["val"] == "abc"
        assert batch[0]["ns"] == {"db": "mydb", "coll": "test_coll"}
        assert token == batch[0]["_id"]

        # Watermark advanced: second pull is empty.
        batch2, _ = manager.pull_stream(stream)
        assert batch2 == []

        manager.close_stream(stream._id)
        assert manager.get_stream(stream._id) is None
    finally:
        manager.invalidate()
        conn.close()


def _open_stream(handler, coll, db="test", pipeline=None, req=1):
    msg = {
        "request_id": req,
        "sections": [
            (
                "body",
                {
                    "aggregate": coll,
                    "pipeline": pipeline or [{"$changeStream": {}}],
                    "$db": db,
                },
            )
        ],
    }
    _, res = handler.handle_command(msg)
    assert res["ok"] == 1
    return res["cursor"]["id"]


def _getmore(handler, cid, coll, db="test", req=2):
    msg = {
        "request_id": req,
        "sections": [("body", {"getMore": cid, "collection": coll, "$db": db})],
    }
    _, res = handler.handle_command(msg)
    assert res["ok"] == 1
    return res["cursor"]


def _wire_insert(handler, db, coll, docs, req=3):
    msg = {
        "request_id": req,
        "sections": [
            ("body", {"insert": coll, "$db": db}),
            ("payload_docs", docs),
        ],
    }
    _, res = handler.handle_insert(msg)
    assert res["ok"] == 1


def test_resume_after_replays(handler):
    cid = _open_stream(handler, "users")
    _wire_insert(handler, "test", "users", [{"_id": 1}, {"_id": 2}])
    cur = _getmore(handler, cid, "users")
    assert len(cur["nextBatch"]) == 2
    token = cur["postBatchResumeToken"]

    _wire_insert(handler, "test", "users", [{"_id": 3}])
    cid2 = _open_stream(
        handler,
        "users",
        pipeline=[{"$changeStream": {"resumeAfter": token}}],
        req=10,
    )
    cur2 = _getmore(handler, cid2, "users", req=11)
    # NOTE: int _ids surface as strings in documentKey (native NeoSQLite
    # behavior); fullDocument keeps the int.
    assert [d["fullDocument"]["_id"] for d in cur2["nextBatch"]] == [3]


def test_match_filters_operation_types(handler):
    cid = _open_stream(
        handler,
        "users",
        pipeline=[
            {"$changeStream": {}},
            {"$match": {"operationType": "insert"}},
        ],
    )
    _wire_insert(handler, "test", "users", [{"_id": 1}])
    handler.handle_command(
        {
            "request_id": 9,
            "sections": [
                (
                    "body",
                    {
                        "update": "users",
                        "updates": [{"q": {"_id": 1}, "u": {"$set": {"v": 1}}}],
                        "$db": "test",
                    },
                )
            ],
        }
    )
    cur = _getmore(handler, cid, "users")
    assert [d["operationType"] for d in cur["nextBatch"]] == ["insert"]


def test_full_document_off(handler):
    cid = _open_stream(
        handler,
        "users",
        pipeline=[{"$changeStream": {"fullDocument": "off"}}],
    )
    _wire_insert(handler, "test", "users", [{"_id": 1, "v": "x"}])
    cur = _getmore(handler, cid, "users")
    assert len(cur["nextBatch"]) == 1
    assert cur["nextBatch"][0]["fullDocument"] is None


def test_writes_behind_nx_back_are_captured(handler, tmp_path):
    """Direct NeoSQLite writes (bulk/TTL-style) still produce events."""
    from neosqlite import Connection

    cid = _open_stream(handler, "users")
    # Write through a second handle on the same file.
    db_path = handler.databases["test"].db_path
    direct = Connection(db_path)
    try:
        direct["users"].insert_one({"_id": 50, "v": "behind"})
    finally:
        direct.close()
    cur = _getmore(handler, cid, "users")
    assert [d["fullDocument"]["_id"] for d in cur["nextBatch"]] == [50]


def test_streams_isolated_across_databases(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
    try:
        cid = _open_stream(h, "users", db="db1")
        _wire_insert(h, "db1", "users", [{"_id": 1}])
        _wire_insert(h, "db2", "users", [{"_id": 2}])
        cur = _getmore(h, cid, "users", db="db1")
        assert [d["fullDocument"]["_id"] for d in cur["nextBatch"]] == [1]
    finally:
        h.close_all()
