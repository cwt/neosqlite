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
