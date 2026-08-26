"""Tests for find on a missing collection."""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestFindMissingCollection:
    def test_find_missing_returns_empty_cursor(self, handler):
        """Finding on a collection that does not exist must return a proper
        find cursor reply (ok:1 with a cursor.firstBatch), never fall through
        to the generic db.command fallback (which would return ok:1/result)."""
        _, resp = handler.handle_command(
            {
                "request_id": 1,
                "sections": [
                    (
                        "body",
                        {
                            "find": "does_not_exist",
                            "filter": {"x": 1},
                            "$db": "test",
                        },
                    )
                ],
            }
        )
        assert resp.get("ok") == 1
        assert "cursor" in resp
        assert "result" not in resp
        assert resp["cursor"]["firstBatch"] == []
