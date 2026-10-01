"""P0 cursor pagination: find/aggregate batching, getMore, killCursors."""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path / "cur.db"))
    yield h
    h.conn.close()


def _insert_many(handler, n, coll="big", db="test"):
    msg = {
        "request_id": 1,
        "sections": [
            ("body", {"insert": coll, "$db": db}),
            ("payload_docs", [{"i": i} for i in range(n)]),
        ],
    }
    _, res = handler.handle_insert(msg)
    assert res["ok"] == 1


def _cmd(handler, body, req=2):
    _, res = handler.handle_command(
        {"request_id": req, "sections": [("body", body)]}
    )
    return res


class TestFindPagination:
    def test_small_result_no_cursor(self, handler):
        _insert_many(handler, 5)
        res = _cmd(handler, {"find": "big", "filter": {}, "$db": "test"})
        assert res["ok"] == 1
        assert res["cursor"]["id"] == 0
        assert len(res["cursor"]["firstBatch"]) == 5

    def test_default_batches_101(self, handler):
        _insert_many(handler, 250)
        res = _cmd(handler, {"find": "big", "filter": {}, "$db": "test"})
        assert len(res["cursor"]["firstBatch"]) == 101
        cid = res["cursor"]["id"]
        assert cid != 0

        res2 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=3,
        )
        assert len(res2["cursor"]["nextBatch"]) == 101
        assert res2["cursor"]["id"] == cid

        res3 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=4,
        )
        assert len(res3["cursor"]["nextBatch"]) == 48
        assert res3["cursor"]["id"] == 0

        # Exhausted cursor is gone.
        res4 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=5,
        )
        assert res4["cursor"]["id"] == 0
        assert res4["cursor"]["nextBatch"] == []

    def test_explicit_batch_size(self, handler):
        _insert_many(handler, 10)
        res = _cmd(
            handler,
            {"find": "big", "filter": {}, "batchSize": 4, "$db": "test"},
        )
        assert len(res["cursor"]["firstBatch"]) == 4
        cid = res["cursor"]["id"]
        assert cid != 0
        res2 = _cmd(
            handler,
            {
                "getMore": cid,
                "collection": "big",
                "batchSize": 4,
                "$db": "test",
            },
            req=3,
        )
        assert len(res2["cursor"]["nextBatch"]) == 4

    def test_kill_cursors(self, handler):
        _insert_many(handler, 250)
        res = _cmd(handler, {"find": "big", "filter": {}, "$db": "test"})
        cid = res["cursor"]["id"]
        assert cid != 0
        kill = _cmd(
            handler,
            {"killCursors": "big", "cursors": [cid], "$db": "test"},
            req=3,
        )
        assert kill["cursorsKilled"] == [cid]
        res2 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=4,
        )
        assert res2["cursor"]["nextBatch"] == []

    def test_kill_unknown_cursor(self, handler):
        kill = _cmd(
            handler,
            {"killCursors": "big", "cursors": [123456789], "$db": "test"},
        )
        assert kill["cursorsKilled"] == []
        assert kill["cursorsNotFound"] == [123456789]


class TestAggregatePagination:
    def test_aggregate_batches(self, handler):
        _insert_many(handler, 150, coll="agg")
        res = _cmd(
            handler,
            {
                "aggregate": "agg",
                "pipeline": [{"$match": {}}],
                "cursor": {"batchSize": 60},
                "$db": "test",
            },
        )
        assert len(res["cursor"]["firstBatch"]) == 60
        cid = res["cursor"]["id"]
        assert cid != 0
        res2 = _cmd(
            handler,
            {"getMore": cid, "collection": "agg", "$db": "test"},
            req=3,
        )
        assert len(res2["cursor"]["nextBatch"]) == 90
