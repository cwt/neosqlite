"""P0 cursor pagination: find/aggregate batching, getMore, killCursors."""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path / "cur.db"))
    yield h
    h.close_all()


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

        # In MongoDB, getMore without batchSize fills up to the 16 MB BSON budget
        res2 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=3,
        )
        assert len(res2["cursor"]["nextBatch"]) == 149
        assert res2["cursor"]["id"] == 0

        # Exhausted cursor is gone.
        res3 = _cmd(
            handler,
            {"getMore": cid, "collection": "big", "$db": "test"},
            req=4,
        )
        assert res3["cursor"]["id"] == 0
        assert res3["cursor"]["nextBatch"] == []

    def test_getmore_explicit_batch_size_paginates(self, handler):
        _insert_many(handler, 250)
        res = _cmd(handler, {"find": "big", "filter": {}, "$db": "test"})
        assert len(res["cursor"]["firstBatch"]) == 101
        cid = res["cursor"]["id"]
        assert cid != 0

        res2 = _cmd(
            handler,
            {
                "getMore": cid,
                "collection": "big",
                "batchSize": 101,
                "$db": "test",
            },
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


class TestBsonBudget:
    def test_fit_keeps_at_least_one(self):
        from nx_27017.handler import NeoSQLiteHandler

        head, tail = NeoSQLiteHandler._fit_bson_budget(
            [{"a": 1}, {"b": 2}], limit=10**9
        )
        assert (head, tail) == ([{"a": 1}, {"b": 2}], [])

    def test_large_docs_paginate_by_bytes(self, tmp_path):
        from nx_27017.handler import NeoSQLiteHandler

        h = NeoSQLiteHandler(str(tmp_path / "big.db"))
        try:
            big = "x" * (2 * 1024 * 1024)
            _, _ = h.handle_insert(
                {
                    "request_id": 1,
                    "sections": [
                        ("body", {"insert": "big", "$db": "test"}),
                        ("payload_docs", [{"v": big} for _ in range(3)]),
                    ],
                }
            )
            _, res = h.handle_command(
                {
                    "request_id": 2,
                    "sections": [
                        ("body", {"find": "big", "filter": {}, "$db": "test"})
                    ],
                }
            )
            # 3 x 2MB docs exceed 16MB only combined with overhead? No:
            # 6MB total fits, so a single batch. Force the budget small
            # via direct split instead.
            docs = [{"v": big} for _ in range(10)]
            first, cid = h._split_first_batch(
                docs,
                101,
                "test.big",
                "test",
            )
            assert 0 < len(first) < 10
            assert cid != 0
            delivered = list(first)
            while cid != 0:
                nxt = h._getmore_data(cid, 101)
                assert nxt is not None
                delivered.extend(nxt["nextBatch"])
                cid = nxt["id"]
            assert len(delivered) == 10
        finally:
            h.close_all()

    def test_fit_bson_budget_handles_neosqlite_objectids_without_exception(
        self, monkeypatch
    ):
        from bson import BSON
        from nx_27017.handler import NeoSQLiteHandler

        from neosqlite.objectid import ObjectId as NeoObjectId

        orig_encode = BSON.encode
        encode_errors = []

        def tracked_encode(doc, *args, **kwargs):
            try:
                return orig_encode(doc, *args, **kwargs)
            except Exception as exc:
                encode_errors.append(exc)
                raise

        monkeypatch.setattr(BSON, "encode", tracked_encode)
        docs = [{"_id": NeoObjectId(), "name": f"doc-{i}"} for i in range(10)]
        head, tail = NeoSQLiteHandler._fit_bson_budget(docs, limit=10**9)
        assert len(head) == 10
        assert tail == []
        assert encode_errors == []

    def test_drop_collection_with_open_lazy_cursor(self, handler):
        _insert_many(handler, 200)
        res = _cmd(
            handler,
            {"find": "big", "filter": {}, "batchSize": 10, "$db": "test"},
        )
        cid = res["cursor"]["id"]
        assert cid != 0

        # Dropping collection while an unexhausted lazy cursor exists must succeed
        drop_res = _cmd(handler, {"drop": "big", "$db": "test"}, req=3)
        assert drop_res["ok"] == 1
        assert cid not in handler._cursors
