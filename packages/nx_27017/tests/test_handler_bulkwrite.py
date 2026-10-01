"""P1 bulk semantics: writeErrors, upserted ids, matched counts."""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path / "bulk.db"))
    yield h
    h.conn.close()


def _insert(handler, coll, docs, ordered=True, db="test", req=1):
    body = {"insert": coll, "$db": db, "ordered": ordered}
    _, res = handler.handle_insert(
        {
            "request_id": req,
            "sections": [("body", body), ("payload_docs", docs)],
        }
    )
    return res


def _update(handler, coll, updates, ordered=True, db="test", req=2):
    _, res = handler.handle_command(
        {
            "request_id": req,
            "sections": [
                (
                    "body",
                    {
                        "update": coll,
                        "updates": updates,
                        "ordered": ordered,
                        "$db": db,
                    },
                )
            ],
        }
    )
    return res


class TestInsertWriteErrors:
    def test_ordered_stops_at_first_error(self, handler):
        res = _insert(
            handler,
            "c",
            [{"_id": 1}, {"_id": 1}, {"_id": 2}],
            ordered=True,
        )
        assert res["ok"] == 1
        assert res["n"] == 1
        assert [(w["index"], w["code"]) for w in res["writeErrors"]] == [
            (1, 11000)
        ]

    def test_unordered_collects_errors(self, handler):
        res = _insert(
            handler,
            "c",
            [{"_id": 1}, {"_id": 1}, {"_id": 2}],
            ordered=False,
        )
        assert res["ok"] == 1
        assert res["n"] == 2
        assert [(w["index"], w["code"]) for w in res["writeErrors"]] == [
            (1, 11000)
        ]

    def test_clean_batch_has_no_write_errors_key(self, handler):
        res = _insert(handler, "c", [{"_id": 1}])
        assert res["ok"] == 1
        assert "writeErrors" not in res


class TestUpdateCounts:
    def test_matched_vs_modified(self, handler):
        _insert(handler, "c", [{"_id": 1, "v": 1}])
        res = _update(
            handler, "c", [{"q": {"_id": 1}, "u": {"$set": {"v": 1}}}]
        )
        assert res["ok"] == 1
        assert res["n"] == 1
        assert res["nModified"] == 0

    def test_upsert_reports_ids(self, handler):
        res = _update(
            handler,
            "c",
            [{"q": {"_id": 9}, "u": {"$set": {"v": 1}}, "upsert": True}],
        )
        assert res["ok"] == 1
        assert len(res["upserted"]) == 1
        assert res["upserted"][0]["index"] == 0
        # Wire n counts the upserted doc as matched (real MongoDB: n:1).
        assert res["n"] == 1
        assert res["nModified"] == 0


class TestClientBulkWrite:
    def _cmd(self, handler, body, req=3):
        _, res = handler.handle_command(
            {"request_id": req, "sections": [("body", body)]}
        )
        return res

    def test_insert_update_delete(self, handler):
        res = self._cmd(
            handler,
            {
                "bulkWrite": 1,
                "ordered": True,
                "errorsOnly": False,
                "ops": [
                    {"insert": 0, "document": {"_id": 1}},
                    {
                        "update": 0,
                        "filter": {"_id": 1},
                        "updateMods": {"$set": {"v": 2}},
                        "multi": False,
                    },
                    {"delete": 0, "filter": {"_id": 9}, "multi": False},
                ],
                "nsInfo": [{"ns": "test.t"}],
                "$db": "admin",
            },
        )
        assert res["ok"] == 1
        assert (
            res["nInserted"],
            res["nMatched"],
            res["nModified"],
            res["nDeleted"],
        ) == (1, 1, 1, 0)
        assert [d["idx"] for d in res["cursor"]["firstBatch"]] == [0, 1, 2]

    def test_duplicate_reports_error_doc(self, handler):
        def body(ops, ordered=True):
            return {
                "bulkWrite": 1,
                "ordered": ordered,
                "errorsOnly": False,
                "ops": ops,
                "nsInfo": [{"ns": "test.t"}],
                "$db": "admin",
            }

        self._cmd(handler, body([{"insert": 0, "document": {"_id": 1}}]))
        res = self._cmd(
            handler,
            body(
                [
                    {"insert": 0, "document": {"_id": 1}},
                    {"insert": 0, "document": {"_id": 2}},
                ],
                False,
            ),
        )
        assert res["nErrors"] == 1
        assert res["nInserted"] == 1
        err = [d for d in res["cursor"]["firstBatch"] if not d["ok"]][0]
        assert (err["idx"], err["code"]) == (0, 11000)

    def test_ops_from_payload_section(self, handler):
        _, res = handler.handle_command(
            {
                "request_id": 4,
                "sections": [
                    ("body", {"bulkWrite": 1, "$db": "admin"}),
                    (
                        "payload",
                        {
                            "ops": [
                                {"insert": 0, "document": {"_id": 5}},
                            ],
                            "nsInfo": [{"ns": "test.t"}],
                        },
                    ),
                ],
            }
        )
        assert res["nInserted"] == 1


class TestOptionForwarding:
    def _cmd(self, handler, body, req=5):
        _, res = handler.handle_command(
            {"request_id": req, "sections": [("body", body)]}
        )
        return res

    def test_count_limit_skip(self, handler):
        _, _ = handler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    ("body", {"insert": "nums", "$db": "test"}),
                    ("payload_docs", [{"x": i} for i in range(5)]),
                ],
            }
        )
        res = self._cmd(
            handler,
            {"count": "nums", "query": {}, "limit": 2, "$db": "test"},
        )
        assert res == {"ok": 1, "n": 2}
        res = self._cmd(
            handler,
            {"count": "nums", "query": {}, "skip": 4, "$db": "test"},
        )
        assert res == {"ok": 1, "n": 1}

    def test_update_array_filters(self, handler):
        # NOTE: core supports $[id] over scalar elements only; nested
        # field access (e.std) is a documented core gap, not wiring.
        _, _ = handler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    ("body", {"insert": "grades", "$db": "test"}),
                    ("payload_docs", [{"_id": 1, "scores": [5, 95]}]),
                ],
            }
        )
        res = self._cmd(
            handler,
            {
                "update": "grades",
                "updates": [
                    {
                        "q": {"_id": 1},
                        "u": {"$set": {"scores.$[e]": 0}},
                        "arrayFilters": [{"e": {"$gte": 90}}],
                    }
                ],
                "$db": "test",
            },
        )
        assert res["ok"] == 1
        assert res["nModified"] == 1


class TestWriteConcernMapping:
    def _insert_wc(self, handler, wc, req=20):
        _, res = handler.handle_insert(
            {
                "request_id": req,
                "sections": [
                    (
                        "body",
                        {
                            "insert": "wc",
                            "$db": "test",
                            "writeConcern": wc,
                        },
                    ),
                    ("payload_docs", [{"x": 1}]),
                ],
            }
        )
        return res

    def test_w0_maps_to_synchronous_off(self, handler):
        assert self._insert_wc(handler, {"w": 0})["ok"] == 1
        level = handler.get_database("test").db.execute(
            "PRAGMA synchronous"
        ).fetchone()[0]
        assert level == 0

    def test_journaled_maps_to_full(self, handler):
        assert self._insert_wc(handler, {"j": True}, req=21)["ok"] == 1
        level = handler.get_database("test").db.execute(
            "PRAGMA synchronous"
        ).fetchone()[0]
        assert level == 2

    def test_majority_accepted_without_effect(self, handler):
        assert self._insert_wc(handler, {"w": "majority"}, req=22)["ok"] == 1
