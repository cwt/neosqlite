"""P1/P2 wire routes for maintenance + estimate commands.

Explicit routes (no more silent fallback): ``vacuum``, ``compact``,
``validate``, ``reindex``/``reIndex``. Also covers ``readConcern``
acceptance and ``estimated_document_count`` over the count verb.
"""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path / "maint.db"))
    yield h
    h.conn.close()


def _cmd(handler, body, req=2):
    _, res = handler.handle_command(
        {"request_id": req, "sections": [("body", body)]}
    )
    return res


def _seed(handler, n=3, coll="c"):
    _, res = handler.handle_insert(
        {
            "request_id": 1,
            "sections": [
                ("body", {"insert": coll, "$db": "test"}),
                ("payload_docs", [{"i": i} for i in range(n)]),
            ],
        }
    )
    assert res["ok"] == 1


class TestVacuum:
    def test_vacuum_ok(self, handler):
        _seed(handler)
        res = _cmd(handler, {"vacuum": 1, "$db": "test"})
        assert res["ok"] == 1


class TestCompact:
    def test_compact_dry_run(self, handler):
        _seed(handler)
        res = _cmd(
            handler,
            {"compact": "c", "dryRun": True, "$db": "test"},
        )
        assert res["ok"] == 1
        assert "estimatedBytesFreed" in res
        # Dry run must not compact (no bytesFreed key).
        assert "bytesFreed" not in res

    def test_compact_real(self, handler):
        _seed(handler)
        res = _cmd(
            handler,
            {"compact": "c", "freeSpaceTargetMB": 0, "$db": "test"},
        )
        assert res["ok"] == 1
        assert "bytesFreed" in res


class TestValidate:
    def test_validate_collection(self, handler):
        _seed(handler)
        res = _cmd(handler, {"validate": "c", "$db": "test"})
        assert res["ok"] == 1
        assert res["valid"] is True

    def test_validate_requires_collection(self, handler):
        res = _cmd(handler, {"validate": 1, "$db": "test"})
        assert res["ok"] == 0
        assert "errmsg" in res


class TestReindex:
    def test_reindex_collection(self, handler):
        _seed(handler)
        res = _cmd(handler, {"reIndex": "c", "$db": "test"})
        assert res["ok"] == 1

    def test_reindex_whole_file(self, handler):
        _seed(handler)
        res = _cmd(handler, {"reindex": 1, "$db": "test"})
        assert res["ok"] == 1


class TestReadConcern:
    def test_read_concern_accepted_on_count(self, handler):
        _seed(handler)
        res = _cmd(
            handler,
            {
                "count": "c",
                "query": {},
                "readConcern": {"level": "majority"},
                "$db": "test",
            },
        )
        assert res["ok"] == 1
        assert res["n"] == 3

    def test_read_concern_accepted_on_find(self, handler):
        _seed(handler)
        res = _cmd(
            handler,
            {
                "find": "c",
                "filter": {},
                "readConcern": {"level": "local"},
                "$db": "test",
            },
        )
        assert res["ok"] == 1
        assert len(res["cursor"]["firstBatch"]) == 3


class TestEstimatedDocumentCount:
    def test_count_without_filter(self, handler):
        """PyMongo estimated_document_count sends the count verb with no
        query; the route must return the document count."""
        _seed(handler, n=5)
        res = _cmd(handler, {"count": "c", "$db": "test"})
        assert res["ok"] == 1
        assert res["n"] == 5

    def test_estimate_after_delete(self, handler):
        _seed(handler, n=5)
        _cmd(
            handler,
            {
                "delete": "c",
                "deletes": [{"q": {"i": 0}, "limit": 1}],
                "$db": "test",
            },
            req=3,
        )
        res = _cmd(handler, {"count": "c", "$db": "test"}, req=4)
        assert res["n"] == 4
