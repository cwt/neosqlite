"""P0 multi-DB isolation: one SQLite file per logical database."""

import os

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler

from neosqlite import Connection


def _insert(handler, db_name, coll, docs, req=1):
    msg = {
        "request_id": req,
        "sections": [
            ("body", {"insert": coll, "$db": db_name}),
            ("payload_docs", docs),
        ],
    }
    return handler.handle_insert(msg)


def _find(handler, db_name, coll, filt=None, req=2):
    msg = {
        "request_id": req,
        "sections": [
            ("body", {"find": coll, "filter": filt or {}, "$db": db_name})
        ],
    }
    return handler.handle_command(msg)


@pytest.fixture
def mhandler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
    yield h
    h.close_all()


class TestMultiFileIsolation:
    def test_db_files_created_on_demand(self, mhandler, tmp_path):
        _insert(mhandler, "db1", "c", [{"x": 1}])
        _insert(mhandler, "db2", "c", [{"x": 2}])
        assert os.path.isfile(tmp_path / "db1.db")
        assert os.path.isfile(tmp_path / "db2.db")

    def test_collections_isolated(self, mhandler):
        _insert(mhandler, "db1", "c", [{"x": 1}])
        _insert(mhandler, "db2", "c", [{"x": 2}, {"x": 3}])
        _, r1 = _find(mhandler, "db1", "c")
        _, r2 = _find(mhandler, "db2", "c")
        assert len(r1["cursor"]["firstBatch"]) == 1
        assert len(r2["cursor"]["firstBatch"]) == 2

    def test_switch_back_and_forth(self, mhandler):
        for i in range(5):
            _insert(mhandler, "a", "c", [{"i": i}], req=10 + i)
            _insert(mhandler, "b", "c", [{"i": 100 + i}], req=20 + i)
        _, ra = _find(mhandler, "a", "c")
        _, rb = _find(mhandler, "b", "c")
        assert sorted(d["i"] for d in ra["cursor"]["firstBatch"]) == list(
            range(5)
        )
        assert sorted(d["i"] for d in rb["cursor"]["firstBatch"]) == list(
            range(100, 105)
        )

    def test_direct_connection_sees_same_data(self, mhandler, tmp_path):
        _insert(mhandler, "shop", "orders", [{"sku": "abc"}])
        with Connection(str(tmp_path / "shop.db")) as direct:
            docs = list(direct["orders"].find({}))
        assert [d["sku"] for d in docs] == ["abc"]

    def test_drop_database_removes_file(self, mhandler, tmp_path):
        _insert(mhandler, "gone", "c", [{"x": 1}])
        _insert(mhandler, "stays", "c", [{"x": 9}])
        msg = {
            "request_id": 7,
            "sections": [("body", {"dropDatabase": 1, "$db": "gone"})],
        }
        _, res = mhandler.handle_command(msg)
        assert res == {"dropped": "gone", "ok": 1}
        assert not os.path.exists(tmp_path / "gone.db")
        assert os.path.isfile(tmp_path / "stays.db")
        _, rs = _find(mhandler, "stays", "c")
        assert len(rs["cursor"]["firstBatch"]) == 1

    def test_list_databases_per_file_sizes(self, mhandler, tmp_path):
        _insert(mhandler, "one", "c", [{"x": 1}])
        _insert(mhandler, "two", "c", [{"x": 2}])
        msg = {
            "request_id": 8,
            "sections": [("body", {"listDatabases": 1})],
        }
        _, res = mhandler.handle_command(msg)
        by_name = {d["name"]: d for d in res["databases"]}
        assert set(by_name) >= {"one", "two", "admin"}
        assert by_name["one"]["sizeOnDisk"] == os.path.getsize(
            tmp_path / "one.db"
        )
        assert res["totalSize"] == sum(
            d["sizeOnDisk"] for d in by_name.values()
        )

    def test_invalid_db_name_rejected(self, mhandler):
        with pytest.raises(ValueError):
            mhandler.get_database("../evil")


class TestMemoryIsolation:
    def test_memory_dbs_isolated(self):
        h = NeoSQLiteHandler(":memory:")
        try:
            _insert(h, "m1", "c", [{"x": 1}])
            _insert(h, "m2", "c", [{"x": 2}, {"x": 3}])
            _, r1 = _find(h, "m1", "c")
            _, r2 = _find(h, "m2", "c")
            assert len(r1["cursor"]["firstBatch"]) == 1
            assert len(r2["cursor"]["firstBatch"]) == 2
        finally:
            h.close_all()

    def test_memory_drop_database(self):
        h = NeoSQLiteHandler(":memory:")
        try:
            _insert(h, "tmp", "c", [{"x": 1}])
            msg = {
                "request_id": 9,
                "sections": [("body", {"dropDatabase": 1, "$db": "tmp"})],
            }
            _, res = h.handle_command(msg)
            assert res["ok"] == 1
            _, rf = _find(h, "tmp", "c")
            assert rf["cursor"]["firstBatch"] == []
        finally:
            h.close_all()


class TestLegacySingleFileCompat:
    def test_file_path_stays_shared(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path / "legacy.db"))
        try:
            assert h._mode == "single"
            _insert(h, "db1", "c", [{"x": 1}])
            _, r = _find(h, "db2", "c")
            # Legacy limitation, documented: shared file sees all tables.
            assert len(r["cursor"]["firstBatch"]) == 1
        finally:
            h.conn.close()

    def test_sessions_scoped_per_db_in_multifile(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            _, r1 = h.handle_command(
                {
                    "request_id": 1,
                    "sections": [("body", {"startSession": 1, "$db": "d1"})],
                }
            )
            _, r2 = h.handle_command(
                {
                    "request_id": 2,
                    "sections": [("body", {"startSession": 1, "$db": "d2"})],
                }
            )
            s1 = r1["session"]["id"]["$oid"]
            s2 = r2["session"]["id"]["$oid"]
            assert h._find_session(s1, "d1") is not None
            assert h._find_session(s1, "d2") is None
            assert h._find_session(s2, "d2") is not None
        finally:
            h.close_all()

    def test_admin_routed_commit_in_multifile(self, tmp_path):
        """Real drivers commit on admin.$cmd while the tx lives on the
        data db; the commit must resolve across scopes."""
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            _, sr = h.handle_command(
                {
                    "request_id": 1,
                    "sections": [("body", {"startSession": 1, "$db": "admin"})],
                }
            )
            sid = sr["session"]["id"]["$oid"]
            lsid = {"id": {"$oid": sid}}
            _, ir = h.handle_insert(
                {
                    "request_id": 2,
                    "sections": [
                        (
                            "body",
                            {
                                "insert": "txc",
                                "$db": "data",
                                "lsid": lsid,
                                "startTransaction": True,
                            },
                        ),
                        ("payload_docs", [{"_id": 1}]),
                    ],
                }
            )
            assert ir["ok"] == 1
            _, cr = h.handle_command(
                {
                    "request_id": 3,
                    "sections": [
                        (
                            "body",
                            {
                                "commitTransaction": 1,
                                "$db": "admin",
                                "lsid": lsid,
                            },
                        )
                    ],
                }
            )
            assert cr == {"ok": 1}
            coll = h.get_database("data")["txc"]
            assert coll.count_documents({}) == 1
        finally:
            h.close_all()


class TestOpenDbLruCap:
    """Files mode caps open connections (LRU-close), memory mode must not."""

    def test_cap_and_eviction_roundtrip(self, mhandler, tmp_path):
        _insert(mhandler, "keep0", "c", [{"x": 1}])
        for i in range(150):
            mhandler.get_database(f"bulk{i}")
        assert len(mhandler._conns) <= 100
        assert "admin" in mhandler._conns
        # keep0 was evicted but its file survives and reopens with data.
        assert "keep0" not in mhandler._conns
        assert os.path.isfile(tmp_path / "keep0.db")
        _, r = _find(mhandler, "keep0", "c")
        assert len(r["cursor"]["firstBatch"]) == 1

    def test_list_databases_includes_evicted(self, mhandler, tmp_path):
        _insert(mhandler, "keep0", "c", [{"x": 1}])
        for i in range(150):
            mhandler.get_database(f"bulk{i}")
        msg = {"request_id": 5, "sections": [("body", {"listDatabases": 1})]}
        _, res = mhandler.handle_command(msg)
        names = {d["name"] for d in res["databases"]}
        assert "keep0" in names

    def test_admin_never_evicted(self, mhandler):
        admin_conn = mhandler._conns["admin"]
        for i in range(150):
            mhandler.get_database(f"bulk{i}")
        assert mhandler._conns["admin"] is admin_conn

    def test_db_with_open_tx_not_evicted(self, mhandler):
        _insert(mhandler, "txa", "c", [{"x": 0}])
        conn_a = mhandler._conns["txa"]
        lsid = {"id": {"$oid": "a" * 24}}
        _, ir = mhandler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    (
                        "body",
                        {
                            "insert": "c",
                            "$db": "txa",
                            "lsid": lsid,
                            "startTransaction": True,
                        },
                    ),
                    ("payload_docs", [{"x": 1}]),
                ],
            }
        )
        assert ir["ok"] == 1
        for i in range(150):
            mhandler.get_database(f"bulk{i}")
        assert len(mhandler._conns) <= 100
        assert mhandler._conns.get("txa") is conn_a

    def test_memory_mode_never_evicts(self):
        h = NeoSQLiteHandler(":memory:")
        try:
            conns = [h.get_database(f"m{i}") for i in range(120)]
            assert len(h._conns) >= 120
            for conn in conns:
                assert any(c is conn for c in h._conns.values())
        finally:
            h.close_all()


class TestCrossDbTransactionGuard:
    """Same lsid may not operate on another db while a tx is open."""

    def test_cross_db_write_rejected(self, mhandler):
        lsid = {"id": {"$oid": "b" * 24}}
        _, ir = mhandler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    (
                        "body",
                        {
                            "insert": "c",
                            "$db": "dbA",
                            "lsid": lsid,
                            "startTransaction": True,
                        },
                    ),
                    ("payload_docs", [{"x": 1}]),
                ],
            }
        )
        assert ir["ok"] == 1
        with pytest.raises(Exception, match="[Cc]ross-database"):
            mhandler.handle_insert(
                {
                    "request_id": 2,
                    "sections": [
                        ("body", {"insert": "c", "$db": "dbB", "lsid": lsid}),
                        ("payload_docs", [{"x": 2}]),
                    ],
                }
            )
        # The second insert never happened.
        _, rb = _find(mhandler, "dbB", "c")
        assert rb["cursor"]["firstBatch"] == []
        # And the first db's tx is still open/committable.
        _, cr = mhandler.handle_command(
            {
                "request_id": 3,
                "sections": [
                    (
                        "body",
                        {"commitTransaction": 1, "$db": "admin", "lsid": lsid},
                    )
                ],
            }
        )
        assert cr == {"ok": 1}
        coll = mhandler.get_database("dbA")["c"]
        assert coll.count_documents({}) == 1

    def test_cross_db_find_rejected(self, mhandler):
        lsid = {"id": {"$oid": "c" * 24}}
        _, ir = mhandler.handle_insert(
            {
                "request_id": 1,
                "sections": [
                    (
                        "body",
                        {
                            "insert": "c",
                            "$db": "dbA",
                            "lsid": lsid,
                            "startTransaction": True,
                        },
                    ),
                    ("payload_docs", [{"x": 1}]),
                ],
            }
        )
        assert ir["ok"] == 1
        with pytest.raises(Exception, match="[Cc]ross-database"):
            mhandler.handle_command(
                {
                    "request_id": 2,
                    "sections": [
                        ("body", {"find": "c", "$db": "dbB", "lsid": lsid})
                    ],
                }
            )

    def test_single_file_mode_keeps_shared_scope(self, tmp_path):
        """Legacy single-file mode shares one connection; same lsid
        transactions span logical dbs and must not error."""
        h = NeoSQLiteHandler(str(tmp_path / "legacy.db"))
        try:
            lsid = {"id": {"$oid": "d" * 24}}
            _, ir = h.handle_insert(
                {
                    "request_id": 1,
                    "sections": [
                        (
                            "body",
                            {
                                "insert": "c1",
                                "$db": "db1",
                                "lsid": lsid,
                                "startTransaction": True,
                            },
                        ),
                        ("payload_docs", [{"x": 1}]),
                    ],
                }
            )
            assert ir["ok"] == 1
            _, ir2 = h.handle_insert(
                {
                    "request_id": 2,
                    "sections": [
                        ("body", {"insert": "c2", "$db": "db2", "lsid": lsid}),
                        ("payload_docs", [{"x": 2}]),
                    ],
                }
            )
            assert ir2["ok"] == 1
        finally:
            h.conn.close()
