"""Regression tests for #206: transaction isolation, locking, DoS, and daemon."""

from __future__ import annotations

import asyncio
import os
import socket
import struct
import sys
import threading
import time
import tracemalloc
from unittest.mock import MagicMock, patch

import pytest
from nx_27017 import daemon as daemon_mod
from nx_27017 import nx_27017 as cli_mod
from nx_27017.handler import _MAX_SINGLE_MODE_DBS, NeoSQLiteHandler
from nx_27017.server import handle_client, handle_client_threaded
from nx_27017.wire_protocol import MAX_MESSAGE_SIZE_BYTES, WireProtocol


def _insert(
    handler: NeoSQLiteHandler,
    db_name: str,
    coll: str,
    docs: list[dict],
    lsid: dict | None = None,
    start_tx: bool = False,
    write_concern: dict | None = None,
    req: int = 1,
) -> dict:
    body: dict = {"insert": coll, "$db": db_name}
    if lsid is not None:
        body["lsid"] = lsid
    if start_tx:
        body["startTransaction"] = True
    if write_concern is not None:
        body["writeConcern"] = write_concern
    _, res = handler.handle_insert(
        {
            "request_id": req,
            "sections": [("body", body), ("payload_docs", docs)],
        }
    )
    return res


def _cmd(
    handler: NeoSQLiteHandler,
    body: dict,
    req: int = 2,
) -> dict:
    _, res = handler.handle_command(
        {"request_id": req, "sections": [("body", body)]}
    )
    return res


class TestTransactionIsolation:
    """Dedicated transactional Connections prevent dirty reads and wiped writes."""

    def test_files_mode_no_dirty_reads_and_abort_preserves_concurrent_write(
        self, tmp_path
    ):
        h = NeoSQLiteHandler(
            str(tmp_path), data_dir=str(tmp_path), tx_timeout_seconds=1.5
        )
        try:
            assert _insert(h, "txdb", "items", [{"seed": 1}])["ok"] == 1
            lsid = {"id": {"$oid": "1" * 24}}
            res = _insert(
                h,
                "txdb",
                "items",
                [{"from": "c1_uncommitted"}],
                lsid=lsid,
                start_tx=True,
                req=2,
            )
            assert res["ok"] == 1 and res["n"] == 1

            # Client 1 sees its own uncommitted row inside the transaction.
            tx_find = _cmd(
                h,
                {
                    "find": "items",
                    "filter": {"from": "c1_uncommitted"},
                    "projection": {"_id": 0},
                    "$db": "txdb",
                    "lsid": lsid,
                },
                req=3,
            )
            assert tx_find["cursor"]["firstBatch"] == [
                {"from": "c1_uncommitted"}
            ]

            # Client 2 (non-transactional) must NOT see uncommitted writes.
            c2_find = _cmd(
                h,
                {
                    "find": "items",
                    "filter": {"from": "c1_uncommitted"},
                    "projection": {"_id": 0},
                    "$db": "txdb",
                },
                req=4,
            )
            assert c2_find["cursor"]["firstBatch"] == []

            # Concurrent Client 2 write waits for Client 1 to abort, then
            # commits independently instead of being wiped by abortTransaction.
            c2_result: list[dict] = []

            def c2_writer() -> None:
                c2_result.append(
                    _insert(
                        h,
                        "txdb",
                        "items",
                        [{"from": "c2_acknowledged"}],
                        req=5,
                    )
                )

            t = threading.Thread(target=c2_writer)
            t.start()
            time.sleep(0.05)

            abort_res = _cmd(
                h,
                {"abortTransaction": 1, "$db": "admin", "lsid": lsid},
                req=6,
            )
            assert abort_res == {"ok": 1}
            t.join(timeout=5)
            assert not t.is_alive()
            assert c2_result and c2_result[0]["n"] == 1

            final = _cmd(
                h,
                {
                    "find": "items",
                    "filter": {},
                    "projection": {"_id": 0},
                    "$db": "txdb",
                },
                req=7,
            )
            docs = final["cursor"]["firstBatch"]
            assert {"seed": 1} in docs
            assert {"from": "c2_acknowledged"} in docs
            assert {"from": "c1_uncommitted"} not in docs
            assert len(h._session_tx_conns) == 0
        finally:
            h.close_all()

    def test_files_mode_same_thread_write_during_open_tx_rejected_not_wiped(
        self, tmp_path
    ):
        h = NeoSQLiteHandler(
            str(tmp_path), data_dir=str(tmp_path), tx_timeout_seconds=0.1
        )
        try:
            _insert(h, "txdb", "items", [{"seed": 1}])
            lsid = {"id": {"$oid": "2" * 24}}
            _insert(
                h,
                "txdb",
                "items",
                [{"from": "c1_uncommitted"}],
                lsid=lsid,
                start_tx=True,
                req=2,
            )

            # Same-thread write while tx is held must report writeErrors (112)
            # instead of falsely acknowledging ok:1/n:1 and later wiping it.
            c2_res = _insert(
                h, "txdb", "items", [{"from": "c2_blocked"}], req=3
            )
            assert c2_res["n"] == 0
            assert c2_res["writeErrors"][0]["code"] == 112

            assert (
                _cmd(
                    h,
                    {"abortTransaction": 1, "$db": "admin", "lsid": lsid},
                    req=4,
                )["ok"]
                == 1
            )
        finally:
            h.close_all()

    def test_files_mode_tx_commit_visible_across_commands(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            lsid = {"id": {"$oid": "3" * 24}}
            _insert(
                h,
                "txdb",
                "newcoll",
                [{"tag": "alpha"}, {"tag": "beta"}],
                lsid=lsid,
                start_tx=True,
                req=1,
            )
            # Inside tx, count/distinct/aggregate see uncommitted rows.
            assert (
                _cmd(
                    h,
                    {"count": "newcoll", "$db": "txdb", "lsid": lsid},
                    req=2,
                )["n"]
                == 2
            )
            assert sorted(
                _cmd(
                    h,
                    {
                        "distinct": "newcoll",
                        "key": "tag",
                        "$db": "txdb",
                        "lsid": lsid,
                    },
                    req=3,
                )["values"]
            ) == ["alpha", "beta"]
            assert (
                len(
                    _cmd(
                        h,
                        {
                            "aggregate": "newcoll",
                            "pipeline": [{"$match": {"tag": "alpha"}}],
                            "$db": "txdb",
                            "lsid": lsid,
                        },
                        req=4,
                    )["cursor"]["firstBatch"]
                )
                == 1
            )
            # Outside tx before commit, newcoll is invisible.
            assert (
                _cmd(
                    h,
                    {"find": "newcoll", "filter": {}, "$db": "txdb"},
                    req=5,
                )["cursor"]["firstBatch"]
                == []
            )
            # Commit makes data visible and closes the dedicated tx connection.
            assert (
                _cmd(
                    h,
                    {"commitTransaction": 1, "$db": "admin", "lsid": lsid},
                    req=6,
                )["ok"]
                == 1
            )
            assert len(h._session_tx_conns) == 0
            assert (
                len(
                    _cmd(
                        h,
                        {"find": "newcoll", "filter": {}, "$db": "txdb"},
                        req=7,
                    )["cursor"]["firstBatch"]
                )
                == 2
            )
        finally:
            h.close_all()

    def test_memory_mode_isolation_and_concurrent_writer_preserved(self):
        h = NeoSQLiteHandler(data_dir="memory", tx_timeout_seconds=0.5)
        try:
            _insert(h, "txdb", "items", [{"seed": 1}])
            lsid = {"id": {"$oid": "4" * 24}}
            _insert(
                h,
                "txdb",
                "items",
                [{"from": "c1_uncommitted"}],
                lsid=lsid,
                start_tx=True,
                req=2,
            )

            # Same-thread write while c1 holds tx must fail instead of being
            # falsely acknowledged and later wiped.
            c2_blocked = _insert(
                h, "txdb", "items", [{"from": "c2_blocked"}], req=3
            )
            assert c2_blocked["n"] == 0
            assert c2_blocked["writeErrors"][0]["code"] == 112

            # Concurrent Client 2 writer waits on _tx_cond and commits once c1
            # aborts.
            c2_result: list[dict] = []

            def c2_writer() -> None:
                c2_result.append(
                    _insert(
                        h,
                        "txdb",
                        "items",
                        [{"from": "c2_acknowledged"}],
                        req=4,
                    )
                )

            t = threading.Thread(target=c2_writer)
            t.start()
            time.sleep(0.05)

            assert (
                _cmd(
                    h,
                    {"abortTransaction": 1, "$db": "admin", "lsid": lsid},
                    req=5,
                )["ok"]
                == 1
            )
            t.join(timeout=5)
            assert not t.is_alive()
            assert c2_result and c2_result[0]["n"] == 1

            docs = _cmd(
                h,
                {
                    "find": "items",
                    "filter": {},
                    "projection": {"_id": 0},
                    "$db": "txdb",
                },
                req=6,
            )["cursor"]["firstBatch"]
            assert {"seed": 1} in docs
            assert {"from": "c2_acknowledged"} in docs
            assert {"from": "c1_uncommitted"} not in docs
        finally:
            h.close_all()

    def test_separate_memory_handlers_isolated(self):
        h1 = NeoSQLiteHandler(":memory:")
        h2 = NeoSQLiteHandler(":memory:")
        try:
            _insert(h1, "shared_name", "items", [{"owner": "h1"}])
            res2 = _cmd(
                h2, {"find": "items", "filter": {}, "$db": "shared_name"}
            )
            assert res2["cursor"]["firstBatch"] == []
        finally:
            h1.close_all()
            h2.close_all()


class TestLockingEvictionAndWriteConcern:
    """Per-DB locking on admin commands, active-op eviction, and _applied_wc."""

    def test_active_op_prevents_lru_eviction(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            _insert(h, "keep0", "c", [{"x": 1}])
            keep_conn = h._conns["keep0"]
            with h._track_active_op("keep0"):
                for i in range(150):
                    h.get_database(f"bulk{i}")
                assert h._conns.get("keep0") is keep_conn

            # After active op finishes, LRU eviction can evict keep0 normally.
            for i in range(150, 260):
                h.get_database(f"bulk{i}")
            assert "keep0" not in h._conns
        finally:
            h.close_all()

    def test_applied_wc_cleared_on_drop_and_eviction(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            # 1. Drop database invalidates _applied_wc.
            assert (
                _insert(
                    h, "wcdb", "c", [{"x": 1}], write_concern={"j": True}, req=1
                )["ok"]
                == 1
            )
            sync_before = (
                h.get_database("wcdb")
                .db.execute("PRAGMA synchronous")
                .fetchone()[0]
            )
            assert sync_before == 2  # FULL

            assert _cmd(h, {"dropDatabase": 1, "$db": "wcdb"}, req=2)["ok"] == 1
            assert "wcdb" not in h._applied_wc

            # Reopen and send same writeConcern -> must re-apply FULL (2).
            assert (
                _insert(
                    h, "wcdb", "c", [{"x": 2}], write_concern={"j": True}, req=3
                )["ok"]
                == 1
            )
            sync_after_drop = (
                h.get_database("wcdb")
                .db.execute("PRAGMA synchronous")
                .fetchone()[0]
            )
            assert sync_after_drop == 2

            # 2. LRU eviction invalidates _applied_wc.
            assert (
                _insert(
                    h,
                    "evictwc",
                    "c",
                    [{"x": 1}],
                    write_concern={"j": True},
                    req=4,
                )["ok"]
                == 1
            )
            for i in range(150):
                h.get_database(f"pad{i}")
            assert "evictwc" not in h._conns
            assert "evictwc" not in h._applied_wc

            assert (
                _insert(
                    h,
                    "evictwc",
                    "c",
                    [{"x": 2}],
                    write_concern={"j": True},
                    req=5,
                )["ok"]
                == 1
            )
            sync_after_evict = (
                h.get_database("evictwc")
                .db.execute("PRAGMA synchronous")
                .fetchone()[0]
            )
            assert sync_after_evict == 2
        finally:
            h.close_all()

    def test_admin_commands_lock_target_dbs_concurrently(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        errors: list[Exception] = []
        try:
            for name in ("db1", "db2"):
                _insert(h, name, "items", [{"seed": 1}])

            def run_bulk(worker_id: int) -> None:
                try:
                    for i in range(15):
                        res = _cmd(
                            h,
                            {
                                "bulkWrite": 1,
                                "ordered": True,
                                "ops": [
                                    {
                                        "insert": 0,
                                        "document": {"w": worker_id, "i": i},
                                    },
                                    {
                                        "insert": 1,
                                        "document": {"w": worker_id, "i": i},
                                    },
                                ],
                                "nsInfo": [
                                    {"ns": "db1.items"},
                                    {"ns": "db2.items"},
                                ],
                                "$db": "admin",
                            },
                            req=100 + worker_id * 100 + i,
                        )
                        assert res["ok"] == 1
                except Exception as exc:
                    errors.append(exc)

            def run_list_dbs() -> None:
                try:
                    for i in range(15):
                        res = _cmd(
                            h,
                            {"listDatabases": 1, "$db": "admin"},
                            req=900 + i,
                        )
                        assert res["ok"] == 1
                except Exception as exc:
                    errors.append(exc)

            threads = [
                threading.Thread(target=run_bulk, args=(0,)),
                threading.Thread(target=run_bulk, args=(1,)),
                threading.Thread(target=run_list_dbs),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            assert errors == []
        finally:
            h.close_all()


class TestBoundedMapsAndCursorSecurity:
    """Bounded locks/databases/sessions/cursors and cross-DB getMore guards."""

    def test_invalid_db_name_does_not_leak_lock(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            for bad in ("../evil", "bad db", "a" * 100, "foo/bar"):
                with pytest.raises(ValueError):
                    h.handle_command(
                        {
                            "request_id": 1,
                            "sections": [
                                ("body", {"find": "c", "$db": bad}),
                            ],
                        }
                    )
            assert len(h._locks) == 0
        finally:
            h.close_all()

    def test_single_mode_databases_map_is_bounded(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path / "single.db"))
        try:
            for i in range(_MAX_SINGLE_MODE_DBS + 50):
                h.get_database(f"db_{i}")
            assert len(h.databases) <= _MAX_SINGLE_MODE_DBS
            assert "admin" in h.databases
        finally:
            h.close_all()

    def test_idle_sessions_expire_and_rollback_tx(self, tmp_path):
        h = NeoSQLiteHandler(
            str(tmp_path),
            data_dir=str(tmp_path),
            session_timeout_seconds=0.05,
        )
        try:
            _insert(h, "txdb", "items", [{"seed": 1}])
            lsid = {"id": {"$oid": "5" * 24}}
            _insert(
                h,
                "txdb",
                "items",
                [{"abandoned": True}],
                lsid=lsid,
                start_tx=True,
                req=2,
            )
            assert len(h._session_tx_conns) == 1
            time.sleep(0.08)

            # Next session call triggers expiration & rolls back abandoned tx.
            _cmd(h, {"startSession": 1, "$db": "admin"}, req=3)
            assert len(h._session_tx_conns) == 0
            docs = _cmd(
                h,
                {
                    "find": "items",
                    "filter": {},
                    "projection": {"_id": 0},
                    "$db": "txdb",
                },
                req=4,
            )["cursor"]["firstBatch"]
            assert docs == [{"seed": 1}]
        finally:
            h.close_all()

    def test_idle_cursors_expire(self, tmp_path):
        h = NeoSQLiteHandler(
            str(tmp_path / "cur.db"), cursor_timeout_seconds=0.05
        )
        try:
            _insert(h, "test", "big", [{"i": i} for i in range(10)])
            res = _cmd(
                h,
                {"find": "big", "filter": {}, "batchSize": 2, "$db": "test"},
            )
            cid = res["cursor"]["id"]
            assert cid != 0
            time.sleep(0.08)

            res2 = _cmd(
                h,
                {"getMore": cid, "collection": "big", "$db": "test"},
                req=3,
            )
            assert res2["cursor"]["id"] == 0
            assert res2["cursor"]["nextBatch"] == []
            assert cid not in h._cursors
        finally:
            h.close_all()

    def test_getmore_and_killcursors_enforce_namespace_isolation(
        self, tmp_path
    ):
        h = NeoSQLiteHandler(str(tmp_path), data_dir=str(tmp_path))
        try:
            _insert(h, "db1", "secret", [{"v": i} for i in range(10)])
            res = _cmd(
                h,
                {"find": "secret", "filter": {}, "batchSize": 2, "$db": "db1"},
            )
            cid = res["cursor"]["id"]
            assert cid != 0

            # Cross-DB getMore must be rejected and not consume the cursor.
            bad_db = _cmd(
                h,
                {"getMore": cid, "collection": "secret", "$db": "db2"},
                req=3,
            )
            assert bad_db["ok"] == 0
            assert bad_db["code"] == 43

            # Cross-collection getMore on same DB must also be rejected.
            bad_coll = _cmd(
                h,
                {"getMore": cid, "collection": "other", "$db": "db1"},
                req=4,
            )
            assert bad_coll["ok"] == 0
            assert bad_coll["code"] == 43

            # Cross-DB killCursors must not kill db1's cursor.
            bad_kill = _cmd(
                h,
                {"killCursors": "secret", "cursors": [cid], "$db": "db2"},
                req=5,
            )
            assert bad_kill["cursorsKilled"] == []
            assert bad_kill["cursorsNotFound"] == [cid]

            # Original namespace still reads the remaining documents cleanly.
            good = _cmd(
                h,
                {"getMore": cid, "collection": "secret", "$db": "db1"},
                req=6,
            )
            assert good["ok"] == 1
            assert len(good["cursor"]["nextBatch"]) == 8

            # Change stream cross-DB getMore is also rejected.
            cs_res = _cmd(
                h,
                {
                    "aggregate": "events",
                    "pipeline": [{"$changeStream": {}}],
                    "$db": "db1",
                },
                req=7,
            )
            cs_id = cs_res["cursor"]["id"]
            cs_bad = _cmd(
                h,
                {"getMore": cs_id, "collection": "events", "$db": "db2"},
                req=8,
            )
            assert cs_bad["ok"] == 0
            assert cs_bad["code"] == 43
        finally:
            h.close_all()

    def test_cursor_ids_are_unpredictable(self, tmp_path):
        h = NeoSQLiteHandler(str(tmp_path / "rand.db"))
        try:
            _insert(h, "test", "big", [{"i": i} for i in range(10)])
            ids = []
            for req in range(5):
                res = _cmd(
                    h,
                    {
                        "find": "big",
                        "filter": {},
                        "batchSize": 2,
                        "$db": "test",
                    },
                    req=req + 10,
                )
                ids.append(res["cursor"]["id"])
            assert len(set(ids)) == 5
            assert all(cid > 0 for cid in ids)
            # Consecutive differences must not all equal 1.
            diffs = {ids[i + 1] - ids[i] for i in range(len(ids) - 1)}
            assert diffs != {1}
        finally:
            h.close_all()


class TestServerHardening:
    """Bounded frame reads, timeouts, connection caps, and off-loop cleanup."""

    def test_async_48mb_stalled_header_no_preallocation_and_timeout(self):
        async def run_test() -> int:
            h = NeoSQLiteHandler(":memory:")
            try:

                async def on_conn(
                    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
                ) -> None:
                    await handle_client(reader, writer, h, frame_timeout=0.08)

                srv = await asyncio.start_server(on_conn, "127.0.0.1", 0)
                port = srv.sockets[0].getsockname()[1]
                async with srv:
                    tracemalloc.start()
                    try:
                        c_reader, c_writer = await asyncio.open_connection(
                            "127.0.0.1", port
                        )
                        # Send 16-byte header claiming 48MB, then stall.
                        hdr = struct.pack(
                            "<iiii",
                            MAX_MESSAGE_SIZE_BYTES,
                            1,
                            0,
                            WireProtocol.OP_MSG,
                        )
                        c_writer.write(hdr)
                        await c_writer.drain()
                        # Server should close connection on timeout.
                        eof = await asyncio.wait_for(
                            c_reader.read(16), timeout=2.0
                        )
                        assert eof == b""
                        c_writer.close()
                        await c_writer.wait_closed()
                        _, peak = tracemalloc.get_traced_memory()
                        return peak
                    finally:
                        tracemalloc.stop()
            finally:
                h.close_all()

        peak_bytes = asyncio.run(run_test())
        # Must stay nowhere near 48 MB!
        assert peak_bytes < 5 * 1024 * 1024

    def test_threaded_48mb_stalled_header_times_out_safely(self):
        h = NeoSQLiteHandler(":memory:")
        s1, s2 = socket.socketpair()
        try:
            t = threading.Thread(
                target=handle_client_threaded,
                args=(s1, h),
                kwargs={"frame_timeout": 0.08},
            )
            t.start()
            hdr = struct.pack(
                "<iiii", MAX_MESSAGE_SIZE_BYTES, 1, 0, WireProtocol.OP_MSG
            )
            s2.sendall(hdr)
            t.join(timeout=2.0)
            assert not t.is_alive()
            assert h._active_connections == 0
        finally:
            s2.close()
            h.close_all()

    def test_max_connections_enforced_async(self):
        async def run_test() -> None:
            h = NeoSQLiteHandler(":memory:", max_connections=2)
            try:

                async def on_conn(
                    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
                ) -> None:
                    await handle_client(reader, writer, h, frame_timeout=1.0)

                srv = await asyncio.start_server(on_conn, "127.0.0.1", 0)
                port = srv.sockets[0].getsockname()[1]
                async with srv:
                    r1, w1 = await asyncio.open_connection("127.0.0.1", port)
                    r2, w2 = await asyncio.open_connection("127.0.0.1", port)
                    await asyncio.sleep(0.05)
                    assert h._active_connections == 2

                    # Third connection exceeds cap and is closed immediately.
                    r3, w3 = await asyncio.open_connection("127.0.0.1", port)
                    data3 = await asyncio.wait_for(r3.read(16), timeout=2.0)
                    assert data3 == b""
                    assert h._active_connections == 2

                    for w in (w1, w2, w3):
                        w.close()
                        await w.wait_closed()
                    await asyncio.sleep(0.05)
                    assert h._active_connections == 0
            finally:
                h.close_all()

        asyncio.run(run_test())

    def test_max_connections_enforced_threaded(self):
        h = NeoSQLiteHandler(":memory:", max_connections=1)
        s1a, s1b = socket.socketpair()
        s2a, s2b = socket.socketpair()
        try:
            t1 = threading.Thread(
                target=handle_client_threaded,
                args=(s1a, h),
                kwargs={"frame_timeout": 1.0},
            )
            t1.start()
            time.sleep(0.05)
            assert h._active_connections == 1

            # Second connection is rejected immediately.
            handle_client_threaded(s2a, h, frame_timeout=0.5)
            assert h._active_connections == 1

            s1b.close()
            t1.join(timeout=2.0)
            assert h._active_connections == 0
        finally:
            s2b.close()
            h.close_all()

    def test_async_disconnect_offloads_stream_cleanup_to_thread(self):
        async def run_test() -> None:
            h = NeoSQLiteHandler(":memory:")
            calls: list[tuple] = []
            orig_to_thread = asyncio.to_thread

            async def spy_to_thread(func, *args, **kwargs):
                calls.append((func, args))
                return await orig_to_thread(func, *args, **kwargs)

            try:
                with patch("asyncio.to_thread", side_effect=spy_to_thread):
                    reader = asyncio.StreamReader()
                    reader.feed_eof()
                    writer = MagicMock()
                    writer.wait_closed = MagicMock(
                        return_value=asyncio.sleep(0)
                    )
                    await handle_client(reader, writer, h)
                assert any(
                    fn == h.close_streams_for_connection for fn, _ in calls
                )
            finally:
                h.close_all()

        asyncio.run(run_test())


class TestDaemonAndCliHardening:
    """Daemon path resolution, stdin redirect, PID safety, and CLI exit codes."""

    def test_run_as_daemon_resolves_relative_log_and_pid_paths(self, tmp_path):
        args = MagicMock()
        args.pid_file = "relative.pid"
        args.log_file = "relative.log"
        args.db_path = "relative.db"
        args.data_dir = None
        args.fts5_tokenizers = None

        with patch.object(daemon_mod, "daemonize", side_effect=SystemExit(0)):
            with pytest.raises(SystemExit):
                daemon_mod.run_as_daemon(args)

        assert os.path.isabs(args.pid_file)
        assert os.path.isabs(args.log_file)
        assert os.path.isabs(args.db_path)

    def test_daemonize_redirects_stdin_to_devnull(self):
        dup_calls: list[tuple[int, int]] = []

        def fake_dup2(src: int, dst: int) -> None:
            dup_calls.append((src, dst))

        with (
            patch("os.fork", return_value=0, create=True),
            patch("os.chdir"),
            patch("os.setsid", create=True),
            patch("os.dup2", side_effect=fake_dup2),
        ):
            fake_stdin = MagicMock()
            fake_stdin.fileno.return_value = 0
            with patch.object(sys, "stdin", fake_stdin):
                daemon_mod.daemonize()

        assert any(dst == 0 for _src, dst in dup_calls)

    def test_pid_file_refuses_symlink_and_enforces_lock(self, tmp_path):
        target = tmp_path / "target.txt"
        target.write_text("keep-me")
        link = tmp_path / "nx.pid"
        try:
            os.symlink(target, link)
        except OSError:
            pytest.skip("Symlinks unavailable on this platform")

        assert daemon_mod.write_pid_file(str(link)) is False
        assert daemon_mod.get_pid(str(link)) is None
        assert target.read_text() == "keep-me"

        # Normal PID file acquires lock; second write fails while running.
        pid_file = tmp_path / "real.pid"
        assert daemon_mod.write_pid_file(str(pid_file)) is True
        assert daemon_mod.get_pid(str(pid_file)) == os.getpid()
        assert daemon_mod.write_pid_file(str(pid_file)) is False
        daemon_mod.remove_pid_file(str(pid_file))
        assert not pid_file.exists()

    def test_stop_daemon_refuses_unrelated_process_on_linux(self, tmp_path):
        if not os.path.exists(f"/proc/{os.getpid()}/cmdline"):
            pytest.skip("/proc/<pid>/cmdline not available on this platform")
        pid_file = tmp_path / "other.pid"
        pid_file.write_text(str(os.getpid()))
        with (
            patch.object(daemon_mod, "_is_pid_running", return_value=True),
            patch("os.kill") as mock_kill,
        ):
            rc = daemon_mod.stop_daemon(str(pid_file))
        assert rc == 1
        mock_kill.assert_not_called()

    def test_fts5_tokenizer_cli_validation(self):
        with patch.object(
            sys,
            "argv",
            ["nx_27017", "--fts5-tokenizer", "missing_equals_sign"],
        ):
            with pytest.raises(SystemExit) as exc_info:
                cli_mod.main()
            assert exc_info.value.code == 2

    def test_run_server_sync_closes_handler_on_exit(self, tmp_path):
        args = MagicMock()
        args.db_path = str(tmp_path / "sync.db")
        args.host = "127.0.0.1"
        args.port = 27017
        args.journal_mode = "WAL"
        args.fts5_tokenizers = None
        args.threaded = False
        args.data_dir = None
        args.single_db_compat = False

        closed: list[bool] = []
        orig_close_all = NeoSQLiteHandler.close_all

        def spy_close_all(self: NeoSQLiteHandler) -> None:
            closed.append(True)
            orig_close_all(self)

        with (
            patch.object(NeoSQLiteHandler, "close_all", spy_close_all),
            patch.object(
                daemon_mod,
                "run_server_threaded",
                side_effect=KeyboardInterrupt,
            ),
        ):
            daemon_mod.run_server_sync(args)

        assert closed == [True]
