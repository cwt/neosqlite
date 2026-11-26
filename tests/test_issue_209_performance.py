"""Regression tests for Issue #209 performance bottlenecks."""

from __future__ import annotations

import hashlib
from unittest.mock import patch

import neosqlite
from neosqlite.gridfs import GridFS, GridFSBucket
from neosqlite.migration_cli import (
    _all_table_names,
    find_legacy_tables,
    migrate_database,
)


def test_migration_cli_find_and_migrate_avoids_duplicate_pass(tmp_path):
    """migrate_database should compute legacy tables once across non-dry-run."""
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"

    conn = neosqlite._sqlite.sqlite3.connect(str(src))
    try:
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, _id TEXT, data TEXT)"
        )
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, _id TEXT, data TEXT, "
            "user_id INTEGER REFERENCES users(id))"
        )
        conn.execute(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY, _id TEXT, data TEXT)"
        )
        conn.commit()
    finally:
        conn.close()

    inspect_conn = neosqlite._sqlite.sqlite3.connect(str(src))
    try:
        assert len(_all_table_names(inspect_conn)) == 3
        found = find_legacy_tables(inspect_conn)
        assert [t.name for t in found] == ["logs"]
    finally:
        inspect_conn.close()

    call_count = 0
    orig_find = find_legacy_tables

    def tracked_find(db_conn):
        nonlocal call_count
        call_count += 1
        return orig_find(db_conn)

    with patch("neosqlite.migration_cli.find_legacy_tables", tracked_find):
        result = migrate_database(str(src), str(dst))

    assert result.tables == ("logs",)
    assert call_count == 1


def test_changestream_buffers_rows_and_prunes_once_per_batch():
    """ChangeStream should buffer fetched batch rows and prune once per batch."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn["events"]
        prune_calls = 0

        with coll.watch(batch_size=5, max_await_time_ms=100) as stream:
            orig_prune = stream._prune_consumed

            def tracked_prune():
                nonlocal prune_calls
                prune_calls += 1
                return orig_prune()

            stream._prune_consumed = tracked_prune

            inserted = coll.insert_many(
                [{"i": i} for i in range(5)]
            ).inserted_ids
            delivered = [next(stream) for _ in range(5)]

            assert [d["documentKey"]["_id"] for d in delivered] == inserted
            assert len(delivered) == 5
            assert prune_calls == 1
            assert stream.resume_token == delivered[-1]["_id"]


def test_gridfs_cursor_list_and_streaming_upload():
    """GridOutCursor batches metadata, GridFS.list delegates, and upload streams chunks."""

    class ChunkedReader:
        """Stream yielding small uneven slices and refusing full reads."""

        def __init__(self, payload: bytes, step: int = 7) -> None:
            self._payload = payload
            self._pos = 0
            self._step = step

        def read(self, size: int = -1) -> bytes:
            if size is None or size < 0:
                raise AssertionError(
                    "upload_from_stream must read bounded chunks"
                )
            take = min(size, self._step)
            chunk = self._payload[self._pos : self._pos + take]
            self._pos += len(chunk)
            return chunk

    with neosqlite.Connection(":memory:") as conn:
        bucket = GridFSBucket(conn.db, chunk_size_bytes=32)
        payload = b"0123456789abcdefghijklmnopqrstuvwxyz" * 5
        oid1 = bucket.upload_from_stream(
            "streamed.bin", ChunkedReader(payload, step=7), metadata={"n": 1}
        )
        bucket.upload_from_stream_with_id(
            "custom-stream",
            "second.bin",
            ChunkedReader(payload, step=11),
            metadata={"n": 2},
        )
        bucket.upload_from_stream("streamed.bin", b"revision2")

        assert bucket.open_download_stream(oid1).read() == payload
        assert bucket.open_download_stream("custom-stream").read() == payload
        assert (
            bucket.open_download_stream(oid1).md5
            == hashlib.md5(payload).hexdigest()
        )

        # Iterating GridOutCursor does not re-query per file via GridOut.__init__
        with patch.object(
            GridFSBucket,
            "_force_sync_if_needed",
            wraps=bucket._force_sync_if_needed,
        ):
            cur = bucket.find().sort("filename", 1).batch_size(2)
            assert cur._file_ids is None
            first = next(cur)
            assert first.filename == "second.bin"
            assert first.metadata == {"n": 2}
            rest = cur.to_list()
            assert [g.filename for g in rest] == [
                "streamed.bin",
                "streamed.bin",
            ]
            cur.close()

        legacy = GridFS(conn.db)
        assert legacy.list() == ["second.bin", "streamed.bin"]


def test_window_exp_moving_avg_and_bucket_addtoset():
    """$expMovingAvg matches O(N^2) semantics in O(N) and $addToSet deduplicates via keys."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn["metrics"]
        coll.insert_many(
            [
                {"p": "A", "t": 1, "v": 10.0, "tag": {"k": 1}},
                {"p": "A", "t": 2, "v": None, "tag": {"k": 1}},
                {"p": "A", "t": 3, "v": 20.0, "tag": [1, 2]},
                {"p": "A", "t": 4, "v": 30.0, "tag": [1, 2]},
                {"p": "B", "t": 1, "v": 5.0, "tag": True},
                {"p": "B", "t": 2, "v": 15.0, "tag": 1},
            ]
        )

        win_res = coll.aggregate(
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$p",
                        "sortBy": {"t": 1},
                        "output": {
                            "ema": {
                                "$expMovingAvg": {"input": "$v", "alpha": 0.5}
                            }
                        },
                    }
                }
            ]
        ).to_list()
        by_key = {(d["p"], d["t"]): d["ema"] for d in win_res}
        assert by_key[("A", 1)] == 10.0
        assert by_key[("A", 2)] == 10.0
        assert by_key[("A", 3)] == 15.0
        assert by_key[("A", 4)] == 22.5
        assert by_key[("B", 1)] == 5.0
        assert by_key[("B", 2)] == 10.0

        from neosqlite.collection.expr_evaluator import ExprEvaluator
        from neosqlite.collection.query_engine.python_aggregation_engine import (
            _compute_bucket_output,
        )

        docs = [
            {"tag": {"k": 1}},
            {"tag": {"k": 1}},
            {"tag": [1, 2]},
            {"tag": True},
            {"tag": 1},
        ]
        out = _compute_bucket_output(
            coll.query_engine,
            0,
            docs,
            {"tags": {"$addToSet": "$tag"}},
            evaluator=ExprEvaluator(),
        )
        assert out["tags"] == [{"k": 1}, [1, 2], True, 1]


def test_query_builder_hoists_fts_and_expr_evaluator():
    """Python fallback queries hoist FTS table lookups and ExprEvaluator across docs."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn["docs"]
        coll.create_index("body", fts=True)
        coll.insert_many(
            [{"body": f"alpha item {i}", "a": i, "b": 2} for i in range(10)]
        )

        helper = coll.query_engine.helpers
        with patch.object(
            helper,
            "_get_fts_field_names",
            wraps=helper._get_fts_field_names,
        ) as spy_fts:
            from neosqlite.collection.query_helper.utils import (
                set_force_fallback,
            )

            set_force_fallback(True)
            try:
                matched = list(
                    coll.find(
                        {
                            "$and": [
                                {"$text": {"$search": "alpha"}},
                                {"$expr": {"$gt": ["$a", 5]}},
                            ]
                        }
                    )
                )
            finally:
                set_force_fallback(False)

        assert [d["a"] for d in matched] == [6, 7, 8, 9]
        assert spy_fts.call_count == 1


def test_metadata_caching_optimizer_and_aggregation_cursor():
    """Avoid redundant sqlite_master/PRAGMA queries and unused result size estimation."""
    with neosqlite.Connection(":memory:") as conn:
        c1 = conn["items"]
        c2 = conn["other"]
        c1.insert_many([{"x": i, "txt": "hello world"} for i in range(10)])
        c2.insert_one({"y": 1})
        c1.create_index("x")

        stats = conn.command("dbStats")
        assert stats["objects"] == 11

        # _estimate_result_size is skipped when _use_quez is False
        cursor = c1.aggregate([{"$match": {"x": {"$gte": 0}}}])
        with patch.object(
            cursor,
            "_estimate_result_size",
            side_effect=AssertionError(
                "should not be called when _use_quez=False"
            ),
        ):
            assert len(cursor.to_list()) == 10

        # _estimate_query_cost calls _get_indexed_fields once across compound trees
        helper = c1.query_engine.helpers
        with patch.object(
            helper, "_get_indexed_fields", wraps=helper._get_indexed_fields
        ) as spy_idx:
            cost = helper._estimate_query_cost(
                {
                    "$and": [
                        {"x": 1},
                        {"$or": [{"x": 2}, {"unindexed": 3}]},
                    ]
                }
            )
            assert cost < 1.0
            assert spy_idx.call_count == 1


def test_graph_lookup_bfs_frontier_and_cycles():
    """$graphLookup level-batched BFS handles multi-hop, maxDepth, cycles, and filters."""
    with neosqlite.Connection(":memory:") as conn:
        nodes = conn["nodes"]
        nodes.insert_many(
            [
                {"_id": 1, "name": "A", "next": [2, 3], "active": True},
                {"_id": 2, "name": "B", "next": [4, 1], "active": True},
                {"_id": 3, "name": "C", "next": [4], "active": False},
                {"_id": 4, "name": "D", "next": [5], "active": True},
                {"_id": 5, "name": "E", "next": [], "active": True},
            ]
        )
        starts = conn["starts"]
        starts.insert_one({"_id": 100, "entry": 1})

        res = starts.aggregate(
            [
                {
                    "$graphLookup": {
                        "from": "nodes",
                        "startWith": "$entry",
                        "connectFromField": "next",
                        "connectToField": "_id",
                        "as": "reachable",
                        "maxDepth": 2,
                        "depthField": "depth",
                        "restrictSearchWithMatch": {"active": True},
                    }
                }
            ]
        ).to_list()
        assert len(res) == 1
        reached = {(d["_id"], d["depth"]) for d in res[0]["reachable"]}
        assert reached == {(1, 0), (2, 1), (4, 2)}
