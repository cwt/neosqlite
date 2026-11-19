"""Tests for JavaScript expression/query operators across sandboxed engines."""

from __future__ import annotations

import io
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime

import pytest

import neosqlite
from neosqlite.js import (
    JsboxWorker,
    JsWorkerError,
    NodeWorker,
    close_js_workers,
    use_js_engine,
)
from neosqlite.js.engines.jsbox_engine import probe_jsbox
from neosqlite.js_runtime import NodeRuntime
from neosqlite.js_worker import close_worker, get_worker
from neosqlite.objectid import ObjectId

_NODE = shutil.which("node")
_HAS_JSBOX = probe_jsbox().available


@pytest.fixture
def supported_node(monkeypatch):
    """Bypass the Node version gate with the local binary for wiring tests."""
    if not _NODE:
        pytest.skip("node not installed")
    runtime = NodeRuntime(path=_NODE, version=(26, 13, 1))

    def _fake_find_node() -> NodeRuntime:
        return runtime

    monkeypatch.setattr(
        "neosqlite.js_runtime.find_node_runtime", _fake_find_node
    )
    close_worker()
    with use_js_engine("node"):
        yield runtime
    close_worker()


@pytest.fixture(params=["jsbox", "node"])
def active_engine(request, monkeypatch):
    """Parametrize operator tests over both jsbox and node (skip if unavailable)."""
    engine_name = request.param
    if engine_name == "jsbox":
        if not _HAS_JSBOX:
            pytest.skip("jsbox not installed")
    elif engine_name == "node":
        if not _NODE:
            pytest.skip("node not installed")
        runtime = NodeRuntime(path=_NODE, version=(26, 13, 1))

        def _fake_find_node() -> NodeRuntime:
            return runtime

        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )
    close_js_workers()
    with use_js_engine(engine_name):
        yield engine_name
    close_js_workers()


def test_function_aggregation_expression(active_engine):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "price": 10, "qty": 3},
                {"_id": 2, "price": 5, "qty": 2},
            ]
        )
        results = list(
            coll.aggregate(
                [
                    {
                        "$addFields": {
                            "total": {
                                "$function": {
                                    "body": (
                                        "function(p, q) { return p * q; }"
                                    ),
                                    "args": ["$price", "$qty"],
                                    "lang": "js",
                                }
                            }
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        )
    assert [doc["total"] for doc in results] == [30, 10]


def test_function_with_object_and_array_args(active_engine):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "items": [1, 2, 3]})
        result = next(
            iter(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "sum": {
                                    "$function": {
                                        "body": (
                                            "function(xs) {"
                                            " return xs.reduce((a,b)=>a+b,0); }"
                                        ),
                                        "args": ["$items"],
                                        "lang": "js",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        )
    assert result["sum"] == 6


def test_where_expression_form(active_engine):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "n": 5},
                {"_id": 2, "n": 20},
            ]
        )
        results = list(coll.find({"$where": "this.n > 10"}))
    assert [doc["_id"] for doc in results] == [2]


def test_where_function_form(active_engine):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "n": 5},
                {"_id": 2, "n": 20},
            ]
        )
        results = list(
            coll.find({"$where": "function() { return this.n > 10; }"})
        )
    assert [doc["_id"] for doc in results] == [2]


def test_where_bson_this_arg_markers(active_engine):
    """thisArg must deserialize $oid (with toString) and $date (as Date)."""
    oid_a = ObjectId("507f1f77bcf86cd799439011")
    oid_b = ObjectId("507f1f77bcf86cd799439012")
    dt_old = datetime(2024, 1, 1, tzinfo=UTC)
    dt_new = datetime(2026, 6, 1, tzinfo=UTC)
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": oid_a, "created": dt_old},
                {"_id": oid_b, "created": dt_new},
            ]
        )
        by_oid = list(
            coll.find(
                {
                    "$where": (
                        "function() { return this._id.toString() === "
                        "'507f1f77bcf86cd799439012'; }"
                    )
                }
            )
        )
        by_date = list(
            coll.find(
                {
                    "$where": (
                        "function() { return (this.created instanceof Date) "
                        "&& this.created.getUTCFullYear() >= 2026; }"
                    )
                }
            )
        )
    assert [doc["_id"] for doc in by_oid] == [oid_b]
    assert [doc["_id"] for doc in by_date] == [oid_b]


def test_where_works_across_crud_and_match(active_engine):
    """$where must work in count, distinct, update, delete, and $match."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_many(
            [
                {"_id": 1, "tag": "a", "n": 5},
                {"_id": 2, "tag": "b", "n": 15},
                {"_id": 3, "tag": "c", "n": 25},
            ]
        )
        assert coll.count_documents({"$where": "this.n > 10"}) == 2
        assert sorted(coll.distinct("tag", {"$where": "this.n > 10"})) == [
            "b",
            "c",
        ]
        agg = list(
            coll.aggregate(
                [
                    {"$match": {"$where": "this.n >= 15"}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        assert [doc["_id"] for doc in agg] == [2, 3]

        upd = coll.update_one(
            {"$where": "this.n === 15"},
            {"$set": {"updated": True}},
        )
        assert upd.modified_count == 1
        assert coll.find_one({"_id": 2})["updated"] is True

        deleted = coll.delete_one({"$where": "this.n === 25"})
        assert deleted.deleted_count == 1
        assert coll.count_documents({}) == 2


def test_function_bson_roundtrip_and_bigint(active_engine):
    oid = ObjectId("507f1f77bcf86cd799439011")
    dt = datetime(2026, 5, 15, 12, 30, 0, tzinfo=UTC)
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": oid, "ts": dt})
        results = list(
            coll.aggregate(
                [
                    {
                        "$project": {
                            "same_id": {
                                "$function": {
                                    "body": "function(x) { return x; }",
                                    "args": ["$_id"],
                                    "lang": "js",
                                }
                            },
                            "next_day": {
                                "$function": {
                                    "body": (
                                        "function(d) { return new Date("
                                        "d.getTime() + 86400000); }"
                                    ),
                                    "args": ["$ts"],
                                    "lang": "js",
                                }
                            },
                            "big": {
                                "$function": {
                                    "body": (
                                        "function() { return "
                                        "9007199254740993n; }"
                                    ),
                                    "args": [],
                                    "lang": "js",
                                }
                            },
                        }
                    }
                ]
            )
        )
    row = results[0]
    assert row["same_id"] == oid
    assert row["next_day"] == datetime(2026, 5, 16, 12, 30, 0, tzinfo=UTC)
    assert row["big"] == 9007199254740993


def test_js_error_is_surfaced(active_engine):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "n": 1})
        with pytest.raises(ValueError, match="JavaScript error"):
            list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "x": {
                                    "$function": {
                                        "body": (
                                            "function(){throw new Error('boom')}"
                                        ),
                                        "args": [],
                                        "lang": "js",
                                    }
                                }
                            }
                        }
                    ]
                )
            )


def test_worker_unit_call(active_engine):
    from neosqlite.js import get_js_worker

    worker = get_js_worker()
    assert worker is not None
    assert worker.engine_name == active_engine
    assert worker.call("function(a,b){return a+b}", [2, 3]) == 5


def test_sandbox_has_no_process_or_env_access(active_engine, monkeypatch):
    """Neither direct access nor constructor escapes may reach process/env."""
    from neosqlite.js import get_js_worker

    monkeypatch.setenv("NEOSQLITE_SECRET_MARKER", "SHOULD_NOT_LEAK")
    close_js_workers()
    worker = get_js_worker()
    assert worker is not None
    assert worker.call("function(){return typeof process}", []) == "undefined"
    assert (
        worker.call(
            "function(){return this.constructor.constructor("
            "'return typeof process')()}",
            [],
            this_arg={"x": 1},
        )
        == "undefined"
    )
    with pytest.raises(JsWorkerError):
        worker.call(
            "function(){return process.env.NEOSQLITE_SECRET_MARKER}", []
        )


def test_env_secret_not_readable_via_function(active_engine, monkeypatch):
    monkeypatch.setenv("NEOSQLITE_SECRET_MARKER", "SHOULD_NOT_LEAK")
    close_js_workers()
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "n": 1})
        results = list(
            coll.aggregate(
                [
                    {
                        "$project": {
                            "secret": {
                                "$function": {
                                    "body": (
                                        "function(){"
                                        "return (typeof process !== 'undefined' "
                                        "&& process.env "
                                        "&& process.env.NEOSQLITE_SECRET_MARKER)"
                                        " || 'ABSENT'}"
                                    ),
                                    "args": [],
                                    "lang": "js",
                                }
                            }
                        }
                    }
                ]
            )
        )
    assert results[0]["secret"] == "ABSENT"


def test_cross_call_prototype_and_global_isolation(active_engine):
    """Mutating globals/prototypes in one call must not affect later calls."""
    from neosqlite.js import get_js_worker

    worker = get_js_worker()
    assert worker is not None
    res1 = worker.call(
        "function(){"
        "globalThis.leaked = 'secret';"
        "Object.prototype.polluted = 'yes';"
        "JSON.stringify = function(){ return 'hacked'; };"
        "return 123;"
        "}",
        [],
    )
    assert res1 == 123
    res2 = worker.call(
        "function(){"
        "return [typeof globalThis.leaked, ({}).polluted || 'clean'];"
        "}",
        [],
    )
    assert res2 == ["undefined", "clean"]


def test_async_function_resolves(active_engine):
    from neosqlite.js import get_js_worker

    worker = get_js_worker()
    assert worker is not None
    assert worker.call("async function(){return 42}", []) == 42


def test_rejected_promise_does_not_kill_worker(active_engine):
    from neosqlite.js import get_js_worker

    worker = get_js_worker()
    assert worker is not None
    with pytest.raises(JsWorkerError, match="rejected"):
        worker.call("async function(){throw new Error('rejected')}", [])
    assert worker.call("function(){return 7}", []) == 7


def test_infinite_loop_timeout_and_recovery(active_engine):
    """Infinite loops must time out cleanly and keep the worker usable."""
    if active_engine == "jsbox":
        worker = JsboxWorker(timeout=0.3)
    else:
        worker = NodeWorker(
            NodeRuntime(path=_NODE, version=(26, 13, 1)), timeout=0.3
        )
    with worker:
        with pytest.raises(JsWorkerError, match="timed out"):
            worker.call("function(){ while (true) {} }")
        assert worker.call("function(a,b){return a+b}", [4, 5]) == 9


def test_worker_crash_isolation_and_transparent_restart(active_engine):
    """Killing the worker (simulating C/memory crash) must not affect host."""
    if active_engine == "jsbox":
        worker = JsboxWorker(timeout=2.0)
    else:
        worker = NodeWorker(
            NodeRuntime(path=_NODE, version=(26, 13, 1)), timeout=2.0
        )
    with worker:
        assert worker.call("function(){return 1}") == 1
        assert worker._proc is not None
        old_pid = worker._proc.pid
        assert old_pid != os.getpid()

        # Simulate fatal worker termination (e.g. SIGSEGV / OOM killer).
        worker._proc.kill()
        worker._proc.wait(timeout=2)

        # Next call transparently restarts a fresh worker process.
        assert worker.call("function(){return 2}") == 2
        assert worker._proc is not None
        assert worker._proc.pid != old_pid


@pytest.mark.skipif(not _HAS_JSBOX, reason="jsbox not installed")
def test_jsbox_heap_limit_and_recovery():
    """jsbox enforces heap caps cleanly and stays usable afterwards."""
    with JsboxWorker(timeout=2.0, heap_mb=8) as worker:
        with pytest.raises(JsWorkerError, match="out of memory"):
            worker.call(
                "function(){const a=[];while(true)a.push('x'.repeat(1048576));}"
            )
        assert worker.call("function(a,b){return a+b}", [2, 3]) == 5

        with pytest.raises(JsWorkerError):
            worker.call("function(){let x=3n; while(true) x = x * x;}")
        assert worker.call("function(a,b){return a*b}", [6, 7]) == 42


def test_supervisor_hard_kills_hung_worker_process():
    """Supervisor must hard-kill a worker that ignores engine-level timeouts."""
    from neosqlite.js import SubprocessJsWorker

    class _HangingWorker(SubprocessJsWorker):
        engine_name = "hangtest"

        def _build_command(self) -> list[str]:
            script = (
                "import json, sys, time\n"
                "for line in sys.stdin:\n"
                "    req = json.loads(line)\n"
                "    if req['code'] == 'hang':\n"
                "        time.sleep(30)\n"
                "    sys.stdout.write("
                "json.dumps({'id': req['id'], 'ok': True, 'value': 'ok'}) + '\\n'"
                ")\n"
                "    sys.stdout.flush()\n"
            )
            return [sys.executable, "-I", "-u", "-c", script]

    with _HangingWorker(timeout=0.2) as worker:
        assert worker.call("ok") == "ok"
        assert worker._proc is not None
        old_pid = worker._proc.pid

        with pytest.raises(JsWorkerError, match="timed out"):
            worker.call("hang")

        # Worker was killed and replaced with a fresh process.
        assert worker._proc is not None
        assert worker._proc.pid != old_pid
        assert worker.call("ok") == "ok"


def test_supervisor_recovers_from_framing_errors():
    """Invalid JSON or response id mismatch must restart worker cleanly."""
    from neosqlite.js import SubprocessJsWorker

    class _BadFrameWorker(SubprocessJsWorker):
        engine_name = "badframe"

        def _build_command(self) -> list[str]:
            script = (
                "import json, sys\n"
                "for line in sys.stdin:\n"
                "    req = json.loads(line)\n"
                "    if req['code'] == 'garbage':\n"
                "        sys.stdout.write('not json\\n')\n"
                "    elif req['code'] == 'wrong_id':\n"
                "        sys.stdout.write("
                "json.dumps({'id': -1, 'ok': True, 'value': 0}) + '\\n')\n"
                "    else:\n"
                "        sys.stdout.write("
                "json.dumps({'id': req['id'], 'ok': True, 'value': 7}) + '\\n')\n"
                "    sys.stdout.flush()\n"
            )
            return [sys.executable, "-I", "-u", "-c", script]

    with _BadFrameWorker(timeout=2.0) as worker:
        assert worker.call("ok") == 7
        with pytest.raises(JsWorkerError, match="Invalid JS worker response"):
            worker.call("garbage")
        assert worker.call("ok") == 7
        with pytest.raises(JsWorkerError, match="id mismatch"):
            worker.call("wrong_id")
        assert worker.call("ok") == 7


def test_node_unsettled_promise_is_hard_killed_and_recovered(supported_node):
    """An unsettled Promise in Node must be killed at timeout and recovered."""
    with NodeWorker(supported_node, timeout=0.25) as worker:
        assert worker.call("function(){return 1}") == 1
        assert worker._proc is not None
        old_pid = worker._proc.pid

        with pytest.raises(JsWorkerError, match="timed out"):
            worker.call("async function(){ await new Promise(function(){}); }")

        assert worker._proc is not None
        assert worker._proc.pid != old_pid
        assert worker.call("function(){return 2}") == 2


def test_node_worker_heap_cap_defaults_and_clamping(supported_node):
    assert NodeWorker(supported_node).heap_mb == 512
    assert NodeWorker(supported_node, memory_mb=128).heap_mb == 128
    assert (
        NodeWorker(supported_node, memory_mb=4096, heap_mb=256).heap_mb == 256
    )


def test_jsbox_worker_heap_cap_defaults_and_clamping():
    assert JsboxWorker(jsbox_search_dir="/tmp").heap_mb == 64
    assert JsboxWorker(memory_mb=32, jsbox_search_dir="/tmp").heap_mb == 32
    assert (
        JsboxWorker(memory_mb=512, heap_mb=128, jsbox_search_dir="/tmp").heap_mb
        == 128
    )


def test_worker_platform_popen_flags(monkeypatch):
    runtime = NodeRuntime(path="/usr/bin/node", version=(26, 13, 1))
    recorded: list[tuple[list[str], object, object]] = []

    class _FakeProc:
        def __init__(self) -> None:
            self.stdin = io.StringIO()
            self.stdout = io.StringIO()
            self.stderr = io.StringIO()

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            pass

        def wait(self, timeout: float | None = None) -> int:
            return 0

    def fake_popen(args, **kwargs):
        recorded.append(
            (list(args), kwargs.get("preexec_fn"), kwargs.get("env"))
        )
        return _FakeProc()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setenv("NEOSQLITE_SECRET_MARKER", "DO_NOT_LEAK")
    monkeypatch.setenv("SYSTEMROOT", "C:\\Windows")

    monkeypatch.setattr(sys, "platform", "darwin")
    with NodeWorker(runtime) as mac_worker:
        mac_worker._preexec()  # Must be a no-op on macOS
        mac_worker._start()
    assert recorded[-1][0][:3] == [
        "/usr/bin/node",
        "--permission",
        "--max-old-space-size=512",
    ]
    assert recorded[-1][1] is None
    assert recorded[-1][2] == {}

    monkeypatch.setattr(sys, "platform", "linux")
    with NodeWorker(runtime) as linux_worker:
        linux_worker._start()
        assert recorded[-1][1] == linux_worker._preexec
    assert recorded[-1][0][:3] == [
        "/usr/bin/node",
        "--permission",
        "--max-old-space-size=512",
    ]
    assert recorded[-1][2] == {}

    monkeypatch.setattr(sys, "platform", "win32")
    with JsboxWorker(jsbox_search_dir="C:\\site-packages") as win_worker:
        win_worker._start()
    assert recorded[-1][0][:3] == [sys.executable, "-I", "-u"]
    assert recorded[-1][1] is None
    assert "NEOSQLITE_SECRET_MARKER" not in recorded[-1][2]
    assert recorded[-1][2].get("SYSTEMROOT") == "C:\\Windows"


def test_node_worker_heap_limit_crash_and_recovery(supported_node):
    with NodeWorker(supported_node, heap_mb=32) as worker:
        with pytest.raises(JsWorkerError, match="exited unexpectedly"):
            worker.call(
                "function(){const a=[];while(true)a.push('x'.repeat(1048576));}"
            )
        assert worker.call("function(a,b){return a+b}", [2, 3]) == 5


def test_legacy_js_worker_get_worker_compat(supported_node):
    """Legacy neosqlite.js_worker.get_worker(runtime) continues working."""
    worker = get_worker(supported_node)
    assert worker is not None
    assert worker.call("function(a,b){return a+b}", [10, 20]) == 30
