"""Tests for JavaScript expression/query operators via the Node worker."""

import io
import shutil
import subprocess
import sys

import pytest

import neosqlite
from neosqlite.js_runtime import NodeRuntime
from neosqlite.js_worker import JsWorkerError, NodeWorker, close_worker

_NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")


@pytest.fixture
def supported_node(monkeypatch):
    """Bypass the version gate with the local binary for wiring tests."""
    runtime = NodeRuntime(path=_NODE, version=(24, 22, 3))
    monkeypatch.setattr(
        "neosqlite.js_runtime.find_node_runtime", lambda: runtime
    )
    close_worker()
    yield runtime
    close_worker()


def test_function_aggregation_expression(supported_node):
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


def test_function_with_object_and_array_args(supported_node):
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.docs
        coll.insert_one({"_id": 1, "items": [1, 2, 3]})
        result = list(
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
        )[0]
    assert result["sum"] == 6


def test_where_expression_form(supported_node):
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


def test_where_function_form(supported_node):
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


def test_js_error_is_surfaced(supported_node):
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


def test_worker_unit_call(supported_node):
    from neosqlite.js_worker import get_worker

    worker = get_worker(supported_node)
    assert worker is not None
    assert worker.call("function(a,b){return a+b}", [2, 3]) == 5


def test_worker_does_not_inherit_parent_environment(
    supported_node, monkeypatch
):
    """process.env must not expose parent secrets (CVE-2026-111497 hardening)."""
    from neosqlite.js_worker import get_worker

    monkeypatch.setenv("NEOSQLITE_SECRET_MARKER", "SHOULD_NOT_LEAK")
    worker = get_worker(supported_node)
    assert worker is not None
    env_keys = worker.call("function(){return Object.keys(process.env)}", [])
    if sys.platform == "darwin":
        # CoreFoundation injects __CF_USER_TEXT_ENCODING during dyld init even
        # when spawned with env={}; it is OS-synthesized, not parent-inherited.
        assert [k for k in env_keys if k != "__CF_USER_TEXT_ENCODING"] == []
    else:
        assert env_keys == []
    assert (
        worker.call(
            "function(){return process.env.NEOSQLITE_SECRET_MARKER || 'ABSENT'}",
            [],
        )
        == "ABSENT"
    )
    # The constructor escape path must not reach it either.
    assert (
        worker.call(
            "function(){return this.constructor.constructor("
            "\"return process.env.NEOSQLITE_SECRET_MARKER\")() || 'ABSENT'}",
            [],
            this_arg={"x": 1},
        )
        == "ABSENT"
    )


def test_env_secret_not_readable_via_function(supported_node, monkeypatch):
    monkeypatch.setenv("NEOSQLITE_SECRET_MARKER", "SHOULD_NOT_LEAK")
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
                                        "function(){return "
                                        "process.env.NEOSQLITE_SECRET_MARKER "
                                        "|| 'ABSENT'}"
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


def test_async_function_resolves(supported_node):
    from neosqlite.js_worker import get_worker

    worker = get_worker(supported_node)
    assert worker is not None
    assert worker.call("async function(){return 42}", []) == 42


def test_rejected_promise_does_not_kill_worker(supported_node):
    from neosqlite.js_worker import JsWorkerError, get_worker

    worker = get_worker(supported_node)
    assert worker is not None
    with pytest.raises(JsWorkerError):
        worker.call("async function(){throw new Error('rejected')}", [])
    # Still usable: the worker survived the rejection.
    assert worker.call("function(){return 7}", []) == 7


def test_worker_heap_cap_defaults_and_clamping(supported_node):
    assert NodeWorker(supported_node).heap_mb == 512
    assert NodeWorker(supported_node, memory_mb=128).heap_mb == 128
    assert (
        NodeWorker(supported_node, memory_mb=4096, heap_mb=256).heap_mb == 256
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

    class _FakeSelector:
        def register(self, fileobj: object, events: int) -> None:
            pass

        def close(self) -> None:
            pass

    def fake_popen(args, **kwargs):
        recorded.append(
            (list(args), kwargs.get("preexec_fn"), kwargs.get("env"))
        )
        return _FakeProc()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        "neosqlite.js_worker.selectors.DefaultSelector", _FakeSelector
    )

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


def test_worker_heap_limit_crash_and_recovery(supported_node):
    with NodeWorker(supported_node, heap_mb=32) as worker:
        with pytest.raises(JsWorkerError, match="exited unexpectedly"):
            worker.call(
                "function(){const a=[];while(true)a.push('x'.repeat(1048576));}"
            )
        assert worker.call("function(a,b){return a+b}", [2, 3]) == 5
