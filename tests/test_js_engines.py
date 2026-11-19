"""Tests for selectable multi-engine JS registry, priority, and codecs."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from typing import Any, Self

import pytest

import neosqlite
from neosqlite.js import (
    JS_ENGINE_ENV_VAR,
    JsEngineInfo,
    JsWorkerError,
    close_js_workers,
    from_marker,
    get_active_js_engine,
    get_js_engine,
    get_js_worker,
    is_js_available,
    js_unavailable_reason,
    list_js_engines,
    register_js_engine,
    reset_js_engine_cache,
    set_js_engine,
    to_marker,
    unregister_js_engine,
    use_js_engine,
)
from neosqlite.js_runtime import NodeRuntime
from neosqlite.objectid import ObjectId


@pytest.fixture(autouse=True)
def _restore_js_engine_state(monkeypatch):
    """Reset selection, env var, and caches around each test."""
    monkeypatch.delenv(JS_ENGINE_ENV_VAR, raising=False)
    set_js_engine(None)
    reset_js_engine_cache()
    close_js_workers()
    yield
    set_js_engine(None)
    reset_js_engine_cache()
    close_js_workers()


class _FakeWorker:
    def __init__(self, engine_name: str) -> None:
        self.engine_name = engine_name
        self.closed = False
        self.calls: list[tuple[str, list[Any] | None, Any]] = []

    def call(
        self,
        code: str,
        args: list[Any] | None = None,
        this_arg: Any = None,
    ) -> Any:
        self.calls.append((code, args, this_arg))
        return f"from:{self.engine_name}"

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _FakeBackend:
    def __init__(
        self,
        name: str,
        *,
        available: bool = True,
        version: str | None = "1.0.0",
        detail: str = "",
    ) -> None:
        self.name = name
        self.available = available
        self.version = version
        self.detail = detail or (
            f"{name} {version}" if available else f"{name} unavailable"
        )
        self.workers_created: list[_FakeWorker] = []

    def probe(self) -> JsEngineInfo:
        return JsEngineInfo(
            name=self.name,
            available=self.available,
            version=self.version if self.available else None,
            detail=self.detail,
        )

    def create_worker(
        self,
        *,
        timeout: float | None = None,
        memory_mb: int | None = None,
        heap_mb: int | None = None,
    ) -> _FakeWorker:
        if not self.available:
            raise JsWorkerError(self.detail)
        worker = _FakeWorker(self.name)
        self.workers_created.append(worker)
        return worker

    def reset_cache(self) -> None:
        pass


class TestEnginePriorityAndSelection:
    def test_auto_prefers_jsbox_over_node(self, monkeypatch):
        jsbox_info = JsEngineInfo(
            name="jsbox", available=True, version="0.1.3", detail="jsbox 0.1.3"
        )
        node_rt = NodeRuntime(path="/usr/bin/node", version=(26, 13, 1))

        def _fake_probe_jsbox() -> JsEngineInfo:
            return jsbox_info

        def _fake_find_node() -> NodeRuntime:
            return node_rt

        monkeypatch.setattr(
            "neosqlite.js.engines.jsbox_engine.probe_jsbox", _fake_probe_jsbox
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )

        assert get_js_engine() == "auto"
        engines = list_js_engines()
        assert [e.name for e in engines[:2]] == ["jsbox", "node"]
        active = get_active_js_engine()
        assert active is not None
        assert active.name == "jsbox"

    def test_auto_falls_back_to_node_when_jsbox_missing(self, monkeypatch):
        missing_jsbox = JsEngineInfo(
            name="jsbox", available=False, detail="jsbox not installed"
        )
        node_rt = NodeRuntime(path="/usr/bin/node", version=(26, 13, 1))

        def _fake_probe_jsbox() -> JsEngineInfo:
            return missing_jsbox

        def _fake_find_node() -> NodeRuntime:
            return node_rt

        monkeypatch.setattr(
            "neosqlite.js.engines.jsbox_engine.probe_jsbox", _fake_probe_jsbox
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )

        active = get_active_js_engine()
        assert active is not None
        assert active.name == "node"
        assert active.version == "26.13.1"

    def test_explicit_node_selection_bypasses_jsbox(self, monkeypatch):
        jsbox_info = JsEngineInfo(
            name="jsbox", available=True, version="0.1.3", detail="jsbox 0.1.3"
        )
        node_rt = NodeRuntime(path="/usr/bin/node", version=(26, 13, 1))

        def _fake_probe_jsbox() -> JsEngineInfo:
            return jsbox_info

        def _fake_find_node() -> NodeRuntime:
            return node_rt

        monkeypatch.setattr(
            "neosqlite.js.engines.jsbox_engine.probe_jsbox", _fake_probe_jsbox
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )

        set_js_engine("node")
        assert get_js_engine() == "node"
        active = get_active_js_engine()
        assert active is not None
        assert active.name == "node"

    def test_explicit_jsbox_does_not_fall_back_to_node(self, monkeypatch):
        missing_jsbox = JsEngineInfo(
            name="jsbox", available=False, detail="jsbox not installed"
        )
        node_rt = NodeRuntime(path="/usr/bin/node", version=(26, 13, 1))

        def _fake_probe_jsbox() -> JsEngineInfo:
            return missing_jsbox

        def _fake_find_node() -> NodeRuntime:
            return node_rt

        monkeypatch.setattr(
            "neosqlite.js.engines.jsbox_engine.probe_jsbox", _fake_probe_jsbox
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )

        set_js_engine("jsbox")
        assert not is_js_available()
        assert "jsbox" in js_unavailable_reason()

    def test_disable_engine_blocks_where_and_function(self):
        set_js_engine("none")
        assert get_js_engine() == "none"
        assert not is_js_available()
        assert "disabled" in js_unavailable_reason()

        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            coll.insert_one({"_id": 1, "x": 10})
            with pytest.raises(NotImplementedError, match=r"\$where"):
                list(coll.find({"$where": "this.x > 5"}))
            with pytest.raises(NotImplementedError, match=r"\$where"):
                coll.count_documents({"$where": "this.x > 5"})
            with pytest.raises(NotImplementedError, match=r"\$function"):
                list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "y": {
                                        "$function": {
                                            "body": "function(v){return v+1}",
                                            "args": ["$x"],
                                            "lang": "js",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )

    def test_disable_aliases_normalize_to_none(self):
        for alias in ("none", "off", "disabled", "FALSE", "0"):
            set_js_engine(alias)
            assert get_js_engine() == "none"

    def test_env_var_selection(self, monkeypatch):
        monkeypatch.setenv(JS_ENGINE_ENV_VAR, "none")
        assert get_js_engine() == "none"
        assert not is_js_available()

        # Programmatic setting takes precedence over env var.
        set_js_engine("auto")
        assert get_js_engine() == "auto"

        # Resetting to None falls back to env var again.
        set_js_engine(None)
        assert get_js_engine() == "none"

    def test_unknown_env_var_reports_clear_reason(self, monkeypatch):
        monkeypatch.setenv(JS_ENGINE_ENV_VAR, "spidermonkey")
        assert get_active_js_engine() is None
        reason = js_unavailable_reason()
        assert "Unknown JavaScript engine" in reason
        assert "spidermonkey" in reason

    def test_invalid_set_js_engine_raises_errors(self):
        with pytest.raises(ValueError, match="Unknown JavaScript engine"):
            set_js_engine("nonexistent")
        with pytest.raises(TypeError, match="must be a string or None"):
            set_js_engine(123)  # type: ignore[arg-type]

    def test_use_js_engine_context_manager_restores_state(self):
        set_js_engine("auto")
        with pytest.raises(RuntimeError), use_js_engine("none"):
            assert get_js_engine() == "none"
            raise RuntimeError("boom")
        assert get_js_engine() == "auto"

    def test_unavailable_reason_combines_all_engines(self, monkeypatch):
        def _fake_probe_jsbox() -> JsEngineInfo:
            return JsEngineInfo(
                name="jsbox", available=False, detail="jsbox missing"
            )

        def _fake_find_node() -> None:
            return None

        def _fake_node_reason() -> str:
            return "node missing"

        monkeypatch.setattr(
            "neosqlite.js.engines.jsbox_engine.probe_jsbox", _fake_probe_jsbox
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.find_node_runtime", _fake_find_node
        )
        monkeypatch.setattr(
            "neosqlite.js_runtime.node_unavailable_reason", _fake_node_reason
        )

        reason = js_unavailable_reason()
        assert "jsbox: jsbox missing" in reason
        assert "node: node missing" in reason


class TestCustomPluggableEngines:
    def test_register_custom_engine_priority_and_execution(self):
        backend = _FakeBackend("customjs", version="9.9.9")
        register_js_engine(backend, priority=0)
        try:
            active = get_active_js_engine()
            assert active is not None
            assert active.name == "customjs"

            worker = get_js_worker()
            assert worker is not None
            assert worker.call("function(){return 1}") == "from:customjs"

            with neosqlite.Connection(":memory:") as conn:
                coll = conn.docs
                coll.insert_one({"_id": 1, "x": 1})
                rows = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "out": {
                                        "$function": {
                                            "body": "function(){return 1}",
                                            "args": [],
                                            "lang": "js",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
            assert rows[0]["out"] == "from:customjs"
        finally:
            assert unregister_js_engine("customjs") is True
            assert unregister_js_engine("customjs") is False
            assert all(w.closed for w in backend.workers_created)

    def test_register_reserved_name_rejected(self):
        for bad in ("auto", "none", "off", ""):
            with pytest.raises(ValueError, match="Invalid JavaScript engine"):
                register_js_engine(_FakeBackend(bad))


class TestMarkerCodec:
    def test_marker_roundtrip(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        dt = datetime(2026, 11, 19, 10, 0, 0, tzinfo=UTC)
        doc = {
            "_id": oid,
            "ts": dt,
            "nested": [oid, {"d": dt}],
            "plain": 42,
        }
        encoded = to_marker(doc)
        assert encoded["_id"] == {"$oid": str(oid)}
        assert encoded["ts"] == {"$date": dt.isoformat()}
        decoded = from_marker(encoded)
        assert decoded == doc

    def test_number_long_and_multi_key_dict_preserved(self):
        assert from_marker({"$numberLong": "9007199254740993"}) == (
            9007199254740993
        )
        # Multi-key dicts must NOT be collapsed into a scalar marker.
        non_marker = {"$date": "2026-01-01T00:00:00+00:00", "extra": 1}
        assert from_marker(non_marker) == non_marker
        # Invalid marker payloads degrade safely instead of raising.
        assert from_marker({"$date": "not-a-date"}) == "not-a-date"
        assert from_marker({"$oid": "not-an-oid"}) == "not-an-oid"
        assert from_marker({"$numberLong": "not-an-int"}) == "not-an-int"


def test_probing_jsbox_does_not_load_native_extension_in_host_process():
    """Main process must never dlopen jsbox._jsbox just by probing."""
    code = (
        "import sys; "
        "import neosqlite; "
        "neosqlite.list_js_engines(); "
        "loaded = [m for m in sys.modules if m == 'jsbox' or m.startswith('jsbox.')]; "
        "assert loaded == [], f'unexpectedly loaded in host: {loaded}'"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
