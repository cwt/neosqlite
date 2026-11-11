# coding: utf-8
import pytest
from nx_27017.handler import NeoSQLiteHandler

import neosqlite


@pytest.fixture(autouse=True)
def _auto_cleanup_nx_resources(monkeypatch):
    """Safety net to track and cleanly close handlers and connections in tests."""
    handlers = []
    orig_handler_init = NeoSQLiteHandler.__init__

    def tracked_handler_init(self, *args, **kwargs):
        handlers.append(self)
        return orig_handler_init(self, *args, **kwargs)

    monkeypatch.setattr(NeoSQLiteHandler, "__init__", tracked_handler_init)

    connections = []
    orig_conn_init = neosqlite.Connection.__init__

    def tracked_conn_init(self, *args, **kwargs):
        connections.append(self)
        return orig_conn_init(self, *args, **kwargs)

    monkeypatch.setattr(neosqlite.Connection, "__init__", tracked_conn_init)

    raw_connections = []
    import sqlite3 as _stdlib_sqlite3

    from neosqlite._sqlite import sqlite3 as _neo_sqlite3

    orig_connect = _stdlib_sqlite3.connect

    def tracked_connect(*args, **kwargs):
        conn = orig_connect(*args, **kwargs)
        raw_connections.append(conn)
        return conn

    monkeypatch.setattr(_stdlib_sqlite3, "connect", tracked_connect)
    if _neo_sqlite3 is not _stdlib_sqlite3:
        monkeypatch.setattr(_neo_sqlite3, "connect", tracked_connect)

    yield

    for handler in handlers:
        try:
            handler.close_all()
        except Exception:
            pass

    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass

    for raw_conn in raw_connections:
        try:
            raw_conn.close()
        except Exception:
            pass
