# coding: utf-8
import logging

import pytest

import neosqlite

# Suppress neosqlite logger warnings during test runs (e.g., pending transaction
# warnings from fixture teardown). These warnings are still useful in production.
logging.getLogger("neosqlite").setLevel(logging.CRITICAL)


@pytest.fixture(autouse=True)
def _auto_close_connections(monkeypatch):
    """Safety net to track and cleanly close all connections created during tests."""
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


@pytest.fixture
def connection():
    """Fixture to set up and tear down a neosqlite connection."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def collection(connection):
    """Fixture to provide a clean collection for each test."""
    return connection["foo"]
