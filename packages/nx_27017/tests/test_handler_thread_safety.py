"""Concurrency tests: the SQLite connection must not be shared across
threads (each OS thread needs its own connection to the same database)."""

import threading

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


def _run_workers(handler, n, insert_via, find_via):
    errors = []

    def worker(i):
        try:
            insert_via(handler, i)
            _, resp = find_via(handler, i)
            first_batch = resp["cursor"]["firstBatch"]
            assert any(d.get("_id") == i for d in first_batch), resp
        except Exception as e:  # noqa: BLE001
            errors.append((i, repr(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


@pytest.fixture
def file_handler(tmp_path):
    db_path = str(tmp_path / "concurrent.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


@pytest.fixture
def memory_handler():
    h = NeoSQLiteHandler(":memory:")
    yield h


def _insert(handler, i):
    handler.handle_insert(
        {
            "request_id": i,
            "sections": [
                ("body", {"insert": "items", "$db": "test"}),
                ("payload_docs", [{"_id": i, "v": i}]),
            ],
        }
    )


def _find(handler, i):
    return handler.handle_command(
        {
            "request_id": i,
            "sections": [
                ("body", {"find": "items", "filter": {"_id": i}, "$db": "test"})
            ],
        }
    )


class TestThreadSafeConnections:
    def test_concurrent_inserts_file(self, file_handler):
        errors = _run_workers(file_handler, 20, _insert, _find)
        assert errors == []

        _, resp = file_handler.handle_command(
            {
                "request_id": 99,
                "sections": [
                    ("body", {"find": "items", "filter": {}, "$db": "test"})
                ],
            }
        )
        assert len(resp["cursor"]["firstBatch"]) == 20

    def test_concurrent_inserts_memory(self, memory_handler):
        errors = _run_workers(memory_handler, 20, _insert, _find)
        assert errors == []

        _, resp = memory_handler.handle_command(
            {
                "request_id": 99,
                "sections": [
                    ("body", {"find": "items", "filter": {}, "$db": "test"})
                ],
            }
        )
        assert len(resp["cursor"]["firstBatch"]) == 20
