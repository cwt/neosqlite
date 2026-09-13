"""Tests for TTL index declaration and document expiry."""

import time
from datetime import datetime, timedelta, timezone

import pytest

import neosqlite


@pytest.fixture
def connection():
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


def _utc_now():
    return datetime.now(timezone.utc)


def test_ttl_declaration_visible_in_index_information(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=43200)
    info = coll.index_information()
    assert info["idx_cache2_date"]["expireAfterSeconds"] == 43200


def test_ttl_specs_listed(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=60)
    specs = coll.get_ttl_specs()
    assert len(specs) == 1
    assert specs[0]["field"] == "date"
    assert specs[0]["expireAfterSeconds"] == 60


def test_purge_expired_datetimes(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=3600)
    now = _utc_now()
    coll.insert_many(
        [
            {"page": "old", "date": now - timedelta(seconds=7200)},
            {"page": "fresh", "date": now},
        ]
    )
    deleted = coll.purge_expired(now=now)
    assert deleted == 1
    remaining = list(coll.find({}))
    assert [d["page"] for d in remaining] == ["fresh"]


def test_purge_expired_epoch_numbers(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=3600)
    now = _utc_now()
    old_epoch = (now - timedelta(seconds=7200)).timestamp()
    fresh_epoch = now.timestamp()
    coll.insert_many(
        [
            {"page": "old", "date": old_epoch},
            {"page": "fresh", "date": fresh_epoch},
        ]
    )
    deleted = coll.purge_expired(now=now)
    assert deleted == 1
    remaining = list(coll.find({}))
    assert [d["page"] for d in remaining] == ["fresh"]


def test_purge_expired_no_ttl_returns_zero(connection):
    coll = connection["plain"]
    coll.insert_one({"x": 1})
    assert coll.purge_expired() == 0


def test_auto_purge_on_find_one(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=3600)
    now = _utc_now()
    coll.insert_one({"page": "old", "date": now - timedelta(seconds=7200)})
    found = coll.find_one({"page": "old"})
    assert found is None


def test_auto_purge_on_find(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=3600)
    now = _utc_now()
    coll.insert_many(
        [
            {"page": "old", "date": now - timedelta(seconds=7200)},
            {"page": "fresh", "date": now},
        ]
    )
    docs = list(coll.find({}))
    assert [d["page"] for d in docs] == ["fresh"]


def test_expire_after_zero_means_expire_at_field_time(connection):
    coll = connection["cache2"]
    coll.create_index("expireAt", expireAfterSeconds=0)
    now = _utc_now()
    coll.insert_many(
        [
            {"page": "past", "expireAt": now - timedelta(seconds=10)},
            {"page": "future", "expireAt": now + timedelta(seconds=3600)},
        ]
    )
    deleted = coll.purge_expired(now=now)
    assert deleted == 1
    assert coll.find_one({"page": "past"}) is None
    assert coll.find_one({"page": "future"}) is not None


def test_drop_index_clears_ttl_metadata(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=60)
    assert len(coll.get_ttl_specs()) == 1
    coll.drop_index("date")
    assert coll.get_ttl_specs() == []
    assert "expireAfterSeconds" not in coll.index_information().get(
        "idx_cache2_date", {}
    )


def test_manual_full_wipe_still_works(connection):
    coll = connection["cache2"]
    coll.create_index("date", expireAfterSeconds=3600)
    now = _utc_now()
    coll.insert_many(
        [
            {"page": "a", "date": now},
            {"page": "b", "date": now},
        ]
    )
    result = coll.delete_many({})
    assert result.deleted_count == 2


def test_sweep_once_file_db(tmp_path):
    db_path = str(tmp_path / "ttl_sweep.db")
    conn = neosqlite.Connection(db_path)
    try:
        coll = conn.create_collection("cache2")
        coll.create_index("date", expireAfterSeconds=3600)
        now = _utc_now()
        coll.insert_one({"page": "old", "date": now - timedelta(seconds=7200)})
        deleted = conn.sweep_ttl_once()
        assert deleted == 1
        assert coll.find_one({"page": "old"}) is None
    finally:
        conn.close()


def test_sweeper_thread_disabled_for_memory():
    conn = neosqlite.Connection(":memory:", ttl_sweep_interval_s=0.05)
    try:
        assert conn._ttl_thread is None
    finally:
        conn.close()


def test_sweeper_thread_deletes_expired(tmp_path):
    db_path = str(tmp_path / "ttl_bg.db")
    conn = neosqlite.Connection(db_path, ttl_sweep_interval_s=0.05)
    try:
        assert conn._ttl_thread is not None
        coll = conn.create_collection("jobs")
        coll.create_index("date", expireAfterSeconds=3600)
        coll.insert_one(
            {"page": "old", "date": _utc_now() - timedelta(seconds=7200)}
        )
        deadline = time.time() + 5.0
        while time.time() < deadline:
            helper = neosqlite.Connection(db_path)
            try:
                if helper.get_collection("jobs").count_documents({}) == 0:
                    break
            finally:
                helper.close()
            time.sleep(0.05)
        checker = neosqlite.Connection(db_path)
        try:
            assert checker.get_collection("jobs").count_documents({}) == 0
        finally:
            checker.close()
    finally:
        conn.close()
