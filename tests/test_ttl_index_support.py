"""Tests for TTL index declaration and document expiry."""

import time
from datetime import UTC, datetime, timedelta

import pytest

import neosqlite


@pytest.fixture
def connection():
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


def _utc_now():
    return datetime.now(UTC)


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


def test_collection_drop_clears_ttl_fts_and_index_keys(connection):
    from neosqlite.collection import Collection

    connection.cache.create_index("date", expireAfterSeconds=10)
    connection.cache.create_search_index("title")
    connection.cache.drop()

    # Recreated collection without TTL must not auto-purge old or epoch docs
    connection.cache.insert_one({"_id": "fresh", "date": 100})
    connection.cache.insert_one(
        {"_id": "past_dt", "date": datetime(2000, 1, 1, tzinfo=UTC)}
    )
    assert [d["_id"] for d in connection.cache.find({})] == ["fresh", "past_dt"]
    assert connection.cache.get_ttl_specs() == []
    assert (
        connection.db.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'cache_%_fts'"
        ).fetchall()
        == []
    )

    # Standalone Collection without _database ref also cleans metadata on drop()
    standalone = Collection(connection.db, "standalone")
    standalone.create_index("date", expireAfterSeconds=10)
    standalone.drop()
    assert (
        connection.db.execute(
            "SELECT COUNT(*) FROM _neosqlite_ttl_indexes "
            "WHERE collection_name = 'standalone'"
        ).fetchone()[0]
        == 0
    )


def test_rename_and_rename_collection_preserve_ttl_and_drop_index(connection):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    connection.old_c.create_index("ts", expireAfterSeconds=60)
    connection.rename_collection("old_c", "new_c")

    info = connection.new_c.index_information()
    assert info["idx_new_c_ts"]["expireAfterSeconds"] == 60
    connection.new_c.insert_one({"_id": 1, "ts": t0})
    assert connection.new_c.purge_expired(now=t0 + timedelta(seconds=120)) == 1

    connection.new_c.drop_index("idx_new_c_ts")
    connection.new_c.insert_one({"_id": 2, "ts": t0})
    assert connection.new_c.purge_expired(now=t0 + timedelta(seconds=120)) == 0

    direct = connection.direct_old
    direct.create_index("ts", expireAfterSeconds=60)
    direct.rename("direct_new")
    assert connection._collections.get("direct_new") is direct
    assert "direct_old" not in connection._collections
    assert (
        direct.index_information()["idx_direct_new_ts"]["expireAfterSeconds"]
        == 60
    )
    direct.drop_index("idx_direct_new_ts")
    direct.insert_one({"_id": 3, "ts": t0})
    assert direct.purge_expired(now=t0 + timedelta(seconds=120)) == 0


def test_purge_expired_exact_cutoff_and_ignores_booleans(connection):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    connection.ttl.create_index("date", expireAfterSeconds=0)
    connection.ttl.insert_many(
        [
            {"_id": "exact_dt", "date": t0},
            {"_id": "exact_epoch", "date": t0.timestamp()},
            {"_id": "future_dt", "date": t0 + timedelta(seconds=1)},
            {"_id": "flag_false", "date": False},
            {"_id": "flag_true", "date": True},
        ]
    )
    deleted = connection.ttl.purge_expired(now=t0)
    assert deleted == 2
    remaining = {
        row[0]
        for row in connection.db.execute("SELECT _id FROM ttl").fetchall()
    }
    assert remaining == {"future_dt", "flag_false", "flag_true"}


def test_auto_purge_ttl_on_find_one_and_modify_and_raw_batches(connection):
    import json

    old_ts = _utc_now() - timedelta(hours=2)
    fresh_ts = _utc_now() + timedelta(hours=1)

    for method in ("delete", "replace", "update"):
        coll = connection[f"ttl_{method}"]
        coll.create_index("date", expireAfterSeconds=60)
        coll.insert_one({"_id": "expired", "date": old_ts, "v": 1})
        if method == "delete":
            assert coll.find_one_and_delete({"_id": "expired"}) is None
        elif method == "replace":
            assert (
                coll.find_one_and_replace(
                    {"_id": "expired"}, {"_id": "expired", "v": 2}
                )
                is None
            )
        else:
            assert (
                coll.find_one_and_update({"_id": "expired"}, {"$set": {"v": 2}})
                is None
            )

    raw_coll = connection["ttl_raw"]
    raw_coll.create_index("date", expireAfterSeconds=60)
    raw_coll.insert_many(
        [
            {"_id": "old", "date": old_ts},
            {"_id": "fresh", "date": fresh_ts},
        ]
    )
    raw_ids = [
        json.loads(line)["_id"]
        for batch in raw_coll.find_raw_batches()
        for line in batch.decode("utf-8").splitlines()
        if line.strip()
    ]
    assert raw_ids == ["fresh"]

    raw_coll.insert_one({"_id": "old2", "date": old_ts})
    agg_ids = [
        json.loads(line)["_id"]
        for batch in raw_coll.aggregate_raw_batches([{"$match": {}}])
        for line in batch.decode("utf-8").splitlines()
        if line.strip()
    ]
    assert agg_ids == ["fresh"]
