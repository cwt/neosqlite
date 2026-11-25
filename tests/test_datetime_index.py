from datetime import UTC, datetime

import neosqlite


def test_datetime_index_creation():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test_datetime

        # Insert a document with a datetime field
        coll.insert_one(
            {"created_at": datetime(2026, 7, 13, 22, 56, 21, tzinfo=UTC)}
        )

        # Create datetime index
        coll.create_index("created_at", datetime_field=True)

        # Retrieve index info to verify the index is present
        info = coll.index_information()
        assert "idx_test_datetime_created_at_utc" in info

        # Verify queries work and can use the index
        res = list(
            coll.find(
                {"created_at": datetime(2026, 7, 13, 22, 56, 21, tzinfo=UTC)}
            )
        )
        assert len(res) == 1


def test_datetime_index_unique_enforces_uniqueness_and_lists_real_key():
    import pytest

    from neosqlite.collection import sqlite3

    with neosqlite.Connection(":memory:") as conn:
        ev = conn.ev
        ev.create_index("created_at", datetime_field=True, unique=True)
        t = datetime(2026, 1, 1, tzinfo=UTC)
        ev.insert_one({"created_at": t})
        with pytest.raises(sqlite3.IntegrityError):
            ev.insert_one({"created_at": t})

        assert ev.count_documents({}) == 1
        assert ev.list_indexes(as_keys=True) == [["created_at"]]
        assert ev.index_information()["idx_ev_created_at_utc"]["key"] == {
            "created_at": 1
        }
