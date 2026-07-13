from datetime import datetime, timezone

import neosqlite


def test_datetime_index_creation():
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test_datetime

        # Insert a document with a datetime field
        coll.insert_one(
            {
                "created_at": datetime(
                    2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
                )
            }
        )

        # Create datetime index
        coll.create_index("created_at", datetime_field=True)

        # Retrieve index info to verify the index is present
        info = coll.index_information()
        assert "idx_test_datetime_created_at_utc" in info

        # Verify queries work and can use the index
        res = list(
            coll.find(
                {
                    "created_at": datetime(
                        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
                    )
                }
            )
        )
        assert len(res) == 1
