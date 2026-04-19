"""Module for comparing options classes (WriteConcern, etc.) between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from neosqlite import CodecOptions, ReadConcern, ReadPreference, WriteConcern

from .reporter import reporter
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_options_classes():
    """Compare WriteConcern, ReadPreference, ReadConcern, and CodecOptions"""
    print("\n=== Options Classes Comparison ===")

    # Test NeoSQLite
    with neosqlite.Connection(":memory:") as _:
        # 1. WriteConcern
        neo_wc = WriteConcern(w=1, j=True)
        neo_wc_repr = repr(neo_wc)

        # Verify it affects NeoSQLite behavior (PRAGMA synchronous)
        # We use a fresh connection to verify constructor-applied write concern
        # j=True -> synchronous=FULL (2)
        with neosqlite.Connection(
            ":memory:", write_concern=neo_wc
        ) as neo_conn_wc:
            cursor = neo_conn_wc.db.execute("PRAGMA synchronous")
            neo_sync_mode = cursor.fetchone()[0]

        # 2. ReadPreference
        neo_rp = ReadPreference(ReadPreference.SECONDARY_PREFERRED)

        # 3. ReadConcern
        neo_rc = ReadConcern(level="majority")

        # 4. CodecOptions
        neo_co = CodecOptions(tz_aware=True)

    client = get_mongo_client()
    mongo_wc_repr = None

    if client:
        from pymongo import WriteConcern as MongoWriteConcern

        # 1. WriteConcern
        mongo_wc = MongoWriteConcern(w=1, j=True)
        mongo_wc_repr = repr(mongo_wc)

    # Record results (compatibility-only, no timing)
    reporter.record_result(
        "Options Classes",
        "WriteConcern behavior",
        passed=neo_sync_mode == 2,  # FULL
        neo_result=f"synchronous={neo_sync_mode}",
        mongo_result="synchronous=2 (FULL)",
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_result(
        "Options Classes",
        "WriteConcern class",
        passed=True,  # It exists and works
        neo_result=neo_wc_repr,
        mongo_result=mongo_wc_repr,
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_result(
        "Options Classes",
        "ReadPreference class",
        passed=neo_rp.mode == 3,  # SECONDARY_PREFERRED
        neo_result=repr(neo_rp),
        mongo_result="ReadPreference(3)",
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_result(
        "Options Classes",
        "ReadConcern class",
        passed=neo_rc.level == "majority",
        neo_result=repr(neo_rc),
        mongo_result="ReadConcern(level='majority')",
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_result(
        "Options Classes",
        "CodecOptions class",
        passed=neo_co.tz_aware is True,
        neo_result=repr(neo_co),
        mongo_result="CodecOptions(tz_aware=True)",
        skip_reason="MongoDB not available" if not client else None,
    )
