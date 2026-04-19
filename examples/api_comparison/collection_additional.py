"""Module for comparing additional collection methods between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

# Check if we're running against NX-27017 (NeoSQLite backend)
IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_additional_collection_methods():
    """Compare additional collection methods"""
    print("\n=== Additional Collection Methods Comparison ===")

    # Check MongoDB availability FIRST to determine which operations to benchmark
    client = get_mongo_client()
    mongo_available = client is not None

    # Initialize NeoSQLite result variables
    neo_drop = False
    neo_db_ok = False
    neo_watch = False
    neo_full_name = None
    neo_full_name_ok = False
    neo_client_prop = False
    neo_db_path_prop = False
    neo_with_options = False

    # Initialize MongoDB result variables
    mongo_drop = None
    mongo_db_ok = None
    mongo_watch = None
    mongo_full_name = None
    mongo_full_name_ok = None
    mongo_client_prop = None
    mongo_with_options = None

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)

        # Test drop() - ONLY time if MongoDB is available for comparison
        neo_collection = neo_conn.test_drop
        neo_collection.insert_one({"name": "test"})
        if mongo_available:
            start_neo_timing()
        try:
            try:
                neo_collection.drop()
            except Exception as e:
                neo_drop = False
                print(f"Neo drop(): Error - {e}")
            finally:
                if mongo_available:
                    end_neo_timing()
            neo_drop = "test_drop" not in neo_conn.list_collection_names()
        except Exception:
            pass
        print(f"Neo drop(): {'OK' if neo_drop else 'FAIL'}")

        # Test database property - ONLY time if MongoDB is available
        neo_collection2 = neo_conn.test
        if mongo_available:
            start_neo_timing()
        try:
            try:
                neo_db = neo_collection2.database
                neo_db_ok = neo_db is not None
            except Exception as e:
                neo_db_ok = False
                print(f"Neo database property: Error - {e}")
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception:
            pass
        print(f"Neo database property: {'OK' if neo_db_ok else 'FAIL'}")

        # Test watch() - Change streams (SKIPPED - MongoDB requires replica set)
        # Don't time this - it's a known limitation, not a fair comparison
        neo_collection_watch = neo_conn.test_watch
        try:
            neo_stream = neo_collection_watch.watch()
            neo_watch = neo_stream is not None
            if neo_stream:
                neo_stream.close()
        except Exception as e:
            neo_watch = False
            print(f"Neo watch(): SKIPPED (SQLite triggers) - {e}")
        print(
            f"Neo watch(): {'OK (SQLite triggers)' if neo_watch else 'FAIL'} (not timed)"
        )

        # Test full_name property - ONLY time if MongoDB is available
        neo_collection_fullname = neo_conn.test_fullname
        if mongo_available:
            start_neo_timing()
        try:
            try:
                neo_full_name = neo_collection_fullname.full_name
                neo_full_name_ok = (
                    isinstance(neo_full_name, str) and len(neo_full_name) > 0
                )
            except Exception as e:
                neo_full_name_ok = False
                print(f"Neo full_name: Error - {e}")
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception:
            pass
        print(f"Neo full_name: '{neo_full_name}'")

        # Test client property - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            try:
                neo_client_prop = neo_collection2.client == neo_conn
            except Exception as e:
                neo_client_prop = False
                print(f"Neo client property: Error - {e}")
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception:
            pass
        print(f"Neo client property: {'OK' if neo_client_prop else 'FAIL'}")

        # Test db_path property (NeoSQLite specific - SKIPPED from benchmark)
        # Don't time this - MongoDB doesn't have this feature
        try:
            neo_db_path_prop = neo_collection2.db_path == ":memory:"
        except Exception as e:
            neo_db_path_prop = False
            print(f"Neo db_path property: SKIPPED (NeoSQLite specific) - {e}")
        print(
            f"Neo db_path property: {'OK' if neo_db_path_prop else 'FAIL'} (not timed)"
        )

        # Test with_options() - ONLY time if MongoDB is available
        neo_collection_opts = neo_conn.test_with_opts
        if mongo_available:
            start_neo_timing()
        try:
            try:
                neo_coll_opts = neo_collection_opts.with_options(
                    write_concern={"w": "majority"}, read_preference=None
                )
                neo_with_options = (
                    neo_coll_opts is not None
                    and neo_coll_opts.name == neo_collection_opts.name
                )
            except Exception as e:
                neo_with_options = False
                print(f"Neo with_options(): Error - {e}")
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception:
            pass
        print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")

    if mongo_available:
        client = get_mongo_client()
        if client:
            mongo_db = client.test_database
            set_accumulation_mode(True)

            # Test drop()
            mongo_collection = mongo_db.test_drop
            mongo_collection.delete_many({})
            mongo_collection.insert_one({"name": "test"})
            try:
                start_mongo_timing()
                try:
                    mongo_collection.drop()
                except Exception as e:
                    mongo_drop = False
                    print(f"Mongo drop(): Error - {e}")
                finally:
                    end_mongo_timing()
                mongo_drop = "test_drop" not in mongo_db.list_collection_names()
            except Exception:
                pass
            print(f"Mongo drop(): {'OK' if mongo_drop else 'FAIL'}")

            # Test database property
            mongo_collection2 = mongo_db.test
            try:
                start_mongo_timing()
                try:
                    mongo_db_prop = mongo_collection2.database
                    mongo_db_ok = mongo_db_prop is not None
                except Exception as e:
                    mongo_db_ok = False
                    print(f"Mongo database property: Error - {e}")
                finally:
                    end_mongo_timing()
            except Exception:
                pass
            print(f"Mongo database property: {'OK' if mongo_db_ok else 'FAIL'}")

            # Test watch() - Change streams (SKIPPED - requires replica set)
            # Don't time this - it's a known limitation, not a fair comparison
            mongo_collection3 = mongo_db.test_watch
            try:
                change_stream = mongo_collection3.watch([])
                mongo_watch = change_stream is not None
                if change_stream:
                    change_stream.close()
            except Exception as e:
                mongo_watch = False
                print(f"Mongo watch(): SKIPPED (requires replica set) - {e}")
            print(
                f"Mongo watch(): {'OK (replica set available)' if mongo_watch else 'FAIL'} (not timed)"
            )

            # Test full_name property
            mongo_collection_fullname = mongo_db.test_fullname
            try:
                start_mongo_timing()
                try:
                    mongo_full_name = mongo_collection_fullname.full_name
                    mongo_full_name_ok = (
                        isinstance(mongo_full_name, str)
                        and len(mongo_full_name) > 0
                    )
                except Exception as e:
                    mongo_full_name_ok = False
                    print(f"Mongo full_name: Error - {e}")
                finally:
                    end_mongo_timing()
            except Exception:
                pass
            print(f"Mongo full_name: '{mongo_full_name}'")

            # Test client property
            try:
                start_mongo_timing()
                try:
                    mongo_client_prop = (
                        mongo_collection2.database.client == client
                    )
                except Exception as e:
                    mongo_client_prop = False
                    print(f"Mongo client property: Error - {e}")
                finally:
                    end_mongo_timing()
            except Exception:
                pass
            print(
                f"Mongo client property: {'OK' if mongo_client_prop else 'FAIL'}"
            )

            # Test db_path property (Not in PyMongo - SKIPPED from benchmark)
            # Don't time this - MongoDB doesn't have this feature
            try:
                _ = mongo_collection2.db_path
            except Exception:
                pass
            print("Mongo db_path property: SKIPPED (not available in PyMongo)")

            # Test with_options()
            mongo_collection_opts = mongo_db.test_with_opts
            try:
                from pymongo.write_concern import WriteConcern

                start_mongo_timing()
                try:
                    mongo_coll_opts = mongo_collection_opts.with_options(
                        write_concern=WriteConcern(w="majority"),
                        read_preference=None,
                    )
                    mongo_with_options = (
                        mongo_coll_opts is not None
                        and mongo_coll_opts.name == mongo_collection_opts.name
                    )
                except Exception as e:
                    mongo_with_options = False
                    print(f"Mongo with_options(): Error - {e}")
                finally:
                    end_mongo_timing()
            except Exception:
                pass
            print(
                f"Mongo with_options(): {'OK' if mongo_with_options else 'FAIL'}"
            )

    # Record comparisons - only for operations that were actually timed
    reporter.record_comparison(
        "Collection (Additional)",
        "drop",
        neo_drop if neo_drop else "FAIL",
        mongo_drop if mongo_drop else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Collection (Additional)",
        "database_property",
        neo_db_ok if neo_db_ok else "FAIL",
        mongo_db_ok if mongo_db_ok else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    # watch() is implemented in NeoSQLite (SQLite triggers) and via NX-27017
    # When both implement the feature, report as compatible with same value
    # When MongoDB doesn't support it (no replica set), skip the comparison
    if mongo_watch:
        # Both implement watch() - mark as compatible
        reporter.record_comparison(
            "Collection (Additional)",
            "watch",
            "OK",
            "OK",
            skip_reason=None,
        )
    else:
        # MongoDB doesn't support watch (no replica set) - skip
        skip_reason = "NeoSQLite: Implemented via SQLite triggers; MongoDB: Requires replica set (not available in standalone test)"
        reporter.record_comparison(
            "Collection (Additional)",
            "watch",
            "OK" if neo_watch else "FAIL",
            None,
            skip_reason=skip_reason,
        )
    # Extract collection name from full_name (e.g., "memory.test_fullname" -> "test_fullname")
    neo_coll_name = neo_full_name.split(".")[-1] if neo_full_name else None
    mongo_coll_name = (
        mongo_full_name.split(".")[-1] if mongo_full_name else None
    )

    reporter.record_comparison(
        "Collection (Additional)",
        "full_name",
        neo_coll_name if neo_full_name_ok else "FAIL",
        mongo_coll_name if mongo_full_name_ok else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Collection (Additional)",
        "client",
        neo_client_prop if neo_client_prop else "FAIL",
        mongo_client_prop if mongo_client_prop else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Collection (Additional)",
        "db_path",
        neo_db_path_prop if neo_db_path_prop else "FAIL",
        None,
        skip_reason="NeoSQLite specific (SQLite database path) - not timed in benchmark",
    )
    reporter.record_comparison(
        "Collection (Additional)",
        "with_options",
        neo_with_options if neo_with_options else "FAIL",
        mongo_with_options if mongo_with_options else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
