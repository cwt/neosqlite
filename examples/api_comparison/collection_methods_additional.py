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
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

# Check if we're running against NX-27017 (NeoSQLite backend)
IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_additional_collection_methods():
    """Compare additional collection methods"""
    print("\n=== Additional Collection Methods Comparison ===")

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
    mongo_db_path_prop = None
    mongo_with_options = None

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)

        # Test drop()
        neo_collection = neo_conn.test_drop
        neo_collection.insert_one({"name": "test"})
        try:
            start_neo_timing()
            try:
                neo_collection.drop()
            except Exception as e:
                neo_drop = False
                print(f"Neo drop(): Error - {e}")
            finally:
                end_neo_timing()
            neo_drop = "test_drop" not in neo_conn.list_collection_names()
        except Exception:
            pass
        print(f"Neo drop(): {'OK' if neo_drop else 'FAIL'}")

        # Test database property
        neo_collection2 = neo_conn.test
        try:
            start_neo_timing()
            try:
                neo_db = neo_collection2.database
                neo_db_ok = neo_db is not None
            except Exception as e:
                neo_db_ok = False
                print(f"Neo database property: Error - {e}")
            finally:
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo database property: {'OK' if neo_db_ok else 'FAIL'}")

        # Test watch() - Change streams
        try:
            start_neo_timing()
            try:
                neo_collection_watch = neo_conn.test_watch
                neo_stream = neo_collection_watch.watch()
                neo_watch = neo_stream is not None
                if neo_stream:
                    neo_stream.close()
            except Exception as e:
                neo_watch = False
                print(f"Neo watch(): Error - {e}")
            finally:
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo watch(): {'OK (SQLite triggers)' if neo_watch else 'FAIL'}")

        # Test full_name property
        neo_collection_fullname = neo_conn.test_fullname
        try:
            start_neo_timing()
            try:
                neo_full_name = neo_collection_fullname.full_name
                neo_full_name_ok = (
                    isinstance(neo_full_name, str) and len(neo_full_name) > 0
                )
            except Exception as e:
                neo_full_name_ok = False
                print(f"Neo full_name: Error - {e}")
            finally:
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo full_name: '{neo_full_name}'")

        # Test client property
        try:
            start_neo_timing()
            try:
                neo_client_prop = neo_collection2.client == neo_conn
            except Exception as e:
                neo_client_prop = False
                print(f"Neo client property: Error - {e}")
            finally:
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo client property: {'OK' if neo_client_prop else 'FAIL'}")

        # Test db_path property (NeoSQLite specific)
        try:
            start_neo_timing()
            try:
                neo_db_path_prop = neo_collection2.db_path == ":memory:"
            except Exception as e:
                neo_db_path_prop = False
                print(f"Neo db_path property: Error - {e}")
            finally:
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo db_path property: {'OK' if neo_db_path_prop else 'FAIL'}")

        # Test with_options()
        neo_collection_opts = neo_conn.test_with_opts
        try:
            start_neo_timing()
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
                end_neo_timing()
        except Exception:
            pass
        print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")

    client = test_pymongo_connection()
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

        # Test watch() - Change streams (requires replica set)
        try:
            start_mongo_timing()
            try:
                mongo_collection3 = mongo_db.test_watch
                change_stream = mongo_collection3.watch([])
                mongo_watch = change_stream is not None
                if change_stream:
                    change_stream.close()
            except Exception as e:
                mongo_watch = False
                print(f"Mongo watch(): SKIPPED (requires replica set) - {e}")
            finally:
                end_mongo_timing()
        except Exception:
            pass
        if mongo_watch:
            print(f"Mongo watch(): OK (replica set available)")

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
                mongo_client_prop = mongo_collection2.database.client == client
            except Exception as e:
                mongo_client_prop = False
                print(f"Mongo client property: Error - {e}")
            finally:
                end_mongo_timing()
        except Exception:
            pass
        print(f"Mongo client property: {'OK' if mongo_client_prop else 'FAIL'}")

        # Test db_path property (Not in PyMongo)
        try:
            start_mongo_timing()
            try:
                _ = mongo_collection2.db_path
                mongo_db_path_prop = True
            except Exception:
                mongo_db_path_prop = False
            finally:
                end_mongo_timing()
        except Exception:
            pass

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
        print(f"Mongo with_options(): {'OK' if mongo_with_options else 'FAIL'}")

        client.close()

    reporter.record_comparison(
        "Collection Methods",
        "drop",
        neo_drop if neo_drop else "FAIL",
        mongo_drop if mongo_drop else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Collection Methods",
        "database_property",
        neo_db_ok if neo_db_ok else "FAIL",
        mongo_db_ok if mongo_db_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    # watch() is implemented in NeoSQLite (SQLite triggers) but can't be compared
    # with MongoDB in this test because MongoDB requires a replica set
    # Skip if MongoDB can't run watch (replica set not available)
    if mongo_watch is False:
        skip_reason = "NeoSQLite: Implemented via SQLite triggers; MongoDB: Requires replica set (not available in standalone test)"
    elif IS_NX27017_BACKEND:
        skip_reason = None  # On NX-27017, compare results
    else:
        skip_reason = "NeoSQLite: Implemented via SQLite triggers; MongoDB: Requires replica set (not available in standalone test)"

    reporter.record_comparison(
        "Collection Methods",
        "watch",
        "IMPLEMENTED (SQLite triggers)" if neo_watch else "FAIL",
        "IMPLEMENTED (replica set)" if mongo_watch else None,
        skip_reason=skip_reason,
    )
    # Extract collection name from full_name (e.g., "memory.test_fullname" -> "test_fullname")
    neo_coll_name = neo_full_name.split(".")[-1] if neo_full_name else None
    mongo_coll_name = (
        mongo_full_name.split(".")[-1] if mongo_full_name else None
    )

    reporter.record_comparison(
        "Collection Methods",
        "full_name",
        neo_coll_name if neo_full_name_ok else "FAIL",
        mongo_coll_name if mongo_full_name_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Collection Methods",
        "client",
        neo_client_prop if neo_client_prop else "FAIL",
        mongo_client_prop if mongo_client_prop else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Collection Methods",
        "db_path",
        neo_db_path_prop if neo_db_path_prop else "FAIL",
        None,
        skip_reason="NeoSQLite specific (SQLite database path)",
    )
    reporter.record_comparison(
        "Collection Methods",
        "with_options",
        neo_with_options if neo_with_options else "FAIL",
        mongo_with_options if mongo_with_options else None,
        skip_reason="MongoDB not available" if not client else None,
    )
