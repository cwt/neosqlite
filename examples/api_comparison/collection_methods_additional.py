"""Module for comparing additional collection methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_collection_methods():
    """Compare additional collection methods"""
    print("\n=== Additional Collection Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        # Test drop()
        neo_collection = neo_conn.test_drop
        neo_collection.insert_one({"name": "test"})
        try:
            neo_collection.drop()
            neo_drop = "test_drop" not in neo_conn.list_collection_names()
            print(f"Neo drop(): {'OK' if neo_drop else 'FAIL'}")
        except Exception as e:
            neo_drop = False
            print(f"Neo drop(): Error - {e}")

        # Test database property
        neo_collection2 = neo_conn.test
        try:
            neo_db = neo_collection2.database
            neo_db_ok = neo_db is not None
            print(f"Neo database property: {'OK' if neo_db_ok else 'FAIL'}")
        except Exception as e:
            neo_db_ok = False
            print(f"Neo database property: Error - {e}")

        # Test watch() - Change streams
        # NeoSQLite: Implemented via SQLite triggers
        # MongoDB: Requires replica set (not available in standalone test)
        try:
            neo_collection_watch = neo_conn.test_watch
            neo_stream = neo_collection_watch.watch()
            neo_watch = neo_stream is not None
            neo_stream.close()
            print(
                f"Neo watch(): {'OK (SQLite triggers)' if neo_watch else 'FAIL'}"
            )
        except Exception as e:
            neo_watch = False
            print(f"Neo watch(): Error - {e}")

        # Test full_name property
        neo_collection_fullname = neo_conn.test_fullname
        neo_full_name = None
        try:
            neo_full_name = neo_collection_fullname.full_name
            neo_full_name_ok = (
                isinstance(neo_full_name, str) and len(neo_full_name) > 0
            )
            print(f"Neo full_name: '{neo_full_name}'")
        except Exception as e:
            neo_full_name_ok = False
            print(f"Neo full_name: Error - {e}")

        # Test client property
        try:
            neo_client_prop = neo_collection2.client == neo_conn
            print(f"Neo client property: {'OK' if neo_client_prop else 'FAIL'}")
        except Exception as e:
            neo_client_prop = False
            print(f"Neo client property: Error - {e}")

        # Test db_path property (NeoSQLite specific)
        try:
            neo_db_path_prop = neo_collection2.db_path == ":memory:"
            print(
                f"Neo db_path property: {'OK' if neo_db_path_prop else 'FAIL'}"
            )
        except Exception as e:
            neo_db_path_prop = False
            print(f"Neo db_path property: Error - {e}")

        # Test with_options()
        neo_collection_opts = neo_conn.test_with_opts
        try:
            neo_coll_opts = neo_collection_opts.with_options(
                write_concern={"w": "majority"}, read_preference=None
            )
            neo_with_options = (
                neo_coll_opts is not None
                and neo_coll_opts.name == neo_collection_opts.name
            )
            print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")
        except Exception as e:
            neo_with_options = False
            print(f"Neo with_options(): Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_collection2 = None
    mongo_db = None
    mongo_db_ok = None
    mongo_drop = None
    mongo_watch = None
    mongo_full_name_ok = None
    mongo_client_prop = None
    mongo_with_options = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        # Test drop()
        mongo_collection = mongo_db.test_drop
        mongo_collection.insert_one({"name": "test"})
        try:
            mongo_collection.drop()
            mongo_drop = "test_drop" not in mongo_db.list_collection_names()
            print(f"Mongo drop(): {'OK' if mongo_drop else 'FAIL'}")
        except Exception as e:
            mongo_drop = False
            print(f"Mongo drop(): Error - {e}")

        # Test database property
        mongo_collection2 = mongo_db.test
        try:
            mongo_db_prop = mongo_collection2.database
            mongo_db_ok = mongo_db_prop is not None
            print(f"Mongo database property: {'OK' if mongo_db_ok else 'FAIL'}")
        except Exception as e:
            mongo_db_ok = False
            print(f"Mongo database property: Error - {e}")

        # Test watch() - Change streams (requires replica set)
        try:
            mongo_collection3 = mongo_db.test_watch
            # watch() requires a replica set - will fail on standalone MongoDB
            pipeline = []
            change_stream = mongo_collection3.watch(pipeline)
            # If we get here without exception, it means replica set is available
            mongo_watch = change_stream is not None
            print(
                f"Mongo watch(): {'OK (replica set available)' if mongo_watch else 'FAIL'}"
            )
            change_stream.close()
        except Exception as e:
            # This is expected on standalone MongoDB (no replica set)
            mongo_watch = False
            print(f"Mongo watch(): SKIPPED (requires replica set) - {e}")

        # Test full_name property
        mongo_collection_fullname = mongo_db.test_fullname
        mongo_full_name = None
        try:
            mongo_full_name = mongo_collection_fullname.full_name
            mongo_full_name_ok = (
                isinstance(mongo_full_name, str) and len(mongo_full_name) > 0
            )
            print(f"Mongo full_name: '{mongo_full_name}'")
        except Exception as e:
            mongo_full_name_ok = False
            print(f"Mongo full_name: Error - {e}")

        # Test client property
        try:
            # In PyMongo, Collection has a .database property, and Database has a .client property
            # NeoSQLite added .client directly to Collection for convenience/compatibility
            mongo_client_prop = mongo_collection2.database.client == client
            print(
                f"Mongo client property: {'OK' if mongo_client_prop else 'FAIL'}"
            )
        except Exception as e:
            mongo_client_prop = False
            print(f"Mongo client property: Error - {e}")

        # Test with_options()
        mongo_collection_opts = mongo_db.test_with_opts
        try:
            from pymongo.write_concern import WriteConcern

            mongo_coll_opts = mongo_collection_opts.with_options(
                write_concern=WriteConcern(w="majority"), read_preference=None
            )
            mongo_with_options = (
                mongo_coll_opts is not None
                and mongo_coll_opts.name == mongo_collection_opts.name
            )
            print(
                f"Mongo with_options(): {'OK' if mongo_with_options else 'FAIL'}"
            )
        except Exception as e:
            mongo_with_options = False
            print(f"Mongo with_options(): Error - {e}")

        end_mongo_timing()
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
    reporter.record_comparison(
        "Collection Methods",
        "watch",
        "IMPLEMENTED (SQLite triggers)" if neo_watch else "FAIL",
        "IMPLEMENTED (replica set)" if mongo_watch else None,
        skip_reason="NeoSQLite: Implemented via SQLite triggers; MongoDB: Requires replica set (not available in standalone test)",
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
