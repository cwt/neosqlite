"""Module for comparing database methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
    set_accumulation_mode,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_database_methods():
    """Compare database/connection methods"""
    print("\n=== Database Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)

        # Test client property
        start_neo_timing()
        neo_client = neo_conn.client == neo_conn
        end_neo_timing()
        print(f"Neo client property: {'OK' if neo_client else 'FAIL'}")

        # Test db_path property (NeoSQLite specific)
        start_neo_timing()
        neo_db_path = neo_conn.db_path == ":memory:"
        end_neo_timing()
        print(f"Neo db_path property: {'OK' if neo_db_path else 'FAIL'}")

        # Test get_collection (doesn't create until used)
        try:
            start_neo_timing()
            coll = neo_conn.get_collection("test_get_coll")
            end_neo_timing()
            neo_get_collection = (
                coll is not None and coll.name == "test_get_coll"
            )
            print(
                f"Neo get_collection: {'OK' if neo_get_collection else 'FAIL'}"
            )
        except Exception as e:
            neo_get_collection = False
            print(f"Neo get_collection: Error - {e}")

        # Test create_collection
        try:
            start_neo_timing()
            new_coll = neo_conn.create_collection("test_create_coll")
            new_coll.insert_one({"name": "test"})
            end_neo_timing()
            neo_create_collection = new_coll is not None
            print(
                f"Neo create_collection: {'OK' if neo_create_collection else 'FAIL'}"
            )
        except Exception as e:
            neo_create_collection = False
            print(f"Neo create_collection: Error - {e}")

        # Test list_collection_names
        try:
            start_neo_timing()
            names = neo_conn.list_collection_names()
            end_neo_timing()
            # Filter out SQLite internal tables
            user_collections = [n for n in names if not n.startswith("sqlite_")]
            neo_list_collections = len(user_collections) >= 1
            print(
                f"Neo list_collection_names: {len(user_collections)} user collections"
            )
        except Exception as e:
            neo_list_collections = False
            print(f"Neo list_collection_names: Error - {e}")

        # Test drop_collection
        try:
            start_neo_timing()
            neo_conn.drop_collection("test_create_coll")
            end_neo_timing()
            names = neo_conn.list_collection_names()
            neo_drop_collection = "test_create_coll" not in names
            print(
                f"Neo drop_collection: {'OK' if neo_drop_collection else 'FAIL'}"
            )
        except Exception as e:
            neo_drop_collection = False
            print(f"Neo drop_collection: Error - {e}")

        # Test rename_collection
        try:
            neo_coll_rename = neo_conn.create_collection("rename_old")
            neo_coll_rename.insert_one({"name": "rename_test"})

            start_neo_timing()
            neo_conn.rename_collection("rename_old", "rename_new")
            end_neo_timing()

            names = neo_conn.list_collection_names()
            neo_rename_collection = (
                "rename_new" in names and "rename_old" not in names
            )
            print(
                f"Neo rename_collection: {'OK' if neo_rename_collection else 'FAIL'}"
            )
        except Exception as e:
            neo_rename_collection = False
            print(f"Neo rename_collection: Error - {e}")

        # Test command()
        try:
            # Test ping command
            start_neo_timing()
            neo_ping = neo_conn.command("ping")
            end_neo_timing()
            neo_ping_ok = neo_ping.get("ok") == 1.0
            print(f"Neo command('ping'): {'OK' if neo_ping_ok else 'FAIL'}")

            # Test serverStatus command
            start_neo_timing()
            neo_server_status = neo_conn.command("serverStatus")
            end_neo_timing()
            neo_server_status_ok = neo_server_status.get("ok") == 1.0
            print(
                f"Neo command('serverStatus'): {'OK' if neo_server_status_ok else 'FAIL'}"
            )

            # Test dbStats command - check keys and value types
            neo_coll_for_stats = neo_conn.create_collection("stats_test_coll")
            neo_coll_for_stats.insert_one({"name": "test", "value": 123})
            neo_coll_for_stats.create_index("value")

            start_neo_timing()
            neo_db_stats = neo_conn.command("dbStats")
            end_neo_timing()

            neo_db_stats_type_ok = isinstance(neo_db_stats, dict)
            expected_dbstats_keys = {
                "ok",
                "db",
                "collections",
                "views",
                "objects",
                "avgObjSize",
                "dataSize",
                "storageSize",
                "indexes",
                "indexSize",
                "totalSize",
                "fsTotalSize",
                "fsUsedSize",
                "scaleFactor",
            }
            neo_db_stats_keys_ok = expected_dbstats_keys.issubset(
                set(neo_db_stats.keys())
            )

            type_checks = {
                "ok": (float, int),
                "db": str,
                "collections": int,
                "views": int,
                "objects": int,
                "avgObjSize": int,
                "dataSize": int,
                "storageSize": int,
                "indexes": int,
                "indexSize": int,
                "totalSize": int,
                "fsTotalSize": int,
                "fsUsedSize": int,
                "scaleFactor": int,
            }
            neo_db_stats_types_ok = True
            for key, expected_type in type_checks.items():
                value = neo_db_stats.get(key)
                if value is not None and not isinstance(value, expected_type):
                    neo_db_stats_types_ok = False
                    break

            neo_db_stats_ok = (
                neo_db_stats_type_ok
                and neo_db_stats_keys_ok
                and neo_db_stats_types_ok
            )
            print(
                f"Neo command('dbStats'): {'OK' if neo_db_stats_ok else 'FAIL'}"
            )
        except Exception as e:
            neo_ping_ok = False
            neo_server_status_ok = False
            print(f"Neo command(): Error - {e}")

        # Test cursor_command()
        try:
            # Ensure collection exists for PRAGMA test
            coll_dummy = neo_conn.create_collection("cursor_test_coll")
            coll_dummy.insert_one({"a": 1})

            start_neo_timing()
            # listCollections can be run as a cursor command
            cursor = neo_conn.cursor_command("listCollections")
            results = list(cursor)

            # PRAGMA test (NeoSQLite specific improvement)
            cursor_pragma = neo_conn.cursor_command(
                "table_info", table="cursor_test_coll"
            )
            results_pragma = list(cursor_pragma)
            end_neo_timing()

            neo_cursor_command = len(results) > 0 and len(results_pragma) > 0
            print(
                f"Neo cursor_command(): {'OK' if neo_cursor_command else 'FAIL'}"
            )
        except Exception as e:
            neo_cursor_command = False
            print(f"Neo cursor_command(): Error - {e}")

        # Test dereference()
        try:
            coll = neo_conn["deref_test"]
            res = coll.insert_one({"a": 1})
            dbref = {"$ref": "deref_test", "$id": res.inserted_id}

            start_neo_timing()
            doc = neo_conn.dereference(dbref)
            end_neo_timing()

            neo_deref = doc is not None and doc.get("a") == 1
            print(f"Neo dereference(): {'OK' if neo_deref else 'FAIL'}")
        except Exception as e:
            neo_deref = False
            print(f"Neo dereference(): Error - {e}")

        # Test with_options()
        try:
            start_neo_timing()
            neo_db_opts = neo_conn.with_options(write_concern={"w": "majority"})
            end_neo_timing()
            neo_with_options = (
                neo_db_opts is not None
                and neo_db_opts.write_concern == {"w": "majority"}
            )
            print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")
        except Exception as e:
            neo_with_options = False
            print(f"Neo with_options(): Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_client = None
    mongo_coll_rename = None
    mongo_create_collection = None
    mongo_db = None
    mongo_drop_collection = None
    mongo_get_collection = None
    mongo_list_collections = None
    mongo_ping_ok = None
    mongo_rename_collection = None
    mongo_server_status_ok = None
    mongo_cursor_command = None
    mongo_deref = None
    mongo_with_options = None
    mongo_db_stats_ok = None

    if client:
        set_accumulation_mode(True)
        mongo_db = client.test_database_methods

        # Test client property
        start_mongo_timing()
        mongo_client = client == client
        end_mongo_timing()
        print(f"Mongo client property: {'OK' if mongo_client else 'FAIL'}")

        # Clean up any leftover collections from previous runs BEFORE testing
        for coll_name in [
            "test_get_coll",
            "test_create_coll",
            "rename_old",
            "rename_new",
            "deref_test",
        ]:
            try:
                mongo_db.drop_collection(coll_name)
            except Exception:
                pass

        # Test get_collection
        try:
            start_mongo_timing()
            coll = mongo_db.get_collection("test_get_coll")
            end_mongo_timing()
            mongo_get_collection = (
                coll is not None and coll.name == "test_get_coll"
            )
            print(
                f"Mongo get_collection: {'OK' if mongo_get_collection else 'FAIL'}"
            )
        except Exception as e:
            mongo_get_collection = False
            print(f"Mongo get_collection: Error - {e}")

        # Test create_collection
        try:
            start_mongo_timing()
            new_coll = mongo_db.create_collection("test_create_coll")
            new_coll.insert_one({"name": "test"})
            end_mongo_timing()
            mongo_create_collection = new_coll is not None
            print(
                f"Mongo create_collection: {'OK' if mongo_create_collection else 'FAIL'}"
            )
        except Exception as e:
            mongo_create_collection = False
            print(f"Mongo create_collection: Error - {e}")

        # Test list_collection_names
        try:
            start_mongo_timing()
            names = mongo_db.list_collection_names()
            end_mongo_timing()
            mongo_list_collections = len(names) >= 1
            print(f"Mongo list_collection_names: {len(names)} collections")
        except Exception as e:
            mongo_list_collections = False
            print(f"Mongo list_collection_names: Error - {e}")

        # Test drop_collection
        try:
            start_mongo_timing()
            mongo_db.drop_collection("test_create_coll")
            end_mongo_timing()
            names = mongo_db.list_collection_names()
            mongo_drop_collection = "test_create_coll" not in names
            print(
                f"Mongo drop_collection: {'OK' if mongo_drop_collection else 'FAIL'}"
            )
        except Exception as e:
            mongo_drop_collection = False
            print(f"Mongo drop_collection: Error - {e}")

        # Test rename_collection - MongoDB uses collection.rename() not db.rename_collection()
        try:
            mongo_coll_rename = mongo_db.create_collection("rename_old")
            mongo_coll_rename.insert_one({"name": "rename_test"})

            start_mongo_timing()
            # MongoDB doesn't have db.rename_collection(), uses collection.rename() instead
            mongo_coll_rename.rename("rename_new")
            end_mongo_timing()

            names = mongo_db.list_collection_names()
            mongo_rename_collection = (
                "rename_new" in names and "rename_old" not in names
            )
            print(
                f"Mongo collection.rename(): {'OK' if mongo_rename_collection else 'FAIL'}"
            )
        except Exception as e:
            mongo_rename_collection = False
            print(f"Mongo collection.rename(): Error - {e}")

        # Test command()
        try:
            # Test ping command
            start_mongo_timing()
            mongo_ping = mongo_db.command("ping")
            end_mongo_timing()
            mongo_ping_ok = mongo_ping.get("ok") == 1.0
            print(f"Mongo command('ping'): {'OK' if mongo_ping_ok else 'FAIL'}")

            # Test serverStatus command
            start_mongo_timing()
            mongo_server_status = mongo_db.command("serverStatus")
            end_mongo_timing()
            mongo_server_status_ok = mongo_server_status.get("ok") == 1.0
            print(
                f"Mongo command('serverStatus'): {'OK' if mongo_server_status_ok else 'FAIL'}"
            )

            # Test dbStats command
            mongo_coll_for_stats = mongo_db["stats_test_coll_mongo"]
            mongo_coll_for_stats.insert_one({"name": "test", "value": 123})
            mongo_coll_for_stats.create_index("value")

            start_mongo_timing()
            mongo_db_stats = mongo_db.command("dbStats")
            end_mongo_timing()

            mongo_db_stats_type_ok = isinstance(mongo_db_stats, dict)
            expected_dbstats_keys = {
                "ok",
                "db",
                "collections",
                "views",
                "objects",
                "avgObjSize",
                "dataSize",
                "storageSize",
                "indexes",
                "indexSize",
                "totalSize",
                "fsTotalSize",
                "fsUsedSize",
                "scaleFactor",
            }
            mongo_db_stats_keys_ok = expected_dbstats_keys.issubset(
                set(mongo_db_stats.keys())
            )

            type_checks = {
                "ok": (float, int),
                "db": str,
                "collections": int,
                "views": int,
                "objects": int,
                "avgObjSize": (int, float),
                "dataSize": (int, float),
                "storageSize": (int, float),
                "indexes": int,
                "indexSize": (int, float),
                "totalSize": (int, float),
                "fsTotalSize": (int, float),
                "fsUsedSize": (int, float),
                "scaleFactor": int,
            }
            mongo_db_stats_types_ok = True
            for key, expected_type in type_checks.items():
                value = mongo_db_stats.get(key)
                if value is None:
                    continue
                if hasattr(value, "__class__") and "Int64" in str(type(value)):
                    continue
                if not isinstance(value, expected_type):
                    mongo_db_stats_types_ok = False
                    break

            mongo_db_stats_ok = (
                mongo_db_stats_type_ok
                and mongo_db_stats_keys_ok
                and mongo_db_stats_types_ok
            )
            print(
                f"Mongo command('dbStats'): {'OK' if mongo_db_stats_ok else 'FAIL'}"
            )

            # Clean up
            try:
                mongo_db.drop_collection("stats_test_coll_mongo")
            except Exception:
                pass
        except Exception as e:
            mongo_ping_ok = False
            mongo_server_status_ok = False
            print(f"Mongo command(): Error - {e}")

        # Test cursor_command()
        try:
            from pymongo.command_cursor import CommandCursor

            start_mongo_timing()
            cursor = mongo_db.cursor_command("listCollections")
            # In some PyMongo versions, cursor_command returns a cursor directly
            # In others it might return a dict with a cursor key
            mongo_cursor_command = isinstance(cursor, CommandCursor) or (
                isinstance(cursor, dict) and "cursor" in cursor
            )
            end_mongo_timing()
            print(
                f"Mongo cursor_command(): {'OK' if mongo_cursor_command else 'FAIL'}"
            )
        except Exception as e:
            mongo_cursor_command = False
            print(f"Mongo cursor_command(): Error - {e}")

        # Test dereference()
        try:
            from bson.dbref import DBRef

            coll = mongo_db["deref_test"]
            res = coll.insert_one({"a": 1})
            dbref = DBRef("deref_test", res.inserted_id)

            start_mongo_timing()
            doc = mongo_db.dereference(dbref)
            end_mongo_timing()

            mongo_deref = doc is not None and doc.get("a") == 1
            print(f"Mongo dereference(): {'OK' if mongo_deref else 'FAIL'}")
        except Exception as e:
            mongo_deref = False
            print(f"Mongo dereference(): Error - {e}")

        # Test with_options()
        # Note: MongoDB with_options() returns a new Database instance
        try:
            from pymongo import WriteConcern

            start_mongo_timing()
            mongo_db_opts = mongo_db.with_options(
                write_concern=WriteConcern(w="majority")
            )
            end_mongo_timing()

            mongo_with_options = mongo_db_opts is not None
            print(
                f"Mongo with_options(): {'OK' if mongo_with_options else 'FAIL'}"
            )
        except Exception as e:
            mongo_with_options = False
            print(f"Mongo with_options(): Error - {e}")

        # Clean up
        for coll_name in ["test_get_coll", "rename_new", "deref_test"]:
            try:
                mongo_db.drop_collection(coll_name)
            except Exception:
                pass

        client.close()

    reporter.record_comparison(
        "Database Methods",
        "client",
        neo_client if neo_client else "FAIL",
        mongo_client if mongo_client else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "db_path",
        neo_db_path if neo_db_path else "FAIL",
        None,
        skip_reason="NeoSQLite specific (SQLite database path)",
    )
    reporter.record_comparison(
        "Database Methods",
        "get_collection",
        neo_get_collection if neo_get_collection else "FAIL",
        mongo_get_collection if mongo_get_collection else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "create_collection",
        neo_create_collection if neo_create_collection else "FAIL",
        mongo_create_collection if mongo_create_collection else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "list_collection_names",
        neo_list_collections if neo_list_collections else "FAIL",
        mongo_list_collections if mongo_list_collections else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "drop_collection",
        neo_drop_collection if neo_drop_collection else "FAIL",
        mongo_drop_collection if mongo_drop_collection else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    # Note: MongoDB uses collection.rename() instead of db.rename_collection()
    reporter.record_comparison(
        "Database Methods",
        "rename_collection",
        neo_rename_collection if neo_rename_collection else "FAIL",
        mongo_rename_collection if mongo_rename_collection else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_ping",
        neo_ping_ok if neo_ping_ok else "FAIL",
        mongo_ping_ok if mongo_ping_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_server_status",
        neo_server_status_ok if neo_server_status_ok else "FAIL",
        mongo_server_status_ok if mongo_server_status_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_db_stats",
        neo_db_stats_ok if neo_db_stats_ok else "FAIL",
        mongo_db_stats_ok if mongo_db_stats_ok else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "cursor_command",
        neo_cursor_command if neo_cursor_command else "FAIL",
        mongo_cursor_command if mongo_cursor_command else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "dereference",
        neo_deref if neo_deref else "FAIL",
        mongo_deref if mongo_deref else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "with_options",
        neo_with_options if neo_with_options else "FAIL",
        mongo_with_options if mongo_with_options else None,
        skip_reason="MongoDB not available" if not client else None,
    )
