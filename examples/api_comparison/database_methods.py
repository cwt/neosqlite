"""Module for comparing database methods between NeoSQLite and PyMongo"""

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


def compare_database_methods():
    """Compare database/connection methods"""
    print("\n=== Database Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        # Test client property
        neo_client = neo_conn.client == neo_conn
        print(f"Neo client property: {'OK' if neo_client else 'FAIL'}")

        # Test db_path property (NeoSQLite specific)
        neo_db_path = neo_conn.db_path == ":memory:"
        print(f"Neo db_path property: {'OK' if neo_db_path else 'FAIL'}")

        # Test get_collection (doesn't create until used)
        try:
            coll = neo_conn.get_collection("test_get_coll")
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
            new_coll = neo_conn.create_collection("test_create_coll")
            new_coll.insert_one({"name": "test"})
            neo_create_collection = new_coll is not None
            print(
                f"Neo create_collection: {'OK' if neo_create_collection else 'FAIL'}"
            )
        except Exception as e:
            neo_create_collection = False
            print(f"Neo create_collection: Error - {e}")

        # Test list_collection_names
        try:
            names = neo_conn.list_collection_names()
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
            neo_conn.drop_collection("test_create_coll")
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
            neo_conn.rename_collection("rename_old", "rename_new")
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
            neo_ping = neo_conn.command("ping")
            neo_ping_ok = neo_ping.get("ok") == 1.0
            print(f"Neo command('ping'): {'OK' if neo_ping_ok else 'FAIL'}")

            # Test serverStatus command
            neo_server_status = neo_conn.command("serverStatus")
            neo_server_status_ok = neo_server_status.get("ok") == 1.0
            print(
                f"Neo command('serverStatus'): {'OK' if neo_server_status_ok else 'FAIL'}"
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

            # listCollections can be run as a cursor command
            cursor = neo_conn.cursor_command("listCollections")
            results = list(cursor)

            # PRAGMA test (NeoSQLite specific improvement)
            cursor_pragma = neo_conn.cursor_command(
                "table_info", table="cursor_test_coll"
            )
            results_pragma = list(cursor_pragma)

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
            doc = neo_conn.dereference(dbref)
            neo_deref = doc is not None and doc.get("a") == 1
            print(f"Neo dereference(): {'OK' if neo_deref else 'FAIL'}")
        except Exception as e:
            neo_deref = False
            print(f"Neo dereference(): Error - {e}")

        # Test with_options()
        try:
            neo_db_opts = neo_conn.with_options(write_concern={"w": "majority"})
            neo_with_options = (
                neo_db_opts is not None
                and neo_db_opts.write_concern == {"w": "majority"}
            )
            print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")
        except Exception as e:
            neo_with_options = False
            print(f"Neo with_options(): Error - {e}")

        end_neo_timing()

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

    if client:
        start_mongo_timing()
        mongo_db = client.test_database_methods

        # Test client property
        mongo_client = client == client
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
            coll = mongo_db.get_collection("test_get_coll")
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
            new_coll = mongo_db.create_collection("test_create_coll")
            new_coll.insert_one({"name": "test"})
            mongo_create_collection = new_coll is not None
            print(
                f"Mongo create_collection: {'OK' if mongo_create_collection else 'FAIL'}"
            )
        except Exception as e:
            mongo_create_collection = False
            print(f"Mongo create_collection: Error - {e}")

        # Test list_collection_names
        try:
            names = mongo_db.list_collection_names()
            mongo_list_collections = len(names) >= 1
            print(f"Mongo list_collection_names: {len(names)} collections")
        except Exception as e:
            mongo_list_collections = False
            print(f"Mongo list_collection_names: Error - {e}")

        # Test drop_collection
        try:
            mongo_db.drop_collection("test_create_coll")
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
            # MongoDB doesn't have db.rename_collection(), uses collection.rename() instead
            mongo_coll_rename.rename("rename_new")
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
            mongo_ping = mongo_db.command("ping")
            mongo_ping_ok = mongo_ping.get("ok") == 1.0
            print(f"Mongo command('ping'): {'OK' if mongo_ping_ok else 'FAIL'}")

            # Test serverStatus command
            mongo_server_status = mongo_db.command("serverStatus")
            mongo_server_status_ok = mongo_server_status.get("ok") == 1.0
            print(
                f"Mongo command('serverStatus'): {'OK' if mongo_server_status_ok else 'FAIL'}"
            )
        except Exception as e:
            mongo_ping_ok = False
            mongo_server_status_ok = False
            print(f"Mongo command(): Error - {e}")

        # Test cursor_command()
        try:
            from pymongo.command_cursor import CommandCursor

            cursor = mongo_db.cursor_command("listCollections")
            # In some PyMongo versions, cursor_command returns a cursor directly
            # In others it might return a dict with a cursor key
            mongo_cursor_command = isinstance(cursor, CommandCursor) or (
                isinstance(cursor, dict) and "cursor" in cursor
            )
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
            doc = mongo_db.dereference(dbref)
            mongo_deref = doc is not None and doc.get("a") == 1
            print(f"Mongo dereference(): {'OK' if mongo_deref else 'FAIL'}")
        except Exception as e:
            mongo_deref = False
            print(f"Mongo dereference(): Error - {e}")

        # Test with_options()
        # Note: MongoDB with_options() returns a new Database instance
        try:
            from pymongo import WriteConcern

            mongo_db_opts = mongo_db.with_options(
                write_concern=WriteConcern(w="majority")
            )
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

        end_mongo_timing()
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
