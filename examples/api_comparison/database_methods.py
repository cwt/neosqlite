"""Module for comparing database methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_database_methods():
    """Compare database/connection methods"""
    print("\n=== Database Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_coll_rename = None

    mongo_create_collection = None

    mongo_db = None

    mongo_drop_collection = None

    mongo_get_collection = None

    mongo_list_collections = None

    mongo_rename_collection = None

    if client:
        mongo_db = client.test_database_methods

        # Clean up any leftover collections from previous runs BEFORE testing
        for coll_name in [
            "test_get_coll",
            "test_create_coll",
            "rename_old",
            "rename_new",
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

        # Clean up
        for coll_name in ["test_get_coll", "rename_new"]:
            try:
                mongo_db.drop_collection(coll_name)
            except Exception:
                pass

        client.close()

        reporter.record_result(
            "Database Methods",
            "get_collection",
            neo_get_collection,
            neo_get_collection,
            mongo_get_collection,
        )
        reporter.record_result(
            "Database Methods",
            "create_collection",
            neo_create_collection,
            neo_create_collection,
            mongo_create_collection,
        )
        reporter.record_result(
            "Database Methods",
            "list_collection_names",
            neo_list_collections,
            neo_list_collections,
            mongo_list_collections,
        )
        reporter.record_result(
            "Database Methods",
            "drop_collection",
            neo_drop_collection,
            neo_drop_collection,
            mongo_drop_collection,
        )
        # Note: MongoDB uses collection.rename() instead of db.rename_collection()
        reporter.record_result(
            "Database Methods",
            "rename_collection",
            neo_rename_collection,
            neo_rename_collection,
            mongo_rename_collection,
        )
