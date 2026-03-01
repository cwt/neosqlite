"""Module for comparing search index operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_search_index_operations():
    """Compare search index (FTS) operations"""
    print("\n=== Search Index Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_search_index
        neo_collection.insert_many(
            [
                {"title": "Python programming", "content": "Learn Python"},
                {"title": "Java guide", "content": "Learn Java"},
                {
                    "title": "Python advanced",
                    "content": "Advanced Python topics",
                },
            ]
        )

        # Test create_search_index
        try:
            neo_collection.create_search_index("content")
            neo_create_search_index = True
            print("Neo create_search_index: OK")
        except Exception as e:
            neo_create_search_index = False
            print(f"Neo create_search_index: Error - {e}")

        # Test list_search_indexes
        try:
            neo_indexes = neo_collection.list_search_indexes()
            neo_list_search_indexes = len(neo_indexes) >= 1
            print(f"Neo list_search_indexes: {len(neo_indexes)} indexes")
        except Exception as e:
            neo_list_search_indexes = False
            print(f"Neo list_search_indexes: Error - {e}")

        # Test update_search_index
        try:
            neo_collection.update_search_index("content", "porter")
            neo_update_search_index = True
            print("Neo update_search_index: OK")
        except Exception as e:
            neo_update_search_index = False
            print(f"Neo update_search_index: Error - {e}")

        # Test drop_search_index
        try:
            neo_collection.drop_search_index("content")
            neo_drop_search_index = True
            print("Neo drop_search_index: OK")
        except Exception as e:
            neo_drop_search_index = False
            print(f"Neo drop_search_index: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_create_search_index = None

    mongo_db = None

    mongo_drop_search_index = None

    mongo_indexes = None

    mongo_list_search_indexes = None

    mongo_update_search_index = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_search_index
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"title": "Python programming", "content": "Learn Python"},
                {"title": "Java guide", "content": "Learn Java"},
                {
                    "title": "Python advanced",
                    "content": "Advanced Python topics",
                },
            ]
        )

        # MongoDB uses create_index with "text" for text search
        try:
            mongo_collection.create_index([("content", "text")])
            mongo_create_search_index = True
            print("Mongo create_index (text): OK")
        except Exception as e:
            mongo_create_search_index = False
            print(f"Mongo create_index (text): Error - {e}")

        # Test list_indexes (MongoDB doesn't have separate search index list)
        try:
            mongo_indexes = list(mongo_collection.list_indexes())
            mongo_list_search_indexes = len(mongo_indexes) >= 1
            print(f"Mongo list_indexes: {len(mongo_indexes)} indexes")
        except Exception as e:
            mongo_list_search_indexes = False
            print(f"Mongo list_indexes: Error - {e}")

        # MongoDB doesn't have update_search_index, would need to drop and recreate
        mongo_update_search_index = True  # Not directly supported
        print("Mongo update_search_index: N/A (not directly supported)")

        # Test drop index
        try:
            mongo_collection.drop_index("content_text")
            mongo_drop_search_index = True
            print("Mongo drop_index: OK")
        except Exception as e:
            mongo_drop_search_index = False
            print(f"Mongo drop_index: Error - {e}")

        client.close()

        reporter.record_result(
            "Search Index Operations",
            "create_search_index",
            neo_create_search_index,
            neo_create_search_index,
            mongo_create_search_index,
        )
        reporter.record_result(
            "Search Index Operations",
            "list_search_indexes",
            neo_list_search_indexes,
            neo_list_search_indexes,
            mongo_list_search_indexes,
        )
        reporter.record_result(
            "Search Index Operations",
            "update_search_index",
            neo_update_search_index,
            neo_update_search_index,
            mongo_update_search_index,
        )
        reporter.record_result(
            "Search Index Operations",
            "drop_search_index",
            neo_drop_search_index,
            neo_drop_search_index,
            mongo_drop_search_index,
        )
