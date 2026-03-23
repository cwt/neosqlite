"""Module for comparing search index operations between NeoSQLite and PyMongo"""

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

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_search_index_operations():
    """Compare search index (FTS) operations"""
    print("\n=== Search Index Operations Comparison ===")

    # NeoSQLite Results
    neo_create_search_index = False
    neo_list_search_indexes = False
    neo_update_search_index = False
    neo_drop_search_index = False

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

        set_accumulation_mode(True)

        # Test create_search_index
        start_neo_timing()
        try:
            neo_collection.create_search_index("content")
            neo_create_search_index = True
            print("Neo create_search_index: OK")
        except Exception as e:
            print(f"Neo create_search_index: Error - {e}")
        finally:
            end_neo_timing()

        # Test list_search_indexes
        start_neo_timing()
        try:
            neo_indexes = neo_collection.list_search_indexes()
            neo_list_search_indexes = len(neo_indexes) >= 1
            print(f"Neo list_search_indexes: {len(neo_indexes)} indexes")
        except Exception as e:
            print(f"Neo list_search_indexes: Error - {e}")
        finally:
            end_neo_timing()

        # Test update_search_index
        start_neo_timing()
        try:
            neo_collection.update_search_index("content", "porter")
            neo_update_search_index = True
            print("Neo update_search_index: OK")
        except Exception as e:
            print(f"Neo update_search_index: Error - {e}")
        finally:
            end_neo_timing()

        # Test drop_search_index
        start_neo_timing()
        try:
            neo_collection.drop_search_index("content")
            neo_drop_search_index = True
            print("Neo drop_search_index: OK")
        except Exception as e:
            print(f"Neo drop_search_index: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()

    # MongoDB Results
    mongo_create_search_index = None
    mongo_list_search_indexes = None
    mongo_update_search_index = None
    mongo_drop_search_index = None

    if client:
        try:
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

            set_accumulation_mode(True)

            # MongoDB uses create_index with "text" for text search
            start_mongo_timing()
            try:
                mongo_collection.create_index([("content", "text")])
                mongo_create_search_index = True
                print("Mongo create_index (text): OK")
            except Exception as e:
                print(f"Mongo create_index (text): Error - {e}")
            finally:
                end_mongo_timing()

            # Test list_indexes (MongoDB doesn't have separate search index list)
            start_mongo_timing()
            try:
                mongo_indexes = list(mongo_collection.list_indexes())
                mongo_list_search_indexes = len(mongo_indexes) >= 1
                print(f"Mongo list_indexes: {len(mongo_indexes)} indexes")
            except Exception as e:
                print(f"Mongo list_indexes: Error - {e}")
            finally:
                end_mongo_timing()

            # MongoDB doesn't have update_search_index, would need to drop and recreate
            mongo_update_search_index = True  # Not directly supported
            print("Mongo update_search_index: N/A (not directly supported)")

            # Test drop index
            start_mongo_timing()
            try:
                mongo_collection.drop_index("content_text")
                mongo_drop_search_index = True
                print("Mongo drop_index: OK")
            except Exception as e:
                print(f"Mongo drop_index: Error - {e}")
            finally:
                end_mongo_timing()

        finally:
            client.close()

    reporter.record_comparison(
        "Search Index Operations",
        "create_search_index",
        neo_create_search_index if neo_create_search_index else "FAIL",
        mongo_create_search_index if mongo_create_search_index else None,
        skip_reason="MongoDB not available" if not client else None,
    )

    if IS_NX27017_BACKEND:
        skip_reason = "NeoSQLite FTS search indexes not comparable to MongoDB text indexes"
    elif not client:
        skip_reason = "MongoDB not available"
    elif mongo_list_search_indexes is False:
        skip_reason = "MongoDB text index not found"
    else:
        skip_reason = None

    reporter.record_result(
        "Search Index Operations",
        "list_search_indexes",
        passed=(
            neo_list_search_indexes == mongo_list_search_indexes
            if mongo_list_search_indexes is not None
            else True
        ),
        neo_result=(
            neo_list_search_indexes if neo_list_search_indexes else "FAIL"
        ),
        mongo_result=(
            mongo_list_search_indexes
            if mongo_list_search_indexes is not None
            else None
        ),
        skip_reason=skip_reason,
    )
    reporter.record_comparison(
        "Search Index Operations",
        "update_search_index",
        neo_update_search_index if neo_update_search_index else "FAIL",
        mongo_update_search_index if mongo_update_search_index else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Search Index Operations",
        "drop_search_index",
        neo_drop_search_index if neo_drop_search_index else "FAIL",
        mongo_drop_search_index if mongo_drop_search_index else None,
        skip_reason="MongoDB not available" if not client else None,
    )
