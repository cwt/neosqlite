"""Module for comparing additional update operators between NeoSQLite and PyMongo"""

from datetime import datetime
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


def compare_additional_update_operators():
    """Compare additional update operators"""
    print("\n=== Additional Update Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one(
            {"name": "Alice", "tags": ["python"], "score": 100}
        )

        # Test $push
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "Alice"}, {"$push": {"tags": "sql"}}
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "Alice"})
            neo_push = "sql" in doc.get("tags", [])
            print(f"Neo $push: {'OK' if neo_push else 'FAIL'}")
        except Exception as e:
            neo_push = False
            print(f"Neo $push: Error - {e}")

        # Reset for $addToSet (not timed)
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
        )

        # Test $addToSet
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
            )
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "Alice"})
            tags = doc.get("tags", [])
            neo_addtoset = (
                "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
            )
            print(f"Neo $addToSet: {'OK' if neo_addtoset else 'FAIL'}")
        except Exception as e:
            neo_addtoset = False
            print(f"Neo $addToSet: Error - {e}")

        # Reset for $pull (not timed)
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql", "mongodb"]}}
        )

        # Test $pull
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "Alice"}, {"$pull": {"tags": "sql"}}
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "Alice"})
            neo_pull = "sql" not in doc.get("tags", [])
            print(f"Neo $pull: {'OK' if neo_pull else 'FAIL'}")
        except Exception as e:
            neo_pull = False
            print(f"Neo $pull: Error - {e}")

        # Reset for $pop (not timed)
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["first", "middle", "last"]}}
        )

        # Test $pop (remove last)
        try:
            start_neo_timing()
            neo_collection.update_one({"name": "Alice"}, {"$pop": {"tags": 1}})
            end_neo_timing()

            doc = neo_collection.find_one({"name": "Alice"})
            neo_pop = doc.get("tags", []) == ["first", "middle"]
            print(f"Neo $pop (last): {'OK' if neo_pop else 'FAIL'}")
        except Exception as e:
            neo_pop = False
            print(f"Neo $pop: Error - {e}")

        # Reset for $currentDate (not timed)
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"updated_at": None}}
        )

        # Test $currentDate
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "Alice"}, {"$currentDate": {"updated_at": True}}
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "Alice"})
            updated_at = doc.get("updated_at")
            # MongoDB returns datetime object, NeoSQLite should too for compatibility
            neo_currentdate = updated_at is not None and isinstance(
                updated_at, datetime
            )
            print(
                f"Neo $currentDate: {'OK' if neo_currentdate else 'FAIL'} (returns {type(updated_at).__name__})"
            )
        except Exception as e:
            neo_currentdate = False
            print(f"Neo $currentDate: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_addtoset = None
    mongo_collection = None
    mongo_currentdate = None
    mongo_db = None
    mongo_pop = None
    mongo_pull = None
    mongo_push = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {"name": "Alice", "tags": ["python"], "score": 100}
        )

        # Test $push
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "Alice"}, {"$push": {"tags": "sql"}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_push = "sql" in doc.get("tags", [])
            print(f"Mongo $push: {'OK' if mongo_push else 'FAIL'}")
        except Exception as e:
            mongo_push = False
            print(f"Mongo $push: Error - {e}")

        # Reset for $addToSet (not timed)
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
        )

        # Test $addToSet
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
            )
            mongo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            tags = doc.get("tags", [])
            mongo_addtoset = (
                "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
            )
            print(f"Mongo $addToSet: {'OK' if mongo_addtoset else 'FAIL'}")
        except Exception as e:
            mongo_addtoset = False
            print(f"Mongo $addToSet: Error - {e}")

        # Reset for $pull (not timed)
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql", "mongodb"]}}
        )

        # Test $pull
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "Alice"}, {"$pull": {"tags": "sql"}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pull = "sql" not in doc.get("tags", [])
            print(f"Mongo $pull: {'OK' if mongo_pull else 'FAIL'}")
        except Exception as e:
            mongo_pull = False
            print(f"Mongo $pull: Error - {e}")

        # Reset for $pop (not timed)
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["first", "middle", "last"]}}
        )

        # Test $pop (remove last)
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "Alice"}, {"$pop": {"tags": 1}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pop = doc.get("tags", []) == ["first", "middle"]
            print(f"Mongo $pop (last): {'OK' if mongo_pop else 'FAIL'}")
        except Exception as e:
            mongo_pop = False
            print(f"Mongo $pop: Error - {e}")

        # Reset for $currentDate (not timed)
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"updated_at": None}}
        )

        # Test $currentDate
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "Alice"}, {"$currentDate": {"updated_at": True}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            updated_at = doc.get("updated_at")
            mongo_currentdate = updated_at is not None
            print(
                f"Mongo $currentDate: {'OK' if mongo_currentdate else 'FAIL'} (returns datetime)"
            )
        except Exception as e:
            mongo_currentdate = False
            print(f"Mongo $currentDate: Error - {e}")

        client.close()

    reporter.record_comparison(
        "Update Operators",
        "$push",
        neo_push if neo_push else neo_push,
        mongo_push if mongo_push else mongo_push,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Operators",
        "$addToSet",
        neo_addtoset if neo_addtoset else neo_addtoset,
        mongo_addtoset if mongo_addtoset else mongo_addtoset,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Operators",
        "$pull",
        neo_pull if neo_pull else neo_pull,
        mongo_pull if mongo_pull else mongo_pull,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Operators",
        "$pop",
        neo_pop if neo_pop else neo_pop,
        mongo_pop if mongo_pop else mongo_pop,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Operators",
        "$currentDate",
        neo_currentdate if neo_currentdate else neo_currentdate,
        mongo_currentdate if mongo_currentdate else mongo_currentdate,
        skip_reason="MongoDB not available" if not client else None,
    )
