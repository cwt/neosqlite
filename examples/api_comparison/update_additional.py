"""Module for comparing additional update operators between NeoSQLite and PyMongo"""

from datetime import datetime
import warnings

import neosqlite

from .reporter import reporter
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
            neo_collection.update_one(
                {"name": "Alice"}, {"$push": {"tags": "sql"}}
            )
            doc = neo_collection.find_one({"name": "Alice"})
            neo_push = "sql" in doc.get("tags", [])
            print(f"Neo $push: {'OK' if neo_push else 'FAIL'}")
        except Exception as e:
            neo_push = False
            print(f"Neo $push: Error - {e}")

        # Reset for $addToSet
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
        )

        # Test $addToSet
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
            )
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
            )
            doc = neo_collection.find_one({"name": "Alice"})
            tags = doc.get("tags", [])
            neo_addtoset = (
                "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
            )
            print(f"Neo $addToSet: {'OK' if neo_addtoset else 'FAIL'}")
        except Exception as e:
            neo_addtoset = False
            print(f"Neo $addToSet: Error - {e}")

        # Reset for $pull
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql", "mongodb"]}}
        )

        # Test $pull
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$pull": {"tags": "sql"}}
            )
            doc = neo_collection.find_one({"name": "Alice"})
            neo_pull = "sql" not in doc.get("tags", [])
            print(f"Neo $pull: {'OK' if neo_pull else 'FAIL'}")
        except Exception as e:
            neo_pull = False
            print(f"Neo $pull: Error - {e}")

        # Reset for $pop
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["first", "middle", "last"]}}
        )

        # Test $pop (remove last)
        try:
            neo_collection.update_one({"name": "Alice"}, {"$pop": {"tags": 1}})
            doc = neo_collection.find_one({"name": "Alice"})
            neo_pop = doc.get("tags", []) == ["first", "middle"]
            print(f"Neo $pop (last): {'OK' if neo_pop else 'FAIL'}")
        except Exception as e:
            neo_pop = False
            print(f"Neo $pop: Error - {e}")

        # Reset for $currentDate
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"updated_at": None}}
        )

        # Test $currentDate
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$currentDate": {"updated_at": True}}
            )
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
            mongo_collection.update_one(
                {"name": "Alice"}, {"$push": {"tags": "sql"}}
            )
            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_push = "sql" in doc.get("tags", [])
            print(f"Mongo $push: {'OK' if mongo_push else 'FAIL'}")
        except Exception as e:
            mongo_push = False
            print(f"Mongo $push: Error - {e}")

        # Reset for $addToSet
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
        )

        # Test $addToSet
        try:
            mongo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
            )
            mongo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
            )
            doc = mongo_collection.find_one({"name": "Alice"})
            tags = doc.get("tags", [])
            mongo_addtoset = (
                "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
            )
            print(f"Mongo $addToSet: {'OK' if mongo_addtoset else 'FAIL'}")
        except Exception as e:
            mongo_addtoset = False
            print(f"Mongo $addToSet: Error - {e}")

        # Reset for $pull
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql", "mongodb"]}}
        )

        # Test $pull
        try:
            mongo_collection.update_one(
                {"name": "Alice"}, {"$pull": {"tags": "sql"}}
            )
            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pull = "sql" not in doc.get("tags", [])
            print(f"Mongo $pull: {'OK' if mongo_pull else 'FAIL'}")
        except Exception as e:
            mongo_pull = False
            print(f"Mongo $pull: Error - {e}")

        # Reset for $pop
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["first", "middle", "last"]}}
        )

        # Test $pop (remove last)
        try:
            mongo_collection.update_one(
                {"name": "Alice"}, {"$pop": {"tags": 1}}
            )
            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pop = doc.get("tags", []) == ["first", "middle"]
            print(f"Mongo $pop (last): {'OK' if mongo_pop else 'FAIL'}")
        except Exception as e:
            mongo_pop = False
            print(f"Mongo $pop: Error - {e}")

        # Reset for $currentDate
        mongo_collection.update_one(
            {"name": "Alice"}, {"$set": {"updated_at": None}}
        )

        # Test $currentDate
        try:
            mongo_collection.update_one(
                {"name": "Alice"}, {"$currentDate": {"updated_at": True}}
            )
            doc = mongo_collection.find_one({"name": "Alice"})
            updated_at = doc.get("updated_at")
            # MongoDB returns a datetime object, NeoSQLite returns ISO string
            mongo_currentdate = updated_at is not None
            print(
                f"Mongo $currentDate: {'OK' if mongo_currentdate else 'FAIL'} (returns datetime)"
            )
        except Exception as e:
            mongo_currentdate = False
            print(f"Mongo $currentDate: Error - {e}")

        client.close()

    reporter.record_result(
        "Update Operators", "$push", neo_push, neo_push, mongo_push
    )
    reporter.record_result(
        "Update Operators",
        "$addToSet",
        neo_addtoset,
        neo_addtoset,
        mongo_addtoset,
    )
    reporter.record_result(
        "Update Operators", "$pull", neo_pull, neo_pull, mongo_pull
    )
    reporter.record_result(
        "Update Operators", "$pop", neo_pop, neo_pop, mongo_pop
    )
    reporter.record_result(
        "Update Operators",
        "$currentDate",
        neo_currentdate,
        neo_currentdate,
        mongo_currentdate,
    )
