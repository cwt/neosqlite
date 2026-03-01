"""Module for comparing update modifiers between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_update_modifiers():
    """Compare update modifiers"""
    print("\n=== Update Modifiers Comparison ===")

    # Note: These update modifiers are not yet implemented in NeoSQLite
    # This test documents the gap for future implementation

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_modifiers
        neo_collection.insert_one(
            {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
        )

        # Test $each with $push - NOT YET IMPLEMENTED
        print("Neo $push $each: NOT YET IMPLEMENTED")

        # Test $position with $push - NOT YET IMPLEMENTED
        print("Neo $push $position: NOT YET IMPLEMENTED")

        # Test $slice with $push - NOT YET IMPLEMENTED
        print("Neo $push $slice: NOT YET IMPLEMENTED")

        # Test $bit - NOT YET IMPLEMENTED
        print("Neo $bit: NOT YET IMPLEMENTED")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_bit_and = None

    mongo_collection = None

    mongo_db = None

    mongo_each = None

    mongo_position = None

    mongo_slice = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_modifiers
        mongo_collection.delete_many({})
        mongo_collection.insert_one(
            {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
        )

        # Test $each with $push
        try:
            mongo_collection.update_one(
                {"name": "A"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
            )
            doc = mongo_collection.find_one({"name": "A"})
            mongo_each = doc and len(doc.get("tags", [])) == 4
            print(f"Mongo $push $each: {'OK' if mongo_each else 'FAIL'}")
        except Exception as e:
            mongo_each = False
            print(f"Mongo $push $each: Error - {e}")

        # Test $position with $push
        try:
            mongo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
            )
            doc = mongo_collection.find_one({"name": "A"})
            mongo_position = doc and doc.get("tags", [])[0] == "x"
            print(
                f"Mongo $push $position: {'OK' if mongo_position else 'FAIL'}"
            )
        except Exception as e:
            mongo_position = False
            print(f"Mongo $push $position: Error - {e}")

        # Test $slice with $push
        try:
            mongo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["y", "z"], "$slice": -3}}},
            )
            doc = mongo_collection.find_one({"name": "A"})
            mongo_slice = doc and len(doc.get("tags", [])) == 3
            print(f"Mongo $push $slice: {'OK' if mongo_slice else 'FAIL'}")
        except Exception as e:
            mongo_slice = False
            print(f"Mongo $push $slice: Error - {e}")

        # Test $bit (AND operation)
        try:
            mongo_collection.update_one(
                {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
            )
            doc = mongo_collection.find_one({"name": "A"})
            mongo_bit_and = doc and doc.get("flags") == (0b0101 & 0b0011)
            print(f"Mongo $bit and: {'OK' if mongo_bit_and else 'FAIL'}")
        except Exception as e:
            mongo_bit_and = False
            print(f"Mongo $bit and: Error - {e}")

        client.close()

        # Record as skipped/known limitation using proper record_result method
        reporter.record_result(
            "Update Modifiers",
            "$each",
            False,  # Not implemented
            "NOT IMPLEMENTED",
            mongo_each,
            skip_reason="Not yet implemented in NeoSQLite",
        )
        reporter.record_result(
            "Update Modifiers",
            "$position",
            False,  # Not implemented
            "NOT IMPLEMENTED",
            mongo_position,
            skip_reason="Not yet implemented in NeoSQLite",
        )
        reporter.record_result(
            "Update Modifiers",
            "$slice",
            False,  # Not implemented
            "NOT IMPLEMENTED",
            mongo_slice,
            skip_reason="Not yet implemented in NeoSQLite",
        )
        reporter.record_result(
            "Update Modifiers",
            "$bit",
            False,  # Not implemented
            "NOT IMPLEMENTED",
            mongo_bit_and,
            skip_reason="Not yet implemented in NeoSQLite",
        )
