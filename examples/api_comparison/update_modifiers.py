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

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_modifiers
        neo_collection.insert_one(
            {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
        )

        # Test $each with $push
        neo_each = False
        try:
            neo_collection.update_one(
                {"name": "A"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
            )
            doc = neo_collection.find_one({"name": "A"})
            neo_each = doc and len(doc.get("tags", [])) == 4
            print(f"Neo $push $each: {'OK' if neo_each else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $each: Error - {e}")

        # Test $position with $push
        neo_position = False
        try:
            neo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
            )
            doc = neo_collection.find_one({"name": "A"})
            neo_position = doc and doc.get("tags", [])[0] == "x"
            print(f"Neo $push $position: {'OK' if neo_position else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $position: Error - {e}")

        # Test $slice with $push
        neo_slice = False
        try:
            neo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["y", "z"], "$slice": -3}}},
            )
            doc = neo_collection.find_one({"name": "A"})
            neo_slice = doc and len(doc.get("tags", [])) == 3
            print(f"Neo $push $slice: {'OK' if neo_slice else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $slice: Error - {e}")

        # Test $bit (AND operation)
        neo_bit_and = False
        try:
            neo_collection.update_one(
                {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
            )
            doc = neo_collection.find_one({"name": "A"})
            neo_bit_and = doc and doc.get("flags") == (0b0101 & 0b0011)
            print(f"Neo $bit and: {'OK' if neo_bit_and else 'FAIL'}")
        except Exception as e:
            print(f"Neo $bit and: Error - {e}")

    client = test_pymongo_connection()
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

    # Record results
    reporter.record_result(
        "Update Modifiers",
        "$each",
        neo_each and mongo_each,
        neo_each,
        mongo_each,
    )
    reporter.record_result(
        "Update Modifiers",
        "$position",
        neo_position and mongo_position,
        neo_position,
        mongo_position,
    )
    reporter.record_result(
        "Update Modifiers",
        "$slice",
        neo_slice and mongo_slice,
        neo_slice,
        mongo_slice,
    )
    reporter.record_result(
        "Update Modifiers",
        "$bit",
        neo_bit_and and mongo_bit_and,
        neo_bit_and,
        mongo_bit_and,
    )
