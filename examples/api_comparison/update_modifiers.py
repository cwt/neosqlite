"""Module for comparing update modifiers between NeoSQLite and PyMongo"""

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


def compare_update_modifiers():
    """Compare update modifiers"""
    print("\n=== Update Modifiers Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_modifiers
        neo_collection.insert_one(
            {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
        )

        set_accumulation_mode(True)
        # Test $each with $push
        neo_each = False
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "A"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "A"})
            neo_each = doc and len(doc.get("tags", [])) == 4
            print(f"Neo $push $each: {'OK' if neo_each else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $each: Error - {e}")

        # Test $position with $push
        neo_position = False
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "A"})
            neo_position = doc and doc.get("tags", [])[0] == "x"
            print(f"Neo $push $position: {'OK' if neo_position else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $position: Error - {e}")

        # Test $slice with $push
        neo_slice = False
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["y", "z"], "$slice": -3}}},
            )
            end_neo_timing()

            doc = neo_collection.find_one({"name": "A"})
            neo_slice = doc and len(doc.get("tags", [])) == 3
            print(f"Neo $push $slice: {'OK' if neo_slice else 'FAIL'}")
        except Exception as e:
            print(f"Neo $push $slice: Error - {e}")

        # Test $bit (AND operation)
        neo_bit_and = False
        try:
            start_neo_timing()
            neo_collection.update_one(
                {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
            )
            end_neo_timing()

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

        set_accumulation_mode(True)
        # Test $each with $push
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "A"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_each = doc and len(doc.get("tags", [])) == 4
            print(f"Mongo $push $each: {'OK' if mongo_each else 'FAIL'}")
        except Exception as e:
            mongo_each = False
            print(f"Mongo $push $each: Error - {e}")

        # Test $position with $push
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
            )
            end_mongo_timing()

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
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "A"},
                {"$push": {"tags": {"$each": ["y", "z"], "$slice": -3}}},
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_slice = doc and len(doc.get("tags", [])) == 3
            print(f"Mongo $push $slice: {'OK' if mongo_slice else 'FAIL'}")
        except Exception as e:
            mongo_slice = False
            print(f"Mongo $push $slice: Error - {e}")

        # Test $bit (AND operation)
        try:
            start_mongo_timing()
            mongo_collection.update_one(
                {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
            )
            end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_bit_and = doc and doc.get("flags") == (0b0101 & 0b0011)
            print(f"Mongo $bit and: {'OK' if mongo_bit_and else 'FAIL'}")
        except Exception as e:
            mongo_bit_and = False
            print(f"Mongo $bit and: Error - {e}")

        client.close()

    reporter.record_comparison(
        "Update Modifiers",
        "$each",
        neo_each if neo_each else "FAIL",
        mongo_each if mongo_each else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$position",
        neo_position if neo_position else "FAIL",
        mongo_position if mongo_position else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$slice",
        neo_slice if neo_slice else "FAIL",
        mongo_slice if mongo_slice else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$bit",
        neo_bit_and if neo_bit_and else "FAIL",
        mongo_bit_and if mongo_bit_and else None,
        skip_reason="MongoDB not available" if not client else None,
    )
