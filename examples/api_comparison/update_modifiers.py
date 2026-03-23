"""Module for comparing update modifiers between NeoSQLite and PyMongo"""

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


def compare_update_modifiers():
    """Compare update modifiers"""
    print("\n=== Update Modifiers Comparison ===")

    # Initialize results
    neo_each = False
    neo_position = False
    neo_slice = False
    neo_bit_and = False

    mongo_each = None
    mongo_position = None
    mongo_slice = None
    mongo_bit_and = None

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_modifiers
        neo_collection.insert_one(
            {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
        )

        set_accumulation_mode(True)

        # Test $each with $push
        start_neo_timing()
        try:
            try:
                neo_collection.update_one(
                    {"name": "A"}, {"$push": {"tags": {"$each": ["c", "d"]}}}
                )
            except Exception as e:
                print(f"Neo $push $each: Error - {e}")
        finally:
            end_neo_timing()

        doc = neo_collection.find_one({"name": "A"})
        neo_each = bool(doc and len(doc.get("tags", [])) == 4)
        print(f"Neo $push $each: {'OK' if neo_each else 'FAIL'}")

        # Test $position with $push
        start_neo_timing()
        try:
            try:
                neo_collection.update_one(
                    {"name": "A"},
                    {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
                )
            except Exception as e:
                print(f"Neo $push $position: Error - {e}")
        finally:
            end_neo_timing()

        doc = neo_collection.find_one({"name": "A"})
        neo_position = bool(doc and doc.get("tags", [])[0] == "x")
        print(f"Neo $push $position: {'OK' if neo_position else 'FAIL'}")

        # Test $slice with $push
        start_neo_timing()
        try:
            try:
                neo_collection.update_one(
                    {"name": "A"},
                    {"$push": {"tags": {"$each": ["y", "z"], "$slice": -3}}},
                )
            except Exception as e:
                print(f"Neo $push $slice: Error - {e}")
        finally:
            end_neo_timing()

        doc = neo_collection.find_one({"name": "A"})
        neo_slice = bool(doc and len(doc.get("tags", [])) == 3)
        print(f"Neo $push $slice: {'OK' if neo_slice else 'FAIL'}")

        # Test $bit (AND operation)
        start_neo_timing()
        try:
            try:
                neo_collection.update_one(
                    {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
                )
            except Exception as e:
                print(f"Neo $bit and: Error - {e}")
        finally:
            end_neo_timing()

        doc = neo_collection.find_one({"name": "A"})
        neo_bit_and = bool(doc and doc.get("flags") == (0b0101 & 0b0011))
        print(f"Neo $bit and: {'OK' if neo_bit_and else 'FAIL'}")

    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_modifiers
            mongo_collection.delete_many({})
            mongo_collection.insert_one(
                {"name": "A", "tags": ["a", "b"], "counter": 0, "flags": 0b0101}
            )

            set_accumulation_mode(True)

            # Test $each with $push
            start_mongo_timing()
            try:
                try:
                    mongo_collection.update_one(
                        {"name": "A"},
                        {"$push": {"tags": {"$each": ["c", "d"]}}},
                    )
                except Exception as e:
                    print(f"Mongo $push $each: Error - {e}")
                    mongo_each = False
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_each = bool(doc and len(doc.get("tags", [])) == 4)
            print(f"Mongo $push $each: {'OK' if mongo_each else 'FAIL'}")

            # Test $position with $push
            start_mongo_timing()
            try:
                try:
                    mongo_collection.update_one(
                        {"name": "A"},
                        {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
                    )
                except Exception as e:
                    print(f"Mongo $push $position: Error - {e}")
                    mongo_position = False
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_position = bool(doc and doc.get("tags", [])[0] == "x")
            print(
                f"Mongo $push $position: {'OK' if mongo_position else 'FAIL'}"
            )

            # Test $slice with $push
            start_mongo_timing()
            try:
                try:
                    mongo_collection.update_one(
                        {"name": "A"},
                        {
                            "$push": {
                                "tags": {"$each": ["y", "z"], "$slice": -3}
                            }
                        },
                    )
                except Exception as e:
                    print(f"Mongo $push $slice: Error - {e}")
                    mongo_slice = False
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_slice = bool(doc and len(doc.get("tags", [])) == 3)
            print(f"Mongo $push $slice: {'OK' if mongo_slice else 'FAIL'}")

            # Test $bit (AND operation)
            start_mongo_timing()
            try:
                try:
                    mongo_collection.update_one(
                        {"name": "A"}, {"$bit": {"flags": {"and": 0b0011}}}
                    )
                except Exception as e:
                    print(f"Mongo $bit and: Error - {e}")
                    mongo_bit_and = False
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "A"})
            mongo_bit_and = bool(doc and doc.get("flags") == (0b0101 & 0b0011))
            print(f"Mongo $bit and: {'OK' if mongo_bit_and else 'FAIL'}")

        finally:
            client.close()

    reporter.record_comparison(
        "Update Modifiers",
        "$each",
        neo_each if neo_each else "FAIL",
        mongo_each,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$position",
        neo_position if neo_position else "FAIL",
        mongo_position,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$slice",
        neo_slice if neo_slice else "FAIL",
        mongo_slice,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Update Modifiers",
        "$bit",
        neo_bit_and if neo_bit_and else "FAIL",
        mongo_bit_and,
        skip_reason="MongoDB not available" if not client else None,
    )
