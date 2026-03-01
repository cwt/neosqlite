"""Module for comparing string operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_string_operators():
    """Compare string operators in aggregation"""
    print("\n=== String Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "  Alice  ", "city": "New York"},
                {"name": "  Bob  ", "city": "Los Angeles"},
            ]
        )

        # Test $substr
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"short": {"$substr": ["$name", 2, 3]}}}]
                )
            )
            neo_substr = len(result) == 2
            print(f"Neo $substr: {'OK' if neo_substr else 'FAIL'}")
        except Exception as e:
            neo_substr = False
            print(f"Neo $substr: Error - {e}")

        # Test $trim
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"trimmed": {"$trim": {"input": "$name"}}}}]
                )
            )
            neo_trim = len(result) == 2
            print(f"Neo $trim: {'OK' if neo_trim else 'FAIL'}")
        except Exception as e:
            neo_trim = False
            print(f"Neo $trim: Error - {e}")

        # Test $split
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"parts": {"$split": ["$city", " "]}}}]
                )
            )
            neo_split = len(result) == 2
            print(f"Neo $split: {'OK' if neo_split else 'FAIL'}")
        except Exception as e:
            neo_split = False
            print(f"Neo $split: Error - {e}")

        # Test $replaceAll
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceAll": {
                                        "input": "$city",
                                        "find": " ",
                                        "replacement": "-",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_replaceall = len(result) == 2
            print(f"Neo $replaceAll: {'OK' if neo_replaceall else 'FAIL'}")
        except Exception as e:
            neo_replaceall = False
            print(f"Neo $replaceAll: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_db = None

    mongo_replaceall = None

    mongo_split = None

    mongo_substr = None

    mongo_trim = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "  Alice  ", "city": "New York"},
                {"name": "  Bob  ", "city": "Los Angeles"},
            ]
        )

        # Test $substr
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"short": {"$substr": ["$name", 2, 3]}}}]
                )
            )
            mongo_substr = len(result) == 2
            print(f"Mongo $substr: {'OK' if mongo_substr else 'FAIL'}")
        except Exception as e:
            mongo_substr = False
            print(f"Mongo $substr: Error - {e}")

        # Test $trim
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"trimmed": {"$trim": {"input": "$name"}}}}]
                )
            )
            mongo_trim = len(result) == 2
            print(f"Mongo $trim: {'OK' if mongo_trim else 'FAIL'}")
        except Exception as e:
            mongo_trim = False
            print(f"Mongo $trim: Error - {e}")

        # Test $split
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"parts": {"$split": ["$city", " "]}}}]
                )
            )
            mongo_split = len(result) == 2
            print(f"Mongo $split: {'OK' if mongo_split else 'FAIL'}")
        except Exception as e:
            mongo_split = False
            print(f"Mongo $split: Error - {e}")

        # Test $replaceAll
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceAll": {
                                        "input": "$city",
                                        "find": " ",
                                        "replacement": "-",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_replaceall = len(result) == 2
            print(f"Mongo $replaceAll: {'OK' if mongo_replaceall else 'FAIL'}")
        except Exception as e:
            mongo_replaceall = False
            print(f"Mongo $replaceAll: Error - {e}")

        client.close()

        reporter.record_result(
            "String Operators", "$substr", neo_substr, neo_substr, mongo_substr
        )
        reporter.record_result(
            "String Operators", "$trim", neo_trim, neo_trim, mongo_trim
        )
        reporter.record_result(
            "String Operators", "$split", neo_split, neo_split, mongo_split
        )
        reporter.record_result(
            "String Operators",
            "$replaceAll",
            neo_replaceall,
            neo_replaceall,
            mongo_replaceall,
        )
