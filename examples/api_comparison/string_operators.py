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

        # Test $ltrim
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "left_trimmed": {"$ltrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
            neo_ltrim = len(result) == 2
            print(f"Neo $ltrim: {'OK' if neo_ltrim else 'FAIL'}")
        except Exception as e:
            neo_ltrim = False
            print(f"Neo $ltrim: Error - {e}")

        # Test $rtrim
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "right_trimmed": {"$rtrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
            neo_rtrim = len(result) == 2
            print(f"Neo $rtrim: {'OK' if neo_rtrim else 'FAIL'}")
        except Exception as e:
            neo_rtrim = False
            print(f"Neo $rtrim: Error - {e}")

        # Test $strLenCP
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"name_len": {"$strLenCP": "$name"}}}]
                )
            )
            neo_strlencp = len(result) == 2
            print(f"Neo $strLenCP: {'OK' if neo_strlencp else 'FAIL'}")
        except Exception as e:
            neo_strlencp = False
            print(f"Neo $strLenCP: Error - {e}")

        # Test $regexFind
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "match": {
                                    "$regexFind": {
                                        "input": "$name",
                                        "regex": "A",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_regexfind = len(result) == 2
            print(f"Neo $regexFind: {'OK' if neo_regexfind else 'FAIL'}")
        except Exception as e:
            neo_regexfind = False
            print(f"Neo $regexFind: Error - {e}")

        # Test $replaceOne
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceOne": {
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
            neo_replaceone = len(result) == 2
            print(f"Neo $replaceOne: {'OK' if neo_replaceone else 'FAIL'}")
        except Exception as e:
            neo_replaceone = False
            print(f"Neo $replaceOne: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_replaceall = None
    mongo_split = None
    mongo_substr = None
    mongo_trim = None
    mongo_ltrim = None
    mongo_rtrim = None
    mongo_strlencp = None
    mongo_regexfind = None
    mongo_replaceone = None

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

        # Test $ltrim
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "left_trimmed": {"$ltrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
            mongo_ltrim = len(result) == 2
            print(f"Mongo $ltrim: {'OK' if mongo_ltrim else 'FAIL'}")
        except Exception as e:
            mongo_ltrim = False
            print(f"Mongo $ltrim: Error - {e}")

        # Test $rtrim
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "right_trimmed": {"$rtrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
            mongo_rtrim = len(result) == 2
            print(f"Mongo $rtrim: {'OK' if mongo_rtrim else 'FAIL'}")
        except Exception as e:
            mongo_rtrim = False
            print(f"Mongo $rtrim: Error - {e}")

        # Test $strLenCP
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"name_len": {"$strLenCP": "$name"}}}]
                )
            )
            mongo_strlencp = len(result) == 2
            print(f"Mongo $strLenCP: {'OK' if mongo_strlencp else 'FAIL'}")
        except Exception as e:
            mongo_strlencp = False
            print(f"Mongo $strLenCP: Error - {e}")

        # Test $regexFind
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "match": {
                                    "$regexFind": {
                                        "input": "$name",
                                        "regex": "A",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_regexfind = len(result) == 2
            print(f"Mongo $regexFind: {'OK' if mongo_regexfind else 'FAIL'}")
        except Exception as e:
            mongo_regexfind = False
            print(f"Mongo $regexFind: Error - {e}")

        # Test $replaceOne
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceOne": {
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
            mongo_replaceone = len(result) == 2
            print(f"Mongo $replaceOne: {'OK' if mongo_replaceone else 'FAIL'}")
        except Exception as e:
            mongo_replaceone = False
            print(f"Mongo $replaceOne: Error - {e}")

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
        reporter.record_result(
            "String Operators", "$ltrim", neo_ltrim, neo_ltrim, mongo_ltrim
        )
        reporter.record_result(
            "String Operators", "$rtrim", neo_rtrim, neo_rtrim, mongo_rtrim
        )
        reporter.record_result(
            "String Operators",
            "$strLenCP",
            neo_strlencp,
            neo_strlencp,
            mongo_strlencp,
        )
        reporter.record_result(
            "String Operators",
            "$regexFind",
            neo_regexfind,
            neo_regexfind,
            mongo_regexfind,
        )
        reporter.record_result(
            "String Operators",
            "$replaceOne",
            neo_replaceone,
            neo_replaceone,
            mongo_replaceone,
        )
