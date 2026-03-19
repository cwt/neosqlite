"""Module for comparing date expression operators between NeoSQLite and PyMongo"""

import warnings
from datetime import datetime, timezone

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


def compare_date_expr_operators():
    """Compare date expression operators in aggregation"""
    print("\n=== Date Expression Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # Use datetime objects (MongoDB-compatible format)
        neo_collection.insert_many(
            [
                {
                    "event": "A",
                    "date": datetime(
                        2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "event": "B",
                    "date": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                },
            ]
        )

        set_accumulation_mode(True)
        date_operators = [
            ("$year", {"$year": "$date"}),
            ("$month", {"$month": "$date"}),
            ("$dayOfMonth", {"$dayOfMonth": "$date"}),
            ("$hour", {"$hour": "$date"}),
            ("$minute", {"$minute": "$date"}),
            ("$second", {"$second": "$date"}),
            ("$dayOfWeek", {"$dayOfWeek": "$date"}),
            ("$dayOfYear", {"$dayOfYear": "$date"}),
            ("$week", {"$week": "$date"}),
            ("$isoDayOfWeek", {"$isoDayOfWeek": "$date"}),
            ("$isoWeek", {"$isoWeek": "$date"}),
            ("$millisecond", {"$millisecond": "$date"}),
            (
                "$dateTrunc",
                {
                    "$dateTrunc": {
                        "date": "$date",
                        "unit": "month",
                        "binSize": 1,
                    }
                },
            ),
            (
                "$dateToString",
                {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
            ),
            (
                "$dateFromString",
                {"$dateFromString": {"dateString": "2024-06-15"}},
            ),
        ]

        # Test $dateAdd
        neo_dateadd = None
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "plus_one_day": {
                                    "$dateAdd": {
                                        "startDate": "$date",
                                        "amount": 1,
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_neo_timing()
            neo_dateadd = result
            print(f"Neo $dateAdd: OK ({len(result)} results)")
        except Exception as e:
            print(f"Neo $dateAdd: Error - {e}")

        # Test $dateSubtract
        neo_datesubtract = None
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "minus_one_day": {
                                    "$dateSubtract": {
                                        "startDate": "$date",
                                        "amount": 1,
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_neo_timing()
            neo_datesubtract = result
            print(f"Neo $dateSubtract: OK ({len(result)} results)")
        except Exception as e:
            print(f"Neo $dateSubtract: Error - {e}")

        # Test $dateDiff
        neo_datediff = None
        try:
            start_neo_timing()
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "days_diff": {
                                    "$dateDiff": {
                                        "startDate": "$date",
                                        "endDate": {
                                            "$dateAdd": {
                                                "startDate": "$date",
                                                "amount": 1,
                                                "unit": "day",
                                            }
                                        },
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_neo_timing()
            neo_datediff = result
            print(f"Neo $dateDiff: OK ({len(result)} results)")
        except Exception as e:
            print(f"Neo $dateDiff: Error - {e}")

        neo_results = {}
        for op_name, op_expr in date_operators:
            try:
                start_neo_timing()
                result = list(
                    neo_collection.aggregate([{"$project": {"val": op_expr}}])
                )
                end_neo_timing()

                neo_results[op_name] = result
                print(f"Neo {op_name}: OK ({len(result)} results)")
            except Exception as e:
                neo_results[op_name] = None
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_results = None
    mongo_dateadd = None
    mongo_datesubtract = None
    mongo_datediff = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        # MongoDB requires actual datetime objects
        mongo_collection.insert_many(
            [
                {
                    "event": "A",
                    "date": datetime(
                        2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "event": "B",
                    "date": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                },
            ]
        )

        set_accumulation_mode(True)
        mongo_results = {}
        for op_name, op_expr in date_operators:
            try:
                start_mongo_timing()
                result = list(
                    mongo_collection.aggregate([{"$project": {"val": op_expr}}])
                )
                end_mongo_timing()

                mongo_results[op_name] = result
                print(f"Mongo {op_name}: OK ({len(result)} results)")
            except Exception as e:
                mongo_results[op_name] = None
                print(f"Mongo {op_name}: Error - {e}")

        # Test $dateAdd for MongoDB
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "plus_one_day": {
                                    "$dateAdd": {
                                        "startDate": "$date",
                                        "amount": 1,
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_mongo_timing()
            mongo_dateadd = result
            print(f"Mongo $dateAdd: OK ({len(result)} results)")
        except Exception as e:
            mongo_dateadd = None
            print(f"Mongo $dateAdd: Error - {e}")

        # Test $dateSubtract for MongoDB
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "minus_one_day": {
                                    "$dateSubtract": {
                                        "startDate": "$date",
                                        "amount": 1,
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_mongo_timing()
            mongo_datesubtract = result
            print(f"Mongo $dateSubtract: OK ({len(result)} results)")
        except Exception as e:
            mongo_datesubtract = None
            print(f"Mongo $dateSubtract: Error - {e}")

        # Test $dateDiff for MongoDB
        try:
            start_mongo_timing()
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "days_diff": {
                                    "$dateDiff": {
                                        "startDate": "$date",
                                        "endDate": {
                                            "$dateAdd": {
                                                "startDate": "$date",
                                                "amount": 1,
                                                "unit": "day",
                                            }
                                        },
                                        "unit": "day",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            end_mongo_timing()
            mongo_datediff = result
            print(f"Mongo $dateDiff: OK ({len(result)} results)")
        except Exception as e:
            mongo_datediff = None
            print(f"Mongo $dateDiff: Error - {e}")

        client.close()

    # Record comparison results with actual values
    if neo_results:
        for op_name in neo_results:
            reporter.record_comparison(
                "Date Expression Operators",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name) if mongo_results else None,
                skip_reason="MongoDB not available" if not client else None,
            )

    reporter.record_comparison(
        "Date Expression Operators",
        "$dateAdd",
        neo_dateadd,
        mongo_dateadd,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Date Expression Operators",
        "$dateSubtract",
        neo_datesubtract,
        mongo_datesubtract,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Date Expression Operators",
        "$dateDiff",
        neo_datediff,
        mongo_datediff,
        skip_reason="MongoDB not available" if not client else None,
    )
