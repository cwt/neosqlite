"""Module for comparing additional $expr operators success stories between NeoSQLite and PyMongo"""

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


def compare_additional_expr_success_stories():
    """Compare additional $expr operators that are working correctly"""
    print("\n=== Additional $expr Operators (Success Stories) ===")
    print(
        "Note: These tests demonstrate NeoSQLite's comprehensive $expr support."
    )
    print("All operators below work correctly in both NeoSQLite and MongoDB.\n")

    # Initialize result variables
    neo_elemMatch = None
    neo_not = None
    neo_concat = None
    neo_ifnull = None
    neo_isarray = None
    neo_round = None
    neo_exp = None
    neo_deg2rad = None
    neo_rad2deg = None

    mongo_elemMatch = None
    mongo_not = None
    mongo_concat = None
    mongo_ifnull = None
    mongo_isarray = None
    mongo_round = None
    mongo_exp = None
    mongo_deg2rad = None
    mongo_rad2deg = None

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)
        neo_collection = neo_conn.test_collection

        # Test $elemMatch
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            neo_elemMatch = len(neo_result)
            print(f"Neo $elemMatch: {neo_elemMatch} matches")
        except Exception as e:
            neo_elemMatch = f"Error: {e}"
            print(f"Neo $elemMatch: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $not
        neo_collection.delete_many({})
        neo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$not": {"$gt": ["$age", 30]}}})
            )
            neo_not = len(neo_result)
            print(f"Neo $expr $not: {neo_not} matches")
        except Exception as e:
            neo_not = f"Error: {e}"
            print(f"Neo $expr $not: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $concat
        neo_collection.delete_many({})
        neo_collection.insert_one({"first": "John", "last": "Doe"})
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$concat": ["$first", " ", "$last"]},
                                "John Doe",
                            ]
                        }
                    }
                )
            )
            neo_concat = len(neo_result)
            print(f"Neo $expr $concat: {neo_concat} matches")
        except Exception as e:
            neo_concat = f"Error: {e}"
            print(f"Neo $expr $concat: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $ifNull
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$ifNull": ["$middle", "N/A"]}, "N/A"]}}
                )
            )
            neo_ifnull = len(neo_result)
            print(f"Neo $expr $ifNull: {neo_ifnull} matches")
        except Exception as e:
            neo_ifnull = f"Error: {e}"
            print(f"Neo $expr $ifNull: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $isArray
        neo_collection.delete_many({})
        neo_collection.insert_many([{"data": [1, 2, 3]}, {"data": "not array"}])
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            neo_isarray = len(neo_result)
            print(f"Neo $expr $isArray: {neo_isarray} matches")
        except Exception as e:
            neo_isarray = f"Error: {e}"
            print(f"Neo $expr $isArray: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $round
        neo_collection.delete_many({})
        neo_collection.insert_one({"value": 3.14159})
        start_neo_timing()
        try:
            neo_result = list(
                neo_collection.find(
                    {"$expr": {"$eq": [{"$round": ["$value", 2]}, 3.14]}}
                )
            )
            neo_round = len(neo_result)
            print(f"Neo $expr $round: {neo_round} matches")
        except Exception as e:
            neo_round = f"Error: {e}"
            print(f"Neo $expr $round: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $exp
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"x": 0},  # exp(0) = 1
                {"x": 1},  # exp(1) ≈ 2.718
                {"x": 2},  # exp(2) ≈ 7.389
            ]
        )
        start_neo_timing()
        try:
            # Test exp(0) = 1 (exact)
            neo_result_exact = list(
                neo_collection.find({"$expr": {"$eq": [{"$exp": 0}, 1]}})
            )
            # Test exp(1) is in reasonable range (2.718 ± 0.001)
            neo_result_range = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$exp": 1}, 2.717]},
                                {"$lte": [{"$exp": 1}, 2.719]},
                            ]
                        }
                    }
                )
            )
            neo_exp = len(neo_result_exact) + len(neo_result_range)
            print(
                f"Neo $expr $exp: exp(0)=1 ({len(neo_result_exact)}), exp(1)≈2.718 ({len(neo_result_range)})"
            )
        except Exception as e:
            neo_exp = f"Error: {e}"
            print(f"Neo $expr $exp: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $degreesToRadians
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},  # radians(180°) = π ≈ 3.14159
                {"angle": 90},  # radians(90°) = π/2 ≈ 1.5708
            ]
        )
        start_neo_timing()
        try:
            neo_result_180 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 180}, 3.1415]},
                                {"$lte": [{"$degreesToRadians": 180}, 3.1417]},
                            ]
                        }
                    }
                )
            )
            neo_result_90 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 90}, 1.570]},
                                {"$lte": [{"$degreesToRadians": 90}, 1.571]},
                            ]
                        }
                    }
                )
            )
            neo_deg2rad = len(neo_result_180) + len(neo_result_90)
            print(
                f"Neo $expr $degreesToRadians: 180°≈π ({len(neo_result_180)}), 90°≈π/2 ({len(neo_result_90)})"
            )
        except Exception as e:
            neo_deg2rad = f"Error: {e}"
            print(f"Neo $expr $degreesToRadians: Error - {e}")
        finally:
            end_neo_timing()

        # Test $expr $radiansToDegrees
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},  # degrees(π) = 180°
                {"angle": 1.5708},  # degrees(π/2) = 90°
            ]
        )
        start_neo_timing()
        try:
            neo_result_pi = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {
                                    "$gte": [
                                        {"$radiansToDegrees": 3.14159},
                                        179.9,
                                    ]
                                },
                                {
                                    "$lte": [
                                        {"$radiansToDegrees": 3.14159},
                                        180.1,
                                    ]
                                },
                            ]
                        }
                    }
                )
            )
            neo_result_pi2 = list(
                neo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$radiansToDegrees": 1.5708}, 89.9]},
                                {"$lte": [{"$radiansToDegrees": 1.5708}, 90.1]},
                            ]
                        }
                    }
                )
            )
            neo_rad2deg = len(neo_result_pi) + len(neo_result_pi2)
            print(
                f"Neo $expr $radiansToDegrees: π≈180° ({len(neo_result_pi)}), π/2≈90° ({len(neo_result_pi2)})"
            )
        except Exception as e:
            neo_rad2deg = f"Error: {e}"
            print(f"Neo $expr $radiansToDegrees: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        set_accumulation_mode(True)

        # Test $elemMatch
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            mongo_elemMatch = len(mongo_result)
            print(f"Mongo $elemMatch: {mongo_elemMatch} matches")
        except Exception as e:
            mongo_elemMatch = f"Error: {e}"
            print(f"Mongo $elemMatch: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $not
        mongo_collection.delete_many({})
        mongo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$not": {"$gt": ["$age", 30]}}}
                )
            )
            mongo_not = len(mongo_result)
            print(f"Mongo $expr $not: {mongo_not} matches")
        except Exception as e:
            mongo_not = f"Error: {e}"
            print(f"Mongo $expr $not: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $concat
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"first": "John", "last": "Doe"})
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$concat": ["$first", " ", "$last"]},
                                "John Doe",
                            ]
                        }
                    }
                )
            )
            mongo_concat = len(mongo_result)
            print(f"Mongo $expr $concat: {mongo_concat} matches")
        except Exception as e:
            mongo_concat = f"Error: {e}"
            print(f"Mongo $expr $concat: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $ifNull
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$ifNull": ["$middle", "N/A"]}, "N/A"]}}
                )
            )
            mongo_ifnull = len(mongo_result)
            print(f"Mongo $expr $ifNull: {mongo_ifnull} matches")
        except Exception as e:
            mongo_ifnull = f"Error: {e}"
            print(f"Mongo $expr $ifNull: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $isArray
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"data": [1, 2, 3]}, {"data": "not array"}]
        )
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            mongo_isarray = len(mongo_result)
            print(f"Mongo $expr $isArray: {mongo_isarray} matches")
        except Exception as e:
            mongo_isarray = f"Error: {e}"
            print(f"Mongo $expr $isArray: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $round
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"value": 3.14159})
        start_mongo_timing()
        try:
            mongo_result = list(
                mongo_collection.find(
                    {"$expr": {"$eq": [{"$round": ["$value", 2]}, 3.14]}}
                )
            )
            mongo_round = len(mongo_result)
            print(f"Mongo $expr $round: {mongo_round} matches")
        except Exception as e:
            mongo_round = f"Error: {e}"
            print(f"Mongo $expr $round: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $exp
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"x": 0},
                {"x": 1},
                {"x": 2},
            ]
        )
        start_mongo_timing()
        try:
            mongo_result_exact = list(
                mongo_collection.find({"$expr": {"$eq": [{"$exp": 0}, 1]}})
            )
            mongo_result_range = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$exp": 1}, 2.717]},
                                {"$lte": [{"$exp": 1}, 2.719]},
                            ]
                        }
                    }
                )
            )
            mongo_exp = len(mongo_result_exact) + len(mongo_result_range)
            print(
                f"Mongo $expr $exp: exp(0)=1 ({len(mongo_result_exact)}), exp(1)≈2.718 ({len(mongo_result_range)})"
            )
        except Exception as e:
            mongo_exp = f"Error: {e}"
            print(f"Mongo $expr $exp: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $degreesToRadians
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},
                {"angle": 90},
            ]
        )
        start_mongo_timing()
        try:
            mongo_result_180 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 180}, 3.1415]},
                                {"$lte": [{"$degreesToRadians": 180}, 3.1417]},
                            ]
                        }
                    }
                )
            )
            mongo_result_90 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$degreesToRadians": 90}, 1.570]},
                                {"$lte": [{"$degreesToRadians": 90}, 1.571]},
                            ]
                        }
                    }
                )
            )
            mongo_deg2rad = len(mongo_result_180) + len(mongo_result_90)
            print(
                f"Mongo $expr $degreesToRadians: 180°≈π ({len(mongo_result_180)}), 90°≈π/2 ({len(mongo_result_90)})"
            )
        except Exception as e:
            mongo_deg2rad = f"Error: {e}"
            print(f"Mongo $expr $degreesToRadians: Error - {e}")
        finally:
            end_mongo_timing()

        # Test $expr $radiansToDegrees
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},
                {"angle": 1.5708},
            ]
        )
        start_mongo_timing()
        try:
            mongo_result_pi = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {
                                    "$gte": [
                                        {"$radiansToDegrees": 3.14159},
                                        179.9,
                                    ]
                                },
                                {
                                    "$lte": [
                                        {"$radiansToDegrees": 3.14159},
                                        180.1,
                                    ]
                                },
                            ]
                        }
                    }
                )
            )
            mongo_result_pi2 = list(
                mongo_collection.find(
                    {
                        "$expr": {
                            "$and": [
                                {"$gte": [{"$radiansToDegrees": 1.5708}, 89.9]},
                                {"$lte": [{"$radiansToDegrees": 1.5708}, 90.1]},
                            ]
                        }
                    }
                )
            )
            mongo_rad2deg = len(mongo_result_pi) + len(mongo_result_pi2)
            print(
                f"Mongo $expr $radiansToDegrees: π≈180° ({len(mongo_result_pi)}), π/2≈90° ({len(mongo_result_pi2)})"
            )
        except Exception as e:
            mongo_rad2deg = f"Error: {e}"
            print(f"Mongo $expr $radiansToDegrees: Error - {e}")
        finally:
            end_mongo_timing()

        client.close()

    reporter.record_comparison(
        "Additional $expr Operators",
        "$elemMatch",
        neo_elemMatch,
        mongo_elemMatch,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $not",
        neo_not,
        mongo_not,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $concat",
        neo_concat,
        mongo_concat,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $ifNull",
        neo_ifnull,
        mongo_ifnull,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $isArray",
        neo_isarray,
        mongo_isarray,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $round",
        neo_round,
        mongo_round,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $exp",
        neo_exp,
        mongo_exp,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $degreesToRadians",
        neo_deg2rad,
        mongo_deg2rad,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Additional $expr Operators",
        "$expr $radiansToDegrees",
        neo_rad2deg,
        mongo_rad2deg,
        skip_reason="MongoDB not available" if not client else None,
    )
