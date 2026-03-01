"""Module for comparing additional $expr operators success stories between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
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

    # Test $elemMatch
    print("--- $elemMatch ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        try:
            neo_result = list(
                neo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            neo_elemMatch = len(neo_result)
            print(f"Neo $elemMatch: {neo_elemMatch} matches")
        except Exception as e:
            neo_elemMatch = f"Error: {e}"
            print(f"Neo $elemMatch: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_concat = None

    mongo_db = None

    mongo_deg2rad = None

    mongo_elemMatch = None

    mongo_exp = None

    mongo_ifnull = None

    mongo_isarray = None

    mongo_not = None

    mongo_rad2deg = None

    mongo_result = None

    mongo_result_180 = None

    mongo_result_90 = None

    mongo_result_exact = None

    mongo_result_pi = None

    mongo_result_pi2 = None

    mongo_result_range = None

    mongo_round = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"scores": [80, 90, 100]},
                {"scores": [70, 80]},
                {"scores": [90, 95]},
            ]
        )
        try:
            mongo_result = list(
                mongo_collection.find({"scores": {"$elemMatch": {"$gte": 90}}})
            )
            mongo_elemMatch = len(mongo_result)
            print(f"Mongo $elemMatch: {mongo_elemMatch} matches")
        except Exception as e:
            mongo_elemMatch = f"Error: {e}"
            print(f"Mongo $elemMatch: Error - {e}")
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$elemMatch",
        (
            neo_elemMatch == mongo_elemMatch
            if mongo_elemMatch is not None
            else (
                False
                if not isinstance(neo_elemMatch, str)
                and not isinstance(mongo_elemMatch, str)
                else False
            )
        ),
        neo_elemMatch,
        mongo_elemMatch,
    )

    # Test $expr $not
    print("\n--- $expr $not ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$not": {"$gt": ["$age", 30]}}})
            )
            neo_not = len(neo_result)
            print(f"Neo $expr $not: {neo_not} matches")
        except Exception as e:
            neo_not = f"Error: {e}"
            print(f"Neo $expr $not: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many([{"age": 25}, {"age": 30}, {"age": 35}])
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $not",
        (
            neo_not == mongo_not
            if mongo_not is not None
            else (
                False
                if not isinstance(neo_not, str)
                and not isinstance(mongo_not, str)
                else False
            )
        ),
        neo_not,
        mongo_not,
    )

    # Test $expr $concat
    print("\n--- $expr $concat ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"first": "John", "last": "Doe"})
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"first": "John", "last": "Doe"})
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $concat",
        (
            neo_concat == mongo_concat
            if mongo_concat is not None
            else (
                False
                if not isinstance(neo_concat, str)
                and not isinstance(mongo_concat, str)
                else False
            )
        ),
        neo_concat,
        mongo_concat,
    )

    # Test $expr $ifNull
    print("\n--- $expr $ifNull ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"name": "Alice", "middle": None}, {"name": "Bob", "middle": "X"}]
        )
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $ifNull",
        (
            neo_ifnull == mongo_ifnull
            if mongo_ifnull is not None
            else (
                False
                if not isinstance(neo_ifnull, str)
                and not isinstance(mongo_ifnull, str)
                else False
            )
        ),
        neo_ifnull,
        mongo_ifnull,
    )

    # Test $expr $isArray
    print("\n--- $expr $isArray ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([{"data": [1, 2, 3]}, {"data": "not array"}])
        try:
            neo_result = list(
                neo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            neo_isarray = len(neo_result)
            print(f"Neo $expr $isArray: {neo_isarray} matches")
        except Exception as e:
            neo_isarray = f"Error: {e}"
            print(f"Neo $expr $isArray: Error - {e}")

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [{"data": [1, 2, 3]}, {"data": "not array"}]
        )
        try:
            mongo_result = list(
                mongo_collection.find({"$expr": {"$isArray": "$data"}})
            )
            mongo_isarray = len(mongo_result)
            print(f"Mongo $expr $isArray: {mongo_isarray} matches")
        except Exception as e:
            mongo_isarray = f"Error: {e}"
            print(f"Mongo $expr $isArray: Error - {e}")
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $isArray",
        (
            neo_isarray == mongo_isarray
            if mongo_isarray is not None
            else (
                False
                if not isinstance(neo_isarray, str)
                and not isinstance(mongo_isarray, str)
                else False
            )
        ),
        neo_isarray,
        mongo_isarray,
    )

    # Test $expr $round
    print("\n--- $expr $round ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one({"value": 3.14159})
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_one({"value": 3.14159})
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $round",
        (
            neo_round == mongo_round
            if mongo_round is not None
            else (
                False
                if not isinstance(neo_round, str)
                and not isinstance(mongo_round, str)
                else False
            )
        ),
        neo_round,
        mongo_round,
    )

    # Test $expr $exp
    print("\n--- $expr $exp ---")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        # Insert documents with different values to test exp function
        neo_collection.insert_many(
            [
                {"x": 0},  # exp(0) = 1
                {"x": 1},  # exp(1) ≈ 2.718
                {"x": 2},  # exp(2) ≈ 7.389
            ]
        )
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"x": 0},
                {"x": 1},
                {"x": 2},
            ]
        )
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $exp",
        (
            neo_exp == mongo_exp
            if mongo_exp is not None
            else (
                False
                if not isinstance(neo_exp, str)
                and not isinstance(mongo_exp, str)
                else False
            )
        ),
        neo_exp,
        mongo_exp,
    )

    # Test $expr $degreesToRadians
    print("\n--- $expr $degreesToRadians ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},  # radians(180°) = π ≈ 3.14159
                {"angle": 90},  # radians(90°) = π/2 ≈ 1.5708
            ]
        )
        try:
            # Test radians(180) ≈ 3.14159 (π)
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
            # Test radians(90) ≈ 1.5708 (π/2)
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 180},
                {"angle": 90},
            ]
        )
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $degreesToRadians",
        (
            neo_deg2rad == mongo_deg2rad
            if mongo_deg2rad is not None
            else (
                False
                if not isinstance(neo_deg2rad, str)
                and not isinstance(mongo_deg2rad, str)
                else False
            )
        ),
        neo_deg2rad,
        mongo_deg2rad,
    )

    # Test $expr $radiansToDegrees
    print("\n--- $expr $radiansToDegrees ---")
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},  # degrees(π) = 180°
                {"angle": 1.5708},  # degrees(π/2) = 90°
            ]
        )
        try:
            # Test degrees(π) ≈ 180
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
            # Test degrees(π/2) ≈ 90
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

    client = test_pymongo_connection()
    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"angle": 0},
                {"angle": 3.14159},
                {"angle": 1.5708},
            ]
        )
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
        client.close()

    reporter.record_result(
        "Additional $expr Operators",
        "$expr $radiansToDegrees",
        (
            neo_rad2deg == mongo_rad2deg
            if mongo_rad2deg is not None
            else (
                False
                if not isinstance(neo_rad2deg, str)
                and not isinstance(mongo_rad2deg, str)
                else False
            )
        ),
        neo_rad2deg,
        mongo_rad2deg,
    )
