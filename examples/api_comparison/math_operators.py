"""Module for comparing math operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_math_operators():
    """Compare additional math operators"""
    print("\n=== Additional Math Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"name": "A", "value": 16, "angle": 1.5708},  # angle ≈ π/2
                {"name": "B", "value": 25, "angle": 0},
            ]
        )

        # Test $pow
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            neo_pow = len(result) == 2
            print(f"Neo $pow: {'OK' if neo_pow else 'FAIL'}")
        except Exception as e:
            neo_pow = False
            print(f"Neo $pow: Error - {e}")

        # Test $sqrt
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            neo_sqrt = len(result) == 2
            print(f"Neo $sqrt: {'OK' if neo_sqrt else 'FAIL'}")
        except Exception as e:
            neo_sqrt = False
            print(f"Neo $sqrt: Error - {e}")

        # Test $asin
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"asin_val": {"$asin": 0.5}}}]
                )
            )
            neo_asin = len(result) == 2
            print(f"Neo $asin: {'OK' if neo_asin else 'FAIL'}")
        except Exception as e:
            neo_asin = False
            print(f"Neo $asin: Error - {e}")

        # Test $acos
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"acos_val": {"$acos": 0.5}}}]
                )
            )
            neo_acos = len(result) == 2
            print(f"Neo $acos: {'OK' if neo_acos else 'FAIL'}")
        except Exception as e:
            neo_acos = False
            print(f"Neo $acos: Error - {e}")

        # Test $atan
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"atan_val": {"$atan": 1}}}]
                )
            )
            neo_atan = len(result) == 2
            print(f"Neo $atan: {'OK' if neo_atan else 'FAIL'}")
        except Exception as e:
            neo_atan = False
            print(f"Neo $atan: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_acos = None

    mongo_asin = None

    mongo_atan = None

    mongo_collection = None

    mongo_db = None

    mongo_pow = None

    mongo_sqrt = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "A", "value": 16, "angle": 1.5708},
                {"name": "B", "value": 25, "angle": 0},
            ]
        )

        # Test $pow
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"squared": {"$pow": ["$value", 2]}}}]
                )
            )
            mongo_pow = len(result) == 2
            print(f"Mongo $pow: {'OK' if mongo_pow else 'FAIL'}")
        except Exception as e:
            mongo_pow = False
            print(f"Mongo $pow: Error - {e}")

        # Test $sqrt
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"root": {"$sqrt": "$value"}}}]
                )
            )
            mongo_sqrt = len(result) == 2
            print(f"Mongo $sqrt: {'OK' if mongo_sqrt else 'FAIL'}")
        except Exception as e:
            mongo_sqrt = False
            print(f"Mongo $sqrt: Error - {e}")

        # Test $asin
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"asin_val": {"$asin": 0.5}}}]
                )
            )
            mongo_asin = len(result) == 2
            print(f"Mongo $asin: {'OK' if mongo_asin else 'FAIL'}")
        except Exception as e:
            mongo_asin = False
            print(f"Mongo $asin: Error - {e}")

        # Test $acos
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"acos_val": {"$acos": 0.5}}}]
                )
            )
            mongo_acos = len(result) == 2
            print(f"Mongo $acos: {'OK' if mongo_acos else 'FAIL'}")
        except Exception as e:
            mongo_acos = False
            print(f"Mongo $acos: Error - {e}")

        # Test $atan
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"atan_val": {"$atan": 1}}}]
                )
            )
            mongo_atan = len(result) == 2
            print(f"Mongo $atan: {'OK' if mongo_atan else 'FAIL'}")
        except Exception as e:
            mongo_atan = False
            print(f"Mongo $atan: Error - {e}")

        client.close()

        reporter.record_result(
            "Math Operators", "$pow", neo_pow, neo_pow, mongo_pow
        )
        reporter.record_result(
            "Math Operators", "$sqrt", neo_sqrt, neo_sqrt, mongo_sqrt
        )
        reporter.record_result(
            "Math Operators", "$asin", neo_asin, neo_asin, mongo_asin
        )
        reporter.record_result(
            "Math Operators", "$acos", neo_acos, neo_acos, mongo_acos
        )
        reporter.record_result(
            "Math Operators", "$atan", neo_atan, neo_atan, mongo_atan
        )
