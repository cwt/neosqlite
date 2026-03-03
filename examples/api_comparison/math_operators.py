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

        # Test $exp
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"exp_val": {"$exp": 1}}}]
                )
            )
            neo_exp = len(result) == 2
            print(f"Neo $exp: {'OK' if neo_exp else 'FAIL'}")
        except Exception as e:
            neo_exp = False
            print(f"Neo $exp: Error - {e}")

        # Test $asinh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"asinh_val": {"$asinh": 0.5}}}]
                )
            )
            neo_asinh = len(result) == 2
            print(f"Neo $asinh: {'OK' if neo_asinh else 'FAIL'}")
        except Exception as e:
            neo_asinh = False
            print(f"Neo $asinh: Error - {e}")

        # Test $acosh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"acosh_val": {"$acosh": 1.5}}}]
                )
            )
            neo_acosh = len(result) == 2
            print(f"Neo $acosh: {'OK' if neo_acosh else 'FAIL'}")
        except Exception as e:
            neo_acosh = False
            print(f"Neo $acosh: Error - {e}")

        # Test $atanh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"atanh_val": {"$atanh": 0.5}}}]
                )
            )
            neo_atanh = len(result) == 2
            print(f"Neo $atanh: {'OK' if neo_atanh else 'FAIL'}")
        except Exception as e:
            neo_atanh = False
            print(f"Neo $atanh: Error - {e}")

        # Test $degreesToRadians
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"rad_val": {"$degreesToRadians": 180}}}]
                )
            )
            neo_degstorad = len(result) == 2
            print(f"Neo $degreesToRadians: {'OK' if neo_degstorad else 'FAIL'}")
        except Exception as e:
            neo_degstorad = False
            print(f"Neo $degreesToRadians: Error - {e}")

        # Test $radiansToDegrees
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"deg_val": {"$radiansToDegrees": 3.14159}}}]
                )
            )
            neo_radtodeg = len(result) == 2
            print(f"Neo $radiansToDegrees: {'OK' if neo_radtodeg else 'FAIL'}")
        except Exception as e:
            neo_radtodeg = False
            print(f"Neo $radiansToDegrees: Error - {e}")

        # Test $log2 (NeoSQLite extension)
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"log2_val": {"$log2": "$value"}}}]
                )
            )
            neo_log2 = len(result) == 2
            print(f"Neo $log2: {'OK' if neo_log2 else 'FAIL'}")
        except Exception as e:
            neo_log2 = False
            print(f"Neo $log2: Error - {e}")

        # Test $sigmoid
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"sigmoid_val": {"$sigmoid": "$value"}}}]
                )
            )
            neo_sigmoid = len(result) == 2
            print(f"Neo $sigmoid: {'OK' if neo_sigmoid else 'FAIL'}")
        except Exception as e:
            neo_sigmoid = False
            print(f"Neo $sigmoid: Error - {e}")

    client = test_pymongo_connection()
    mongo_acos = None
    mongo_asin = None
    mongo_atan = None
    mongo_collection = None
    mongo_db = None
    mongo_pow = None
    mongo_sqrt = None
    mongo_exp = None
    mongo_asinh = None
    mongo_acosh = None
    mongo_atanh = None
    mongo_degstorad = None
    mongo_radtodeg = None
    mongo_sigmoid = None

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

        # Test $exp
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"exp_val": {"$exp": 1}}}]
                )
            )
            mongo_exp = len(result) == 2
            print(f"Mongo $exp: {'OK' if mongo_exp else 'FAIL'}")
        except Exception as e:
            mongo_exp = False
            print(f"Mongo $exp: Error - {e}")

        # Test $asinh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"asinh_val": {"$asinh": 0.5}}}]
                )
            )
            mongo_asinh = len(result) == 2
            print(f"Mongo $asinh: {'OK' if mongo_asinh else 'FAIL'}")
        except Exception as e:
            mongo_asinh = False
            print(f"Mongo $asinh: Error - {e}")

        # Test $acosh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"acosh_val": {"$acosh": 1.5}}}]
                )
            )
            mongo_acosh = len(result) == 2
            print(f"Mongo $acosh: {'OK' if mongo_acosh else 'FAIL'}")
        except Exception as e:
            mongo_acosh = False
            print(f"Mongo $acosh: Error - {e}")

        # Test $atanh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"atanh_val": {"$atanh": 0.5}}}]
                )
            )
            mongo_atanh = len(result) == 2
            print(f"Mongo $atanh: {'OK' if mongo_atanh else 'FAIL'}")
        except Exception as e:
            mongo_atanh = False
            print(f"Mongo $atanh: Error - {e}")

        # Test $degreesToRadians
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"rad_val": {"$degreesToRadians": 180}}}]
                )
            )
            mongo_degstorad = len(result) == 2
            print(
                f"Mongo $degreesToRadians: {'OK' if mongo_degstorad else 'FAIL'}"
            )
        except Exception as e:
            mongo_degstorad = False
            print(f"Mongo $degreesToRadians: Error - {e}")

        # Test $radiansToDegrees
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"deg_val": {"$radiansToDegrees": 3.14159}}}]
                )
            )
            mongo_radtodeg = len(result) == 2
            print(
                f"Mongo $radiansToDegrees: {'OK' if mongo_radtodeg else 'FAIL'}"
            )
        except Exception as e:
            mongo_radtodeg = False
            print(f"Mongo $radiansToDegrees: Error - {e}")

        # Test $sigmoid (MongoDB 8.0+)
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"sigmoid_val": {"$sigmoid": "$value"}}}]
                )
            )
            mongo_sigmoid = len(result) == 2
            print(f"Mongo $sigmoid: {'OK' if mongo_sigmoid else 'FAIL'}")
        except Exception as e:
            mongo_sigmoid = False
            print(f"Mongo $sigmoid: Error - {e}")

        # NeoSQLite extensions - these don't exist in MongoDB
        # Set to False as they are NeoSQLite-specific

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
            "Math Operators",
            "$log2 (NeoSQLite extension)",
            neo_log2,
            neo_log2,
            False,
        )
        reporter.record_result(
            "Math Operators",
            "$sigmoid",
            neo_sigmoid,
            neo_sigmoid,
            mongo_sigmoid,
        )
        reporter.record_result(
            "Math Operators", "$atan", neo_atan, neo_atan, mongo_atan
        )
        reporter.record_result(
            "Math Operators", "$exp", neo_exp, neo_exp, mongo_exp
        )
        reporter.record_result(
            "Math Operators", "$asinh", neo_asinh, neo_asinh, mongo_asinh
        )
        reporter.record_result(
            "Math Operators", "$acosh", neo_acosh, neo_acosh, mongo_acosh
        )
        reporter.record_result(
            "Math Operators", "$atanh", neo_atanh, neo_atanh, mongo_atanh
        )
        reporter.record_result(
            "Math Operators",
            "$degreesToRadians",
            neo_degstorad,
            neo_degstorad,
            mongo_degstorad,
        )
        reporter.record_result(
            "Math Operators",
            "$radiansToDegrees",
            neo_radtodeg,
            neo_radtodeg,
            mongo_radtodeg,
        )
