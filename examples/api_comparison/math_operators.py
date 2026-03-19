"""Module for comparing math operators between NeoSQLite and PyMongo"""

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

        set_accumulation_mode(True)

        # Helper to run a project aggregate
        def neo_project(expr):
            start_neo_timing()
            res = list(neo_collection.aggregate([{"$project": {"val": expr}}]))
            end_neo_timing()
            return res

        # Run NeoSQLite tests
        neo_pow = neo_project({"$pow": ["$value", 2]})
        neo_sqrt = neo_project({"$sqrt": "$value"})
        neo_asin = neo_project({"$asin": 0.5})
        neo_acos = neo_project({"$acos": 0.5})
        neo_atan = neo_project({"$atan": 1})
        neo_exp = neo_project({"$exp": 1})
        neo_asinh = neo_project({"$asinh": 0.5})
        neo_acosh = neo_project({"$acosh": 1.5})
        neo_atanh = neo_project({"$atanh": 0.5})
        neo_degstorad = neo_project({"$degreesToRadians": 180})
        neo_radtodeg = neo_project({"$radiansToDegrees": 3.14159})
        neo_ln = neo_project({"$ln": 10})
        neo_log = neo_project({"$log": ["$value", 10]})
        neo_log10 = neo_project({"$log10": "$value"})
        neo_log2 = neo_project({"$log2": "$value"})
        neo_sigmoid = neo_project({"$sigmoid": "$value"})

    client = test_pymongo_connection()
    # Initialize MongoDB result variables
    mongo_pow = mongo_sqrt = mongo_asin = mongo_acos = mongo_atan = None
    mongo_exp = mongo_asinh = mongo_acosh = mongo_atanh = None
    mongo_degstorad = mongo_radtodeg = mongo_ln = mongo_log = mongo_log10 = None
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

        set_accumulation_mode(True)

        def mongo_project(expr):
            start_mongo_timing()
            res = list(
                mongo_collection.aggregate([{"$project": {"val": expr}}])
            )
            end_mongo_timing()
            return res

        # Run MongoDB tests
        try:
            mongo_pow = mongo_project({"$pow": ["$value", 2]})
            mongo_sqrt = mongo_project({"$sqrt": "$value"})
            mongo_asin = mongo_project({"$asin": 0.5})
            mongo_acos = mongo_project({"$acos": 0.5})
            mongo_atan = mongo_project({"$atan": 1})
            mongo_exp = mongo_project({"$exp": 1})
            mongo_asinh = mongo_project({"$asinh": 0.5})
            mongo_acosh = mongo_project({"$acosh": 1.5})
            mongo_atanh = mongo_project({"$atanh": 0.5})
            mongo_degstorad = mongo_project({"$degreesToRadians": 180})
            mongo_radtodeg = mongo_project({"$radiansToDegrees": 3.14159})
            mongo_ln = mongo_project({"$ln": 10})
            mongo_log = mongo_project({"$log": ["$value", 10]})
            mongo_log10 = mongo_project({"$log10": "$value"})

            # $sigmoid is MongoDB 8.0+
            try:
                mongo_sigmoid = mongo_project({"$sigmoid": "$value"})
            except Exception:
                mongo_sigmoid = None
        except Exception as e:
            print(f"Mongo math operators: Error - {e}")

        client.close()

    # Record comparisons
    ops = [
        ("$pow", neo_pow, mongo_pow),
        ("$sqrt", neo_sqrt, mongo_sqrt),
        ("$asin", neo_asin, mongo_asin),
        ("$acos", neo_acos, mongo_acos),
        ("$atan", neo_atan, mongo_atan),
        ("$exp", neo_exp, mongo_exp),
        ("$asinh", neo_asinh, mongo_asinh),
        ("$acosh", neo_acosh, mongo_acosh),
        ("$atanh", neo_atanh, mongo_atanh),
        ("$degreesToRadians", neo_degstorad, mongo_degstorad),
        ("$radiansToDegrees", neo_radtodeg, mongo_radtodeg),
        ("$ln", neo_ln, mongo_ln),
        ("$log", neo_log, mongo_log),
        ("$log10", neo_log10, mongo_log10),
        ("$sigmoid", neo_sigmoid, mongo_sigmoid),
    ]

    for name, neo_res, mongo_res in ops:
        reporter.record_comparison(
            "Math Operators",
            name,
            neo_res,
            mongo_res,
            skip_reason="MongoDB not available" if not client else None,
        )

    reporter.record_comparison(
        "Math Operators",
        "$log2 (NeoSQLite extension)",
        neo_log2,
        None,
        skip_reason="NeoSQLite extension only",
    )
