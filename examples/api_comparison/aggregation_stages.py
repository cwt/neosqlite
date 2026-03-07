"""Module for comparing aggregation pipeline stages between NeoSQLite and PyMongo"""

import warnings

from neosqlite import DESCENDING
import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection, compare_results

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_aggregation_stages():
    """Compare aggregation pipeline stages between NeoSQLite and PyMongo"""
    print("\n=== Aggregation Pipeline Stages Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                    "tags": ["python", "sql"],
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                    "tags": ["java"],
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                    "tags": ["marketing", "sql"],
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                    "tags": ["marketing"],
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "dept": "HR",
                    "salary": 52000,
                    "tags": ["hr", "sql"],
                },
            ]
        )

        pipelines = [
            ([{"$match": {"age": {"$gte": 28}}}], "$match"),
            ([{"$project": {"name": 1, "age": 1, "_id": 0}}], "$project"),
            (
                [{"$addFields": {"bonus": {"$multiply": ["$salary", 0.1]}}}],
                "$addFields",
            ),
            (
                [
                    {
                        "$group": {
                            "_id": "$dept",
                            "avg_salary": {"$avg": "$salary"},
                        }
                    }
                ],
                "$group $avg",
            ),
            (
                [{"$group": {"_id": "$dept", "total": {"$sum": "$salary"}}}],
                "$group $sum",
            ),
            (
                [{"$group": {"_id": "$dept", "min_sal": {"$min": "$salary"}}}],
                "$group $min",
            ),
            (
                [{"$group": {"_id": "$dept", "max_sal": {"$max": "$salary"}}}],
                "$group $max",
            ),
            (
                [{"$group": {"_id": "$dept", "count": {"$count": {}}}}],
                "$group $count",
            ),
            # $first/$last accumulators - MongoDB docs state these are "only meaningful
            # when documents are in a defined order" - require $sort for deterministic results
            # Without $sort, order is undefined and results may differ between databases
            (
                [
                    {
                        "$sort": {"name": 1}
                    },  # Sort by name for deterministic order
                    {
                        "$group": {
                            "_id": "$dept",
                            "first_salary": {"$first": "$salary"},
                        }
                    },
                ],
                "$group $first (with name sort)",
            ),
            (
                [
                    {
                        "$sort": {"name": 1}
                    },  # Sort by name for deterministic order
                    {
                        "$group": {
                            "_id": "$dept",
                            "last_salary": {"$last": "$salary"},
                        }
                    },
                ],
                "$group $last (with name sort)",
            ),
            # $first/$last with $sort on value field (tests Tier-3 Python fallback)
            (
                [
                    {"$sort": {"salary": 1}},
                    {
                        "$group": {
                            "_id": "$dept",
                            "first_salary": {"$first": "$salary"},
                        }
                    },
                ],
                "$group $first (with salary sort)",
            ),
            (
                [
                    {"$sort": {"salary": 1}},
                    {
                        "$group": {
                            "_id": "$dept",
                            "last_salary": {"$last": "$salary"},
                        }
                    },
                ],
                "$group $last (with salary sort)",
            ),
            ([{"$sort": {"age": DESCENDING}}], "$sort"),
            ([{"$skip": 2}], "$skip"),
            ([{"$limit": 2}], "$limit"),
            (
                [
                    {"$match": {"age": {"$gte": 25}}},
                    {"$sort": {"age": DESCENDING}},
                    {"$limit": 3},
                ],
                "combined",
            ),
        ]

        neo_results = {}
        neo_raw_results = {}
        for pipeline, op_name in pipelines:
            try:
                result = list(neo_collection.aggregate(pipeline))
                neo_raw_results[op_name] = result
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                neo_raw_results[op_name] = None
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_count = None

    mongo_db = None

    mongo_results = None

    mongo_raw_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "dept": "Engineering",
                    "salary": 50000,
                    "tags": ["python", "sql"],
                },
                {
                    "name": "Bob",
                    "age": 25,
                    "dept": "Engineering",
                    "salary": 45000,
                    "tags": ["java"],
                },
                {
                    "name": "Charlie",
                    "age": 35,
                    "dept": "Marketing",
                    "salary": 55000,
                    "tags": ["marketing", "sql"],
                },
                {
                    "name": "David",
                    "age": 28,
                    "dept": "Marketing",
                    "salary": 48000,
                    "tags": ["marketing"],
                },
                {
                    "name": "Eve",
                    "age": 32,
                    "dept": "HR",
                    "salary": 52000,
                    "tags": ["hr", "sql"],
                },
            ]
        )

        mongo_results = {}
        for pipeline, op_name in pipelines:
            try:
                result = list(mongo_collection.aggregate(pipeline))
                mongo_raw_results[op_name] = result
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                mongo_raw_results[op_name] = None
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")

            # Check for errors
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
                error_msg = (
                    f"Error occurred: Neo={neo_count}, Mongo={mongo_count}"
                )
            elif (
                neo_raw_results[op_name] is None
                or mongo_raw_results[op_name] is None
            ):
                passed = False
                error_msg = "Results are None"
            else:
                # Compare full results (not just counts)
                passed, error_msg = compare_results(
                    neo_raw_results[op_name],
                    mongo_raw_results[op_name],
                    ignore_order=True,  # Allow different ordering
                )

            reporter.record_result(
                "Aggregation Stages",
                op_name,
                passed,
                neo_count if not passed else neo_raw_results[op_name],
                mongo_count if not passed else mongo_raw_results[op_name],
                error_msg if not passed else None,
                show_results=True,  # Show JSON results for passed tests
            )
        client.close()
