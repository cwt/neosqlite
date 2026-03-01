"""Module for comparing aggregation pipeline stages between NeoSQLite and PyMongo"""

import warnings

from neosqlite import DESCENDING
import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

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
        for pipeline, op_name in pipelines:
            try:
                result = list(neo_collection.aggregate(pipeline))
                neo_results[op_name] = len(result)
                print(f"Neo {op_name}: {len(result)}")
            except Exception as e:
                neo_results[op_name] = f"Error: {e}"
                print(f"Neo {op_name}: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_count = None

    mongo_db = None

    mongo_results = None

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
                mongo_results[op_name] = len(result)
                print(f"Mongo {op_name}: {len(result)}")
            except Exception as e:
                mongo_results[op_name] = f"Error: {e}"
                print(f"Mongo {op_name}: Error - {e}")

        for op_name in neo_results:
            neo_count = neo_results[op_name]
            mongo_count = mongo_results.get(op_name, "N/A")
            if isinstance(neo_count, str) or isinstance(mongo_count, str):
                passed = False
            else:
                passed = (
                    neo_count == mongo_count
                    if mongo_count is not None
                    else False
                )
            reporter.record_result(
                "Aggregation Stages", op_name, passed, neo_count, mongo_count
            )
        client.close()
