"""Module for comparing aggregation pipeline stages between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from neosqlite import DESCENDING

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import sanitize_for_mongodb, test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_aggregation_stages():
    """Compare aggregation pipeline stages between NeoSQLite and PyMongo"""
    print("\n=== Aggregation Pipeline Stages Comparison ===")

    data = [
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
                {"$sort": {"name": 1}},  # Sort by name for deterministic order
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
                {"$sort": {"name": 1}},  # Sort by name for deterministic order
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
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(data)

        set_accumulation_mode(True)
        start_neo_timing()
        try:
            for pipeline, op_name in pipelines:
                try:
                    result = list(neo_collection.aggregate(pipeline))
                    neo_results[op_name] = result
                    print(f"Neo {op_name}: {len(result)}")
                except Exception as e:
                    neo_results[op_name] = f"Error: {e}"
                    print(f"Neo {op_name}: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_many(sanitize_for_mongodb(data))

            set_accumulation_mode(True)
            start_mongo_timing()
            try:
                for pipeline, op_name in pipelines:
                    try:
                        result = list(mongo_collection.aggregate(pipeline))
                        mongo_results[op_name] = result
                        print(f"Mongo {op_name}: {len(result)}")
                    except Exception as e:
                        mongo_results[op_name] = f"Error: {e}"
                        print(f"Mongo {op_name}: Error - {e}")
            finally:
                end_mongo_timing()
        finally:
            client.close()

    # Record comparisons
    for pipeline, op_name in pipelines:
        reporter.record_comparison(
            "Aggregation Stages",
            op_name,
            neo_results.get(op_name),
            mongo_results.get(op_name),
            skip_reason="MongoDB not available" if not client else None,
        )
