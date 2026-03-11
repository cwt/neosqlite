"""Module for comparing graph lookup between NeoSQLite and PyMongo"""

import warnings

import neosqlite
from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_graph_lookup():
    """Compare $graphLookup and recursive operators"""
    print("\n=== Graph Lookup Comparison ===")

    test_data = [
        {"_id": 1, "name": "CEO", "reportsTo": None, "dept": "Exec"},
        {"_id": 2, "name": "VP Eng", "reportsTo": 1, "dept": "Eng"},
        {"_id": 3, "name": "VP Sales", "reportsTo": 1, "dept": "Sales"},
        {"_id": 4, "name": "Eng Manager", "reportsTo": 2, "dept": "Eng"},
        {"_id": 5, "name": "Sales Manager", "reportsTo": 3, "dept": "Sales"},
        {"_id": 6, "name": "Engineer", "reportsTo": 4, "dept": "Eng"},
    ]

    pipelines = {
        "basic": [
            {"$match": {"_id": 6}},
            {
                "$graphLookup": {
                    "from": "employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "managementChain",
                }
            },
            {"$sort": {"_id": 1}},
        ],
        "maxDepth": [
            {"$match": {"_id": 6}},
            {
                "$graphLookup": {
                    "from": "employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "managementChain",
                    "maxDepth": 1,
                }
            },
        ],
        "depthField": [
            {"$match": {"_id": 6}},
            {
                "$graphLookup": {
                    "from": "employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "managementChain",
                    "depthField": "dist",
                }
            },
        ],
        "restrict": [
            {"$match": {"_id": 6}},
            {
                "$graphLookup": {
                    "from": "employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "managementChain",
                    "restrictSearchWithMatch": {"dept": "Eng"},
                }
            },
        ],
    }

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.employees
        neo_collection.insert_many(test_data)

        for name, pipeline in pipelines.items():
            try:
                neo_results[name] = list(neo_collection.aggregate(pipeline))
                print(f"Neo $graphLookup ({name}): OK")
            except Exception as e:
                neo_results[name] = f"Error: {e}"
                print(f"Neo $graphLookup ({name}): Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.employees
        mongo_collection.delete_many({})
        mongo_collection.insert_many(test_data)

        for name, pipeline in pipelines.items():
            try:
                mongo_results[name] = list(mongo_collection.aggregate(pipeline))
                print(f"Mongo $graphLookup ({name}): OK")
            except Exception as e:
                mongo_results[name] = f"Error: {e}"
                print(f"Mongo $graphLookup ({name}): Error - {e}")

        client.close()

    # Record comparisons
    for name in pipelines:
        # Order of managementChain array is not guaranteed by MongoDB
        # We need to sort it for comparison
        neo_res = neo_results.get(name)
        mongo_res = mongo_results.get(name)

        if (
            isinstance(neo_res, list)
            and neo_res
            and isinstance(mongo_res, list)
            and mongo_res
        ):
            if "managementChain" in neo_res[0]:
                neo_res[0]["managementChain"].sort(key=lambda x: x["_id"])
            if "managementChain" in mongo_res[0]:
                mongo_res[0]["managementChain"].sort(key=lambda x: x["_id"])

        reporter.record_comparison(
            "Graph Lookup",
            f"$graphLookup ({name})",
            neo_res,
            mongo_res,
            skip_reason="MongoDB not available" if not client else None,
        )
