"""Module for comparing $pullAll update operator between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    start_neo_timing,
    end_neo_timing,
    start_mongo_timing,
    end_mongo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_pullall_operator():
    """Compare $pullAll update operator between NeoSQLite and PyMongo"""
    print("\n=== $pullAll Update Operator Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection

        # Test 1: Basic pullAll
        neo_collection.insert_one(
            {"_id": 1, "name": "test1", "scores": [80, 90, 80, 100, 90, 80]}
        )

        neo_results = {}

        # Test basic pullAll
        try:
            result = neo_collection.update_one(
                {"_id": 1}, {"$pullAll": {"scores": [80, 90]}}
            )
            doc = neo_collection.find_one({"_id": 1})
            neo_results["basic"] = {
                "modified": result.modified_count,
                "result": sorted(doc["scores"]),
            }
            print(
                f"Neo basic: modified={result.modified_count}, result={sorted(doc['scores'])}"
            )
        except Exception as e:
            neo_results["basic"] = f"Error: {e}"
            print(f"Neo basic: Error - {e}")

        # Test 2: PullAll with strings
        neo_collection.insert_one(
            {"_id": 2, "name": "test2", "tags": ["a", "b", "c", "a", "b", "a"]}
        )

        try:
            result = neo_collection.update_one(
                {"_id": 2}, {"$pullAll": {"tags": ["a", "b"]}}
            )
            doc = neo_collection.find_one({"_id": 2})
            neo_results["strings"] = {
                "modified": result.modified_count,
                "result": sorted(doc["tags"]),
            }
            print(
                f"Neo strings: modified={result.modified_count}, result={sorted(doc['tags'])}"
            )
        except Exception as e:
            neo_results["strings"] = f"Error: {e}"
            print(f"Neo strings: Error - {e}")

        # Test 3: PullAll with no matches
        neo_collection.insert_one(
            {"_id": 3, "name": "test3", "numbers": [1, 2, 3, 4, 5]}
        )

        try:
            result = neo_collection.update_one(
                {"_id": 3}, {"$pullAll": {"numbers": [10, 20]}}
            )
            doc = neo_collection.find_one({"_id": 3})
            neo_results["no_matches"] = {
                "modified": result.modified_count,
                "result": sorted(doc["numbers"]),
            }
            print(
                f"Neo no_matches: modified={result.modified_count}, result={sorted(doc['numbers'])}"
            )
        except Exception as e:
            neo_results["no_matches"] = f"Error: {e}"
            print(f"Neo no_matches: Error - {e}")

        # Test 4: PullAll with nested arrays
        neo_collection.insert_one(
            {"_id": 4, "name": "test4", "nested": [[1, 2], [3, 4], [1, 2]]}
        )

        try:
            result = neo_collection.update_one(
                {"_id": 4}, {"$pullAll": {"nested": [[1, 2]]}}
            )
            doc = neo_collection.find_one({"_id": 4})
            neo_results["nested"] = {
                "modified": result.modified_count,
                "result": [
                    list(x) if isinstance(x, list) else x for x in doc["nested"]
                ],
            }
            print(
                f"Neo nested: modified={result.modified_count}, result={doc['nested']}"
            )
        except Exception as e:
            neo_results["nested"] = f"Error: {e}"
            print(f"Neo nested: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_results = {}

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})

        # Test 1: Basic pullAll
        mongo_collection.insert_one(
            {"_id": 1, "name": "test1", "scores": [80, 90, 80, 100, 90, 80]}
        )

        try:
            result = mongo_collection.update_one(
                {"_id": 1}, {"$pullAll": {"scores": [80, 90]}}
            )
            doc = mongo_collection.find_one({"_id": 1})
            mongo_results["basic"] = {
                "modified": result.modified_count,
                "result": sorted(doc["scores"]),
            }
            print(
                f"Mongo basic: modified={result.modified_count}, result={sorted(doc['scores'])}"
            )
        except Exception as e:
            mongo_results["basic"] = f"Error: {e}"
            print(f"Mongo basic: Error - {e}")

        # Test 2: PullAll with strings
        mongo_collection.insert_one(
            {"_id": 2, "name": "test2", "tags": ["a", "b", "c", "a", "b", "a"]}
        )

        try:
            result = mongo_collection.update_one(
                {"_id": 2}, {"$pullAll": {"tags": ["a", "b"]}}
            )
            doc = mongo_collection.find_one({"_id": 2})
            mongo_results["strings"] = {
                "modified": result.modified_count,
                "result": sorted(doc["tags"]),
            }
            print(
                f"Mongo strings: modified={result.modified_count}, result={sorted(doc['tags'])}"
            )
        except Exception as e:
            mongo_results["strings"] = f"Error: {e}"
            print(f"Mongo strings: Error - {e}")

        # Test 3: PullAll with no matches
        mongo_collection.insert_one(
            {"_id": 3, "name": "test3", "numbers": [1, 2, 3, 4, 5]}
        )

        try:
            result = mongo_collection.update_one(
                {"_id": 3}, {"$pullAll": {"numbers": [10, 20]}}
            )
            doc = mongo_collection.find_one({"_id": 3})
            mongo_results["no_matches"] = {
                "modified": result.modified_count,
                "result": sorted(doc["numbers"]),
            }
            print(
                f"Mongo no_matches: modified={result.modified_count}, result={sorted(doc['numbers'])}"
            )
        except Exception as e:
            mongo_results["no_matches"] = f"Error: {e}"
            print(f"Mongo no_matches: Error - {e}")

        # Test 4: PullAll with nested arrays
        mongo_collection.insert_one(
            {"_id": 4, "name": "test4", "nested": [[1, 2], [3, 4], [1, 2]]}
        )

        try:
            result = mongo_collection.update_one(
                {"_id": 4}, {"$pullAll": {"nested": [[1, 2]]}}
            )
            doc = mongo_collection.find_one({"_id": 4})
            mongo_results["nested"] = {
                "modified": result.modified_count,
                "result": [
                    list(x) if isinstance(x, list) else x for x in doc["nested"]
                ],
            }
            print(
                f"Mongo nested: modified={result.modified_count}, result={doc['nested']}"
            )
        except Exception as e:
            mongo_results["nested"] = f"Error: {e}"
            print(f"Mongo nested: Error - {e}")

        # Compare results
        for test_name in neo_results:
            neo_result = neo_results[test_name]
            mongo_result = (
                mongo_results.get(test_name) if mongo_results else None
            )

            reporter.record_comparison(
                "Update Operators",
                f"$pullAll ({test_name})",
                neo_result,
                mongo_result,
                skip_reason="MongoDB not available" if not client else None,
            )

        end_mongo_timing()
        client.close()
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for test_name in neo_results:
            reporter.record_comparison(
                "Update Operators",
                f"$pullAll ({test_name})",
                neo_results[test_name],
                None,
                skip_reason="MongoDB not available",
            )
