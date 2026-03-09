"""Module for comparing newly implemented operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_new_operators():
    """Compare newly implemented operators between NeoSQLite and PyMongo"""
    print("\n=== New Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # Test data for various operators
        neo_collection.insert_one(
            {
                "_id": 1,
                "bits": 5,  # 0101
                "arr": [1, 2, 3, 4, 5],
                "items": [{"x": 1}, {"x": 2}, {"x": 3}],
                "obj1": {"a": 1, "b": 2},
                "obj2": {"b": 3, "c": 4},
                "set1": [1, 2, 3],
                "set2": [3, 4, 5],
                "date": "2024-01-15T10:30:00",
                "text": "Hello World",
            }
        )

        neo_results = {}

        # Test bitwise operators
        try:
            result = list(neo_collection.find({"bits": {"$bitsAllClear": 2}}))
            neo_results["$bitsAllClear"] = len(result)
            print(f"Neo $bitsAllClear: {len(result)} documents")
        except Exception as e:
            neo_results["$bitsAllClear"] = f"Error: {e}"
            print(f"Neo $bitsAllClear: Error - {e}")

        # Test $pullAll
        try:
            neo_collection.insert_one({"_id": 2, "arr": [1, 2, 3, 2, 4]})
            result = neo_collection.update_one(
                {"_id": 2}, {"$pullAll": {"arr": [2]}}
            )
            doc = neo_collection.find_one({"_id": 2})
            neo_results["$pullAll"] = doc["arr"]
            print(f"Neo $pullAll: {doc['arr']}")
        except Exception as e:
            neo_results["$pullAll"] = f"Error: {e}"
            print(f"Neo $pullAll: Error - {e}")

        # Test $bucket
        try:
            # Use unique _ids and clean collection to avoid constraint errors
            neo_collection.delete_many({})
            neo_collection.insert_many(
                [{"_id": 100 + i, "value": i * 10} for i in range(1, 6)]
            )
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$value",
                                "boundaries": [0, 20, 40, 60],
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            neo_results["$bucket"] = len(result)
            print(f"Neo $bucket: {len(result)} buckets")
        except Exception as e:
            neo_results["$bucket"] = f"Error: {e}"
            print(f"Neo $bucket: Error - {e}")

        # Test $firstN
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "first2": {"$firstN": {"input": "$arr", "n": 2}}
                            }
                        },
                    ]
                )
            )
            neo_results["$firstN"] = result[0].get("first2") if result else None
            print(f"Neo $firstN: {neo_results['$firstN']}")
        except Exception as e:
            neo_results["$firstN"] = f"Error: {e}"
            print(f"Neo $firstN: Error - {e}")

        # Test $strcasecmp
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "cmp": {"$strcasecmp": ["hello", "HELLO"]}
                            }
                        },
                    ]
                )
            )
            neo_results["$strcasecmp"] = (
                result[0].get("cmp") if result else None
            )
            print(f"Neo $strcasecmp: {neo_results['$strcasecmp']}")
        except Exception as e:
            neo_results["$strcasecmp"] = f"Error: {e}"
            print(f"Neo $strcasecmp: Error - {e}")

        # Test $isNumber
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"is_num": {"$isNumber": "$value"}}},
                    ]
                )
            )
            neo_results["$isNumber"] = (
                result[0].get("is_num") if result else None
            )
            print(f"Neo $isNumber: {neo_results['$isNumber']}")
        except Exception as e:
            neo_results["$isNumber"] = f"Error: {e}"
            print(f"Neo $isNumber: Error - {e}")

        # Test $dateFromString
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "date": {
                                    "$dateFromString": {"dateString": "$date"}
                                }
                            }
                        },
                    ]
                )
            )
            neo_results["$dateFromString"] = "parsed" if result else None
            print(f"Neo $dateFromString: {neo_results['$dateFromString']}")
        except Exception as e:
            neo_results["$dateFromString"] = f"Error: {e}"
            print(f"Neo $dateFromString: Error - {e}")

        # Test $mergeObjects
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$obj1", "$obj2"]}
                            }
                        },
                    ]
                )
            )
            neo_results["$mergeObjects"] = (
                result[0].get("merged") if result else None
            )
            print(f"Neo $mergeObjects: {neo_results['$mergeObjects']}")
        except Exception as e:
            neo_results["$mergeObjects"] = f"Error: {e}"
            print(f"Neo $mergeObjects: Error - {e}")

        # Test $setIntersection
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "intersection": {
                                    "$setIntersection": ["$set1", "$set2"]
                                }
                            }
                        },
                    ]
                )
            )
            neo_results["$setIntersection"] = (
                result[0].get("intersection") if result else None
            )
            print(f"Neo $setIntersection: {neo_results['$setIntersection']}")
        except Exception as e:
            neo_results["$setIntersection"] = f"Error: {e}"
            print(f"Neo $setIntersection: Error - {e}")

        # Test $rand
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"random": {"$rand": {}}}},
                    ]
                )
            )
            val = result[0].get("random") if result else None
            neo_results["$rand"] = (
                "generated" if val is not None and 0 <= val <= 1 else "invalid"
            )
            print(f"Neo $rand: {neo_results['$rand']}")
        except Exception as e:
            neo_results["$rand"] = f"Error: {e}"
            print(f"Neo $rand: Error - {e}")

    client = test_pymongo_connection()
    mongo_results = {}

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})

        # Run same tests on MongoDB
        mongo_collection.insert_one(
            {
                "_id": 1,
                "bits": 5,
                "arr": [1, 2, 3, 4, 5],
                "items": [{"x": 1}, {"x": 2}, {"x": 3}],
                "obj1": {"a": 1, "b": 2},
                "obj2": {"b": 3, "c": 4},
                "set1": [1, 2, 3],
                "set2": [3, 4, 5],
                "date": "2024-01-15T10:30:00",
                "text": "Hello World",
            }
        )

        # Test bitwise operators
        try:
            result = list(mongo_collection.find({"bits": {"$bitsAllClear": 2}}))
            mongo_results["$bitsAllClear"] = len(result)
            print(f"Mongo $bitsAllClear: {len(result)} documents")
        except Exception as e:
            mongo_results["$bitsAllClear"] = f"Error: {e}"
            print(f"Mongo $bitsAllClear: Error - {e}")

        # Test $pullAll
        try:
            mongo_collection.insert_one({"_id": 2, "arr": [1, 2, 3, 2, 4]})
            result = mongo_collection.update_one(
                {"_id": 2}, {"$pullAll": {"arr": [2]}}
            )
            doc = mongo_collection.find_one({"_id": 2})
            mongo_results["$pullAll"] = doc["arr"]
            print(f"Mongo $pullAll: {doc['arr']}")
        except Exception as e:
            mongo_results["$pullAll"] = f"Error: {e}"
            print(f"Mongo $pullAll: Error - {e}")

        # Test $bucket
        try:
            # Delete existing docs and insert fresh data with value field
            mongo_collection.delete_many({})
            mongo_collection.insert_many(
                [{"_id": 100 + i, "value": i * 10} for i in range(1, 6)]
            )
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$value",
                                "boundaries": [0, 20, 40, 60],
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            mongo_results["$bucket"] = len(result)
            print(f"Mongo $bucket: {len(result)} buckets")
        except Exception as e:
            mongo_results["$bucket"] = f"Error: {e}"
            print(f"Mongo $bucket: Error - {e}")

        # Test $firstN
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "first2": {"$firstN": {"input": "$arr", "n": 2}}
                            }
                        },
                    ]
                )
            )
            mongo_results["$firstN"] = (
                result[0].get("first2") if result else None
            )
            print(f"Mongo $firstN: {mongo_results['$firstN']}")
        except Exception as e:
            mongo_results["$firstN"] = f"Error: {e}"
            print(f"Mongo $firstN: Error - {e}")

        # Test $strcasecmp
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "cmp": {"$strcasecmp": ["hello", "HELLO"]}
                            }
                        },
                    ]
                )
            )
            mongo_results["$strcasecmp"] = (
                result[0].get("cmp") if result else None
            )
            print(f"Mongo $strcasecmp: {mongo_results['$strcasecmp']}")
        except Exception as e:
            mongo_results["$strcasecmp"] = f"Error: {e}"
            print(f"Mongo $strcasecmp: Error - {e}")

        # Test $isNumber
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"is_num": {"$isNumber": "$value"}}},
                    ]
                )
            )
            mongo_results["$isNumber"] = (
                result[0].get("is_num") if result else None
            )
            print(f"Mongo $isNumber: {mongo_results['$isNumber']}")
        except Exception as e:
            mongo_results["$isNumber"] = f"Error: {e}"
            print(f"Mongo $isNumber: Error - {e}")

        # Test $dateFromString
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "date": {
                                    "$dateFromString": {"dateString": "$date"}
                                }
                            }
                        },
                    ]
                )
            )
            mongo_results["$dateFromString"] = "parsed" if result else None
            print(f"Mongo $dateFromString: {mongo_results['$dateFromString']}")
        except Exception as e:
            mongo_results["$dateFromString"] = f"Error: {e}"
            print(f"Mongo $dateFromString: Error - {e}")

        # Test $mergeObjects
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$obj1", "$obj2"]}
                            }
                        },
                    ]
                )
            )
            mongo_results["$mergeObjects"] = (
                result[0].get("merged") if result else None
            )
            print(f"Mongo $mergeObjects: {mongo_results['$mergeObjects']}")
        except Exception as e:
            mongo_results["$mergeObjects"] = f"Error: {e}"
            print(f"Mongo $mergeObjects: Error - {e}")

        # Test $setIntersection
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "intersection": {
                                    "$setIntersection": ["$set1", "$set2"]
                                }
                            }
                        },
                    ]
                )
            )
            mongo_results["$setIntersection"] = (
                result[0].get("intersection") if result else None
            )
            print(
                f"Mongo $setIntersection: {mongo_results['$setIntersection']}"
            )
        except Exception as e:
            mongo_results["$setIntersection"] = f"Error: {e}"
            print(f"Mongo $setIntersection: Error - {e}")

        # Test $rand
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {"$project": {"random": {"$rand": {}}}},
                    ]
                )
            )
            val = result[0].get("random") if result else None
            mongo_results["$rand"] = (
                "generated" if val is not None and 0 <= val <= 1 else "invalid"
            )
            print(f"Mongo $rand: {mongo_results['$rand']}")
        except Exception as e:
            mongo_results["$rand"] = f"Error: {e}"
            print(f"Mongo $rand: Error - {e}")

        # Compare results
        for op_name in neo_results:
            reporter.record_comparison(
                "New Operators",
                op_name,
                neo_results[op_name],
                mongo_results.get(op_name) if mongo_results else None,
                skip_reason="MongoDB not available" if not client else None,
            )

        client.close()
    else:
        # MongoDB not available, record NeoSQLite results as skipped
        for op_name in neo_results:
            reporter.record_comparison(
                "New Operators",
                op_name,
                neo_results[op_name],
                None,
                skip_reason="MongoDB not available",
            )
