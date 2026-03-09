"""Module for comparing object operators between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_object_operators():
    """Compare object operators in aggregation"""
    print("\n=== Object Operators Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {
                    "name": "A",
                    "meta": {"city": "NYC", "zip": 10001},
                    "extra": {"country": "USA"},
                },
                {
                    "name": "B",
                    "meta": {"city": "LA", "zip": 90001},
                    "extra": {"country": "USA"},
                },
            ]
        )

        # Test $mergeObjects
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$meta", "$extra"]}
                            }
                        }
                    ]
                )
            )
            neo_mergeobjects = len(result) == 2
            print(f"Neo $mergeObjects: {'OK' if neo_mergeobjects else 'FAIL'}")
        except Exception as e:
            neo_mergeobjects = False
            print(f"Neo $mergeObjects: Error - {e}")

        # Test $getField
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "city": {
                                    "$getField": {
                                        "field": "city",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_getfield = len(result) == 2
            print(f"Neo $getField: {'OK' if neo_getfield else 'FAIL'}")
        except Exception as e:
            neo_getfield = False
            print(f"Neo $getField: Error - {e}")

        # Test $setField
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "with_field": {
                                    "$setField": {
                                        "field": "newField",
                                        "input": "$meta",
                                        "value": "newValue",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_setfield = len(result) == 2
            print(f"Neo $setField: {'OK' if neo_setfield else 'FAIL'}")
        except Exception as e:
            neo_setfield = False
            print(f"Neo $setField: Error - {e}")

        # Test $unsetField
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "without_zip": {
                                    "$unsetField": {
                                        "field": "zip",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_unsetfield = len(result) == 2
            print(f"Neo $unsetField: {'OK' if neo_unsetfield else 'FAIL'}")
        except Exception as e:
            neo_unsetfield = False
            print(f"Neo $unsetField: Error - {e}")

        # Test $objectToArray
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"as_array": {"$objectToArray": "$meta"}}}]
                )
            )
            neo_objecttoarray = len(result) == 2
            print(
                f"Neo $objectToArray: {'OK' if neo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            neo_objecttoarray = False
            print(f"Neo $objectToArray: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_getfield = None
    mongo_mergeobjects = None
    mongo_setfield = None
    mongo_unsetfield = None
    mongo_objecttoarray = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "A",
                    "meta": {"city": "NYC", "zip": 10001},
                    "extra": {"country": "USA"},
                },
                {
                    "name": "B",
                    "meta": {"city": "LA", "zip": 90001},
                    "extra": {"country": "USA"},
                },
            ]
        )

        # Test $mergeObjects
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "merged": {"$mergeObjects": ["$meta", "$extra"]}
                            }
                        }
                    ]
                )
            )
            mongo_mergeobjects = len(result) == 2
            print(
                f"Mongo $mergeObjects: {'OK' if mongo_mergeobjects else 'FAIL'}"
            )
        except Exception as e:
            mongo_mergeobjects = False
            print(f"Mongo $mergeObjects: Error - {e}")

        # Test $getField
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "city": {
                                    "$getField": {
                                        "field": "city",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_getfield = len(result) == 2
            print(f"Mongo $getField: {'OK' if mongo_getfield else 'FAIL'}")
        except Exception as e:
            mongo_getfield = False
            print(f"Mongo $getField: Error - {e}")

        # Test $setField
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "with_field": {
                                    "$setField": {
                                        "field": "newField",
                                        "input": "$meta",
                                        "value": "newValue",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_setfield = len(result) == 2
            print(f"Mongo $setField: {'OK' if mongo_setfield else 'FAIL'}")
        except Exception as e:
            mongo_setfield = False
            print(f"Mongo $setField: Error - {e}")

        # Test $unsetField
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "without_zip": {
                                    "$unsetField": {
                                        "field": "zip",
                                        "input": "$meta",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_unsetfield = len(result) == 2
            print(f"Mongo $unsetField: {'OK' if mongo_unsetfield else 'FAIL'}")
        except Exception as e:
            mongo_unsetfield = False
            print(f"Mongo $unsetField: Error - {e}")

        # Test $objectToArray
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"as_array": {"$objectToArray": "$meta"}}}]
                )
            )
            mongo_objecttoarray = len(result) == 2
            print(
                f"Mongo $objectToArray: {'OK' if mongo_objecttoarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_objecttoarray = False
            print(f"Mongo $objectToArray: Error - {e}")

        client.close()

        reporter.record_comparison(
            "Object Operators",
            "$mergeObjects",
            neo_mergeobjects if neo_mergeobjects else "FAIL",
            mongo_mergeobjects if mongo_mergeobjects else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Object Operators",
            "$getField",
            neo_getfield if neo_getfield else "FAIL",
            mongo_getfield if mongo_getfield else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Object Operators",
            "$setField",
            neo_setfield if neo_setfield else "FAIL",
            mongo_setfield if mongo_setfield else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Object Operators",
            "$unsetField",
            neo_unsetfield if neo_unsetfield else "FAIL",
            mongo_unsetfield if mongo_unsetfield else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Object Operators",
            "$objectToArray",
            neo_objecttoarray if neo_objecttoarray else "FAIL",
            mongo_objecttoarray if mongo_objecttoarray else None,
            skip_reason="MongoDB not available" if not client else None,
        )
