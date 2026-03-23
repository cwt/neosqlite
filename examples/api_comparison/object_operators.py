"""Module for comparing object operators between NeoSQLite and PyMongo"""

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


def compare_object_operators():
    """Compare object operators in aggregation"""
    print("\n=== Object Operators Comparison ===")

    neo_mergeobjects = False
    neo_getfield = False
    neo_setfield = False
    neo_unsetfield = False
    neo_objecttoarray = False

    mongo_mergeobjects = None
    mongo_getfield = None
    mongo_setfield = None
    mongo_unsetfield = None
    mongo_objecttoarray = None

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

        set_accumulation_mode(True)

        # Test $mergeObjects
        start_neo_timing()
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
        except Exception as e:
            print(f"Neo $mergeObjects: Error - {e}")
            neo_mergeobjects = False
        finally:
            end_neo_timing()
        print(f"Neo $mergeObjects: {'OK' if neo_mergeobjects else 'FAIL'}")

        # Test $getField
        start_neo_timing()
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
        except Exception as e:
            print(f"Neo $getField: Error - {e}")
            neo_getfield = False
        finally:
            end_neo_timing()
        print(f"Neo $getField: {'OK' if neo_getfield else 'FAIL'}")

        # Test $setField
        start_neo_timing()
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
        except Exception as e:
            print(f"Neo $setField: Error - {e}")
            neo_setfield = False
        finally:
            end_neo_timing()
        print(f"Neo $setField: {'OK' if neo_setfield else 'FAIL'}")

        # Test $unsetField
        start_neo_timing()
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
        except Exception as e:
            print(f"Neo $unsetField: Error - {e}")
            neo_unsetfield = False
        finally:
            end_neo_timing()
        print(f"Neo $unsetField: {'OK' if neo_unsetfield else 'FAIL'}")

        # Test $objectToArray
        start_neo_timing()
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"as_array": {"$objectToArray": "$meta"}}}]
                )
            )
            neo_objecttoarray = len(result) == 2
        except Exception as e:
            print(f"Neo $objectToArray: Error - {e}")
            neo_objecttoarray = False
        finally:
            end_neo_timing()
        print(f"Neo $objectToArray: {'OK' if neo_objecttoarray else 'FAIL'}")

    client = test_pymongo_connection()
    if client:
        try:
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

            set_accumulation_mode(True)

            # Test $mergeObjects
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "merged": {
                                        "$mergeObjects": ["$meta", "$extra"]
                                    }
                                }
                            }
                        ]
                    )
                )
                mongo_mergeobjects = len(result) == 2
            except Exception as e:
                print(f"Mongo $mergeObjects: Error - {e}")
                mongo_mergeobjects = False
            finally:
                end_mongo_timing()
            print(
                f"Mongo $mergeObjects: {'OK' if mongo_mergeobjects else 'FAIL'}"
            )

            # Test $getField
            start_mongo_timing()
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
            except Exception as e:
                print(f"Mongo $getField: Error - {e}")
                mongo_getfield = False
            finally:
                end_mongo_timing()
            print(f"Mongo $getField: {'OK' if mongo_getfield else 'FAIL'}")

            # Test $setField
            start_mongo_timing()
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
            except Exception as e:
                print(f"Mongo $setField: Error - {e}")
                mongo_setfield = False
            finally:
                end_mongo_timing()
            print(f"Mongo $setField: {'OK' if mongo_setfield else 'FAIL'}")

            # Test $unsetField
            start_mongo_timing()
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
            except Exception as e:
                print(f"Mongo $unsetField: Error - {e}")
                mongo_unsetfield = False
            finally:
                end_mongo_timing()
            print(f"Mongo $unsetField: {'OK' if mongo_unsetfield else 'FAIL'}")

            # Test $objectToArray
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "as_array": {"$objectToArray": "$meta"}
                                }
                            }
                        ]
                    )
                )
                mongo_objecttoarray = len(result) == 2
            except Exception as e:
                print(f"Mongo $objectToArray: Error - {e}")
                mongo_objecttoarray = False
            finally:
                end_mongo_timing()
            print(
                f"Mongo $objectToArray: {'OK' if mongo_objecttoarray else 'FAIL'}"
            )

        finally:
            client.close()

    reporter.record_comparison(
        "Object Operators",
        "$mergeObjects",
        neo_mergeobjects if neo_mergeobjects else "FAIL",
        mongo_mergeobjects,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Object Operators",
        "$getField",
        neo_getfield if neo_getfield else "FAIL",
        mongo_getfield,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Object Operators",
        "$setField",
        neo_setfield if neo_setfield else "FAIL",
        mongo_setfield,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Object Operators",
        "$unsetField",
        neo_unsetfield if neo_unsetfield else "FAIL",
        mongo_unsetfield,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Object Operators",
        "$objectToArray",
        neo_objecttoarray if neo_objecttoarray else "FAIL",
        mongo_objecttoarray,
        skip_reason="MongoDB not available" if not client else None,
    )
