"""Module for comparing additional $expr operators complete coverage between NeoSQLite and PyMongo"""

import os
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

# Check if we're running against NX-27017 (NeoSQLite backend)
IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_expr_comprehensive():
    """Compare all remaining $expr operators not yet tested"""
    print("\n=== Additional $expr Operators (Complete Coverage) ===")

    neo_results = {}
    mongo_results = {}

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)
        neo_collection = neo_conn.test_expr_complete
        neo_collection.insert_many(
            [
                {
                    "name": "A",
                    "values": [1, 2, 3, 4, 5],
                    "sets": [[1, 2], [2, 3], [3, 4]],
                    "meta": {"status": "active", "count": 10},
                    "str": "Hello World",
                    "num": 16,
                },
                {
                    "name": "B",
                    "values": [10, 20, 30],
                    "sets": [[10, 20], [30]],
                    "meta": {"status": "inactive", "count": 5},
                    "str": "foo bar",
                    "num": 81,
                },
            ]
        )

        # $map
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "doubled": {
                            "$map": {
                                "input": "$values",
                                "as": "v",
                                "in": {"$multiply": ["$$v", 2]},
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["map"] = result
            print("Neo $map: OK")
        except Exception as e:
            neo_results["map"] = f"Error: {e}"
            print(f"Neo $map: Error - {e}")
        finally:
            end_neo_timing()

        # $reduce
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "sum": {
                            "$reduce": {
                                "input": "$values",
                                "initialValue": 0,
                                "in": {"$add": ["$$value", "$$this"]},
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["reduce"] = result
            print("Neo $reduce: OK")
        except Exception as e:
            neo_results["reduce"] = f"Error: {e}"
            print(f"Neo $reduce: Error - {e}")
        finally:
            end_neo_timing()

        # $indexOfArray
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"index": {"$indexOfArray": ["$values", 3]}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["indexOfArray"] = result
            print("Neo $indexOfArray: OK")
        except Exception as e:
            neo_results["indexOfArray"] = f"Error: {e}"
            print(f"Neo $indexOfArray: Error - {e}")
        finally:
            end_neo_timing()

        # $setEquals
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"equals": {"$setEquals": [[1, 2, 3], [3, 2, 1]]}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setEquals"] = result
            print("Neo $setEquals: OK")
        except Exception as e:
            neo_results["setEquals"] = f"Error: {e}"
            print(f"Neo $setEquals: Error - {e}")
        finally:
            end_neo_timing()

        # $setIntersection
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "intersection": {
                            "$setIntersection": [[1, 2, 3], [2, 3, 4]]
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setIntersection"] = result
            print("Neo $setIntersection: OK")
        except Exception as e:
            neo_results["setIntersection"] = f"Error: {e}"
            print(f"Neo $setIntersection: Error - {e}")
        finally:
            end_neo_timing()

        # $setUnion
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"union": {"$setUnion": [[1, 2], [3, 4]]}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setUnion"] = result
            print("Neo $setUnion: OK")
        except Exception as e:
            neo_results["setUnion"] = f"Error: {e}"
            print(f"Neo $setUnion: Error - {e}")
        finally:
            end_neo_timing()

        # $setDifference
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "difference": {"$setDifference": [[1, 2, 3], [2, 3, 4]]}
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setDifference"] = result
            print("Neo $setDifference: OK")
        except Exception as e:
            neo_results["setDifference"] = f"Error: {e}"
            print(f"Neo $setDifference: Error - {e}")
        finally:
            end_neo_timing()

        # $setIsSubset
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "issubset": {"$setIsSubset": [[1, 2], [1, 2, 3]]}
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setIsSubset"] = result
            print("Neo $setIsSubset: OK")
        except Exception as e:
            neo_results["setIsSubset"] = f"Error: {e}"
            print(f"Neo $setIsSubset: Error - {e}")
        finally:
            end_neo_timing()

        # $anyElementTrue
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "anytrue": {"$anyElementTrue": [[True, False, True]]}
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["anyElementTrue"] = result
            print("Neo $anyElementTrue: OK")
        except Exception as e:
            neo_results["anyElementTrue"] = f"Error: {e}"
            print(f"Neo $anyElementTrue: Error - {e}")
        finally:
            end_neo_timing()

        # $allElementsTrue
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "alltrue": {"$allElementsTrue": [[True, True, True]]}
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["allElementsTrue"] = result
            print("Neo $allElementsTrue: OK")
        except Exception as e:
            neo_results["allElementsTrue"] = f"Error: {e}"
            print(f"Neo $allElementsTrue: Error - {e}")
        finally:
            end_neo_timing()

        # $nor
        start_neo_timing()
        try:
            result = list(
                neo_collection.find({"$nor": [{"name": "A"}, {"name": "B"}]})
            )
            neo_results["nor"] = result
            print("Neo $nor: OK")
        except Exception as e:
            neo_results["nor"] = f"Error: {e}"
            print(f"Neo $nor: Error - {e}")
        finally:
            end_neo_timing()

        # $literal
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"literal": {"$literal": "literal_value"}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["literal"] = result
            print("Neo $literal: OK")
        except Exception as e:
            neo_results["literal"] = f"Error: {e}"
            print(f"Neo $literal: Error - {e}")
        finally:
            end_neo_timing()

        # $setField
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "updated": {
                            "$setField": {
                                "field": "new_field",
                                "input": "$meta",
                                "value": "new_value",
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["setField"] = result
            print("Neo $setField: OK")
        except Exception as e:
            neo_results["setField"] = f"Error: {e}"
            print(f"Neo $setField: Error - {e}")
        finally:
            end_neo_timing()

        # $unsetField
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "removed": {
                            "$unsetField": {"field": "count", "input": "$meta"}
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["unsetField"] = result
            print("Neo $unsetField: OK")
        except Exception as e:
            neo_results["unsetField"] = f"Error: {e}"
            print(f"Neo $unsetField: Error - {e}")
        finally:
            end_neo_timing()

        # $log2
        start_neo_timing()
        try:
            pipeline = [{"$project": {"log2": {"$log2": "$num"}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["log2"] = result
            print("Neo $log2: OK")
        except Exception as e:
            neo_results["log2"] = f"Error: {e}"
            print(f"Neo $log2: Error - {e}")
        finally:
            end_neo_timing()

        # $sigmoid
        start_neo_timing()
        try:
            pipeline = [{"$project": {"sigmoid": {"$sigmoid": 0}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["sigmoid"] = result
            print("Neo $sigmoid: OK")
        except Exception as e:
            neo_results["sigmoid"] = f"Error: {e}"
            print(f"Neo $sigmoid: Error - {e}")
        finally:
            end_neo_timing()

        # $asinh
        start_neo_timing()
        try:
            pipeline = [{"$project": {"asinh": {"$asinh": 1}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["asinh"] = result
            print("Neo $asinh: OK")
        except Exception as e:
            neo_results["asinh"] = f"Error: {e}"
            print(f"Neo $asinh: Error - {e}")
        finally:
            end_neo_timing()

        # $acosh
        start_neo_timing()
        try:
            pipeline = [{"$project": {"acosh": {"$acosh": 2}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["acosh"] = result
            print("Neo $acosh: OK")
        except Exception as e:
            neo_results["acosh"] = f"Error: {e}"
            print(f"Neo $acosh: Error - {e}")
        finally:
            end_neo_timing()

        # $atanh
        start_neo_timing()
        try:
            pipeline = [{"$project": {"atanh": {"$atanh": 0.5}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["atanh"] = result
            print("Neo $atanh: OK")
        except Exception as e:
            neo_results["atanh"] = f"Error: {e}"
            print(f"Neo $atanh: Error - {e}")
        finally:
            end_neo_timing()

        # $regexMatch
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "matches": {
                            "$regexMatch": {
                                "input": "$str",
                                "regex": "Hello|foo",
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["regexMatch"] = result
            print("Neo $regexMatch: OK")
        except Exception as e:
            neo_results["regexMatch"] = f"Error: {e}"
            print(f"Neo $regexMatch: Error - {e}")
        finally:
            end_neo_timing()

        # $replaceOne
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "replaced": {
                            "$replaceOne": {
                                "input": "$str",
                                "find": "o",
                                "replacement": "0",
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["replaceOne"] = result
            print("Neo $replaceOne: OK")
        except Exception as e:
            neo_results["replaceOne"] = f"Error: {e}"
            print(f"Neo $replaceOne: Error - {e}")
        finally:
            end_neo_timing()

        # $ltrim
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"trimmed": {"$ltrim": {"input": "  hello  "}}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["ltrim"] = result
            print("Neo $ltrim: OK")
        except Exception as e:
            neo_results["ltrim"] = f"Error: {e}"
            print(f"Neo $ltrim: Error - {e}")
        finally:
            end_neo_timing()

        # $rtrim
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"trimmed": {"$rtrim": {"input": "  hello  "}}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["rtrim"] = result
            print("Neo $rtrim: OK")
        except Exception as e:
            neo_results["rtrim"] = f"Error: {e}"
            print(f"Neo $rtrim: Error - {e}")
        finally:
            end_neo_timing()

        # $indexOfBytes
        start_neo_timing()
        try:
            pipeline = [
                {"$project": {"index": {"$indexOfBytes": ["$str", "World"]}}}
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["indexOfBytes"] = result
            print("Neo $indexOfBytes: OK")
        except Exception as e:
            neo_results["indexOfBytes"] = f"Error: {e}"
            print(f"Neo $indexOfBytes: Error - {e}")
        finally:
            end_neo_timing()

        # $toLong
        start_neo_timing()
        try:
            pipeline = [{"$project": {"long": {"$toLong": "$num"}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["toLong"] = result
            print("Neo $toLong: OK")
        except Exception as e:
            neo_results["toLong"] = f"Error: {e}"
            print(f"Neo $toLong: Error - {e}")
        finally:
            end_neo_timing()

        # $toDecimal
        start_neo_timing()
        try:
            pipeline = [{"$project": {"decimal": {"$toDecimal": "$num"}}}]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["toDecimal"] = result
            print("Neo $toDecimal: OK")
        except Exception as e:
            neo_results["toDecimal"] = f"Error: {e}"
            print(f"Neo $toDecimal: Error - {e}")
        finally:
            end_neo_timing()

        # $toObjectId
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "oid": {"$toObjectId": "507f1f77bcf86cd799439011"}
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["toObjectId"] = result
            print("Neo $toObjectId: OK")
        except Exception as e:
            neo_results["toObjectId"] = f"Error: {e}"
            print(f"Neo $toObjectId: Error - {e}")
        finally:
            end_neo_timing()

        # $convert
        start_neo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "converted": {
                            "$convert": {
                                "input": "$str",
                                "to": "int",
                                "onError": None,
                            }
                        }
                    }
                }
            ]
            result = list(neo_collection.aggregate(pipeline))
            neo_results["convert"] = result
            print("Neo $convert: OK")
        except Exception as e:
            neo_results["convert"] = f"Error: {e}"
            print(f"Neo $convert: Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    if client:
        set_accumulation_mode(True)
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_expr_complete
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "name": "A",
                    "values": [1, 2, 3, 4, 5],
                    "sets": [[1, 2], [2, 3], [3, 4]],
                    "meta": {"status": "active", "count": 10},
                    "str": "Hello World",
                    "num": 16,
                },
                {
                    "name": "B",
                    "values": [10, 20, 30],
                    "sets": [[10, 20], [30]],
                    "meta": {"status": "inactive", "count": 5},
                    "str": "foo bar",
                    "num": 81,
                },
            ]
        )

        # $map
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "doubled": {
                            "$map": {
                                "input": "$values",
                                "as": "v",
                                "in": {"$multiply": ["$$v", 2]},
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["map"] = result
            print("Mongo $map: OK")
        except Exception as e:
            mongo_results["map"] = f"Error: {e}"
            print(f"Mongo $map: Error - {e}")
        finally:
            end_mongo_timing()

        # $reduce
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "sum": {
                            "$reduce": {
                                "input": "$values",
                                "initialValue": 0,
                                "in": {"$add": ["$$value", "$$this"]},
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["reduce"] = result
            print("Mongo $reduce: OK")
        except Exception as e:
            mongo_results["reduce"] = f"Error: {e}"
            print(f"Mongo $reduce: Error - {e}")
        finally:
            end_mongo_timing()

        # $indexOfArray
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"index": {"$indexOfArray": ["$values", 3]}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["indexOfArray"] = result
            print("Mongo $indexOfArray: OK")
        except Exception as e:
            mongo_results["indexOfArray"] = f"Error: {e}"
            print(f"Mongo $indexOfArray: Error - {e}")
        finally:
            end_mongo_timing()

        # $setEquals
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"equals": {"$setEquals": [[1, 2, 3], [3, 2, 1]]}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setEquals"] = result
            print("Mongo $setEquals: OK")
        except Exception as e:
            mongo_results["setEquals"] = f"Error: {e}"
            print(f"Mongo $setEquals: Error - {e}")
        finally:
            end_mongo_timing()

        # $setIntersection
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "intersection": {
                            "$setIntersection": [[1, 2, 3], [2, 3, 4]]
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setIntersection"] = result
            print("Mongo $setIntersection: OK")
        except Exception as e:
            mongo_results["setIntersection"] = f"Error: {e}"
            print(f"Mongo $setIntersection: Error - {e}")
        finally:
            end_mongo_timing()

        # $setUnion
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"union": {"$setUnion": [[1, 2], [3, 4]]}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setUnion"] = result
            print("Mongo $setUnion: OK")
        except Exception as e:
            mongo_results["setUnion"] = f"Error: {e}"
            print(f"Mongo $setUnion: Error - {e}")
        finally:
            end_mongo_timing()

        # $setDifference
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "difference": {"$setDifference": [[1, 2, 3], [2, 3, 4]]}
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setDifference"] = result
            print("Mongo $setDifference: OK")
        except Exception as e:
            mongo_results["setDifference"] = f"Error: {e}"
            print(f"Mongo $setDifference: Error - {e}")
        finally:
            end_mongo_timing()

        # $setIsSubset
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "issubset": {"$setIsSubset": [[1, 2], [1, 2, 3]]}
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setIsSubset"] = result
            print("Mongo $setIsSubset: OK")
        except Exception as e:
            mongo_results["setIsSubset"] = f"Error: {e}"
            print(f"Mongo $setIsSubset: Error - {e}")
        finally:
            end_mongo_timing()

        # $anyElementTrue
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "anytrue": {"$anyElementTrue": [[True, False, True]]}
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["anyElementTrue"] = result
            print("Mongo $anyElementTrue: OK")
        except Exception as e:
            mongo_results["anyElementTrue"] = f"Error: {e}"
            print(f"Mongo $anyElementTrue: Error - {e}")
        finally:
            end_mongo_timing()

        # $allElementsTrue
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "alltrue": {"$allElementsTrue": [[True, True, True]]}
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["allElementsTrue"] = result
            print("Mongo $allElementsTrue: OK")
        except Exception as e:
            mongo_results["allElementsTrue"] = f"Error: {e}"
            print(f"Mongo $allElementsTrue: Error - {e}")
        finally:
            end_mongo_timing()

        # $nor
        start_mongo_timing()
        try:
            result = list(
                mongo_collection.find({"$nor": [{"name": "A"}, {"name": "B"}]})
            )
            mongo_results["nor"] = result
            print("Mongo $nor: OK")
        except Exception as e:
            mongo_results["nor"] = f"Error: {e}"
            print(f"Mongo $nor: Error - {e}")
        finally:
            end_mongo_timing()

        # $literal
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"literal": {"$literal": "literal_value"}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["literal"] = result
            print("Mongo $literal: OK")
        except Exception as e:
            mongo_results["literal"] = f"Error: {e}"
            print(f"Mongo $literal: Error - {e}")
        finally:
            end_mongo_timing()

        # $setField
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "updated": {
                            "$setField": {
                                "field": "new_field",
                                "input": "$meta",
                                "value": "new_value",
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["setField"] = result
            print("Mongo $setField: OK")
        except Exception as e:
            mongo_results["setField"] = f"Error: {e}"
            print(f"Mongo $setField: Error - {e}")
        finally:
            end_mongo_timing()

        # $unsetField
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "removed": {
                            "$unsetField": {"field": "count", "input": "$meta"}
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["unsetField"] = result
            print("Mongo $unsetField: OK")
        except Exception as e:
            mongo_results["unsetField"] = f"Error: {e}"
            print(f"Mongo $unsetField: Error - {e}")
        finally:
            end_mongo_timing()

        # $log2 (NeoSQLite extension)
        start_mongo_timing()
        try:
            if IS_NX27017_BACKEND:
                pipeline = [{"$project": {"log2": {"$log2": "$num"}}}]
                result = list(mongo_collection.aggregate(pipeline))
                mongo_results["log2"] = result
                print("Mongo $log2: OK")
            else:
                # MongoDB doesn't support $log2, we record it as None to mark it as skipped
                mongo_results["log2"] = None
                print("Mongo $log2: Skipped (extension)")
        except Exception as e:
            mongo_results["log2"] = f"Error: {e}"
            print(f"Mongo $log2: Error - {e}")
        finally:
            end_mongo_timing()

        # $sigmoid
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"sigmoid": {"$sigmoid": 0}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["sigmoid"] = result
            print("Mongo $sigmoid: OK")
        except Exception as e:
            mongo_results["sigmoid"] = f"Error: {e}"
            print(f"Mongo $sigmoid: Error - {e}")
        finally:
            end_mongo_timing()

        # $asinh
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"asinh": {"$asinh": 1}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["asinh"] = result
            print("Mongo $asinh: OK")
        except Exception as e:
            mongo_results["asinh"] = f"Error: {e}"
            print(f"Mongo $asinh: Error - {e}")
        finally:
            end_mongo_timing()

        # $acosh
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"acosh": {"$acosh": 2}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["acosh"] = result
            print("Mongo $acosh: OK")
        except Exception as e:
            mongo_results["acosh"] = f"Error: {e}"
            print(f"Mongo $acosh: Error - {e}")
        finally:
            end_mongo_timing()

        # $atanh
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"atanh": {"$atanh": 0.5}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["atanh"] = result
            print("Mongo $atanh: OK")
        except Exception as e:
            mongo_results["atanh"] = f"Error: {e}"
            print(f"Mongo $atanh: Error - {e}")
        finally:
            end_mongo_timing()

        # $regexMatch
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "matches": {
                            "$regexMatch": {
                                "input": "$str",
                                "regex": "Hello|foo",
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["regexMatch"] = result
            print("Mongo $regexMatch: OK")
        except Exception as e:
            mongo_results["regexMatch"] = f"Error: {e}"
            print(f"Mongo $regexMatch: Error - {e}")
        finally:
            end_mongo_timing()

        # $replaceOne
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "replaced": {
                            "$replaceOne": {
                                "input": "$str",
                                "find": "o",
                                "replacement": "0",
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["replaceOne"] = result
            print("Mongo $replaceOne: OK")
        except Exception as e:
            mongo_results["replaceOne"] = f"Error: {e}"
            print(f"Mongo $replaceOne: Error - {e}")
        finally:
            end_mongo_timing()

        # $ltrim
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"trimmed": {"$ltrim": {"input": "  hello  "}}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["ltrim"] = result
            print("Mongo $ltrim: OK")
        except Exception as e:
            mongo_results["ltrim"] = f"Error: {e}"
            print(f"Mongo $ltrim: Error - {e}")
        finally:
            end_mongo_timing()

        # $rtrim
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"trimmed": {"$rtrim": {"input": "  hello  "}}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["rtrim"] = result
            print("Mongo $rtrim: OK")
        except Exception as e:
            mongo_results["rtrim"] = f"Error: {e}"
            print(f"Mongo $rtrim: Error - {e}")
        finally:
            end_mongo_timing()

        # $indexOfBytes
        start_mongo_timing()
        try:
            pipeline = [
                {"$project": {"index": {"$indexOfBytes": ["$str", "World"]}}}
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["indexOfBytes"] = result
            print("Mongo $indexOfBytes: OK")
        except Exception as e:
            mongo_results["indexOfBytes"] = f"Error: {e}"
            print(f"Mongo $indexOfBytes: Error - {e}")
        finally:
            end_mongo_timing()

        # $toLong
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"long": {"$toLong": "$num"}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["toLong"] = result
            print("Mongo $toLong: OK")
        except Exception as e:
            mongo_results["toLong"] = f"Error: {e}"
            print(f"Mongo $toLong: Error - {e}")
        finally:
            end_mongo_timing()

        # $toDecimal
        start_mongo_timing()
        try:
            pipeline = [{"$project": {"decimal": {"$toDecimal": "$num"}}}]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["toDecimal"] = result
            print("Mongo $toDecimal: OK")
        except Exception as e:
            mongo_results["toDecimal"] = f"Error: {e}"
            print(f"Mongo $toDecimal: Error - {e}")
        finally:
            end_mongo_timing()

        # $toObjectId
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "oid": {"$toObjectId": "507f1f77bcf86cd799439011"}
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["toObjectId"] = result
            print("Mongo $toObjectId: OK")
        except Exception as e:
            mongo_results["toObjectId"] = f"Error: {e}"
            print(f"Mongo $toObjectId: Error - {e}")
        finally:
            end_mongo_timing()

        # $convert
        start_mongo_timing()
        try:
            pipeline = [
                {
                    "$project": {
                        "converted": {
                            "$convert": {
                                "input": "$str",
                                "to": "int",
                                "onError": None,
                            }
                        }
                    }
                }
            ]
            result = list(mongo_collection.aggregate(pipeline))
            mongo_results["convert"] = result
            print("Mongo $convert: OK")
        except Exception as e:
            mongo_results["convert"] = f"Error: {e}"
            print(f"Mongo $convert: Error - {e}")
        finally:
            end_mongo_timing()

        client.close()

    def check_res(res, op):
        if op == "nor":
            return isinstance(res, list) and len(res) == 0
        return isinstance(res, list) and len(res) == 2

    operators = [
        "map",
        "reduce",
        "indexOfArray",
        "setEquals",
        "setIntersection",
        "setUnion",
        "setDifference",
        "setIsSubset",
        "anyElementTrue",
        "allElementsTrue",
        "nor",
        "literal",
        "setField",
        "unsetField",
        "log2",
        "sigmoid",
        "asinh",
        "acosh",
        "atanh",
        "regexMatch",
        "replaceOne",
        "ltrim",
        "rtrim",
        "indexOfBytes",
        "toLong",
        "toDecimal",
        "toObjectId",
        "convert",
    ]

    for op in operators:
        op_label = f"${op}"
        neo_status = check_res(neo_results.get(op), op)
        mongo_status = check_res(mongo_results.get(op), op)

        skip_reason = None
        if not client:
            skip_reason = "MongoDB not available"
        elif op == "log2" and not IS_NX27017_BACKEND:
            skip_reason = "NeoSQLite extension not in MongoDB"

        reporter.record_comparison(
            "$expr (Comprehensive)",
            op_label,
            neo_status if neo_status else "FAIL",
            (
                mongo_status
                if mongo_status
                else (None if not client or skip_reason else "FAIL")
            ),
            skip_reason=skip_reason,
        )
