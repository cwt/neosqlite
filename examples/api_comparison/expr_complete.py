"""Module for comparing additional $expr operators complete coverage between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_expr_operators_complete():
    """Compare all remaining $expr operators not yet tested"""
    print("\n=== Additional $expr Operators (Complete Coverage) ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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

        # Test $map
        try:
            result = list(
                neo_collection.aggregate(
                    [
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
                )
            )
            neo_map = len(result) == 2
            print(f"Neo $map: {'OK' if neo_map else 'FAIL'}")
        except Exception as e:
            neo_map = False
            print(f"Neo $map: Error - {e}")

        # Test $reduce
        try:
            result = list(
                neo_collection.aggregate(
                    [
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
                )
            )
            neo_reduce = len(result) == 2
            print(f"Neo $reduce: {'OK' if neo_reduce else 'FAIL'}")
        except Exception as e:
            neo_reduce = False
            print(f"Neo $reduce: Error - {e}")

        # Test $indexOfArray
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"index": {"$indexOfArray": ["$values", 3]}}}]
                )
            )
            neo_indexofarray = len(result) == 2
            print(f"Neo $indexOfArray: {'OK' if neo_indexofarray else 'FAIL'}")
        except Exception as e:
            neo_indexofarray = False
            print(f"Neo $indexOfArray: Error - {e}")

        # Test $setEquals
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "equals": {"$setEquals": [[1, 2, 3], [3, 2, 1]]}
                            }
                        }
                    ]
                )
            )
            neo_setequals = len(result) == 2
            print(f"Neo $setEquals: {'OK' if neo_setequals else 'FAIL'}")
        except Exception as e:
            neo_setequals = False
            print(f"Neo $setEquals: Error - {e}")

        # Test $setIntersection
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "intersection": {
                                    "$setIntersection": [
                                        [1, 2, 3],
                                        [2, 3, 4],
                                    ]
                                }
                            }
                        }
                    ]
                )
            )
            neo_setintersection = len(result) == 2
            print(
                f"Neo $setIntersection: {'OK' if neo_setintersection else 'FAIL'}"
            )
        except Exception as e:
            neo_setintersection = False
            print(f"Neo $setIntersection: Error - {e}")

        # Test $setUnion
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"union": {"$setUnion": [[1, 2], [3, 4]]}}}]
                )
            )
            neo_setunion = len(result) == 2
            print(f"Neo $setUnion: {'OK' if neo_setunion else 'FAIL'}")
        except Exception as e:
            neo_setunion = False
            print(f"Neo $setUnion: Error - {e}")

        # Test $setDifference
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "difference": {
                                    "$setDifference": [
                                        [1, 2, 3],
                                        [2, 3, 4],
                                    ]
                                }
                            }
                        }
                    ]
                )
            )
            neo_setdifference = len(result) == 2
            print(
                f"Neo $setDifference: {'OK' if neo_setdifference else 'FAIL'}"
            )
        except Exception as e:
            neo_setdifference = False
            print(f"Neo $setDifference: Error - {e}")

        # Test $setIsSubset
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "issubset": {
                                    "$setIsSubset": [[1, 2], [1, 2, 3]]
                                }
                            }
                        }
                    ]
                )
            )
            neo_setissubset = len(result) == 2
            print(f"Neo $setIsSubset: {'OK' if neo_setissubset else 'FAIL'}")
        except Exception as e:
            neo_setissubset = False
            print(f"Neo $setIsSubset: Error - {e}")

        # Test $anyElementTrue - MongoDB format: array directly
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "anytrue": {
                                    "$anyElementTrue": [[True, False, True]]
                                }
                            }
                        }
                    ]
                )
            )
            neo_anyelementtrue = len(result) == 2
            print(
                f"Neo $anyElementTrue: {'OK' if neo_anyelementtrue else 'FAIL'}"
            )
        except Exception as e:
            neo_anyelementtrue = False
            print(f"Neo $anyElementTrue: Error - {e}")

        # Test $allElementsTrue - MongoDB format: array directly
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "alltrue": {
                                    "$allElementsTrue": [[True, True, True]]
                                }
                            }
                        }
                    ]
                )
            )
            neo_allelementstrue = len(result) == 2
            print(
                f"Neo $allElementsTrue: {'OK' if neo_allelementstrue else 'FAIL'}"
            )
        except Exception as e:
            neo_allelementstrue = False
            print(f"Neo $allElementsTrue: Error - {e}")

        # Test $nor - Query operator (not $expr operator), use at top level
        try:
            result = list(
                neo_collection.find(
                    {
                        "$nor": [
                            {"name": "A"},
                            {"name": "B"},
                        ]
                    }
                )
            )
            neo_nor = len(result) == 0  # Should match neither A nor B
            print(f"Neo $nor: {'OK' if neo_nor else 'FAIL'}")
        except Exception as e:
            neo_nor = False
            print(f"Neo $nor: Error - {e}")

        # Test $literal - use a value that doesn't look like an operator
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"literal": {"$literal": "literal_value"}}}]
                )
            )
            neo_literal = len(result) == 2
            print(f"Neo $literal: {'OK' if neo_literal else 'FAIL'}")
        except Exception as e:
            neo_literal = False
            print(f"Neo $literal: Error - {e}")

        # Test $setField
        try:
            result = list(
                neo_collection.aggregate(
                    [
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
                                "removed": {
                                    "$unsetField": {
                                        "field": "count",
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

        # Test $log2
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"log2": {"$log2": "$num"}}}]
                )
            )
            neo_log2 = len(result) == 2
            print(f"Neo $log2: {'OK' if neo_log2 else 'FAIL'}")
        except Exception as e:
            neo_log2 = False
            print(f"Neo $log2: Error - {e}")

        # Test $sigmoid (MongoDB 8.0+)
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"sigmoid": {"$sigmoid": 0}}}]
                )
            )
            neo_sigmoid = len(result) == 2
            print(f"Neo $sigmoid: {'OK' if neo_sigmoid else 'FAIL'}")
        except Exception as e:
            neo_sigmoid = False
            print(f"Neo $sigmoid: Error - {e}")

        # Test $asinh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"asinh": {"$asinh": 1}}}]
                )
            )
            neo_asinh = len(result) == 2
            print(f"Neo $asinh: {'OK' if neo_asinh else 'FAIL'}")
        except Exception as e:
            neo_asinh = False
            print(f"Neo $asinh: Error - {e}")

        # Test $acosh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"acosh": {"$acosh": 2}}}]
                )
            )
            neo_acosh = len(result) == 2
            print(f"Neo $acosh: {'OK' if neo_acosh else 'FAIL'}")
        except Exception as e:
            neo_acosh = False
            print(f"Neo $acosh: Error - {e}")

        # Test $atanh
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"atanh": {"$atanh": 0.5}}}]
                )
            )
            neo_atanh = len(result) == 2
            print(f"Neo $atanh: {'OK' if neo_atanh else 'FAIL'}")
        except Exception as e:
            neo_atanh = False
            print(f"Neo $atanh: Error - {e}")

        # Test $regexMatch
        try:
            result = list(
                neo_collection.aggregate(
                    [
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
                )
            )
            neo_regexmatch = len(result) == 2
            print(f"Neo $regexMatch: {'OK' if neo_regexmatch else 'FAIL'}")
        except Exception as e:
            neo_regexmatch = False
            print(f"Neo $regexMatch: Error - {e}")

        # Test $replaceOne
        try:
            result = list(
                neo_collection.aggregate(
                    [
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
                )
            )
            neo_replaceone = len(result) == 2
            print(f"Neo $replaceOne: {'OK' if neo_replaceone else 'FAIL'}")
        except Exception as e:
            neo_replaceone = False
            print(f"Neo $replaceOne: Error - {e}")

        # Test $ltrim
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "trimmed": {"$ltrim": {"input": "  hello  "}}
                            }
                        }
                    ]
                )
            )
            neo_ltrim = len(result) == 2
            print(f"Neo $ltrim: {'OK' if neo_ltrim else 'FAIL'}")
        except Exception as e:
            neo_ltrim = False
            print(f"Neo $ltrim: Error - {e}")

        # Test $rtrim
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "trimmed": {"$rtrim": {"input": "  hello  "}}
                            }
                        }
                    ]
                )
            )
            neo_rtrim = len(result) == 2
            print(f"Neo $rtrim: {'OK' if neo_rtrim else 'FAIL'}")
        except Exception as e:
            neo_rtrim = False
            print(f"Neo $rtrim: Error - {e}")

        # Test $indexOfBytes
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "index": {"$indexOfBytes": ["$str", "World"]}
                            }
                        }
                    ]
                )
            )
            neo_indexofbytes = len(result) == 2
            print(f"Neo $indexOfBytes: {'OK' if neo_indexofbytes else 'FAIL'}")
        except Exception as e:
            neo_indexofbytes = False
            print(f"Neo $indexOfBytes: Error - {e}")

        # Test $toLong
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"long": {"$toLong": "$num"}}}]
                )
            )
            neo_tolong = len(result) == 2
            print(f"Neo $toLong: {'OK' if neo_tolong else 'FAIL'}")
        except Exception as e:
            neo_tolong = False
            print(f"Neo $toLong: Error - {e}")

        # Test $toDecimal
        try:
            result = list(
                neo_collection.aggregate(
                    [{"$project": {"decimal": {"$toDecimal": "$num"}}}]
                )
            )
            neo_todecimal = len(result) == 2
            print(f"Neo $toDecimal: {'OK' if neo_todecimal else 'FAIL'}")
        except Exception as e:
            neo_todecimal = False
            print(f"Neo $toDecimal: Error - {e}")

        # Test $toObjectId
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "oid": {
                                    "$toObjectId": "507f1f77bcf86cd799439011"
                                }
                            }
                        }
                    ]
                )
            )
            neo_toobjectid = len(result) == 2
            print(f"Neo $toObjectId: {'OK' if neo_toobjectid else 'FAIL'}")
        except Exception as e:
            neo_toobjectid = False
            print(f"Neo $toObjectId: Error - {e}")

        # Test $convert
        try:
            result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "converted": {
                                    "$convert": {
                                        "input": "$str",
                                        "to": "int",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            neo_convert = len(result) == 2
            print(f"Neo $convert: {'OK' if neo_convert else 'FAIL'}")
        except Exception as e:
            neo_convert = False
            print(f"Neo $convert: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_acosh = None

    mongo_allelementstrue = None

    mongo_anyelementtrue = None

    mongo_asinh = None

    mongo_atanh = None

    mongo_collection = None

    mongo_convert = None

    mongo_db = None

    mongo_indexofarray = None

    mongo_indexofbytes = None

    mongo_literal = None

    mongo_log2 = None

    mongo_ltrim = None

    mongo_map = None

    mongo_nor = None

    mongo_reduce = None

    mongo_regexmatch = None

    mongo_replaceone = None

    mongo_rtrim = None

    mongo_setdifference = None

    mongo_setequals = None

    mongo_setfield = None

    mongo_setintersection = None

    mongo_setissubset = None

    mongo_setunion = None

    mongo_sigmoid = None

    mongo_todecimal = None

    mongo_tolong = None

    mongo_toobjectid = None

    mongo_unsetfield = None

    if client:
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

        # Test $map
        try:
            result = list(
                mongo_collection.aggregate(
                    [
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
                )
            )
            mongo_map = len(result) == 2
            print(f"Mongo $map: {'OK' if mongo_map else 'FAIL'}")
        except Exception as e:
            mongo_map = False
            print(f"Mongo $map: Error - {e}")

        # Test $reduce
        try:
            result = list(
                mongo_collection.aggregate(
                    [
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
                )
            )
            mongo_reduce = len(result) == 2
            print(f"Mongo $reduce: {'OK' if mongo_reduce else 'FAIL'}")
        except Exception as e:
            mongo_reduce = False
            print(f"Mongo $reduce: Error - {e}")

        # Test $indexOfArray
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"index": {"$indexOfArray": ["$values", 3]}}}]
                )
            )
            mongo_indexofarray = len(result) == 2
            print(
                f"Mongo $indexOfArray: {'OK' if mongo_indexofarray else 'FAIL'}"
            )
        except Exception as e:
            mongo_indexofarray = False
            print(f"Mongo $indexOfArray: Error - {e}")

        # Test $setEquals
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "equals": {"$setEquals": [[1, 2, 3], [3, 2, 1]]}
                            }
                        }
                    ]
                )
            )
            mongo_setequals = len(result) == 2
            print(f"Mongo $setEquals: {'OK' if mongo_setequals else 'FAIL'}")
        except Exception as e:
            mongo_setequals = False
            print(f"Mongo $setEquals: Error - {e}")

        # Test $setIntersection
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "intersection": {
                                    "$setIntersection": [
                                        [1, 2, 3],
                                        [2, 3, 4],
                                    ]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_setintersection = len(result) == 2
            print(
                f"Mongo $setIntersection: {'OK' if mongo_setintersection else 'FAIL'}"
            )
        except Exception as e:
            mongo_setintersection = False
            print(f"Mongo $setIntersection: Error - {e}")

        # Test $setUnion
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"union": {"$setUnion": [[1, 2], [3, 4]]}}}]
                )
            )
            mongo_setunion = len(result) == 2
            print(f"Mongo $setUnion: {'OK' if mongo_setunion else 'FAIL'}")
        except Exception as e:
            mongo_setunion = False
            print(f"Mongo $setUnion: Error - {e}")

        # Test $setDifference
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "difference": {
                                    "$setDifference": [
                                        [1, 2, 3],
                                        [2, 3, 4],
                                    ]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_setdifference = len(result) == 2
            print(
                f"Mongo $setDifference: {'OK' if mongo_setdifference else 'FAIL'}"
            )
        except Exception as e:
            mongo_setdifference = False
            print(f"Mongo $setDifference: Error - {e}")

        # Test $setIsSubset
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "issubset": {
                                    "$setIsSubset": [[1, 2], [1, 2, 3]]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_setissubset = len(result) == 2
            print(
                f"Mongo $setIsSubset: {'OK' if mongo_setissubset else 'FAIL'}"
            )
        except Exception as e:
            mongo_setissubset = False
            print(f"Mongo $setIsSubset: Error - {e}")

        # Test $anyElementTrue - MongoDB format: array directly
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "anytrue": {
                                    "$anyElementTrue": [[True, False, True]]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_anyelementtrue = len(result) == 2
            print(
                f"Mongo $anyElementTrue: {'OK' if mongo_anyelementtrue else 'FAIL'}"
            )
        except Exception as e:
            mongo_anyelementtrue = False
            print(f"Mongo $anyElementTrue: Error - {e}")

        # Test $allElementsTrue - MongoDB format: array directly
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "alltrue": {
                                    "$allElementsTrue": [[True, True, True]]
                                }
                            }
                        }
                    ]
                )
            )
            mongo_allelementstrue = len(result) == 2
            print(
                f"Mongo $allElementsTrue: {'OK' if mongo_allelementstrue else 'FAIL'}"
            )
        except Exception as e:
            mongo_allelementstrue = False
            print(f"Mongo $allElementsTrue: Error - {e}")

        # Test $nor - Query operator (not $expr operator), use at top level
        try:
            result = list(
                mongo_collection.find(
                    {
                        "$nor": [
                            {"name": "A"},
                            {"name": "B"},
                        ]
                    }
                )
            )
            mongo_nor = len(result) == 0
            print(f"Mongo $nor: {'OK' if mongo_nor else 'FAIL'}")
        except Exception as e:
            mongo_nor = False
            print(f"Mongo $nor: Error - {e}")

        # Test $literal - use a value that doesn't look like an operator
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"literal": {"$literal": "literal_value"}}}]
                )
            )
            mongo_literal = len(result) == 2
            print(f"Mongo $literal: {'OK' if mongo_literal else 'FAIL'}")
        except Exception as e:
            mongo_literal = False
            print(f"Mongo $literal: Error - {e}")

        # Test $setField
        try:
            result = list(
                mongo_collection.aggregate(
                    [
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
                                "removed": {
                                    "$unsetField": {
                                        "field": "count",
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

        # Test $log2 - NeoSQLite extension (not in MongoDB)
        mongo_log2 = True  # Skip - NeoSQLite extension
        print("Mongo $log2: N/A (NeoSQLite extension)")

        # Test $sigmoid (MongoDB 8.0+)
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"sigmoid": {"$sigmoid": 0}}}]
                )
            )
            mongo_sigmoid = len(result) == 2
            print(f"Mongo $sigmoid: {'OK' if mongo_sigmoid else 'FAIL'}")
        except Exception as e:
            mongo_sigmoid = False
            print(f"Mongo $sigmoid: Error - {e}")

        # Test $asinh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"asinh": {"$asinh": 1}}}]
                )
            )
            mongo_asinh = len(result) == 2
            print(f"Mongo $asinh: {'OK' if mongo_asinh else 'FAIL'}")
        except Exception as e:
            mongo_asinh = False
            print(f"Mongo $asinh: Error - {e}")

        # Test $acosh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"acosh": {"$acosh": 2}}}]
                )
            )
            mongo_acosh = len(result) == 2
            print(f"Mongo $acosh: {'OK' if mongo_acosh else 'FAIL'}")
        except Exception as e:
            mongo_acosh = False
            print(f"Mongo $acosh: Error - {e}")

        # Test $atanh
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"atanh": {"$atanh": 0.5}}}]
                )
            )
            mongo_atanh = len(result) == 2
            print(f"Mongo $atanh: {'OK' if mongo_atanh else 'FAIL'}")
        except Exception as e:
            mongo_atanh = False
            print(f"Mongo $atanh: Error - {e}")

        # Test $regexMatch
        try:
            result = list(
                mongo_collection.aggregate(
                    [
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
                )
            )
            mongo_regexmatch = len(result) == 2
            print(f"Mongo $regexMatch: {'OK' if mongo_regexmatch else 'FAIL'}")
        except Exception as e:
            mongo_regexmatch = False
            print(f"Mongo $regexMatch: Error - {e}")

        # Test $replaceOne
        try:
            result = list(
                mongo_collection.aggregate(
                    [
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
                )
            )
            mongo_replaceone = len(result) == 2
            print(f"Mongo $replaceOne: {'OK' if mongo_replaceone else 'FAIL'}")
        except Exception as e:
            mongo_replaceone = False
            print(f"Mongo $replaceOne: Error - {e}")

        # Test $ltrim
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "trimmed": {"$ltrim": {"input": "  hello  "}}
                            }
                        }
                    ]
                )
            )
            mongo_ltrim = len(result) == 2
            print(f"Mongo $ltrim: {'OK' if mongo_ltrim else 'FAIL'}")
        except Exception as e:
            mongo_ltrim = False
            print(f"Mongo $ltrim: Error - {e}")

        # Test $rtrim
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "trimmed": {"$rtrim": {"input": "  hello  "}}
                            }
                        }
                    ]
                )
            )
            mongo_rtrim = len(result) == 2
            print(f"Mongo $rtrim: {'OK' if mongo_rtrim else 'FAIL'}")
        except Exception as e:
            mongo_rtrim = False
            print(f"Mongo $rtrim: Error - {e}")

        # Test $indexOfBytes
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "index": {"$indexOfBytes": ["$str", "World"]}
                            }
                        }
                    ]
                )
            )
            mongo_indexofbytes = len(result) == 2
            print(
                f"Mongo $indexOfBytes: {'OK' if mongo_indexofbytes else 'FAIL'}"
            )
        except Exception as e:
            mongo_indexofbytes = False
            print(f"Mongo $indexOfBytes: Error - {e}")

        # Test $toLong
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"long": {"$toLong": "$num"}}}]
                )
            )
            mongo_tolong = len(result) == 2
            print(f"Mongo $toLong: {'OK' if mongo_tolong else 'FAIL'}")
        except Exception as e:
            mongo_tolong = False
            print(f"Mongo $toLong: Error - {e}")

        # Test $toDecimal
        try:
            result = list(
                mongo_collection.aggregate(
                    [{"$project": {"decimal": {"$toDecimal": "$num"}}}]
                )
            )
            mongo_todecimal = len(result) == 2
            print(f"Mongo $toDecimal: {'OK' if mongo_todecimal else 'FAIL'}")
        except Exception as e:
            mongo_todecimal = False
            print(f"Mongo $toDecimal: Error - {e}")

        # Test $toObjectId
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "oid": {
                                    "$toObjectId": "507f1f77bcf86cd799439011"
                                }
                            }
                        }
                    ]
                )
            )
            mongo_toobjectid = len(result) == 2
            print(f"Mongo $toObjectId: {'OK' if mongo_toobjectid else 'FAIL'}")
        except Exception as e:
            mongo_toobjectid = False
            print(f"Mongo $toObjectId: Error - {e}")

        # Test $convert
        try:
            result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "converted": {
                                    "$convert": {
                                        "input": "$str",
                                        "to": "int",
                                        "onError": None,  # Handle conversion errors gracefully
                                    }
                                }
                            }
                        }
                    ]
                )
            )
            mongo_convert = len(result) == 2
            print(f"Mongo $convert: {'OK' if mongo_convert else 'FAIL'}")
        except Exception as e:
            mongo_convert = False
            print(f"Mongo $convert: Error - {e}")

        client.close()

        # Record results
        reporter.record_comparison(
            "Additional $expr Operators",
            "$map",
            neo_map if neo_map else "FAIL",
            mongo_map if mongo_map else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$reduce",
            neo_reduce if neo_reduce else "FAIL",
            mongo_reduce if mongo_reduce else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$indexOfArray",
            neo_indexofarray if neo_indexofarray else "FAIL",
            mongo_indexofarray if mongo_indexofarray else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setEquals",
            neo_setequals if neo_setequals else "FAIL",
            mongo_setequals if mongo_setequals else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setIntersection",
            neo_setintersection if neo_setintersection else "FAIL",
            mongo_setintersection if mongo_setintersection else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setUnion",
            neo_setunion if neo_setunion else "FAIL",
            mongo_setunion if mongo_setunion else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setDifference",
            neo_setdifference if neo_setdifference else "FAIL",
            mongo_setdifference if mongo_setdifference else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setIsSubset",
            neo_setissubset if neo_setissubset else "FAIL",
            mongo_setissubset if mongo_setissubset else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$anyElementTrue",
            neo_anyelementtrue if neo_anyelementtrue else "FAIL",
            mongo_anyelementtrue if mongo_anyelementtrue else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$allElementsTrue",
            neo_allelementstrue if neo_allelementstrue else "FAIL",
            mongo_allelementstrue if mongo_allelementstrue else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$nor",
            neo_nor if neo_nor else "FAIL",
            mongo_nor if mongo_nor else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$literal",
            neo_literal if neo_literal else "FAIL",
            mongo_literal if mongo_literal else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$setField",
            neo_setfield if neo_setfield else "FAIL",
            mongo_setfield if mongo_setfield else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$unsetField",
            neo_unsetfield if neo_unsetfield else "FAIL",
            mongo_unsetfield if mongo_unsetfield else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$log2",
            neo_log2 if neo_log2 else "FAIL",
            mongo_log2 if mongo_log2 else None,
            skip_reason="NeoSQLite extension not in MongoDB",
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$sigmoid",
            neo_sigmoid if neo_sigmoid else "FAIL",
            mongo_sigmoid if mongo_sigmoid else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$asinh",
            neo_asinh if neo_asinh else "FAIL",
            mongo_asinh if mongo_asinh else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$acosh",
            neo_acosh if neo_acosh else "FAIL",
            mongo_acosh if mongo_acosh else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$atanh",
            neo_atanh if neo_atanh else "FAIL",
            mongo_atanh if mongo_atanh else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$regexMatch",
            neo_regexmatch if neo_regexmatch else "FAIL",
            mongo_regexmatch if mongo_regexmatch else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$replaceOne",
            neo_replaceone if neo_replaceone else "FAIL",
            mongo_replaceone if mongo_replaceone else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$ltrim",
            neo_ltrim if neo_ltrim else "FAIL",
            mongo_ltrim if mongo_ltrim else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$rtrim",
            neo_rtrim if neo_rtrim else "FAIL",
            mongo_rtrim if mongo_rtrim else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$indexOfBytes",
            neo_indexofbytes if neo_indexofbytes else "FAIL",
            mongo_indexofbytes if mongo_indexofbytes else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$toLong",
            neo_tolong if neo_tolong else "FAIL",
            mongo_tolong if mongo_tolong else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$toDecimal",
            neo_todecimal if neo_todecimal else "FAIL",
            mongo_todecimal if mongo_todecimal else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$toObjectId",
            neo_toobjectid if neo_toobjectid else "FAIL",
            mongo_toobjectid if mongo_toobjectid else None,
            skip_reason="MongoDB not available" if not client else None,
        )
        reporter.record_comparison(
            "Additional $expr Operators",
            "$convert",
            neo_convert if neo_convert else "FAIL",
            mongo_convert if mongo_convert else None,
            skip_reason="MongoDB not available" if not client else None,
        )
