"""Module for comparing string operators between NeoSQLite and PyMongo"""

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
from .utils import sanitize_for_mongodb, test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_string_operators():
    """Compare string operators in aggregation"""
    print("\n=== String Operators Comparison ===")

    # Initialize result variables
    neo_results = {}
    mongo_results = {}

    operators = [
        "$substr",
        "$trim",
        "$split",
        "$replaceAll",
        "$ltrim",
        "$rtrim",
        "$strLenCP",
        "$regexFind",
        "$regexMatch",
        "$replaceOne",
        "$indexOfCP",
        "$regexFindAll",
    ]

    for op in operators:
        neo_results[op] = None
        mongo_results[op] = None

    test_data = [
        {"name": "  Alice  ", "city": "New York", "text": ""},
        {"name": "  Bob  ", "city": "Los Angeles", "text": ""},
        {"name": "", "city": "", "text": "Line 1\nLine 2"},
    ]

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(test_data)

        set_accumulation_mode(True)

        # $substr
        start_neo_timing()
        try:
            neo_results["$substr"] = list(
                neo_collection.aggregate(
                    [{"$project": {"short": {"$substr": ["$name", 2, 3]}}}]
                )
            )
        except Exception as e:
            neo_results["$substr"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $trim
        start_neo_timing()
        try:
            neo_results["$trim"] = list(
                neo_collection.aggregate(
                    [{"$project": {"trimmed": {"$trim": {"input": "$name"}}}}]
                )
            )
        except Exception as e:
            neo_results["$trim"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $split
        start_neo_timing()
        try:
            neo_results["$split"] = list(
                neo_collection.aggregate(
                    [{"$project": {"parts": {"$split": ["$city", " "]}}}]
                )
            )
        except Exception as e:
            neo_results["$split"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $replaceAll
        start_neo_timing()
        try:
            neo_results["$replaceAll"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceAll": {
                                        "input": "$city",
                                        "find": " ",
                                        "replacement": "-",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$replaceAll"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $ltrim
        start_neo_timing()
        try:
            neo_results["$ltrim"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "left_trimmed": {"$ltrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$ltrim"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $rtrim
        start_neo_timing()
        try:
            neo_results["$rtrim"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "right_trimmed": {"$rtrim": {"input": "$name"}}
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$rtrim"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $strLenCP
        start_neo_timing()
        try:
            neo_results["$strLenCP"] = list(
                neo_collection.aggregate(
                    [{"$project": {"name_len": {"$strLenCP": "$name"}}}]
                )
            )
        except Exception as e:
            neo_results["$strLenCP"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $regexFind
        start_neo_timing()
        try:
            neo_results["$regexFind"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "match": {
                                    "$regexFind": {
                                        "input": "$name",
                                        "regex": "alice",
                                        "options": "i",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$regexFind"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $regexMatch
        start_neo_timing()
        try:
            neo_results["$regexMatch"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "match_i": {
                                    "$regexMatch": {
                                        "input": "$name",
                                        "regex": "ALICE",
                                        "options": "i",
                                    }
                                },
                                "match_m": {
                                    "$regexMatch": {
                                        "input": "$text",
                                        "regex": "^Line 2",
                                        "options": "m",
                                    }
                                },
                                "match_s": {
                                    "$regexMatch": {
                                        "input": "$text",
                                        "regex": "Line 1.Line 2",
                                        "options": "s",
                                    }
                                },
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$regexMatch"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $replaceOne
        start_neo_timing()
        try:
            neo_results["$replaceOne"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceOne": {
                                        "input": "$city",
                                        "find": " ",
                                        "replacement": "-",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$replaceOne"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $indexOfCP
        start_neo_timing()
        try:
            neo_results["$indexOfCP"] = list(
                neo_collection.aggregate(
                    [{"$project": {"index": {"$indexOfCP": ["$name", "ice"]}}}]
                )
            )
        except Exception as e:
            neo_results["$indexOfCP"] = f"Error: {e}"
        finally:
            end_neo_timing()

        # $regexFindAll
        start_neo_timing()
        try:
            neo_results["$regexFindAll"] = list(
                neo_collection.aggregate(
                    [
                        {
                            "$project": {
                                "matches": {
                                    "$regexFindAll": {
                                        "input": "$name",
                                        "regex": "O",
                                        "options": "i",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        except Exception as e:
            neo_results["$regexFindAll"] = f"Error: {e}"
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_many(sanitize_for_mongodb(test_data))

            set_accumulation_mode(True)

            # $substr
            start_mongo_timing()
            try:
                mongo_results["$substr"] = list(
                    mongo_collection.aggregate(
                        [{"$project": {"short": {"$substr": ["$name", 2, 3]}}}]
                    )
                )
            except Exception as e:
                mongo_results["$substr"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $trim
            start_mongo_timing()
            try:
                mongo_results["$trim"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "trimmed": {"$trim": {"input": "$name"}}
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$trim"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $split
            start_mongo_timing()
            try:
                mongo_results["$split"] = list(
                    mongo_collection.aggregate(
                        [{"$project": {"parts": {"$split": ["$city", " "]}}}]
                    )
                )
            except Exception as e:
                mongo_results["$split"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $replaceAll
            start_mongo_timing()
            try:
                mongo_results["$replaceAll"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "replaced": {
                                        "$replaceAll": {
                                            "input": "$city",
                                            "find": " ",
                                            "replacement": "-",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$replaceAll"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $ltrim
            start_mongo_timing()
            try:
                mongo_results["$ltrim"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "left_trimmed": {
                                        "$ltrim": {"input": "$name"}
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$ltrim"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $rtrim
            start_mongo_timing()
            try:
                mongo_results["$rtrim"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "right_trimmed": {
                                        "$rtrim": {"input": "$name"}
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$rtrim"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $strLenCP
            start_mongo_timing()
            try:
                mongo_results["$strLenCP"] = list(
                    mongo_collection.aggregate(
                        [{"$project": {"name_len": {"$strLenCP": "$name"}}}]
                    )
                )
            except Exception as e:
                mongo_results["$strLenCP"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $regexFind
            start_mongo_timing()
            try:
                mongo_results["$regexFind"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "match": {
                                        "$regexFind": {
                                            "input": "$name",
                                            "regex": "alice",
                                            "options": "i",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$regexFind"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $regexMatch
            start_mongo_timing()
            try:
                mongo_results["$regexMatch"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "match_i": {
                                        "$regexMatch": {
                                            "input": "$name",
                                            "regex": "ALICE",
                                            "options": "i",
                                        }
                                    },
                                    "match_m": {
                                        "$regexMatch": {
                                            "input": "$text",
                                            "regex": "^Line 2",
                                            "options": "m",
                                        }
                                    },
                                    "match_s": {
                                        "$regexMatch": {
                                            "input": "$text",
                                            "regex": "Line 1.Line 2",
                                            "options": "s",
                                        }
                                    },
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$regexMatch"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $replaceOne
            start_mongo_timing()
            try:
                mongo_results["$replaceOne"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "replaced": {
                                        "$replaceOne": {
                                            "input": "$city",
                                            "find": " ",
                                            "replacement": "-",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$replaceOne"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $indexOfCP
            start_mongo_timing()
            try:
                mongo_results["$indexOfCP"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "index": {"$indexOfCP": ["$name", "ice"]}
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$indexOfCP"] = f"Error: {e}"
            finally:
                end_mongo_timing()

            # $regexFindAll
            start_mongo_timing()
            try:
                mongo_results["$regexFindAll"] = list(
                    mongo_collection.aggregate(
                        [
                            {
                                "$project": {
                                    "matches": {
                                        "$regexFindAll": {
                                            "input": "$name",
                                            "regex": "O",
                                            "options": "i",
                                        }
                                    }
                                }
                            }
                        ]
                    )
                )
            except Exception as e:
                mongo_results["$regexFindAll"] = f"Error: {e}"
            finally:
                end_mongo_timing()

        finally:
            client.close()

    # Record comparisons
    for op in operators:
        neo_res = neo_results[op]
        mongo_res = mongo_results[op]

        reporter.record_comparison(
            "String Operators",
            op,
            neo_res if not isinstance(neo_res, str) else "FAIL",
            (
                mongo_res
                if mongo_res is not None and not isinstance(mongo_res, str)
                else None
            ),
            skip_reason="MongoDB not available" if not client else None,
        )
        neo_ok = not isinstance(neo_res, str)
        mongo_ok = client and not isinstance(mongo_res, str)

        def normalize_docs(docs):
            """Normalize documents for comparison: sort keys, convert ObjectId to str."""
            if not isinstance(docs, list):
                docs = [docs]
            result = []
            for doc in docs:
                if isinstance(doc, dict):
                    normalized = {}
                    for k, v in sorted(doc.items(), key=lambda x: str(x[0])):
                        if hasattr(v, "__str__") and "ObjectId" in str(type(v)):
                            normalized[k] = str(v)
                        elif isinstance(v, dict):
                            normalized[k] = normalize_docs([v])[0]
                        elif isinstance(v, list):
                            normalized[k] = normalize_docs(v)
                        else:
                            normalized[k] = v
                    result.append(normalized)
                else:
                    result.append(doc)
            return result

        neo_normalized = normalize_docs(neo_res)
        print(f"Neo {op}: {'OK' if neo_ok else 'FAIL'} {neo_normalized}")
        if client:
            mongo_normalized = normalize_docs(mongo_res)
            print(
                f"Mongo {op}: {'OK' if mongo_ok else 'FAIL'} {mongo_normalized}"
            )
            if neo_ok and mongo_ok and neo_normalized != mongo_normalized:
                print("  >>> MISMATCH!")
