"""Module for comparing additional aggregation pipeline stages between NeoSQLite and PyMongo"""

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


def compare_additional_aggregation_stages():
    """Compare additional aggregation pipeline stages"""
    print("\n=== Additional Aggregation Pipeline Stages Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        start_neo_timing()
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "category": "books"},
                {"item": "B", "price": 20, "quantity": 1, "category": "books"},
                {"item": "C", "price": 15, "quantity": 3, "category": "toys"},
                {"item": "D", "price": 25, "quantity": 2, "category": "toys"},
                {"item": "E", "price": 30, "quantity": 1, "category": "games"},
            ]
        )

        # Test $sample
        try:
            # We only compare length because results are random
            result = list(neo_collection.aggregate([{"$sample": {"size": 2}}]))
            neo_sample = len(result)
            print(f"Neo $sample: {neo_sample} documents")
        except Exception as e:
            neo_sample = f"Error: {e}"
            print(f"Neo $sample: Error - {e}")

        # Test $facet
        try:
            neo_facet = list(
                neo_collection.aggregate(
                    [
                        {
                            "$facet": {
                                "by_category": [
                                    {
                                        "$group": {
                                            "_id": "$category",
                                            "count": {"$count": {}},
                                        }
                                    },
                                    {
                                        "$sort": {"_id": 1}
                                    },  # Sort for consistent comparison
                                ],
                                "avg_price": [
                                    {
                                        "$group": {
                                            "_id": None,
                                            "avg": {"$avg": "$price"},
                                        }
                                    }
                                ],
                            }
                        }
                    ]
                )
            )
            print("Neo $facet: OK")
        except Exception as e:
            neo_facet = f"Error: {e}"
            print(f"Neo $facet: Error - {e}")

        # Test $lookup
        neo_collection2 = neo_conn.orders
        neo_collection2.insert_many(
            [
                {"order_id": 1, "item": "A", "qty": 2},
                {"order_id": 2, "item": "B", "qty": 1},
                {"order_id": 3, "item": "C", "qty": 3},
            ]
        )

        try:
            # We strip _id and internal id for lookup comparison due to complex nesting
            neo_lookup_raw = list(
                neo_collection.aggregate(
                    [
                        {
                            "$lookup": {
                                "from": "orders",
                                "localField": "item",
                                "foreignField": "item",
                                "as": "orders",
                            }
                        },
                        {"$sort": {"item": 1}},
                    ]
                )
            )

            def clean_lookup(docs):
                for doc in docs:
                    doc.pop("_id", None)
                    doc.pop("id", None)
                    if "orders" in doc:
                        for o in doc["orders"]:
                            o.pop("_id", None)
                            o.pop("id", None)
                return docs

            neo_lookup = clean_lookup(neo_lookup_raw)
            print("Neo $lookup: OK")
        except Exception as e:
            neo_lookup = f"Error: {e}"
            print(f"Neo $lookup: Error - {e}")

        # Test $bucket
        try:
            neo_bucket = list(
                neo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$price",
                                "boundaries": [0, 15, 25, 100],
                                "default": "other",
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            print("Neo $bucket: OK")
        except Exception as e:
            neo_bucket = f"Error: {e}"
            print(f"Neo $bucket: Error - {e}")

        # Test $bucketAuto
        try:
            neo_bucketauto = list(
                neo_collection.aggregate(
                    [{"$bucketAuto": {"groupBy": "$price", "buckets": 2}}]
                )
            )
            print("Neo $bucketAuto: OK")
        except Exception as e:
            neo_bucketauto = f"Error: {e}"
            print(f"Neo $bucketAuto: Error - {e}")

        # Test $unionWith
        neo_conn.other_coll.insert_one({"item": "Other", "price": 50})
        try:
            neo_unionwith_raw = list(
                neo_collection.aggregate(
                    [
                        {"$unionWith": {"coll": "other_coll"}},
                        {"$sort": {"item": 1}},
                    ]
                )
            )
            for d in neo_unionwith_raw:
                d.pop("_id", None)
                d.pop("id", None)
            neo_unionwith = neo_unionwith_raw
            print("Neo $unionWith: OK")
        except Exception as e:
            neo_unionwith = f"Error: {e}"
            print(f"Neo $unionWith: Error - {e}")

        # Test $redact
        try:
            neo_redact_raw = list(
                neo_collection.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": {
                                    "if": {"$eq": ["$category", "books"]},
                                    "then": "$$KEEP",
                                    "else": "$$PRUNE",
                                }
                            }
                        },
                        {"$sort": {"item": 1}},
                    ]
                )
            )
            for d in neo_redact_raw:
                d.pop("_id", None)
                d.pop("id", None)
            neo_redact = neo_redact_raw
            print("Neo $redact: OK")
        except Exception as e:
            neo_redact = f"Error: {e}"
            print(f"Neo $redact: Error - {e}")

        # Test $densify
        neo_conn.densify_coll.insert_many([{"t": 1}, {"t": 3}])
        try:
            # NeoSQLite includes upper bound, Mongo does not. Use bounds that align.
            neo_densify_raw = list(
                neo_conn.densify_coll.aggregate(
                    [
                        {
                            "$densify": {
                                "field": "t",
                                "range": {
                                    "step": 1,
                                    "bounds": [1, 3],
                                },  # Match Mongo [1, 3) behavior
                            }
                        }
                    ]
                )
            )
            for d in neo_densify_raw:
                d.pop("_id", None)
                d.pop("id", None)
            neo_densify = neo_densify_raw
            print("Neo $densify: OK")
        except Exception as e:
            neo_densify = f"Error: {e}"
            print(f"Neo $densify: Error - {e}")

        # Test $merge
        try:
            # We use a file-based database for $merge to ensure results are readable across connections
            merge_db_path = "test_merge.db"
            import os

            if os.path.exists(merge_db_path):
                os.remove(merge_db_path)

            with neosqlite.Connection(merge_db_path) as merge_conn:
                m_coll = merge_conn.test_collection
                m_coll.insert_many(
                    [
                        {
                            "item": "A",
                            "price": 10,
                            "quantity": 2,
                            "category": "books",
                        },
                        {
                            "item": "B",
                            "price": 20,
                            "quantity": 1,
                            "category": "books",
                        },
                        {
                            "item": "C",
                            "price": 15,
                            "quantity": 3,
                            "category": "toys",
                        },
                    ]
                )

                target_name = "merged_results"
                # Run merge
                list(
                    m_coll.aggregate(
                        [
                            {"$match": {"category": "books"}},
                            {"$merge": {"into": target_name}},
                        ]
                    )
                )

                # Verify the target collection
                target_coll = merge_conn[target_name]
                neo_merge_raw = list(target_coll.find({}))
                for d in neo_merge_raw:
                    d.pop("_id", None)
                    d.pop("id", None)
                neo_merge = sorted(neo_merge_raw, key=lambda x: x["item"])

            if os.path.exists(merge_db_path):
                os.remove(merge_db_path)
            print("Neo $merge: OK")
        except Exception as e:
            neo_merge = f"Error: {e}"
            print(f"Neo $merge: Error - {e}")

        end_neo_timing()

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None
    mongo_db = None
    mongo_facet = None
    mongo_lookup = None
    mongo_sample = None

    mongo_bucket = None
    mongo_bucketauto = None
    mongo_unionwith = None
    mongo_redact = None
    mongo_densify = None
    mongo_merge = None

    if client:
        start_mongo_timing()
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"item": "A", "price": 10, "quantity": 2, "category": "books"},
                {"item": "B", "price": 20, "quantity": 1, "category": "books"},
                {"item": "C", "price": 15, "quantity": 3, "category": "toys"},
                {"item": "D", "price": 25, "quantity": 2, "category": "toys"},
                {"item": "E", "price": 30, "quantity": 1, "category": "games"},
            ]
        )

        # Test $sample
        try:
            result = list(
                mongo_collection.aggregate([{"$sample": {"size": 2}}])
            )
            mongo_sample = len(result)
            print(f"Mongo $sample: {mongo_sample} documents")
        except Exception as e:
            mongo_sample = f"Error: {e}"
            print(f"Mongo $sample: Error - {e}")

        # Test $facet
        try:
            mongo_facet = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$facet": {
                                "by_category": [
                                    {
                                        "$group": {
                                            "_id": "$category",
                                            "count": {"$count": {}},
                                        }
                                    },
                                    {"$sort": {"_id": 1}},
                                ],
                                "avg_price": [
                                    {
                                        "$group": {
                                            "_id": None,
                                            "avg": {"$avg": "$price"},
                                        }
                                    }
                                ],
                            }
                        }
                    ]
                )
            )
            print("Mongo $facet: OK")
        except Exception as e:
            mongo_facet = f"Error: {e}"
            print(f"Mongo $facet: Error - {e}")

        # Test $lookup
        mongo_collection2 = mongo_db.orders
        mongo_collection2.delete_many({})
        mongo_collection2.insert_many(
            [
                {"order_id": 1, "item": "A", "qty": 2},
                {"order_id": 2, "item": "B", "qty": 1},
                {"order_id": 3, "item": "C", "qty": 3},
            ]
        )

        try:
            mongo_lookup_raw = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$lookup": {
                                "from": "orders",
                                "localField": "item",
                                "foreignField": "item",
                                "as": "orders",
                            }
                        },
                        {"$sort": {"item": 1}},
                    ]
                )
            )

            def clean_mongo_lookup(docs):
                for doc in docs:
                    doc.pop("_id", None)
                    if "orders" in doc:
                        for o in doc["orders"]:
                            o.pop("_id", None)
                return docs

            mongo_lookup = clean_mongo_lookup(mongo_lookup_raw)
            print("Mongo $lookup: OK")
        except Exception as e:
            mongo_lookup = f"Error: {e}"
            print(f"Mongo $lookup: Error - {e}")

        # Test $bucket
        try:
            mongo_bucket = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": "$price",
                                "boundaries": [0, 15, 25, 100],
                                "default": "other",
                                "output": {"count": {"$sum": 1}},
                            }
                        }
                    ]
                )
            )
            print("Mongo $bucket: OK")
        except Exception as e:
            mongo_bucket = f"Error: {e}"
            print(f"Mongo $bucket: Error - {e}")

        # Test $bucketAuto
        try:
            mongo_bucketauto = list(
                mongo_collection.aggregate(
                    [{"$bucketAuto": {"groupBy": "$price", "buckets": 2}}]
                )
            )
            print("Mongo $bucketAuto: OK")
        except Exception as e:
            mongo_bucketauto = f"Error: {e}"
            print(f"Mongo $bucketAuto: Error - {e}")

        # Test $unionWith
        mongo_db.other_coll.delete_many({})
        mongo_db.other_coll.insert_one({"item": "Other", "price": 50})
        try:
            mongo_unionwith_raw = list(
                mongo_collection.aggregate(
                    [
                        {"$unionWith": {"coll": "other_coll"}},
                        {"$sort": {"item": 1}},
                    ]
                )
            )
            for d in mongo_unionwith_raw:
                d.pop("_id", None)
            mongo_unionwith = mongo_unionwith_raw
            print("Mongo $unionWith: OK")
        except Exception as e:
            mongo_unionwith = f"Error: {e}"
            print(f"Mongo $unionWith: Error - {e}")

        # Test $redact
        try:
            mongo_redact_raw = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$redact": {
                                "$cond": {
                                    "if": {"$eq": ["$category", "books"]},
                                    "then": "$$KEEP",
                                    "else": "$$PRUNE",
                                }
                            }
                        },
                        {"$sort": {"item": 1}},
                    ]
                )
            )
            for d in mongo_redact_raw:
                d.pop("_id", None)
            mongo_redact = mongo_redact_raw
            print("Mongo $redact: OK")
        except Exception as e:
            mongo_redact = f"Error: {e}"
            print(f"Mongo $redact: Error - {e}")

        # Test $densify
        mongo_db.densify_coll.delete_many({})
        mongo_db.densify_coll.insert_many([{"t": 1}, {"t": 3}])
        try:
            mongo_densify_raw = list(
                mongo_db.densify_coll.aggregate(
                    [
                        {
                            "$densify": {
                                "field": "t",
                                "range": {"step": 1, "bounds": [1, 3]},
                            }
                        }
                    ]
                )
            )
            for d in mongo_densify_raw:
                d.pop("_id", None)
            mongo_densify = mongo_densify_raw
            print("Mongo $densify: OK")
        except Exception as e:
            mongo_densify = f"Error: {e}"
            print(f"Mongo $densify: Error - {e}")

        # Test $merge
        try:
            target_name = "merged_results"
            mongo_db[target_name].delete_many({})
            mongo_collection.aggregate(
                [
                    {"$match": {"category": "books"}},
                    {"$merge": {"into": target_name}},
                ]
            )
            mongo_merge_raw = list(mongo_db[target_name].find({}))
            for d in mongo_merge_raw:
                d.pop("_id", None)
            mongo_merge = sorted(mongo_merge_raw, key=lambda x: x["item"])
            print("Mongo $merge: OK")
        except Exception as e:
            mongo_merge = f"Error: {e}"
            print(f"Mongo $merge: Error - {e}")

        end_mongo_timing()
        client.close()

    reporter.record_comparison(
        "Aggregation Stages",
        "$sample",
        neo_sample,
        mongo_sample,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$facet",
        neo_facet,
        mongo_facet,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$lookup",
        neo_lookup,
        mongo_lookup,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$bucket",
        neo_bucket,
        mongo_bucket,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$bucketAuto",
        neo_bucketauto,
        mongo_bucketauto,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$unionWith",
        neo_unionwith,
        mongo_unionwith,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$redact",
        neo_redact,
        mongo_redact,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$densify",
        neo_densify,
        mongo_densify,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Aggregation Stages",
        "$merge",
        neo_merge,
        mongo_merge,
        skip_reason="MongoDB not available" if not client else None,
    )
