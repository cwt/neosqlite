"""Module for comparing additional aggregation pipeline stages between NeoSQLite and PyMongo"""

import operator
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
from .utils import sanitize_for_mongodb, test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_aggregation_stages():
    """Compare additional aggregation pipeline stages"""
    print("\n=== Additional Aggregation Pipeline Stages Comparison ===")

    neo_results = {}
    mongo_results = {}

    def clean_docs(docs, sub_fields=None):
        if not isinstance(docs, list):
            return docs
        cleaned = []
        for doc in docs:
            if not isinstance(doc, dict):
                cleaned.append(doc)
                continue
            d = doc.copy()
            d.pop("_id", None)
            d.pop("id", None)
            if sub_fields:
                for field in sub_fields:
                    if field in d and isinstance(d[field], list):
                        new_sub = []
                        for sub_doc in d[field]:
                            if isinstance(sub_doc, dict):
                                sd = sub_doc.copy()
                                sd.pop("_id", None)
                                sd.pop("id", None)
                                new_sub.append(sd)
                            else:
                                new_sub.append(sub_doc)
                        d[field] = new_sub
            cleaned.append(d)
        return cleaned

    data = [
        {"item": "A", "price": 10, "quantity": 2, "category": "books"},
        {"item": "B", "price": 20, "quantity": 1, "category": "books"},
        {"item": "C", "price": 15, "quantity": 3, "category": "toys"},
        {"item": "D", "price": 25, "quantity": 2, "category": "toys"},
        {"item": "E", "price": 30, "quantity": 1, "category": "games"},
    ]

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many(data)

        set_accumulation_mode(True)

        # $sample
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate([{"$sample": {"size": 2}}]))
            neo_results["sample"] = result
            print("Neo sample: OK")
        except Exception as e:
            neo_results["sample"] = f"Error: {e}"
            print(f"Neo sample: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["sample"], list):
            neo_results["sample"] = len(neo_results["sample"])

        # $facet
        facet_pipeline = [
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
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(facet_pipeline))
            neo_results["facet"] = result
            print("Neo facet: OK")
        except Exception as e:
            neo_results["facet"] = f"Error: {e}"
            print(f"Neo facet: Error - {e}")
        finally:
            end_neo_timing()

        # $lookup
        neo_conn.orders.insert_many(
            [
                {"order_id": 1, "item": "A", "qty": 2},
                {"order_id": 2, "item": "B", "qty": 1},
                {"order_id": 3, "item": "C", "qty": 3},
            ]
        )
        lookup_pipeline = [
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
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(lookup_pipeline))
            neo_results["lookup"] = result
            print("Neo lookup: OK")
        except Exception as e:
            neo_results["lookup"] = f"Error: {e}"
            print(f"Neo lookup: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["lookup"], list):
            neo_results["lookup"] = clean_docs(
                neo_results["lookup"], ["orders"]
            )

        # $lookup with pipeline
        lookup_p_pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "item",
                    "foreignField": "item",
                    "pipeline": [
                        {"$match": {"qty": {"$gte": 2}}},
                        {"$sort": {"order_id": 1}},
                    ],
                    "as": "matched_orders",
                }
            },
            {"$sort": {"item": 1}},
        ]
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(lookup_p_pipeline))
            neo_results["lookup_pipeline"] = result
            print("Neo lookup_pipeline: OK")
        except Exception as e:
            neo_results["lookup_pipeline"] = f"Error: {e}"
            print(f"Neo lookup_pipeline: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["lookup_pipeline"], list):
            neo_results["lookup_pipeline"] = clean_docs(
                neo_results["lookup_pipeline"], ["matched_orders"]
            )

        # $bucket
        bucket_pipeline = [
            {
                "$bucket": {
                    "groupBy": "$price",
                    "boundaries": [0, 15, 25, 100],
                    "default": "other",
                    "output": {"count": {"$sum": 1}},
                }
            }
        ]
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(bucket_pipeline))
            neo_results["bucket"] = result
            print("Neo bucket: OK")
        except Exception as e:
            neo_results["bucket"] = f"Error: {e}"
            print(f"Neo bucket: Error - {e}")
        finally:
            end_neo_timing()

        # $bucketAuto
        bucket_auto_pipeline = [
            {"$bucketAuto": {"groupBy": "$price", "buckets": 2}}
        ]
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(bucket_auto_pipeline))
            neo_results["bucketauto"] = result
            print("Neo bucketauto: OK")
        except Exception as e:
            neo_results["bucketauto"] = f"Error: {e}"
            print(f"Neo bucketauto: Error - {e}")
        finally:
            end_neo_timing()

        # $unionWith
        neo_conn.other_coll.insert_one({"item": "Other", "price": 50})
        union_pipeline = [
            {"$unionWith": {"coll": "other_coll"}},
            {"$sort": {"item": 1}},
        ]
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(union_pipeline))
            neo_results["unionwith"] = result
            print("Neo unionwith: OK")
        except Exception as e:
            neo_results["unionwith"] = f"Error: {e}"
            print(f"Neo unionwith: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["unionwith"], list):
            neo_results["unionwith"] = clean_docs(neo_results["unionwith"])

        # $redact
        redact_pipeline = [
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
        start_neo_timing()
        try:
            result = list(neo_collection.aggregate(redact_pipeline))
            neo_results["redact"] = result
            print("Neo redact: OK")
        except Exception as e:
            neo_results["redact"] = f"Error: {e}"
            print(f"Neo redact: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["redact"], list):
            neo_results["redact"] = clean_docs(neo_results["redact"])

        # $densify
        neo_conn.densify_coll.insert_many([{"t": 1}, {"t": 3}])
        densify_pipeline = [
            {
                "$densify": {
                    "field": "t",
                    "range": {"step": 1, "bounds": [1, 3]},
                }
            }
        ]
        start_neo_timing()
        try:
            result = list(neo_conn.densify_coll.aggregate(densify_pipeline))
            neo_results["densify"] = result
            print("Neo densify: OK")
        except Exception as e:
            neo_results["densify"] = f"Error: {e}"
            print(f"Neo densify: Error - {e}")
        finally:
            end_neo_timing()
        if isinstance(neo_results["densify"], list):
            neo_results["densify"] = clean_docs(neo_results["densify"])

        # $merge
        merge_db_path = "test_merge.db"

        def cleanup_merge_db():
            for f in [
                merge_db_path,
                f"{merge_db_path}-wal",
                f"{merge_db_path}-shm",
            ]:
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except OSError:
                        pass

        cleanup_merge_db()
        try:
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

                start_neo_timing()
                try:
                    result = list(
                        m_coll.aggregate(
                            [
                                {"$match": {"category": "books"}},
                                {"$merge": {"into": target_name}},
                            ]
                        )
                    )
                    neo_results["merge"] = result
                    print("Neo merge: OK")
                except Exception as e:
                    neo_results["merge"] = f"Error: {e}"
                    print(f"Neo merge: Error - {e}")
                finally:
                    end_neo_timing()

                if not isinstance(neo_results["merge"], str):
                    target_coll = merge_conn[target_name]
                    neo_merge_raw = list(target_coll.find({}))
                    neo_results["merge"] = sorted(
                        clean_docs(neo_merge_raw),
                        key=operator.itemgetter("item"),
                    )
        finally:
            cleanup_merge_db()

    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_many(sanitize_for_mongodb(data))
            set_accumulation_mode(True)

            # $sample
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate([{"$sample": {"size": 2}}])
                )
                mongo_results["sample"] = result
                print("Mongo sample: OK")
            except Exception as e:
                mongo_results["sample"] = f"Error: {e}"
                print(f"Mongo sample: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["sample"], list):
                mongo_results["sample"] = len(mongo_results["sample"])

            # $facet
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(facet_pipeline))
                mongo_results["facet"] = result
                print("Mongo facet: OK")
            except Exception as e:
                mongo_results["facet"] = f"Error: {e}"
                print(f"Mongo facet: Error - {e}")
            finally:
                end_mongo_timing()

            # $lookup
            mongo_db.orders.delete_many({})
            mongo_db.orders.insert_many(
                [
                    {"order_id": 1, "item": "A", "qty": 2},
                    {"order_id": 2, "item": "B", "qty": 1},
                    {"order_id": 3, "item": "C", "qty": 3},
                ]
            )
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(lookup_pipeline))
                mongo_results["lookup"] = result
                print("Mongo lookup: OK")
            except Exception as e:
                mongo_results["lookup"] = f"Error: {e}"
                print(f"Mongo lookup: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["lookup"], list):
                mongo_results["lookup"] = clean_docs(
                    mongo_results["lookup"], ["orders"]
                )

            # $lookup with pipeline
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(lookup_p_pipeline))
                mongo_results["lookup_pipeline"] = result
                print("Mongo lookup_pipeline: OK")
            except Exception as e:
                mongo_results["lookup_pipeline"] = f"Error: {e}"
                print(f"Mongo lookup_pipeline: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["lookup_pipeline"], list):
                mongo_results["lookup_pipeline"] = clean_docs(
                    mongo_results["lookup_pipeline"], ["matched_orders"]
                )

            # $bucket
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(bucket_pipeline))
                mongo_results["bucket"] = result
                print("Mongo bucket: OK")
            except Exception as e:
                mongo_results["bucket"] = f"Error: {e}"
                print(f"Mongo bucket: Error - {e}")
            finally:
                end_mongo_timing()

            # $bucketAuto
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(bucket_auto_pipeline))
                mongo_results["bucketauto"] = result
                print("Mongo bucketauto: OK")
            except Exception as e:
                mongo_results["bucketauto"] = f"Error: {e}"
                print(f"Mongo bucketauto: Error - {e}")
            finally:
                end_mongo_timing()

            # $unionWith
            mongo_db.other_coll.delete_many({})
            mongo_db.other_coll.insert_one({"item": "Other", "price": 50})
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(union_pipeline))
                mongo_results["unionwith"] = result
                print("Mongo unionwith: OK")
            except Exception as e:
                mongo_results["unionwith"] = f"Error: {e}"
                print(f"Mongo unionwith: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["unionwith"], list):
                mongo_results["unionwith"] = clean_docs(
                    mongo_results["unionwith"]
                )

            # $redact
            start_mongo_timing()
            try:
                result = list(mongo_collection.aggregate(redact_pipeline))
                mongo_results["redact"] = result
                print("Mongo redact: OK")
            except Exception as e:
                mongo_results["redact"] = f"Error: {e}"
                print(f"Mongo redact: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["redact"], list):
                mongo_results["redact"] = clean_docs(mongo_results["redact"])

            # $densify
            mongo_db.densify_coll.delete_many({})
            mongo_db.densify_coll.insert_many([{"t": 1}, {"t": 3}])
            start_mongo_timing()
            try:
                result = list(mongo_db.densify_coll.aggregate(densify_pipeline))
                mongo_results["densify"] = result
                print("Mongo densify: OK")
            except Exception as e:
                mongo_results["densify"] = f"Error: {e}"
                print(f"Mongo densify: Error - {e}")
            finally:
                end_mongo_timing()
            if isinstance(mongo_results["densify"], list):
                mongo_results["densify"] = clean_docs(mongo_results["densify"])

            # $merge
            target_name = "merged_results"
            mongo_db[target_name].delete_many({})
            start_mongo_timing()
            try:
                result = list(
                    mongo_collection.aggregate(
                        [
                            {"$match": {"category": "books"}},
                            {"$merge": {"into": target_name}},
                        ]
                    )
                )
                mongo_results["merge"] = result
                print("Mongo merge: OK")
            except Exception as e:
                mongo_results["merge"] = f"Error: {e}"
                print(f"Mongo merge: Error - {e}")
            finally:
                end_mongo_timing()

            if not isinstance(mongo_results["merge"], str):
                mongo_merge_raw = list(mongo_db[target_name].find({}))
                mongo_results["merge"] = sorted(
                    clean_docs(mongo_merge_raw), key=operator.itemgetter("item")
                )

        finally:
            client.close()

    # Final reporting
    stages = [
        ("$sample", "sample"),
        ("$facet", "facet"),
        ("$lookup", "lookup"),
        ("$lookup with pipeline", "lookup_pipeline"),
        ("$bucket", "bucket"),
        ("$bucketAuto", "bucketauto"),
        ("$unionWith", "unionwith"),
        ("$redact", "redact"),
        ("$densify", "densify"),
        ("$merge", "merge"),
    ]

    for stage_label, key in stages:
        reporter.record_comparison(
            "Aggregation (Complex Stages)",
            stage_label,
            neo_results.get(key),
            mongo_results.get(key),
            skip_reason="MongoDB not available" if not client else None,
        )
