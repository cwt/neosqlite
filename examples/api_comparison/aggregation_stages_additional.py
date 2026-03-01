"""Module for comparing additional aggregation pipeline stages between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_additional_aggregation_stages():
    """Compare additional aggregation pipeline stages"""
    print("\n=== Additional Aggregation Pipeline Stages Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
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
            neo_sample = len(
                list(neo_collection.aggregate([{"$sample": {"size": 2}}]))
            )
            print(f"Neo $sample: {neo_sample} documents")
        except Exception as e:
            neo_sample = f"Error: {e}"
            print(f"Neo $sample: Error - {e}")

        # Test $facet
        try:
            neo_facet_result = list(
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
                                    }
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
            neo_facet = (
                len(neo_facet_result) > 0
                and "by_category" in neo_facet_result[0]
                and "avg_price" in neo_facet_result[0]
            )
            print(f"Neo $facet: {'OK' if neo_facet else 'FAIL'}")
        except Exception as e:
            neo_facet = False
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
            neo_lookup_result = list(
                neo_collection.aggregate(
                    [
                        {
                            "$lookup": {
                                "from": "orders",
                                "localField": "item",
                                "foreignField": "item",
                                "as": "orders",
                            }
                        }
                    ]
                )
            )
            neo_lookup = len(neo_lookup_result) == 5 and all(
                "orders" in doc for doc in neo_lookup_result
            )
            print(f"Neo $lookup: {'OK' if neo_lookup else 'FAIL'}")
        except Exception as e:
            neo_lookup = False
            print(f"Neo $lookup: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_collection = None

    mongo_collection2 = None

    mongo_db = None

    mongo_facet = None

    mongo_facet_result = None

    mongo_lookup = None

    mongo_lookup_result = None

    mongo_sample = None

    if client:
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
            mongo_sample = len(
                list(mongo_collection.aggregate([{"$sample": {"size": 2}}]))
            )
            print(f"Mongo $sample: {mongo_sample} documents")
        except Exception as e:
            mongo_sample = f"Error: {e}"
            print(f"Mongo $sample: Error - {e}")

        # Test $facet
        try:
            mongo_facet_result = list(
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
                                    }
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
            mongo_facet = (
                len(mongo_facet_result) > 0
                and "by_category" in mongo_facet_result[0]
                and "avg_price" in mongo_facet_result[0]
            )
            print(f"Mongo $facet: {'OK' if mongo_facet else 'FAIL'}")
        except Exception as e:
            mongo_facet = False
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
            mongo_lookup_result = list(
                mongo_collection.aggregate(
                    [
                        {
                            "$lookup": {
                                "from": "orders",
                                "localField": "item",
                                "foreignField": "item",
                                "as": "orders",
                            }
                        }
                    ]
                )
            )
            mongo_lookup = len(mongo_lookup_result) == 5 and all(
                "orders" in doc for doc in mongo_lookup_result
            )
            print(f"Mongo $lookup: {'OK' if mongo_lookup else 'FAIL'}")
        except Exception as e:
            mongo_lookup = False
            print(f"Mongo $lookup: Error - {e}")

        client.close()

    reporter.record_result(
        "Aggregation Stages", "$sample", True, neo_sample, mongo_sample
    )
    reporter.record_result(
        "Aggregation Stages", "$facet", neo_facet, neo_facet, mongo_facet
    )
    reporter.record_result(
        "Aggregation Stages", "$lookup", neo_lookup, neo_lookup, mongo_lookup
    )
