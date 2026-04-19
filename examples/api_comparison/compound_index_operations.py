"""Module for comparing compound index operations between NeoSQLite and PyMongo"""

import os
import warnings

from pymongo import (
    ASCENDING as MONGO_ASCENDING,
)
from pymongo import (
    DESCENDING as MONGO_DESCENDING,
)

import neosqlite
from neosqlite import ASCENDING, DESCENDING

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_compound_indexes():
    """Compare compound index operations between NeoSQLite and PyMongo"""
    print("\n=== Compound Index Operations Comparison ===")

    neo_ok = False
    mongo_ok = False
    neo_results = []
    neo_results_3 = []

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)
        neo_collection = neo_conn.test_collection

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"user_{i}",
                    "age": 20 + (i % 30),
                    "city": ["NYC", "LA", "Chicago", "Boston", "Seattle"][
                        i % 5
                    ],
                    "score": i * 1.5,
                }
            )
        neo_collection.insert_many(docs)

        try:
            # Create compound index using PyMongo tuple format
            start_neo_timing()
            try:
                neo_collection.create_index(
                    [("city", ASCENDING), ("age", DESCENDING)]
                )
            finally:
                end_neo_timing()

            # Query using compound index fields
            start_neo_timing()
            try:
                neo_results = list(
                    neo_collection.find({"city": "NYC", "age": {"$gte": 25}})
                )
            finally:
                end_neo_timing()

            # Create compound index with 3 fields
            start_neo_timing()
            try:
                neo_collection.create_index(
                    [
                        ("city", ASCENDING),
                        ("age", DESCENDING),
                        ("score", ASCENDING),
                    ]
                )
            finally:
                end_neo_timing()

            # Query using 3-field compound index
            start_neo_timing()
            try:
                neo_results_3 = list(
                    neo_collection.find(
                        {
                            "city": "LA",
                            "age": {"$gte": 30},
                            "score": {"$gt": 100},
                        }
                    )
                )
            finally:
                end_neo_timing()

            print(
                f"NeoSQLite (direct): create_index tuple format, compound queries "
                f"({len(neo_results)} results, {len(neo_results_3)} results)"
            )
            neo_ok = True
        except Exception as e:
            print(f"NeoSQLite (direct): Error - {e}")
            import traceback

            traceback.print_exc()

    # Test MongoDB via PyMongo (works with both real MongoDB and NX-27017)
    client = get_mongo_client()
    if client:
        set_accumulation_mode(True)
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection

            # Insert same test data
            docs = []
            for i in range(100):
                docs.append(
                    {
                        "name": f"user_{i}",
                        "age": 20 + (i % 30),
                        "city": [
                            "NYC",
                            "LA",
                            "Chicago",
                            "Boston",
                            "Seattle",
                        ][i % 5],
                        "score": i * 1.5,
                    }
                )
            mongo_collection.insert_many(docs)

            # Create compound index using PyMongo tuple format
            start_mongo_timing()
            try:
                mongo_collection.create_index(
                    [("city", MONGO_ASCENDING), ("age", MONGO_DESCENDING)]
                )
            finally:
                end_mongo_timing()

            # Query using compound index fields
            start_mongo_timing()
            try:
                mongo_results = list(
                    mongo_collection.find({"city": "NYC", "age": {"$gte": 25}})
                )
            finally:
                end_mongo_timing()

            # Create compound index with 3 fields
            start_mongo_timing()
            try:
                mongo_collection.create_index(
                    [
                        ("city", MONGO_ASCENDING),
                        ("age", MONGO_DESCENDING),
                        ("score", MONGO_ASCENDING),
                    ]
                )
            finally:
                end_mongo_timing()

            # Query using 3-field compound index
            start_mongo_timing()
            try:
                mongo_results_3 = list(
                    mongo_collection.find(
                        {
                            "city": "LA",
                            "age": {"$gte": 30},
                            "score": {"$gt": 100},
                        }
                    )
                )
            finally:
                end_mongo_timing()

            backend_name = "NX-27017" if IS_NX27017_BACKEND else "MongoDB"
            print(
                f"{backend_name} (PyMongo): create_index tuple format, compound queries "
                f"({len(mongo_results)} results, {len(mongo_results_3)} results)"
            )
            mongo_ok = True

            # Verify results match
            if neo_ok and len(neo_results) == len(mongo_results):
                print("  Results match: OK")
            elif neo_ok:
                print(
                    f"  Results MISMATCH: NeoSQLite={len(neo_results)}, MongoDB={len(mongo_results)}"
                )

        except Exception as e:
            backend_name = "NX-27017" if IS_NX27017_BACKEND else "MongoDB"
            print(f"{backend_name} (PyMongo): Error - {e}")
            import traceback

            traceback.print_exc()
    else:
        print("MongoDB: Failed to connect")

    reporter.record_comparison(
        "Compound Index Operations",
        "createIndex with tuple format via PyMongo",
        "OK" if neo_ok else "FAIL",
        "OK" if mongo_ok else "FAIL",
    )
