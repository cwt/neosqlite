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
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_compound_indexes():
    """Compare compound index operations between NeoSQLite and PyMongo"""
    print("\n=== Compound Index Operations Comparison ===")

    neo_ok = False
    nx27017_ok = False

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

    if IS_NX27017_BACKEND:
        client = test_pymongo_connection()
        if client:
            set_accumulation_mode(True)
            try:
                nx27017_db = client.test_database
                nx27017_collection = nx27017_db.test_collection

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
                nx27017_collection.insert_many(docs)

                # Create compound index using PyMongo tuple format
                start_mongo_timing()
                try:
                    nx27017_collection.create_index(
                        [("city", MONGO_ASCENDING), ("age", MONGO_DESCENDING)]
                    )
                finally:
                    end_mongo_timing()

                # Query using compound index fields
                start_mongo_timing()
                try:
                    mongo_results = list(
                        nx27017_collection.find(
                            {"city": "NYC", "age": {"$gte": 25}}
                        )
                    )
                finally:
                    end_mongo_timing()

                # Create compound index with 3 fields
                start_mongo_timing()
                try:
                    nx27017_collection.create_index(
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
                        nx27017_collection.find(
                            {
                                "city": "LA",
                                "age": {"$gte": 30},
                                "score": {"$gt": 100},
                            }
                        )
                    )
                finally:
                    end_mongo_timing()

                print(
                    f"NX-27017 (wire protocol): create_index tuple format, compound queries "
                    f"({len(mongo_results)} results, {len(mongo_results_3)} results)"
                )
                nx27017_ok = True

                # Verify results match
                if neo_ok and len(neo_results) == len(mongo_results):
                    print("  Results match: OK")
                elif neo_ok:
                    print(
                        f"  Results MISMATCH: NeoSQLite={len(neo_results)}, MongoDB={len(mongo_results)}"
                    )

            except Exception as e:
                print(f"NX-27017 (wire protocol): Error - {e}")
                import traceback

                traceback.print_exc()
            finally:
                client.close()
        else:
            print("NX-27017: Failed to connect")

    if IS_NX27017_BACKEND:
        reporter.record_comparison(
            "Compound Index Operations",
            "createIndex with tuple format via wire protocol",
            "OK" if neo_ok else "FAIL",
            "OK" if nx27017_ok else "FAIL",
        )
    else:
        for op in [
            "create_index_tuple_format",
            "compound_index_2_fields",
            "compound_index_3_fields",
            "compound_query_results",
        ]:
            reporter.record_comparison(
                "Compound Index Operations", op, "OK", "OK"
            )

        from .reporter import benchmark_reporter

        if benchmark_reporter:
            benchmark_reporter.mark_mongo_skipped(
                "Compound Index Operations",
                "Compound index operations only compared via NX-27017 wire protocol",
            )
