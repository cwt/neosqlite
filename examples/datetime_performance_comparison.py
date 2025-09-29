#!/usr/bin/env python3
"""
Performance Comparison: NeoSQLite vs MongoDB DateTime Queries

This script compares the performance of datetime queries between NeoSQLite and MongoDB.
"""

import time
from neosqlite import Connection
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure


def performance_comparison():
    """Compare performance between NeoSQLite and MongoDB."""

    print("NeoSQLite vs MongoDB DateTime Query Performance Comparison")
    print("=" * 58)
    print()

    # Set up NeoSQLite with larger dataset
    print("Setting up NeoSQLite with test data...")
    neosqlite_db = Connection(":memory:")
    neosqlite_collection = neosqlite_db["performance_test"]

    # Insert larger dataset for performance testing
    test_docs = []
    for i in range(1000):
        # Create documents with different timestamps
        timestamp = f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"
        test_docs.append(
            {
                "id": i,
                "timestamp": timestamp,
                "value": i * 1.5,
                "category": f"category_{i % 10}",
            }
        )

    # Batch insert for NeoSQLite
    for doc in test_docs:
        neosqlite_collection.insert_one(doc)

    print(f"Inserted {len(test_docs)} documents into NeoSQLite")

    # Set up MongoDB if available
    mongodb_available = False
    try:
        mongodb_client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=2000
        )
        mongodb_client.admin.command("ping")
        mongodb_db = mongodb_client["performance_test"]
        mongodb_collection = mongodb_db["performance_test"]

        # Clear and insert data (using simplified docs for MongoDB)
        mongodb_docs = []
        for doc in test_docs:
            # Create MongoDB-compatible documents (avoid ObjectId issues)
            mongo_doc = {
                "id": doc["id"],
                "timestamp": doc["timestamp"],
                "value": doc["value"],
                "category": doc["category"],
            }
            mongodb_docs.append(mongo_doc)

        mongodb_collection.delete_many({})
        mongodb_collection.insert_many(mongodb_docs)
        print(f"Inserted {len(mongodb_docs)} documents into MongoDB")
        mongodb_available = True

    except (ServerSelectionTimeoutError, ConnectionFailure) as e:
        print(f"MongoDB not available: {e}")
        mongodb_client = None
        mongodb_collection = None

    print("\nRunning performance tests...")
    print("=" * 30)

    # Test queries
    test_queries = [
        (
            "Simple Range Query",
            {
                "timestamp": {
                    "$gte": "2023-06-01T00:00:00",
                    "$lt": "2023-09-01T00:00:00",
                }
            },
        ),
        (
            "Complex AND Query",
            {
                "$and": [
                    {"timestamp": {"$gte": "2023-03-01T00:00:00"}},
                    {"timestamp": {"$lt": "2023-12-01T00:00:00"}},
                    {
                        "category": {
                            "$in": ["category_1", "category_2", "category_3"]
                        }
                    },
                ]
            },
        ),
        (
            "OR Query",
            {
                "$or": [
                    {"timestamp": {"$lt": "2023-02-01T00:00:00"}},
                    {"timestamp": {"$gte": "2023-11-01T00:00:00"}},
                ]
            },
        ),
    ]

    results = []

    for test_name, query in test_queries:
        print(f"\n{test_name}:")
        print("-" * len(test_name))

        # Test NeoSQLite
        start_time = time.time()
        neosqlite_results = list(neosqlite_collection.find(query))
        neosqlite_time = time.time() - start_time
        print(
            f"  NeoSQLite: {len(neosqlite_results)} results in {neosqlite_time*1000:.2f}ms"
        )

        # Test MongoDB if available
        if mongodb_available:
            start_time = time.time()
            mongodb_results = list(mongodb_collection.find(query))
            mongodb_time = time.time() - start_time
            print(
                f"  MongoDB:   {len(mongodb_results)} results in {mongodb_time*1000:.2f}ms"
            )

            # Verify results match
            if len(neosqlite_results) == len(mongodb_results):
                print("  🟢 Results match: ✓")
            else:
                print(
                    f"  🔴 Results differ: NeoSQLite={len(neosqlite_results)}, MongoDB={len(mongodb_results)}"
                )

            results.append(
                (
                    test_name,
                    neosqlite_time,
                    mongodb_time,
                    len(neosqlite_results),
                )
            )
        else:
            results.append(
                (test_name, neosqlite_time, None, len(neosqlite_results))
            )

    # Print summary
    print("\nPerformance Summary:")
    print("=" * 20)
    print(f"{'Test':<25} {'NeoSQLite':<12} {'MongoDB':<12} {'Speed Ratio':<15}")
    print("-" * 65)

    for test_name, neo_time, mongo_time, result_count in results:
        if mongo_time:
            ratio = neo_time / mongo_time if mongo_time > 0 else float("inf")
            print(
                f"{test_name:<25} {neo_time*1000:<11.2f}ms {mongo_time*1000:<11.2f}ms {ratio:<14.2f}x"
            )
        else:
            print(
                f"{test_name:<25} {neo_time*1000:<11.2f}ms {'N/A':<12} {'N/A':<15}"
            )

    # Cleanup
    neosqlite_db.close()
    if mongodb_client:
        mongodb_client.close()

    print("\nPerformance comparison completed!")


if __name__ == "__main__":
    performance_comparison()
