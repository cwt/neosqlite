#!/usr/bin/env python3
"""
Direct Comparison Test: NeoSQLite vs MongoDB DateTime Queries

This script runs identical datetime queries on both NeoSQLite and MongoDB
to verify complete compatibility and compare results.
"""

import time
from neosqlite import Connection
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure


def setup_mongodb():
    """Set up MongoDB connection and test data."""
    try:
        # Connect to MongoDB
        client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        # Test the connection
        client.admin.command("ping")
        db = client["datetime_test"]
        collection = db["events"]

        # Clear existing data
        collection.delete_many({})

        # Insert test data (same as NeoSQLite test)
        test_data = [
            {
                "name": "User Login",
                "timestamp": "2023-01-15T08:30:00",
                "user_id": "user_001",
                "event_type": "login",
            },
            {
                "name": "Purchase Order",
                "timestamp": "2023-01-15T14:45:30",
                "user_id": "user_002",
                "event_type": "purchase",
                "amount": 99.99,
            },
            {
                "name": "User Logout",
                "timestamp": "2023-01-15T18:20:15",
                "user_id": "user_001",
                "event_type": "logout",
            },
            {
                "name": "System Maintenance",
                "timestamp": "2023-01-16T02:00:00",
                "event_type": "maintenance",
            },
            {
                "name": "Weekly Report",
                "timestamp": "2023-01-22T09:00:00",
                "event_type": "report",
            },
        ]

        # Insert documents
        for doc in test_data:
            collection.insert_one(doc)

        return client, collection
    except (ServerSelectionTimeoutError, ConnectionFailure) as e:
        print(f"⚠️  MongoDB connection failed: {e}")
        return None, None


def setup_neosqlite():
    """Set up NeoSQLite connection and test data."""
    # Connect to in-memory database
    db = Connection(":memory:")
    collection = db["events"]

    # Insert test data (same as MongoDB test)
    test_data = [
        {
            "name": "User Login",
            "timestamp": "2023-01-15T08:30:00",
            "user_id": "user_001",
            "event_type": "login",
        },
        {
            "name": "Purchase Order",
            "timestamp": "2023-01-15T14:45:30",
            "user_id": "user_002",
            "event_type": "purchase",
            "amount": 99.99,
        },
        {
            "name": "User Logout",
            "timestamp": "2023-01-15T18:20:15",
            "user_id": "user_001",
            "event_type": "logout",
        },
        {
            "name": "System Maintenance",
            "timestamp": "2023-01-16T02:00:00",
            "event_type": "maintenance",
        },
        {
            "name": "Weekly Report",
            "timestamp": "2023-01-22T09:00:00",
            "event_type": "report",
        },
    ]

    # Insert documents
    for doc in test_data:
        collection.insert_one(doc)

    return db, collection


def run_datetime_queries(client, collection, is_mongodb=False):
    """Run the same datetime queries on both databases."""

    print(
        f"Running datetime queries on {'MongoDB' if is_mongodb else 'NeoSQLite'}:"
    )
    print("=" * 50)

    # Test 1: Simple datetime comparison
    print("1. Events after Jan 15, 2023 12:00 PM:")
    query1 = {"timestamp": {"$gt": "2023-01-15T12:00:00"}}
    start_time = time.time()

    if is_mongodb:
        results1 = list(collection.find(query1))
    else:
        results1 = list(collection.find(query1))

    end_time = time.time()
    print(
        f"   Found {len(results1)} documents in {(end_time - start_time)*1000:.2f}ms"
    )
    for doc in results1:
        print(f"   - {doc['name']}: {doc['timestamp']}")
    print()

    # Test 2: Range query
    print("2. Events between Jan 15 and Jan 16:")
    query2 = {
        "timestamp": {
            "$gte": "2023-01-15T00:00:00",
            "$lt": "2023-01-16T00:00:00",
        }
    }
    start_time = time.time()

    if is_mongodb:
        results2 = list(collection.find(query2))
    else:
        results2 = list(collection.find(query2))

    end_time = time.time()
    print(
        f"   Found {len(results2)} documents in {(end_time - start_time)*1000:.2f}ms"
    )
    for doc in results2:
        print(f"   - {doc['name']}: {doc['timestamp']}")
    print()

    # Test 3: Multiple conditions
    print("3. Events with user_001 after Jan 15, 2023 10:00 AM:")
    query3 = {
        "user_id": "user_001",
        "timestamp": {"$gt": "2023-01-15T10:00:00"},
    }
    start_time = time.time()

    if is_mongodb:
        results3 = list(collection.find(query3))
    else:
        results3 = list(collection.find(query3))

    end_time = time.time()
    print(
        f"   Found {len(results3)} documents in {(end_time - start_time)*1000:.2f}ms"
    )
    for doc in results3:
        print(f"   - {doc['name']}: {doc['timestamp']}")
    print()

    # Test 4: Complex nested query
    print("4. Events in January 2023 with specific types:")
    query4 = {
        "$and": [
            {"timestamp": {"$gte": "2023-01-01T00:00:00"}},
            {"timestamp": {"$lt": "2023-02-01T00:00:00"}},
            {"$or": [{"event_type": "login"}, {"event_type": "purchase"}]},
        ]
    }
    start_time = time.time()

    if is_mongodb:
        results4 = list(collection.find(query4))
    else:
        results4 = list(collection.find(query4))

    end_time = time.time()
    print(
        f"   Found {len(results4)} documents in {(end_time - start_time)*1000:.2f}ms"
    )
    for doc in results4:
        print(f"   - {doc['name']}: {doc['timestamp']} ({doc['event_type']})")
    print()

    return [
        (len(results1), [(doc["name"], doc["timestamp"]) for doc in results1]),
        (len(results2), [(doc["name"], doc["timestamp"]) for doc in results2]),
        (len(results3), [(doc["name"], doc["timestamp"]) for doc in results3]),
        (len(results4), [(doc["name"], doc["timestamp"]) for doc in results4]),
    ]


def compare_results(neosqlite_results, mongodb_results):
    """Compare results from both databases."""
    print("Comparison Results:")
    print("=" * 20)

    if len(neosqlite_results) != len(mongodb_results):
        print("❌ Different number of test cases!")
        return False

    all_match = True
    for i, (neo_result, mongo_result) in enumerate(
        zip(neosqlite_results, mongodb_results)
    ):
        neo_count, neo_docs = neo_result
        mongo_count, mongo_docs = mongo_result

        if neo_count != mongo_count:
            print(
                f"❌ Test case {i+1}: Different document counts ({neo_count} vs {mongo_count})"
            )
            all_match = False
        else:
            # Sort documents for comparison (since order might differ)
            neo_sorted = sorted(neo_docs, key=lambda x: x[0])
            mongo_sorted = sorted(mongo_docs, key=lambda x: x[0])

            if neo_sorted != mongo_sorted:
                print(f"❌ Test case {i+1}: Different document contents")
                print(f"   NeoSQLite: {neo_sorted}")
                print(f"   MongoDB:   {mongo_sorted}")
                all_match = False
            else:
                print(f"✅ Test case {i+1}: Match ({neo_count} documents)")

    return all_match


def main():
    """Main comparison function."""
    print("NeoSQLite vs MongoDB DateTime Query Compatibility Test")
    print("=" * 55)
    print()

    # Set up both databases
    print("Setting up NeoSQLite...")
    neosqlite_db, neosqlite_collection = setup_neosqlite()

    print("Setting up MongoDB...")
    mongodb_client, mongodb_collection = setup_mongodb()

    if mongodb_client is None:
        print("Skipping MongoDB tests due to connection failure.")
        print()
        # Still run NeoSQLite tests
        print("Running NeoSQLite datetime queries only:")
        neosqlite_results = run_datetime_queries(
            neosqlite_db, neosqlite_collection, is_mongodb=False
        )
        neosqlite_db.close()
        return

    # Run queries on both databases
    neosqlite_results = run_datetime_queries(
        neosqlite_db, neosqlite_collection, is_mongodb=False
    )
    mongodb_results = run_datetime_queries(
        mongodb_client, mongodb_collection, is_mongodb=True
    )

    # Compare results
    print("Comparing results...")
    match = compare_results(neosqlite_results, mongodb_results)

    if match:
        print("\n🎉 SUCCESS: All datetime queries produced identical results!")
        print("   NeoSQLite is fully compatible with MongoDB datetime queries.")
    else:
        print("\n❌ FAILURE: Some datetime queries produced different results.")
        print("   There may be compatibility issues.")

    # Cleanup
    neosqlite_db.close()
    mongodb_client.close()

    print("\nCompatibility test completed.")


if __name__ == "__main__":
    main()
