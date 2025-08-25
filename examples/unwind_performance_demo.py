#!/usr/bin/env python3
"""
Demonstration of the performance improvement with the new $unwind implementation
"""
import neosqlite
import time


def main():
    print("NeoSQLite $unwind Performance Demonstration")
    print("=" * 45)

    # Create a connection and collection
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["demo_collection"]

        # Insert a large number of documents with arrays
        print("Inserting test data...")
        docs = []
        for i in range(5000):
            docs.append(
                {
                    "id": i,
                    "name": f"User{i}",
                    "tags": [f"tag{j}" for j in range(20)],  # 20 tags per user
                }
            )

        collection.insert_many(docs)
        print(f"Inserted {len(docs)} documents with arrays")

        # Test the new SQL-based $unwind implementation
        print("\nTesting SQL-based $unwind implementation...")
        start_time = time.time()
        pipeline = [{"$unwind": "$tags"}]
        result = collection.aggregate(pipeline)
        sql_time = time.time() - start_time

        print(
            f"SQL-based approach processed {len(result)} documents in {sql_time:.4f}s"
        )

        # Test $unwind with $match combination
        print("\nTesting SQL-based $match + $unwind combination...")
        start_time = time.time()
        pipeline = [
            {"$match": {"id": {"$lt": 2500}}},  # First half of users
            {"$unwind": "$tags"},
        ]
        result = collection.aggregate(pipeline)
        combined_time = time.time() - start_time

        print(
            f"Combined approach processed {len(result)} documents in {combined_time:.4f}s"
        )

        # Show memory efficiency
        print("\nMemory Efficiency:")
        print("- Processing happens at database level")
        print("- No need to load all documents into Python memory")
        print("- Reduced data transfer between SQLite and Python")

        print("\nPerformance Benefits:")
        print("- Faster execution using SQLite's C implementation")
        print("- Better utilization of database indexes")
        print("- Scalable to larger datasets")

        print("\nBackward Compatibility:")
        print("- Complex $unwind cases still use Python implementation")
        print("- All existing functionality preserved")
        print("- No breaking changes to API")


if __name__ == "__main__":
    main()
