#!/usr/bin/env python3
"""
Performance comparison for enhanced multiple $unwind stages
"""
import neosqlite
import time


def main():
    print("Multiple $unwind Stages Performance Comparison")
    print("=" * 48)

    # Create a connection and collection
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["demo_collection"]

        # Insert a large number of documents with multiple arrays
        print("Inserting test data...")
        docs = []
        for i in range(1000):
            docs.append(
                {
                    "id": i,
                    "tags": [f"tag{j}" for j in range(5)],  # 5 tags
                    "categories": [f"cat{k}" for k in range(3)],  # 3 categories
                    "levels": [f"level{l}" for l in range(2)],  # 2 levels
                }
            )

        collection.insert_many(docs)
        print(f"Inserted {len(docs)} documents with multiple arrays")

        # Test the enhanced SQL-based multiple $unwind implementation
        print("\nTesting enhanced SQL-based multiple $unwind implementation...")
        start_time = time.time()
        pipeline = [
            {"$unwind": "$tags"},
            {"$unwind": "$categories"},
            {"$unwind": "$levels"},
        ]
        result = collection.aggregate(pipeline)
        sql_time = time.time() - start_time

        print(
            f"SQL-based approach processed {len(result)} documents in {sql_time:.4f}s"
        )

        # Show what the result looks like
        print(f"\nResult structure example:")
        if result:
            print(f"  First document: {result[0]}")
            print(f"  Second document: {result[1]}")

        # Test with $match filter
        print("\nTesting SQL-based $match + multiple $unwind combination...")
        start_time = time.time()
        pipeline = [
            {"$match": {"id": {"$lt": 500}}},  # First half of users
            {"$unwind": "$tags"},
            {"$unwind": "$categories"},
            {"$unwind": "$levels"},
        ]
        result = collection.aggregate(pipeline)
        combined_time = time.time() - start_time

        print(
            f"Combined approach processed {len(result)} documents in {combined_time:.4f}s"
        )

        print("\nPerformance Benefits:")
        print("- All processing happens at database level")
        print("- No intermediate Python data structures")
        print("- Chained json_each() for efficient Cartesian products")
        print("- Reduced memory footprint")

        print("\nEnhanced Features:")
        print("- Supports multiple consecutive $unwind stages")
        print("- Works with $match as first stage")
        print("- Handles 2, 3, or more $unwind stages")
        print("- Maintains backward compatibility")


if __name__ == "__main__":
    main()
