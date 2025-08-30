#!/usr/bin/env python3
"""
Performance demonstration for $unwind + $group SQL optimization
"""
import neosqlite
import time


def main():
    print("$unwind + $group SQL Optimization Performance Demo")
    print("=" * 50)

    # Create a connection and collection
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["demo_collection"]

        # Insert a large number of documents with arrays
        print("Inserting test data...")
        docs = []
        for i in range(1000):
            # Each document has 10 tags
            tags = [
                f"tag{j % 50}" for j in range(10)
            ]  # Reuse tags to create groups
            docs.append(
                {
                    "id": i,
                    "category": f"category_{i % 10}",  # 10 categories
                    "tags": tags,
                }
            )

        collection.insert_many(docs)
        print(f"Inserted {len(docs)} documents with arrays")

        # Test the enhanced SQL-based $unwind + $group implementation
        print("\nTesting enhanced SQL-based $unwind + $group implementation...")
        start_time = time.time()
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = list(collection.aggregate(pipeline))
        sql_time = time.time() - start_time

        print(
            f"SQL-based approach processed {len(result)} groups in {sql_time:.4f}s"
        )

        # Show some results
        print(f"\nSample results (first 5 groups):")
        for i, doc in enumerate(result[:5]):
            print(f"  {doc['_id']}: {doc['count']}")

        # Test with $match filter
        print("\nTesting SQL-based $match + $unwind + $group combination...")
        start_time = time.time()
        pipeline = [
            {"$match": {"id": {"$lt": 500}}},  # First half of documents
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        ]
        result = list(collection.aggregate(pipeline))
        combined_time = time.time() - start_time

        print(
            f"Combined approach processed {len(result)} groups in {combined_time:.4f}s"
        )

        # Show some results
        print(f"\nSample results (first 5 groups):")
        for i, doc in enumerate(result[:5]):
            print(f"  {doc['_id']}: {doc['count']}")

        print("\nPerformance Benefits:")
        print("- All processing happens at database level")
        print("- No intermediate Python data structures")
        print("- Single SQL query instead of multiple operations")
        print("- Reduced memory footprint")
        print("- Faster execution using SQLite's C implementation")

        print("\nEnhanced Features:")
        print("- Supports $unwind + $group optimization")
        print("- Works with $match as first stage")
        print("- Handles grouping by unwound field or other fields")
        print("- Supports both $sum and $count accumulators")
        print("- Maintains backward compatibility")


if __name__ == "__main__":
    main()
