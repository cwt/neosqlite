#!/usr/bin/env python3
"""
Debug script to investigate the mismatch in the $unwind + $group with multiple accumulators test
"""

import neosqlite


def debug_unwind_group_mismatch():
    """Debug the mismatch in $unwind + $group with multiple accumulators"""
    print(
        "=== Debugging $unwind + $group with multiple accumulators mismatch ===\n"
    )

    with neosqlite.Connection(":memory:") as conn:
        # Create collection with smaller dataset for easier debugging
        products = conn["products"]

        # Insert small test dataset
        test_docs = [
            {
                "_id": 1,
                "name": "Product 1",
                "category": "Electronics",
                "price": 100.0,
                "tags": ["tag1", "tag2"],
            },
            {
                "_id": 2,
                "name": "Product 2",
                "category": "Electronics",
                "price": 200.0,
                "tags": ["tag2", "tag3"],
            },
            {
                "_id": 3,
                "name": "Product 3",
                "category": "Books",
                "price": 150.0,
                "tags": ["tag1", "tag3"],
            },
        ]
        products.insert_many(test_docs)
        print("Inserted test data:")
        for doc in test_docs:
            print(f"  {doc}")
        print()

        # Test pipeline
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "count": {"$sum": 1},
                    "avgPrice": {"$avg": "$price"},
                    "minPrice": {"$min": "$price"},
                    "maxPrice": {"$max": "$price"},
                }
            },
        ]

        # Test optimized path
        print("--- Optimized Path Results ---")
        neosqlite.collection.query_helper.set_force_fallback(False)
        result_optimized = products.aggregate(pipeline)
        for i, doc in enumerate(result_optimized):
            print(f"  {i+1}. {doc}")
        print(f"  Total count: {len(result_optimized)}")
        print()

        # Test fallback path
        print("--- Fallback Path Results ---")
        neosqlite.collection.query_helper.set_force_fallback(True)
        result_fallback = products.aggregate(pipeline)
        for i, doc in enumerate(result_fallback):
            print(f"  {i+1}. {doc}")
        print(f"  Total count: {len(result_fallback)}")
        print()

        # Compare results in detail
        print("--- Detailed Comparison ---")
        print(f"Same count: {len(result_optimized) == len(result_fallback)}")

        if len(result_optimized) == len(result_fallback):
            # Sort both results for comparison
            result_optimized.sort(key=lambda x: x["_id"])
            result_fallback.sort(key=lambda x: x["_id"])

            print("\nDocument-by-document comparison:")
            for i, (opt_doc, fall_doc) in enumerate(
                zip(result_optimized, result_fallback)
            ):
                print(f"  Document {i+1}:")
                print(f"    Optimized: {opt_doc}")
                print(f"    Fallback:  {fall_doc}")
                print(f"    Match: {opt_doc == fall_doc}")
                print()

        # Reset
        neosqlite.collection.query_helper.set_force_fallback(False)


if __name__ == "__main__":
    debug_unwind_group_mismatch()
