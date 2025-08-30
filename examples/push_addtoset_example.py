#!/usr/bin/env python3
"""
Example demonstrating the new $push and $addToSet accumulators in NeoSQLite.

This example shows how to use the $push and $addToSet accumulators in aggregation pipelines
for building arrays with all values (including duplicates) or unique values only.
"""

import neosqlite


def main():
    print("=== NeoSQLite $push and $addToSet Accumulator Examples ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        products = conn.products

        # Insert sample data
        print("1. Inserting sample product data...")
        sample_docs = [
            {
                "category": "Electronics",
                "name": "Laptop",
                "tags": ["computer", "portable", "work"],
                "price": 1200,
            },
            {
                "category": "Electronics",
                "name": "Smartphone",
                "tags": ["mobile", "communication", "portable"],
                "price": 800,
            },
            {
                "category": "Books",
                "name": "Python Guide",
                "tags": ["programming", "education", "computer"],
                "price": 30,
            },
            {
                "category": "Books",
                "name": "Cookbook",
                "tags": ["cooking", "recipes", "food"],
                "price": 25,
            },
            {
                "category": "Electronics",
                "name": "Tablet",
                "tags": ["portable", "entertainment", "computer"],
                "price": 500,
            },
        ]

        products.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} products\n")

        # Example 1: Using $push to collect all values (including duplicates)
        print(
            "2. Example: Using $push to collect all product names by category"
        )
        print("   Pipeline: Group by category and collect all product names")
        pipeline1 = [
            {
                "$group": {
                    "_id": "$category",
                    "productNames": {"$push": "$name"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result1_cursor = products.aggregate(pipeline1)
        print("   Results:")
        for doc in result1_cursor:
            print(f"     {doc['_id']}: {doc.get('productNames', [])}")
        print()

        # Example 2: Using $addToSet to collect unique values only
        print(
            "3. Example: Using $addToSet to collect unique prices by category"
        )
        print("   Pipeline: Group by category and collect unique prices")
        pipeline2 = [
            {
                "$group": {
                    "_id": "$category",
                    "uniquePrices": {"$addToSet": "$price"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result2_cursor = products.aggregate(pipeline2)
        print("   Results:")
        for doc in result2_cursor:
            # Sort the prices for consistent output
            sorted_prices = sorted(doc.get("uniquePrices", []))
            print(f"     {doc['_id']}: {sorted_prices}")
        print()

        print("=== Key Benefits ===")
        print("• $push preserves all values including duplicates")
        print("• $addToSet automatically removes duplicates")
        print("• Both are optimized at the SQL level using json_group_array()")
        print("• Work seamlessly with other accumulator functions")
        print("• Maintain full backward compatibility")
        print(
            "• Provide significant performance improvements over Python-based processing"
        )

        print("\n=== SQL Implementation Details ===")
        print("• $push uses: json_group_array(json_extract(data, '$.field'))")
        print(
            "• $addToSet uses: json_group_array(DISTINCT json_extract(data, '$.field'))"
        )
        print("• Both execute at the database level for maximum efficiency")
        print("• Results are automatically parsed back to Python lists")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
