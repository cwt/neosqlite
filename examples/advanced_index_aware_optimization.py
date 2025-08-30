#!/usr/bin/env python3
"""
Example demonstrating the Advanced Index-Aware Optimization feature in NeoSQLite.

This example shows how NeoSQLite leverages existing indexes to optimize query performance
by estimating query costs and selecting optimal execution paths.
"""

import neosqlite
import time


def main():
    print("=== NeoSQLite Advanced Index-Aware Optimization Example ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        products = conn.products

        # Insert sample data
        print("1. Inserting sample product data...")
        sample_docs = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        statuses = ["active", "inactive", "discontinued"]

        # Insert 5000 documents for meaningful performance differences
        for i in range(5000):
            sample_docs.append(
                {
                    "_id": i + 1,
                    "name": f"Product {i + 1}",
                    "category": categories[i % len(categories)],
                    "status": statuses[i % len(statuses)],
                    "price": float(10 + (i % 100)),
                    "tags": [f"tag{j}" for j in range(3)],  # 3 tags per product
                    "brand": f"Brand {(i // 100) % 10}",
                }
            )

        products.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} products\n")

        # Create indexes on frequently queried fields
        print("2. Creating indexes on frequently queried fields...")
        products.create_index("category")
        products.create_index("status")
        products.create_index("price")
        products.create_index("brand")
        print("   Created indexes on: category, status, price, brand\n")

        # Show available indexes
        print("3. Available indexes:")
        indexes = products.list_indexes()
        for idx in indexes:
            print(f"   - {idx}")
        print()

        # Example 1: Simple query with indexed field
        print("4. Example: Query with indexed field")
        print("   Pipeline: Find active products in Electronics category")

        pipeline1 = [
            {"$match": {"category": "Electronics", "status": "active"}},
            {"$limit": 10},
        ]

        # Measure performance
        start_time = time.time()
        result1 = list(products.aggregate(pipeline1))
        elapsed_time1 = time.time() - start_time

        print(f"   Found {len(result1)} products")
        print(f"   Executed in {elapsed_time1:.6f} seconds\n")

        # Example 2: Complex pipeline with multiple indexed fields
        print("5. Example: Complex pipeline with multiple indexed fields")
        print(
            "   Pipeline: Find active products, unwind tags, sort by price, limit to 20"
        )

        pipeline2 = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"price": -1}},  # Sort by price descending
            {"$limit": 20},
        ]

        # Measure performance
        start_time = time.time()
        result2 = list(products.aggregate(pipeline2))
        elapsed_time2 = time.time() - start_time

        print(f"   Found {len(result2)} products/tags combinations")
        print(
            f"   Most expensive: ${result2[0]['price'] if result2 else 'N/A'}"
        )
        print(f"   Executed in {elapsed_time2:.6f} seconds\n")

        # Example 3: Nested array operations with indexed fields
        print("6. Example: Nested array operations with indexed fields")
        print("   Pipeline: Find products by brand, unwind tags, group by tag")

        pipeline3 = [
            {"$match": {"brand": "Brand 5"}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]

        # Measure performance
        start_time = time.time()
        result3 = list(products.aggregate(pipeline3))
        elapsed_time3 = time.time() - start_time

        print(f"   Found {len(result3)} unique tags")
        if result3:
            print(
                f"   Most common tag: {result3[0]['_id']} (appears {result3[0]['count']} times)"
            )
        print(f"   Executed in {elapsed_time3:.6f} seconds\n")

        # Example 4: Demonstrating cost estimation (internal feature)
        print("7. Example: Query cost estimation (internal feature)")
        query_helper = products.query_engine.helpers

        # Estimate cost for queries with and without indexes
        simple_query = {"category": "Electronics"}
        complex_query = {
            "category": "Electronics",
            "status": "active",
            "price": {"$gt": 50},
        }

        cost_simple = query_helper._estimate_query_cost(simple_query)
        cost_complex = query_helper._estimate_query_cost(complex_query)

        print(
            f"   Cost estimate for simple query {simple_query}: {cost_simple}"
        )
        print(
            f"   Cost estimate for complex query {complex_query}: {cost_complex}"
        )

        # Show indexed fields
        indexed_fields = query_helper._get_indexed_fields()
        print(f"   Currently indexed fields: {indexed_fields}\n")

        # Example 5: Performance comparison with and without indexes
        print("8. Example: Performance impact of indexes")

        # Create a collection without indexes for comparison
        unindexed_products = conn.unindexed_products
        unindexed_products.insert_many(sample_docs)  # Same data, no indexes

        # Query with indexed collection
        start_time = time.time()
        indexed_result = list(
            products.find({"category": "Electronics", "status": "active"})
        )
        indexed_time = time.time() - start_time

        # Query with unindexed collection
        start_time = time.time()
        unindexed_result = list(
            unindexed_products.find(
                {"category": "Electronics", "status": "active"}
            )
        )
        unindexed_time = time.time() - start_time

        print(
            f"   Indexed collection: {len(indexed_result)} results in {indexed_time:.6f} seconds"
        )
        print(
            f"   Unindexed collection: {len(unindexed_result)} results in {unindexed_time:.6f} seconds"
        )
        if unindexed_time > 0:
            speedup = unindexed_time / indexed_time
            print(
                f"   Performance improvement: {speedup:.2f}x faster with indexes\n"
            )

        print("=== Key Benefits of Index-Aware Optimization ===")
        print("• Automatic detection of indexed fields for query optimization")
        print("• Cost estimation to select optimal execution paths")
        print("• Seamless integration with existing aggregation pipelines")
        print("• Significant performance improvements for indexed queries")
        print("• No changes required to existing code - works automatically")
        print()

        print("=== How It Works ===")
        print("1. NeoSQLite automatically detects which fields have indexes")
        print("2. Query cost estimation evaluates different execution paths")
        print("3. Optimized SQL queries are generated when possible")
        print("4. Complex queries fall back to Python processing when needed")
        print(
            "5. Index information is used to reorder operations for better performance"
        )
        print()

        print("=== Best Practices ===")
        print("• Create indexes on frequently queried fields")
        print("• Use compound indexes for multi-field queries")
        print("• Monitor query performance with and without indexes")
        print(
            "• Consider the trade-off between index storage and query performance"
        )
        print("• Regularly review index usage to remove unused indexes")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
