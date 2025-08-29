#!/usr/bin/env python3
"""
Example demonstrating pipeline reordering optimization in NeoSQLite.

This example shows how NeoSQLite automatically reorders pipeline stages to improve
performance by moving indexed $match operations to the beginning of pipelines.
"""

import neosqlite
import time


def main():
    print("=== NeoSQLite Pipeline Reordering Optimization Example ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        products = conn.products

        # Insert a larger dataset to make performance differences more apparent
        print("1. Inserting larger sample product data...")
        sample_docs = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        statuses = ["active", "inactive", "discontinued"]

        # Insert 5000 documents for more meaningful performance differences
        for i in range(5000):
            # Each product has 10 tags to make unwind operations more expensive
            sample_docs.append(
                {
                    "_id": i + 1,
                    "name": f"Product {i + 1}",
                    "category": categories[i % len(categories)],
                    "status": statuses[i % len(statuses)],
                    "price": float(10 + (i % 200)),
                    "tags": [
                        f"tag{j}_{i}" for j in range(10)
                    ],  # 10 unique tags per product
                    "brand": f"Brand {(i // 100) % 10}",
                }
            )

        products.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} products (each with 10 tags)\n")

        # Create indexes on frequently queried fields
        print("2. Creating indexes on frequently queried fields...")
        products.create_index("category")
        products.create_index("status")
        products.create_index("price")
        print("   Created indexes on: category, status, price\n")

        # Example 1: Show the optimization in action by manually comparing pipelines
        print("3. Example: Demonstrating pipeline reordering optimization")
        print(
            "   Comparing two equivalent pipelines with different stage orders:"
        )

        # Pipeline 1: Inefficient order (unwind first, then match)
        pipeline1 = [
            {
                "$unwind": "$tags"
            },  # Expensive - unwinds all 50,000 tag documents
            {
                "$match": {"category": "Electronics", "status": "active"}
            },  # Filter after unwind
            {"$sort": {"price": -1}},
            {"$limit": 15},
        ]

        # Pipeline 2: Efficient order (match first, then unwind)
        pipeline2 = [
            {
                "$match": {"category": "Electronics", "status": "active"}
            },  # Filter first using indexes
            {"$unwind": "$tags"},  # Now only unwinds matching documents
            {"$sort": {"price": -1}},
            {"$limit": 15},
        ]

        print("\n   Pipeline 1 (inefficient): $unwind → $match")
        print("   - First unwinds 50,000 tag documents from all products")
        print("   - Then filters results to only Electronics & active products")

        print("\n   Pipeline 2 (efficient): $match → $unwind")
        print(
            "   - First filters to ~1,000 Electronics & active products (using indexes)"
        )
        print(
            "   - Then unwinds only 10,000 tag documents from matching products"
        )
        print("   - Processes 5x fewer documents!")

        # Run both pipelines to show they produce the same results
        result1 = products.aggregate(pipeline1)
        result2 = products.aggregate(pipeline2)

        print(f"\n   Results comparison:")
        print(f"   Pipeline 1 found: {len(result1)} documents")
        print(f"   Pipeline 2 found: {len(result2)} documents")
        # Compare just the essential data since _id fields will differ
        if len(result1) == len(result2) and len(result1) > 0:
            # Compare a few key fields from the first result
            same_category = result1[0].get("category") == result2[0].get(
                "category"
            )
            same_status = result1[0].get("status") == result2[0].get("status")
            same_tag = result1[0].get("tags") == result2[0].get("tags")
            print(
                f"   Key fields match: category={same_category}, status={same_status}, tag={same_tag}"
            )
        else:
            print(f"   Results count match: {len(result1) == len(result2)}")

        # Example 2: Show internal optimization by examining the reordering logic
        print("\n4. Example: How NeoSQLite automatically optimizes pipelines")
        print("   Original pipeline (user writes this):")
        original_pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"category": "Books", "status": "active"}},
            {"$limit": 10},
        ]

        for i, stage in enumerate(original_pipeline, 1):
            stage_name = next(iter(stage.keys()))
            print(f"     {i}. {stage_name}: {stage[stage_name]}")

        # Show what the query helper would do
        query_helper = products.query_engine.helpers
        optimized_pipeline = query_helper._reorder_pipeline_for_indexes(
            original_pipeline
        )

        print("\n   NeoSQLite automatically optimizes to:")
        for i, stage in enumerate(optimized_pipeline, 1):
            stage_name = next(iter(stage.keys()))
            print(f"     {i}. {stage_name}: {stage[stage_name]}")

        # Show cost estimation
        original_cost = query_helper._estimate_pipeline_cost(original_pipeline)
        optimized_cost = query_helper._estimate_pipeline_cost(
            optimized_pipeline
        )

        print(f"\n   Cost estimation:")
        print(f"   Original pipeline cost: {original_cost:.2f}")
        print(f"   Optimized pipeline cost: {optimized_cost:.2f}")
        if original_cost > 0:
            improvement = (
                (original_cost - optimized_cost) / original_cost
            ) * 100
            print(f"   Cost reduction: {improvement:.1f}%")

        # Example 3: Complex pipeline with multiple matches
        print(
            "\n5. Example: Complex pipeline with multiple optimization opportunities"
        )
        complex_pipeline = [
            {"$unwind": "$tags"},
            {"$sort": {"name": 1}},
            {"$match": {"brand": "Brand 5", "price": {"$gt": 100}}},
            {"$limit": 5},
        ]

        print("   Original complex pipeline:")
        for i, stage in enumerate(complex_pipeline, 1):
            stage_name = next(iter(stage.keys()))
            print(f"     {i}. {stage_name}: {stage[stage_name]}")

        optimized_complex = query_helper._reorder_pipeline_for_indexes(
            complex_pipeline
        )

        print("\n   NeoSQLite optimized pipeline:")
        for i, stage in enumerate(optimized_complex, 1):
            stage_name = next(iter(stage.keys()))
            print(f"     {i}. {stage_name}: {stage[stage_name]}")

        original_complex_cost = query_helper._estimate_pipeline_cost(
            complex_pipeline
        )
        optimized_complex_cost = query_helper._estimate_pipeline_cost(
            optimized_complex
        )

        print(f"\n   Cost estimation:")
        print(f"   Original complex pipeline cost: {original_complex_cost:.2f}")
        print(
            f"   Optimized complex pipeline cost: {optimized_complex_cost:.2f}"
        )
        if original_complex_cost > 0:
            improvement = (
                (original_complex_cost - optimized_complex_cost)
                / original_complex_cost
            ) * 100
            print(f"   Cost reduction: {improvement:.1f}%")

        # Example 4: Show indexed fields detection
        print("\n6. Example: Automatic index detection")
        indexed_fields = query_helper._get_indexed_fields()
        print(f"   Currently indexed fields: {indexed_fields}")

        # Show how the system uses this information
        match_query = {"category": "Electronics", "status": "active"}
        cost_with_indexes = query_helper._estimate_query_cost(match_query)
        print(
            f"   Cost of query {match_query} with indexes: {cost_with_indexes}"
        )

        # Show what happens with non-indexed fields
        no_index_query = {"description": "premium product"}
        cost_without_indexes = query_helper._estimate_query_cost(no_index_query)
        print(
            f"   Cost of query {no_index_query} without indexes: {cost_without_indexes}"
        )
        if (1 - cost_without_indexes) != 0:
            reduction = (
                (1 - cost_with_indexes) / (1 - cost_without_indexes) * 100
            )
            print(f"   Indexes provide {reduction:.0f}% cost reduction")
        else:
            print(
                f"   Indexes reduce cost from {cost_without_indexes} to {cost_with_indexes}"
            )

        print("\n=== Key Benefits of Pipeline Reordering Optimization ===")
        print("• Automatic optimization without requiring code changes")
        print("• Significant performance improvements for complex pipelines")
        print(
            "• Early filtering reduces data processing for expensive operations"
        )
        print("• Cost-based decision making ensures optimal execution paths")
        print("• Full backward compatibility with existing code")
        print("• Works with nested operations and complex queries")

        print("\n=== How It Works Internally ===")
        print("1. NeoSQLite analyzes each pipeline stage")
        print("2. Identifies $match stages that use indexed fields")
        print("3. Estimates the cost of the current pipeline")
        print("4. Reorders stages to move indexed matches to the front")
        print("5. Compares costs and selects the better execution path")
        print("6. Applies the optimization automatically")

        print("\n=== Performance Impact ===")
        print("The optimization is most beneficial when:")
        print("• Working with large datasets (1000+ documents)")
        print("• Using expensive operations like $unwind with large arrays")
        print("• Having multiple pipeline stages that can be reordered")
        print("• Filtering on indexed fields early can reduce processing")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
