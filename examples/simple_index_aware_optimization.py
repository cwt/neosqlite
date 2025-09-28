#!/usr/bin/env python3
"""
Simple example demonstrating the Advanced Index-Aware Optimization feature.

This example shows how NeoSQLite automatically uses indexes to improve query performance.
"""

import neosqlite


def main():
    print("=== Simple Index-Aware Optimization Example ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        collection = conn.test_collection

        # Insert sample data
        print("1. Inserting sample data...")
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User {i}",
                    "category": f"Category {i % 5}",  # 5 categories
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i * 10,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )

        collection.insert_many(docs)
        print(f"   Inserted {len(docs)} documents\n")

        # Create indexes
        print("2. Creating indexes...")
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("score")
        print("   Created indexes on category, status, and score\n")

        # Show how indexes are used automatically
        print("3. Running queries that automatically use indexes...")

        # This query will benefit from the category index
        pipeline1 = [{"$match": {"category": "Category 2"}}, {"$limit": 5}]
        result1_cursor = collection.aggregate(pipeline1)
        print(f"   Query with category filter: {len(result1_cursor)} results")

        # This query will benefit from both category and status indexes
        pipeline2 = [
            {"$match": {"category": "Category 1", "status": "active"}},
            {"$limit": 5},
        ]
        result2_cursor = collection.aggregate(pipeline2)
        print(
            f"   Query with category and status filters: {len(result2_cursor)} results"
        )

        # This complex pipeline uses multiple optimizations
        pipeline3 = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"score": -1}},
            {"$limit": 10},
        ]
        result3 = list(collection.aggregate(pipeline3))
        print(
            f"   Complex pipeline with unwind, sort, and limit: {len(result3)} results"
        )
        if result3:
            print(f"   Highest score in results: {result3[0]['score']}")
        print()

        # Show internal cost estimation (this is how the optimization works internally)
        print("4. Internal cost estimation (how the optimization works):")
        query_helper = collection.query_engine.helpers

        # Without indexes, cost is 1.0
        cost_no_index = query_helper._estimate_query_cost({"name": "User 1"})
        print(f"   Query without index: cost = {cost_no_index}")

        # With indexes, cost is reduced (0.3 for regular fields, 0.1 for _id)
        cost_with_index = query_helper._estimate_query_cost(
            {"category": "Category 1"}
        )
        print(f"   Query with index: cost = {cost_with_index}")

        # Query with integer ID (for backward compatibility and existing docs)
        cost_with_int_id = query_helper._estimate_query_cost({"_id": 1})
        print(
            f"   Query with integer _id (always indexed): cost = {cost_with_int_id}"
        )

        # Query with ObjectId (for new documents)
        from neosqlite.objectid import ObjectId

        test_oid = ObjectId()
        cost_with_oid = query_helper._estimate_query_cost({"_id": test_oid})
        print(
            f"   Query with ObjectId _id (always indexed): cost = {cost_with_oid}"
        )

        # Show which fields are indexed
        indexed_fields = query_helper._get_indexed_fields()
        print(f"   Currently indexed fields: {indexed_fields}")
        print()

        print("=== Key Points ===")
        print("• Indexes are automatically detected and used for optimization")
        print("• Queries with indexed fields execute faster")
        print("• Complex pipelines benefit from multiple optimizations")
        print("• No code changes needed - optimization works automatically")
        print("• Performance improves significantly with proper indexing")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
