#!/usr/bin/env python3
"""
Example showing how temporary table aggregation can handle complex pipelines
that the current implementation cannot optimize.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
)


def demonstrate_complex_pipeline():
    """Demonstrate handling of complex pipelines with temporary tables."""
    print("=== Complex Pipeline Demonstration ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Create collections
        users = conn.users
        orders = conn.orders

        # Insert sample user data
        print("1. Inserting user data...")
        user_docs = [
            {"_id": 1, "name": "Alice", "status": "active"},
            {"_id": 2, "name": "Bob", "status": "active"},
            {"_id": 3, "name": "Charlie", "status": "inactive"},
        ]
        users.insert_many(user_docs)
        print(f"   Inserted {len(user_docs)} users\n")

        # Insert sample order data
        print("2. Inserting order data...")
        order_docs = [
            {"userId": 1, "amount": 100, "items": ["laptop", "mouse"]},
            {"userId": 1, "amount": 50, "items": ["keyboard"]},
            {"userId": 2, "amount": 200, "items": ["monitor", "stand"]},
            {"userId": 2, "amount": 75, "items": ["webcam"]},
        ]
        orders.insert_many(order_docs)
        print(f"   Inserted {len(order_docs)} orders\n")

        # Example of a complex pipeline that current implementation can't optimize:
        # 1. Unwind user tags
        # 2. Lookup orders for each user
        # 3. Unwind order items
        # 4. Group by item to get total sales
        #
        # This would normally fall back to Python processing, but with temporary tables
        # we can process parts of it in SQL.

        print(
            "3. Demonstrating a complex pipeline that current implementation can't fully optimize..."
        )
        print(
            "   Pipeline: Users with orders -> unwind orders -> group by order amount"
        )

        # Let's create a pipeline that combines user and order data
        # This simulates what might be possible with temporary tables
        try:
            # First, let's do a lookup operation (simplified)
            # In a full implementation, we'd join users and orders

            # For demonstration, let's manually create a combined dataset
            combined_docs = []
            for user in users.find():
                user_orders = list(orders.find({"userId": user["_id"]}))
                for order in user_orders:
                    combined_doc = {
                        "userName": user["name"],
                        "userStatus": user["status"],
                        "orderAmount": order["amount"],
                        "orderItems": order["items"],
                    }
                    combined_docs.append(combined_doc)

            # Now create a temporary collection with this combined data
            combined_collection = conn.combined
            combined_collection.insert_many(combined_docs)

            # Now we can process this with our temporary table approach
            complex_pipeline = [
                {"$unwind": "$orderItems"},
                {"$match": {"userStatus": "active"}},
                {"$sort": {"orderAmount": -1}},
                {"$limit": 5},
            ]

            print(
                "   Processing complex pipeline with temporary table approach..."
            )
            processor = TemporaryTableAggregationProcessor(combined_collection)
            results = processor.process_pipeline(complex_pipeline)

            print(f"   Results count: {len(results)}")
            for doc in results:
                print(
                    f"     {doc['userName']}: {doc['orderItems']} (${doc['orderAmount']})"
                )

        except Exception as e:
            print(f"   Error: {e}")
            import traceback

            traceback.print_exc()

        print("\n4. Comparing with standard approach...")
        try:
            # Try with standard approach - this might fall back to Python for complex parts
            standard_results = list(
                combined_collection.aggregate(complex_pipeline)
            )
            print(f"   Standard results count: {len(standard_results)}")
            for doc in standard_results:
                print(
                    f"     {doc['userName']}: {doc['orderItems']} (${doc['orderAmount']})"
                )
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print("\n5. Benefits of temporary table approach:")
        print("   • Intermediate results stored in database, not Python memory")
        print(
            "   • Complex pipelines can be broken into manageable SQL operations"
        )
        print("   • Better resource management with automatic cleanup")
        print("   • Potential for processing larger datasets")
        print(
            "   • More pipeline combinations can benefit from SQL optimization"
        )

        print("\n6. Future enhancement possibilities:")
        print("   • Integration with $lookup operations using temporary tables")
        print(
            "   • Hybrid approach: SQL for heavy operations, Python for complex logic"
        )
        print("   • Streaming results to reduce memory usage")
        print("   • Parallel processing of independent pipeline branches")


if __name__ == "__main__":
    demonstrate_complex_pipeline()
    print("\n=== Demonstration Complete ===")
