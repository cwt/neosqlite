#!/usr/bin/env python3
"""
Integration example showing how temporary table aggregation can work alongside
the existing NeoSQLite aggregation pipeline processing.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
)


def test_hybrid_approach():
    """Test a hybrid approach that tries SQL optimization first, then falls back to temporary tables."""
    print("=== Hybrid Aggregation Approach ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        products = conn.products

        # Insert sample data
        print("1. Inserting sample data...")
        sample_docs = [
            {
                "name": "Laptop",
                "category": "Electronics",
                "tags": ["computer", "portable"],
                "price": 1200,
            },
            {
                "name": "Mouse",
                "category": "Electronics",
                "tags": ["computer", "accessory"],
                "price": 25,
            },
            {
                "name": "Keyboard",
                "category": "Electronics",
                "tags": ["computer", "accessory"],
                "price": 75,
            },
            {
                "name": "Book",
                "category": "Education",
                "tags": ["learning", "paper"],
                "price": 30,
            },
            {
                "name": "Desk",
                "category": "Furniture",
                "tags": ["office", "wood"],
                "price": 300,
            },
        ]
        products.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} products\n")

        # Test case 1: Pipeline that can be optimized with single SQL query (existing approach)
        print(
            "2. Testing pipeline that can be optimized with single SQL query..."
        )
        simple_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$sort": {"price": 1}},
        ]

        # Try with standard approach first
        try:
            standard_results = list(products.aggregate(simple_pipeline))
            print(
                f"   Standard approach results count: {len(standard_results)}"
            )
            for doc in standard_results:
                print(f"     {doc['name']}: ${doc['price']}")
        except Exception as e:
            print(f"   Error in standard approach: {e}")

        print()

        # Test case 2: Pipeline that would benefit from temporary tables
        print(
            "3. Testing complex pipeline that benefits from temporary tables..."
        )
        complex_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$unwind": "$tags"},
            {"$match": {"tags": {"$in": ["computer", "accessory"]}}},
            {"$sort": {"tags": 1, "price": -1}},
            {"$limit": 5},
        ]

        # Try with standard approach first (may fall back to Python)
        try:
            standard_results = list(products.aggregate(complex_pipeline))
            print(
                f"   Standard approach results count: {len(standard_results)}"
            )
            for doc in standard_results:
                print(f"     {doc['name']}: {doc['tags']} (${doc['price']})")
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Now try with temporary table approach
        print("4. Processing same pipeline with temporary table approach...")
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(complex_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                print(f"     {doc['name']}: {doc['tags']} (${doc['price']})")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 3: Pipeline with multiple unwind operations
        print(
            "5. Testing pipeline with multiple consecutive unwind operations..."
        )
        # First, add some data with nested arrays
        orders = conn.orders
        order_docs = [
            {
                "orderId": "001",
                "customer": "Alice",
                "items": [
                    {"name": "Laptop", "specs": ["16GB RAM", "512GB SSD"]},
                    {"name": "Mouse", "specs": ["Wireless", "Optical"]},
                ],
            },
            {
                "orderId": "002",
                "customer": "Bob",
                "items": [
                    {"name": "Keyboard", "specs": ["Mechanical", "RGB"]},
                    {"name": "Monitor", "specs": ["27 inch", "4K"]},
                ],
            },
        ]
        orders.insert_many(order_docs)

        multi_unwind_pipeline = [
            {"$unwind": "$items"},
            {"$unwind": "$items.specs"},
        ]

        # Try with standard approach
        try:
            standard_results = list(orders.aggregate(multi_unwind_pipeline))
            print(
                f"   Standard approach results count: {len(standard_results)}"
            )
            for doc in standard_results:
                print(
                    f"     {doc['customer']}: {doc['items.name']} - {doc['items.specs']}"
                )
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Try with temporary table approach
        print(
            "6. Processing multi-unwind pipeline with temporary table approach..."
        )
        try:
            processor = TemporaryTableAggregationProcessor(orders)
            temp_table_results = processor.process_pipeline(
                multi_unwind_pipeline
            )
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                print(
                    f"     {doc['customer']}: {doc.get('items', {}).get('name', 'N/A')} - {doc['items.specs']}"
                )
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        print("7. Benefits of hybrid approach:")
        print("   • Uses existing SQL optimization for simple pipelines")
        print("   • Falls back to temporary tables for complex pipelines")
        print("   • Maintains full compatibility with existing code")
        print("   • Provides better performance for a wider range of pipelines")
        print("   • Automatic resource management with guaranteed cleanup")


if __name__ == "__main__":
    test_hybrid_approach()
    print("\n=== Test Complete ===")
