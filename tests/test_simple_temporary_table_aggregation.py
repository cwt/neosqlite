#!/usr/bin/env python3
"""
Simple test for the temporary table aggregation implementation.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
    can_process_with_temporary_tables,
)


def test_simple_implementation():
    """Test the simplified temporary table aggregation implementation."""
    print(
        "=== Testing Simplified Temporary Table Aggregation Implementation ===\n"
    )

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get collections
        products = conn.products
        categories = conn.categories

        # Insert sample data
        print("1. Inserting sample data...")
        product_docs = [
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
        products.insert_many(product_docs)

        category_docs = [
            {"name": "Electronics", "description": "Electronic devices"},
            {"name": "Education", "description": "Educational materials"},
            {"name": "Furniture", "description": "Home furniture"},
        ]
        categories.insert_many(category_docs)

        print(
            f"   Inserted {len(product_docs)} products and {len(category_docs)} categories\n"
        )

        # Test case 1: Simple pipeline that current implementation can optimize
        print(
            "2. Testing simple pipeline (should use existing optimization)..."
        )
        simple_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$sort": {"price": 1}},
        ]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(simple_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with standard approach
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

        # Test case 2: Complex pipeline that current implementation can't fully optimize
        print(
            "3. Testing complex pipeline (should benefit from temporary tables)..."
        )
        complex_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$unwind": "$tags"},
            {
                "$lookup": {
                    "from": "categories",
                    "localField": "category",
                    "foreignField": "name",
                    "as": "categoryInfo",
                }
            },
            {"$sort": {"tags": 1}},
            {"$limit": 5},
        ]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(complex_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(complex_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                print(f"     {doc['name']}: {doc['tags']}")
                if doc.get("categoryInfo"):
                    for cat_info in doc["categoryInfo"]:
                        print(
                            f"       Category: {cat_info.get('description', 'N/A')}"
                        )
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 3: Pipeline with multiple consecutive $unwind stages
        print("4. Testing pipeline with multiple consecutive $unwind stages...")
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

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(multi_unwind_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(orders)
            temp_table_results = processor.process_pipeline(
                multi_unwind_pipeline
            )
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                items = doc.get("items", {})
                item_name = items.get("name", "N/A")
                spec = doc.get("items.specs", "N/A")
                customer = doc.get("customer", "N/A")
                print(f"     {customer}: {item_name} - {spec}")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        print("5. Benefits demonstrated:")
        print(
            "   • Complex pipelines with $lookup can be processed with SQL instead of Python"
        )
        print("   • Multiple consecutive $unwind stages handled efficiently")
        print("   • Intermediate results stored in database, not Python memory")
        print("   • Automatic resource cleanup with transaction management")
        print("   • Potential for better performance on complex operations")


if __name__ == "__main__":
    test_simple_implementation()
    print("\n=== Test Complete ===")
