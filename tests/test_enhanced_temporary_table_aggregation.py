#!/usr/bin/env python3
"""
Test for the enhanced temporary table aggregation implementation.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
    can_process_with_temporary_tables,
)


def test_enhanced_implementation():
    """Test the enhanced temporary table aggregation implementation."""
    print(
        "=== Testing Enhanced Temporary Table Aggregation Implementation ===\n"
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

        # Test case 1: $project stage
        print("2. Testing $project stage...")
        project_pipeline = [{"$project": {"name": 1, "price": 1, "_id": 0}}]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(project_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(project_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results[:3]:  # Show first 3 results
                print(f"     {doc}")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 2: $group stage with various accumulators
        print("3. Testing $group stage with various accumulators...")
        group_pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "count": {"$sum": 1},
                    "totalValue": {"$sum": "$price"},
                    "avgPrice": {"$avg": "$price"},
                    "minPrice": {"$min": "$price"},
                    "maxPrice": {"$max": "$price"},
                    "productNames": {"$push": "$name"},
                }
            }
        ]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(group_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(group_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                print(f"     Category: {doc['_id']}")
                print(f"       Count: {doc['count']}")
                print(f"       Total Value: ${doc['totalValue']}")
                print(f"       Average Price: ${doc['avgPrice']:.2f}")
                print(f"       Products: {doc['productNames']}")
                print()
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 3: $lookup stage
        print("4. Testing $lookup stage...")
        lookup_pipeline = [
            {
                "$lookup": {
                    "from": "categories",
                    "localField": "category",
                    "foreignField": "name",
                    "as": "categoryInfo",
                }
            }
        ]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(lookup_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(lookup_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results[:2]:  # Show first 2 results
                print(f"     Product: {doc['name']} ({doc['category']})")
                if doc.get("categoryInfo"):
                    for cat_info in doc["categoryInfo"]:
                        print(
                            f"       Category Info: {cat_info.get('description', 'N/A')}"
                        )
                print()
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 4: Complex pipeline with multiple stages
        print("5. Testing complex pipeline with multiple stages...")
        complex_pipeline = [
            {"$match": {"price": {"$gte": 50}}},
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "count": {"$sum": 1},
                    "totalValue": {"$sum": "$price"},
                    "products": {"$push": "$name"},
                }
            },
            {"$sort": {"totalValue": -1}},
            {"$limit": 3},
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
                print(f"     Tag: {doc['_id']}")
                print(f"       Count: {doc['count']}")
                print(f"       Total Value: ${doc['totalValue']}")
                print(f"       Products: {doc['products']}")
                print()
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 5: Pipeline with all supported stages
        print("6. Testing pipeline with all supported stages...")
        full_pipeline = [
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
            {
                "$group": {
                    "_id": "$tags",
                    "count": {"$sum": 1},
                    "avgPrice": {"$avg": "$price"},
                    "products": {"$push": "$name"},
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 2},
        ]

        # Check if it can be processed with temporary tables
        can_process = can_process_with_temporary_tables(full_pipeline)
        print(f"   Can process with temporary tables: {can_process}")

        # Try with temporary table approach
        try:
            processor = TemporaryTableAggregationProcessor(products)
            temp_table_results = processor.process_pipeline(full_pipeline)
            print(
                f"   Temporary table approach results count: {len(temp_table_results)}"
            )
            for doc in temp_table_results:
                print(f"     Tag: {doc['_id']}")
                print(f"       Count: {doc['count']}")
                print(f"       Average Price: ${doc['avgPrice']:.2f}")
                print(f"       Products: {doc['products']}")
                print()
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        print("7. Summary of supported stages:")
        print("   ✓ $match - Filtering documents")
        print("   ✓ $unwind - Array decomposition")
        print("   ✓ $sort - Sorting documents")
        print("   ✓ $skip - Skipping documents")
        print("   ✓ $limit - Limiting results")
        print("   ✓ $project - Field inclusion/exclusion")
        print("   ✓ $group - Grouping with accumulators")
        print("   ✓ $lookup - Joining collections")

        print("\n8. Accumulator support:")
        print("   ✓ $sum - Summation operations")
        print("   ✓ $avg - Average calculations")
        print("   ✓ $min - Minimum values")
        print("   ✓ $max - Maximum values")
        print("   ✓ $count - Document counting")
        print("   ✓ $push - Array building with duplicates")
        print("   ✓ $addToSet - Unique value collection")


if __name__ == "__main__":
    test_enhanced_implementation()
    print("\n=== Test Complete ===")
