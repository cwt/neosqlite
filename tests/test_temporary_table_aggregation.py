#!/usr/bin/env python3
"""
Test file for temporary table aggregation pipeline implementation.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
)


def test_temporary_table_implementation():
    """Test the temporary table aggregation implementation."""
    print("=== Testing Temporary Table Aggregation Implementation ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data
        print("1. Inserting sample data...")
        sample_docs = [
            {
                "name": "Alice",
                "age": 30,
                "tags": ["python", "javascript"],
                "status": "active",
            },
            {
                "name": "Bob",
                "age": 25,
                "tags": ["java", "python"],
                "status": "active",
            },
            {
                "name": "Charlie",
                "age": 35,
                "tags": ["javascript", "go"],
                "status": "inactive",
            },
            {
                "name": "Diana",
                "age": 28,
                "tags": ["python", "rust"],
                "status": "active",
            },
        ]
        users.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} documents\n")

        # Test 1: Simple $match + $unwind pipeline
        print("2. Testing $match + $unwind pipeline...")
        pipeline1 = [{"$match": {"status": "active"}}, {"$unwind": "$tags"}]

        try:
            processor = TemporaryTableAggregationProcessor(users)
            results1 = processor.process_pipeline(pipeline1)
            print(f"   Temporary table results count: {len(results1)}")
            for doc in results1:
                print(f"     {doc['name']}: {doc['tags']}")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        # Compare with standard approach
        try:
            standard_results1 = list(users.aggregate(pipeline1))
            print(f"   Standard results count: {len(standard_results1)}")
            for doc in standard_results1:
                print(f"     {doc['name']}: {doc['tags']}")

            # Verify results match
            if len(results1) == len(standard_results1):
                print(
                    "   ✓ Results match between temporary table and standard approaches"
                )
            else:
                print("   ✗ Results do not match!")
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test 2: $match + $unwind + $sort + $limit pipeline
        print("3. Testing $match + $unwind + $sort + $limit pipeline...")
        pipeline2 = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$limit": 5},
        ]

        try:
            processor = TemporaryTableAggregationProcessor(users)
            results2 = processor.process_pipeline(pipeline2)
            print(f"   Temporary table results count: {len(results2)}")
            for doc in results2:
                print(f"     {doc['name']}: {doc['tags']}")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        # Compare with standard approach
        try:
            standard_results2 = list(users.aggregate(pipeline2))
            print(f"   Standard results count: {len(standard_results2)}")
            for doc in standard_results2:
                print(f"     {doc['name']}: {doc['tags']}")

            # Verify results match
            if len(results2) == len(standard_results2):
                print(
                    "   ✓ Results match between temporary table and standard approaches"
                )
            else:
                print("   ✗ Results do not match!")
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test 3: Multiple consecutive $unwind stages
        print("4. Testing multiple consecutive $unwind stages...")
        # First, add some data with nested arrays
        orders = conn.orders
        order_docs = [
            {
                "userId": 1,
                "items": [
                    {
                        "name": "Laptop",
                        "categories": ["electronics", "computers"],
                    },
                    {
                        "name": "Mouse",
                        "categories": ["electronics", "accessories"],
                    },
                ],
            },
            {
                "userId": 2,
                "items": [
                    {
                        "name": "Keyboard",
                        "categories": ["electronics", "accessories"],
                    },
                    {
                        "name": "Monitor",
                        "categories": ["electronics", "displays"],
                    },
                ],
            },
        ]
        orders.insert_many(order_docs)

        pipeline3 = [{"$unwind": "$items"}, {"$unwind": "$items.categories"}]

        try:
            processor = TemporaryTableAggregationProcessor(orders)
            results3 = processor.process_pipeline(pipeline3)
            print(f"   Temporary table results count: {len(results3)}")
            for doc in results3:
                print(
                    f"     Item: {doc.get('items', {}).get('name', 'N/A')}, Category: {doc['items.categories']}"
                )
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        # Compare with standard approach
        try:
            standard_results3 = list(orders.aggregate(pipeline3))
            print(f"   Standard results count: {len(standard_results3)}")
            for doc in standard_results3:
                print(
                    f"     Item: {doc.get('items', {}).get('name', 'N/A')}, Category: {doc['items.categories']}"
                )

            # Verify results match
            if len(results3) == len(standard_results3):
                print(
                    "   ✓ Results match between temporary table and standard approaches"
                )
            else:
                print("   ✗ Results do not match!")
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test 4: $match with operators
        print("5. Testing $match with operators...")
        pipeline4 = [{"$match": {"age": {"$gte": 30}}}, {"$sort": {"age": 1}}]

        try:
            processor = TemporaryTableAggregationProcessor(users)
            results4 = processor.process_pipeline(pipeline4)
            print(f"   Temporary table results count: {len(results4)}")
            for doc in results4:
                print(f"     {doc['name']}: {doc['age']} years old")
        except Exception as e:
            print(f"   Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        # Compare with standard approach
        try:
            standard_results4 = list(users.aggregate(pipeline4))
            print(f"   Standard results count: {len(standard_results4)}")
            for doc in standard_results4:
                print(f"     {doc['name']}: {doc['age']} years old")

            # Verify results match
            if len(results4) == len(standard_results4):
                print(
                    "   ✓ Results match between temporary table and standard approaches"
                )
            else:
                print("   ✗ Results do not match!")
        except Exception as e:
            print(f"   Error in standard approach: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    test_temporary_table_implementation()
    print("\n=== Test Complete ===")
