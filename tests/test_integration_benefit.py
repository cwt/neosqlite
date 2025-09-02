"""
Test to demonstrate the integration of temporary table aggregation with existing NeoSQLite functionality.
"""

import neosqlite
from neosqlite.collection.temporary_table_aggregation import (
    execute_2nd_tier_aggregation,
)


def test_integration_benefit():
    """Test that demonstrates the integration benefit."""
    with neosqlite.Connection(":memory:") as conn:
        # Create collections
        users = conn.users
        orders = conn.orders

        # Insert user data
        users.insert_many(
            [
                {"_id": 1, "name": "Alice", "status": "active"},
                {"_id": 2, "name": "Bob", "status": "active"},
                {"_id": 3, "name": "Charlie", "status": "inactive"},
            ]
        )

        # Insert order data
        orders.insert_many(
            [
                {"userId": 1, "product": "Laptop", "amount": 1000},
                {"userId": 1, "product": "Mouse", "amount": 25},
                {"userId": 2, "product": "Book", "amount": 30},
            ]
        )

        # Test a pipeline that current implementation might not optimize well
        # This pipeline has $lookup not in the last position, which is a limitation
        # of the current NeoSQLite implementation
        complex_pipeline = [
            {"$match": {"status": "active"}},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            },
            {"$unwind": "$userOrders"},
            {"$match": {"userOrders.amount": {"$gt": 50}}},
            {"$sort": {"name": 1}},
        ]

        # Test with the integrated approach
        results = execute_2nd_tier_aggregation(
            users.query_engine, complex_pipeline
        )

        # Should get results without crashing
        assert isinstance(results, list)

        # Print results for verification
        print(f"Integration test results: {len(results)} documents")
        for result in results:
            print(
                f"  User: {result.get('name')}, Order: {result.get('userOrders', {}).get('product')}"
            )


def test_comparison_with_standard_approach():
    """Compare the results of our approach with the standard approach."""
    with neosqlite.Connection(":memory:") as conn:
        # Create a collection
        products = conn.products

        # Insert product data
        products.insert_many(
            [
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
            ]
        )

        # Simple pipeline that should work with both approaches
        simple_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$sort": {"price": 1}},
        ]

        # Test with integrated approach
        integrated_results = execute_2nd_tier_aggregation(
            products.query_engine, simple_pipeline
        )

        # Test with standard approach
        standard_results = list(products.aggregate(simple_pipeline))

        # Results should be the same
        assert len(integrated_results) == len(standard_results)

        # Sort both result sets for comparison
        integrated_sorted = sorted(integrated_results, key=lambda x: x["price"])
        standard_sorted = sorted(standard_results, key=lambda x: x["price"])

        # Prices should be the same
        integrated_prices = [doc["price"] for doc in integrated_sorted]
        standard_prices = [doc["price"] for doc in standard_sorted]
        assert integrated_prices == standard_prices

        print(
            f"Comparison test: Both approaches returned {len(integrated_results)} documents"
        )
        print(f"Prices: {standard_prices}")


if __name__ == "__main__":
    test_integration_benefit()
    test_comparison_with_standard_approach()
    print("All integration tests passed!")
