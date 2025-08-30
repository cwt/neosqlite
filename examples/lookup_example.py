#!/usr/bin/env python3
"""
Example demonstrating the $lookup aggregation stage in NeoSQLite.

This example shows how to use $lookup to join data from different collections,
similar to SQL joins but with MongoDB-style syntax.
"""

import neosqlite


def main():
    print("=== NeoSQLite $lookup Example ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Create collections for customers and orders
        customers = conn["customers"]
        orders = conn["orders"]

        # Insert sample customer data
        print("1. Inserting customer data...")
        customer_docs = [
            {
                "_id": 1,
                "name": "Alice Smith",
                "email": "alice@example.com",
                "customerId": "C001",
            },
            {
                "_id": 2,
                "name": "Bob Johnson",
                "email": "bob@example.com",
                "customerId": "C002",
            },
            {
                "_id": 3,
                "name": "Charlie Brown",
                "email": "charlie@example.com",
                "customerId": "C003",
            },
        ]
        customers.insert_many(customer_docs)
        print(f"   Inserted {len(customer_docs)} customers\n")

        # Insert sample order data
        print("2. Inserting order data...")
        order_docs = [
            {
                "_id": 101,
                "customerId": "C001",
                "product": "Laptop",
                "amount": 1200.00,
                "status": "shipped",
            },
            {
                "_id": 102,
                "customerId": "C001",
                "product": "Mouse",
                "amount": 25.00,
                "status": "shipped",
            },
            {
                "_id": 103,
                "customerId": "C002",
                "product": "Keyboard",
                "amount": 75.00,
                "status": "pending",
            },
            {
                "_id": 104,
                "customerId": "C002",
                "product": "Monitor",
                "amount": 300.00,
                "status": "shipped",
            },
            {
                "_id": 105,
                "customerId": "C001",
                "product": "Webcam",
                "amount": 80.00,
                "status": "shipped",
            },
        ]
        orders.insert_many(order_docs)
        print(f"   Inserted {len(order_docs)} orders\n")

        # Example 1: Basic $lookup
        print("3. Example: Basic $lookup")
        print("   Join customers with their orders")
        pipeline1 = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            }
        ]

        result1 = list(customers.aggregate(pipeline1))
        print("   Results:")
        for customer in result1:
            print(
                f"     {customer['name']}: {len(customer['customerOrders'])} orders"
            )
            for order in customer["customerOrders"]:
                print(f"       - {order['product']} (${order['amount']})")
        print()

        # Example 2: $lookup with $match
        print("4. Example: $lookup with $match")
        print("   Join only active customers with their orders")
        pipeline2 = [
            {"$match": {"name": {"$regex": "Alice|Bob"}}},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
        ]

        result2 = list(customers.aggregate(pipeline2))
        print("   Results:")
        for customer in result2:
            print(
                f"     {customer['name']}: {len(customer['customerOrders'])} orders"
            )
        print()

        # Example 3: $lookup followed by $unwind
        print("5. Example: $lookup followed by $unwind")
        print("   Flatten customer-order relationships")
        pipeline3 = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
            {"$unwind": "$customerOrders"},
        ]

        result3 = list(customers.aggregate(pipeline3))
        print("   Results (each line is one customer-order combination):")
        for doc in result3:
            print(
                f"     {doc['name']} ordered {doc['customerOrders']['product']} (${doc['customerOrders']['amount']})"
            )
        print()

        # Example 4: $lookup with $unwind and $match
        print("6. Example: $lookup with $unwind and $match")
        print("   Show only shipped orders")
        pipeline4 = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
            {"$unwind": "$customerOrders"},
            {"$match": {"customerOrders.status": "shipped"}},
        ]

        result4 = list(customers.aggregate(pipeline4))
        print("   Results:")
        for doc in result4:
            print(
                f"     {doc['name']} shipped order: {doc['customerOrders']['product']} (${doc['customerOrders']['amount']})"
            )
        print()

        # Example 5: $lookup with empty results
        print("7. Example: $lookup with no matching documents")
        print("   Customer with no orders")
        # Add a customer with no orders
        customers.insert_one(
            {
                "_id": 4,
                "name": "Diana Prince",
                "email": "diana@example.com",
                "customerId": "C004",
            }
        )

        pipeline5 = [
            {"$match": {"customerId": "C004"}},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "customerId",
                    "foreignField": "customerId",
                    "as": "customerOrders",
                }
            },
        ]

        result5 = list(customers.aggregate(pipeline5))
        print("   Results:")
        for customer in result5:
            print(
                f"     {customer['name']}: {len(customer['customerOrders'])} orders (should be 0)"
            )
            print(f"     customerOrders field: {customer['customerOrders']}")
        print()

        print("=== Performance Benefits ===")
        print("• Simple $lookup operations are processed at the database level")
        print("• No intermediate Python data structures needed")
        print("• Efficient SQL queries using subqueries and json_group_array")
        print("• Reduced memory footprint for large datasets")
        print()

        print("=== Supported Patterns ===")
        print("✓ Basic $lookup operations")
        print("✓ $lookup with preceding $match stage")
        print("✓ $lookup followed by $unwind operations")
        print("✓ Complex pipelines with multiple stages")
        print("✓ Handling of empty result sets")
        print()

        print("=== How It Works ===")
        print("1. NeoSQLite detects $lookup stages in aggregation pipelines")
        print("2. For simple cases, generates optimized SQL with subqueries")
        print("3. For complex cases, falls back to Python processing")
        print(
            "4. Uses json_group_array to collect related documents into arrays"
        )
        print(
            "5. Maintains full compatibility with existing aggregation features"
        )


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
