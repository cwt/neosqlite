#!/usr/bin/env python3
"""
Example demonstrating the usage of stable, predictable, and repeatable
temporary table naming system in NeoSQLite.
"""

import neosqlite
from neosqlite.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
    can_process_with_temporary_tables,
)


def demonstrate_deterministic_temp_table_names():
    """Demonstrate the benefits of deterministic temp table names."""
    print("=== NeoSQLite Deterministic Temp Table Names Example ===\n")

    with neosqlite.Connection(":memory:") as conn:
        # Get collections
        users = conn.users
        orders = conn.orders

        # Insert sample data
        print("1. Inserting sample data...")
        user_docs = [
            {
                "name": "Alice",
                "age": 30,
                "status": "active",
                "tags": ["python", "javascript", "sql"],
            },
            {
                "name": "Bob",
                "age": 25,
                "status": "active",
                "tags": ["java", "python", "go"],
            },
            {
                "name": "Charlie",
                "age": 35,
                "status": "inactive",
                "tags": ["javascript", "rust", "go"],
            },
            {
                "name": "Diana",
                "age": 28,
                "status": "active",
                "tags": ["python", "rust", "sql"],
            },
        ]
        users.insert_many(user_docs)

        order_docs = [
            {"userId": 1, "product": "Laptop", "amount": 1200},
            {"userId": 1, "product": "Mouse", "amount": 25},
            {"userId": 2, "product": "Keyboard", "amount": 75},
            {"userId": 2, "product": "Monitor", "amount": 300},
            {"userId": 3, "product": "Tablet", "amount": 500},
        ]
        orders.insert_many(order_docs)
        print(
            f"   Inserted {len(user_docs)} users and {len(order_docs)} orders\n"
        )

        # Define a complex pipeline that benefits from temp tables
        complex_pipeline = [
            {"$match": {"status": "active", "age": {"$gte": 25}}},
            {"$unwind": "$tags"},
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            },
            {"$unwind": "$userOrders"},
            {"$match": {"userOrders.amount": {"$gte": 50}}},
            {"$sort": {"userOrders.amount": -1}},
            {"$limit": 5},
        ]

        print("2. Processing complex pipeline with temporary tables...")
        print("   Pipeline:")
        for i, stage in enumerate(complex_pipeline, 1):
            stage_name = next(iter(stage.keys()))
            print(f"     {i}. {stage_name}: {stage[stage_name]}")

        # Check if pipeline can be processed with temporary tables
        if can_process_with_temporary_tables(complex_pipeline):
            print("\n   ✓ Pipeline can be processed with temporary tables")

            # Process with temporary table aggregation
            try:
                processor = TemporaryTableAggregationProcessor(users)
                results = processor.process_pipeline(complex_pipeline)
                print(f"\n   Processed {len(results)} results:")
                for i, result in enumerate(results[:3]):  # Show first 3 results
                    print(
                        f"     {i+1}. {result.get('name', 'N/A')}: {result.get('tags', 'N/A')} - ${result.get('userOrders', {}).get('amount', 'N/A')}"
                    )
            except Exception as e:
                print(f"\n   ✗ Error processing pipeline: {e}")
                import traceback

                traceback.print_exc()
        else:
            print("\n   ✗ Pipeline cannot be processed with temporary tables")

        print("\n3. Benefits of deterministic temp table names:")
        print("   ✓ Predictable and repeatable table names")
        print("   ✓ Better query plan caching in SQLite")
        print("   ✓ Easier debugging and tracing")
        print("   ✓ Consistent performance across runs")
        print("   ✓ Reduced memory overhead from name generation")

        print("\n4. How it works:")
        print("   • Each pipeline gets a deterministic ID based on its content")
        print(
            "   • Each stage gets a deterministic name based on its specification"
        )
        print("   • Names are generated using SHA256 hashing for consistency")
        print("   • SQLite can better cache query plans for predictable names")


if __name__ == "__main__":
    demonstrate_deterministic_temp_table_names()
    print("\n=== Example Complete ===")
