#!/usr/bin/env python3
"""
Debug script to investigate why the $unwind + $group optimization is not being applied
"""

import neosqlite


def debug_query_building():
    """Debug the query building for $unwind + $group"""
    print("=== Debugging Query Building for $unwind + $group ===\n")

    with neosqlite.Connection(":memory:") as conn:
        # Create collection
        products = conn["products"]

        # Insert test data
        test_docs = [
            {
                "_id": 1,
                "name": "Product 1",
                "category": "Electronics",
                "price": 100.0,
                "tags": ["tag1", "tag2"],
            },
            {
                "_id": 2,
                "name": "Product 2",
                "category": "Electronics",
                "price": 200.0,
                "tags": ["tag2", "tag3"],
            },
        ]
        products.insert_many(test_docs)

        # Test pipeline
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "count": {"$sum": 1},
                    "avgPrice": {"$avg": "$price"},
                    "minPrice": {"$min": "$price"},
                    "maxPrice": {"$max": "$price"},
                }
            },
        ]

        # Check what the query helper builds
        query_helper = products.query_engine.helpers
        query_result = query_helper._build_aggregation_query(pipeline)

        print("Query building result:")
        if query_result is None:
            print("  No SQL query generated - falling back to Python")
        else:
            cmd, params, output_fields = query_result
            print(f"  SQL Command: {cmd}")
            print(f"  Parameters: {params}")
            print(f"  Output Fields: {output_fields}")
            print()

            # Try executing the SQL query directly
            print("Executing SQL query directly:")
            try:
                db_cursor = products.db.execute(cmd, params)
                if output_fields:
                    results = []
                    for row in db_cursor.fetchall():
                        processed_row = []
                        for i, value in enumerate(row):
                            # If this field contains a JSON array string, parse it
                            # This handles $push and $addToSet results
                            if (
                                output_fields[i] != "_id"
                                and isinstance(value, str)
                                and value.startswith("[")
                                and value.endswith("]")
                            ):
                                try:
                                    from neosqlite.collection.json_helpers import (
                                        neosqlite_json_loads,
                                    )

                                    processed_row.append(
                                        neosqlite_json_loads(value)
                                    )
                                except:
                                    processed_row.append(value)
                            else:
                                processed_row.append(value)
                        results.append(dict(zip(output_fields, processed_row)))

                    print("  Results:")
                    for result in results:
                        print(f"    {result}")
                else:
                    print("  No output fields - this shouldn't happen")
            except Exception as e:
                print(f"  Error executing query: {e}")

        print()

        # Also test with force fallback disabled to see what actually gets executed
        print("What actually gets executed:")
        neosqlite.collection.query_helper.set_force_fallback(False)
        result = products.aggregate(pipeline)
        print(f"  Result count: {len(result)}")
        for i, doc in enumerate(result):
            print(f"  {i+1}. {doc}")


if __name__ == "__main__":
    debug_query_building()
