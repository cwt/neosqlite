#!/usr/bin/env python3
"""
Example demonstrating memory-constrained processing with quez in NeoSQLite.

This example shows how to use the AggregationCursor with quez for memory-efficient
processing of large result sets.
"""

import neosqlite
import time


def demonstrate_memory_constrained_processing():
    """Demonstrate memory-constrained processing with quez."""
    print("=== Memory-Constrained Processing with Quez ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert a large number of sample documents
        print("1. Inserting sample data...")
        sample_docs = []
        for i in range(10000):  # 10,000 documents
            doc = {
                "name": f"User {i}",
                "age": 20 + (i % 50),  # Ages 20-69
                "department": f"Department {i % 10}",  # 10 departments
                "salary": 30000 + (i * 100),  # Salaries from 30k to 1.03M
                "tags": [f"tag{j}" for j in range(i % 5)],  # 0-4 tags per user
            }
            sample_docs.append(doc)

        result = users.insert_many(sample_docs)
        print(f"   Inserted {len(result.inserted_ids)} documents\n")

        # Example 1: Normal processing (loads all results into memory)
        print("2. Normal processing (loads all results into memory):")
        pipeline1 = [
            {"$match": {"age": {"$gte": 30}}},
            {"$sort": {"salary": -1}},
            {"$limit": 1000},  # Limit to 1000 results
        ]

        start_time = time.time()
        cursor1 = users.aggregate(pipeline1)
        # Force execution by converting to list (loads all into memory)
        results1 = list(cursor1)
        normal_time = time.time() - start_time

        print(f"   Found {len(results1)} documents")
        print(f"   Processing time: {normal_time:.4f} seconds")
        print(f"   First result: {results1[0] if results1 else 'None'}")
        print()

        # Example 2: Memory-constrained processing with quez
        print("3. Memory-constrained processing with quez:")
        try:
            # Create a cursor and enable quez processing
            cursor2 = users.aggregate(pipeline1)
            cursor2.use_quez(True)  # Enable quez memory-constrained processing

            start_time = time.time()
            # Process results incrementally (memory-efficient)
            processed_count = 0
            for doc in cursor2:
                processed_count += 1
                # In a real application, you might process each document here
                # For example, send to another service, write to file, etc.
                if processed_count <= 3:  # Just show first 3 for demo
                    print(
                        f"   Processed document: {doc['name']} - ${doc['salary']}"
                    )

            quez_time = time.time() - start_time
            print(f"   Processed {processed_count} documents")
            print(f"   Processing time: {quez_time:.4f} seconds")
            print()

            # Compare memory efficiency
            print("4. Comparison:")
            print(f"   Normal processing time: {normal_time:.4f} seconds")
            print(f"   Quez processing time: {quez_time:.4f} seconds")
            print(
                "   Note: Quez processing is more memory-efficient for large result sets"
            )
            print(
                "   even if it might be slightly slower due to compression/decompression overhead"
            )

        except ImportError:
            print("   Quez library not available. Skipping quez example.")
        except Exception as e:
            print(f"   Error with quez processing: {e}")

        print()

        # Example 3: Memory-constrained group operation
        print("5. Memory-constrained group operation:")
        pipeline3 = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]

        try:
            cursor3 = users.aggregate(pipeline3)
            cursor3.use_quez(True)  # Enable quez

            start_time = time.time()
            group_results = list(cursor3)  # Process all results
            group_time = time.time() - start_time

            print(f"   Found {len(group_results)} tag groups")
            print(f"   Processing time: {group_time:.4f} seconds")
            for result in group_results[:3]:  # Show top 3
                print(
                    f"   Tag '{result['_id']}': {result['count']} occurrences"
                )

        except ImportError:
            print("   Quez library not available. Skipping quez example.")
        except Exception as e:
            print(f"   Error with quez processing: {e}")

        print("\n=== Memory-Constrained Processing Demo Complete ===")


if __name__ == "__main__":
    demonstrate_memory_constrained_processing()
