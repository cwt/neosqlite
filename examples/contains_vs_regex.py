#!/usr/bin/env python3
"""
Performance comparison between $contains and $regex operators.
"""

import neosqlite
import time
import re


def main():
    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data
        sample_data = []
        for i in range(10000):
            sample_data.append(
                {
                    "name": f"User {i}",
                    "email": f"user{i}@example.com",
                    "bio": f"This is user {i} who loves Python and SQL. User {i} has been using Python for years.",
                }
            )

        users.insert_many(sample_data)
        print(f"Inserted {users.count_documents({})} documents")

        # Test $contains performance
        print("\nTesting $contains operator...")
        start_time = time.time()
        results = list(users.find({"bio": {"$contains": "python"}}))
        contains_time = time.time() - start_time
        print(f"Found {len(results)} documents in {contains_time:.4f} seconds")

        # Test $regex performance
        print("\nTesting $regex operator...")
        start_time = time.time()
        results = list(
            users.find({"bio": {"$regex": ".*[Pp][Yy][Tt][Hh][Oo][Nn].*"}})
        )
        regex_time = time.time() - start_time
        print(f"Found {len(results)} documents in {regex_time:.4f} seconds")

        # Compare performance
        print(f"\nPerformance comparison:")
        print(f"$contains: {contains_time:.4f} seconds")
        print(f"$regex: {regex_time:.4f} seconds")
        print(
            f"$contains is {regex_time/contains_time:.2f}x faster than $regex"
        )

        # Test with simpler regex pattern
        print("\nTesting $regex operator with simpler pattern...")
        start_time = time.time()
        results = list(
            users.find({"bio": {"$regex": "(?i)python"}})
        )  # Case insensitive
        simple_regex_time = time.time() - start_time
        print(
            f"Found {len(results)} documents in {simple_regex_time:.4f} seconds"
        )

        print(f"\nWith simpler regex pattern:")
        print(f"$contains: {contains_time:.4f} seconds")
        print(f"$regex (simple): {simple_regex_time:.4f} seconds")
        print(
            f"$contains is {simple_regex_time/contains_time:.2f}x faster than simple $regex"
        )


if __name__ == "__main__":
    main()
