#!/usr/bin/env python3
"""
Performance test script demonstrating the behavior of the $contains operator
and how it interacts with indexes.
"""

import neosqlite
import time


def main():
    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert a large number of sample data
        print("Inserting sample data...")
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

        # Test performance without index
        print("\nTesting $contains without index...")
        start_time = time.time()
        results = list(users.find({"bio": {"$contains": "python"}}))
        end_time = time.time()
        print(
            f"Found {len(results)} documents in {end_time - start_time:.4f} seconds"
        )

        # Create an index on the bio field
        print("\nCreating index on bio field...")
        users.create_index("bio")
        print("Index created")

        # Test performance with index
        print("\nTesting $contains with index...")
        start_time = time.time()
        results = list(users.find({"bio": {"$contains": "python"}}))
        end_time = time.time()
        print(
            f"Found {len(results)} documents in {end_time - start_time:.4f} seconds"
        )

        # Test performance of exact match with index for comparison
        print("\nTesting exact match with index for comparison...")
        start_time = time.time()
        results = list(
            users.find(
                {
                    "bio": "This is user 1 who loves Python and SQL. User 1 has been using Python for years."
                }
            )
        )
        end_time = time.time()
        print(
            f"Found {len(results)} documents in {end_time - start_time:.4f} seconds"
        )


if __name__ == "__main__":
    main()
