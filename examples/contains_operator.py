#!/usr/bin/env python3
"""
Example script demonstrating the new $contains operator in neosqlite.
"""

import neosqlite


def main():
    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data
        users.insert_many(
            [
                {
                    "name": "Alice Smith",
                    "email": "alice@example.com",
                    "bio": "Loves Python and SQL",
                },
                {
                    "name": "Bob Johnson",
                    "email": "bob@example.com",
                    "bio": "Enjoys JavaScript and HTML",
                },
                {
                    "name": "Charlie Brown",
                    "email": "charlie@example.com",
                    "bio": "Prefers Go and Rust",
                },
                {
                    "name": "Diana Prince",
                    "email": "diana@example.com",
                    "bio": "Expert in Python and Django",
                },
            ]
        )

        print("All users:")
        for user in users.find():
            print(f"  {user['name']}: {user['bio']} (ID: {user['_id']})")

        print("\nUsers whose bio contains 'python' (case-insensitive):")
        for user in users.find({"bio": {"$contains": "python"}}):
            print(f"  {user['name']}: {user['bio']}")

        print("\nUsers whose name contains 'ali' (case-insensitive):")
        for user in users.find({"name": {"$contains": "ali"}}):
            print(f"  {user['name']}: {user['bio']}")

        print(
            "\nNote: The $contains operator performs substring searches and may not"
        )
        print(
            "efficiently use indexes, especially for large datasets. However, for"
        )
        print(
            "simple substring matching, $contains is faster than $regex because it"
        )
        print(
            "uses optimized string operations instead of regular expression compilation."
        )
        print(
            "For high-performance text search requirements, consider using SQLite's"
        )
        print("FTS extensions or other specialized search solutions.")


if __name__ == "__main__":
    main()
