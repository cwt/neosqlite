# coding: utf-8
"""
Enhanced implementation plan for multiple $unwind stages with json_each()
"""
import neosqlite
import pytest


def test_multiple_unwind_sql_concept():
    """Test the SQL concept for multiple $unwind stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "tags": ["python", "javascript"],
                    "categories": ["programming", "web"],
                }
            ]
        )

        # Concept: Multiple json_each() in one query
        # This is what we want to generate:
        cmd = f"""
        SELECT {collection.name}.id,
               json_set(
                   json_set({collection.name}.data, '$."tags"', je1.value),
                   '$."categories"', je2.value
               ) as data
        FROM {collection.name},
             json_each(json_extract({collection.name}.data, '$.tags')) as je1,
             json_each(json_extract({collection.name}.data, '$.categories')) as je2
        """

        # Execute the concept query
        cursor = collection.db.execute(cmd)
        results = []
        for row in cursor.fetchall():
            results.append(collection._load(row[0], row[1]))

        # Should have 2*2 = 4 results
        assert len(results) == 4

        # Check the structure
        tags = [doc["tags"] for doc in results]
        categories = [doc["categories"] for doc in results]

        assert all(tag in ["python", "javascript"] for tag in tags)
        assert all(
            category in ["programming", "web"] for category in categories
        )


def test_multiple_unwind_with_match_concept():
    """Test the SQL concept for multiple $unwind with $match"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "name": "Alice",
                    "status": "active",
                    "tags": ["python", "javascript"],
                    "categories": ["programming", "web"],
                },
                {
                    "name": "Bob",
                    "status": "inactive",
                    "tags": ["java"],
                    "categories": ["enterprise"],
                },
            ]
        )

        # Concept: Multiple json_each() with WHERE clause
        cmd = f"""
        SELECT {collection.name}.id,
               json_set(
                   json_set({collection.name}.data, '$."tags"', je1.value),
                   '$."categories"', je2.value
               ) as data
        FROM {collection.name},
             json_each(json_extract({collection.name}.data, '$.tags')) as je1,
             json_each(json_extract({collection.name}.data, '$.categories')) as je2
        WHERE json_extract({collection.name}.data, '$.status') = 'active'
        """

        # Execute the concept query
        cursor = collection.db.execute(cmd)
        results = []
        for row in cursor.fetchall():
            results.append(collection._load(row[0], row[1]))

        # Should have 2*2 = 4 results from Alice only
        assert len(results) == 4

        # Check that only Alice appears
        names = [doc["name"] for doc in results]
        assert all(name == "Alice" for name in names)


if __name__ == "__main__":
    pytest.main([__file__])
