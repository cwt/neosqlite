# coding: utf-8
"""
Test the SQL concept for $unwind + $group optimization
"""
import neosqlite
import pytest


def test_unwind_group_sql_concept():
    """Test the SQL concept for $unwind + $group optimization"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "tags": ["python", "javascript", "python"]},
                {"category": "B", "tags": ["java", "python", "java"]},
            ]
        )

        # Concept: $unwind + $group in a single SQL query
        # First, we unwind using json_each
        # Then we group by the unwound values and count
        cmd = f"""
        SELECT json_each.value as _id, COUNT(*) as count
        FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.tags'))
        GROUP BY json_each.value
        ORDER BY json_each.value
        """

        # Execute the concept query
        cursor = collection.db.execute(cmd)
        results = []
        for row in cursor.fetchall():
            results.append({"_id": row[0], "count": row[1]})

        # Should have 3 groups: java(2), javascript(1), python(3)
        assert len(results) == 3

        # Check the results
        counts = {doc["_id"]: doc["count"] for doc in results}
        assert counts["java"] == 2
        assert counts["javascript"] == 1
        assert counts["python"] == 3

        # Check ordering
        tags = [doc["_id"] for doc in results]
        assert tags == ["java", "javascript", "python"]


def test_unwind_group_with_field_sql_concept():
    """Test the SQL concept for $unwind + $group by another field"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "tags": ["python", "javascript"]},
                {"category": "B", "tags": ["java", "python"]},
            ]
        )

        # Concept: $unwind + $group by category (not the unwound field)
        # We need to group by the category field while unwinding tags
        cmd = f"""
        SELECT json_extract({collection.name}.data, '$.category') as _id, COUNT(*) as count
        FROM {collection.name}, json_each(json_extract({collection.name}.data, '$.tags'))
        GROUP BY json_extract({collection.name}.data, '$.category')
        ORDER BY json_extract({collection.name}.data, '$.category')
        """

        # Execute the concept query
        cursor = collection.db.execute(cmd)
        results = []
        for row in cursor.fetchall():
            results.append({"_id": row[0], "count": row[1]})

        # Should have 2 groups: A(2), B(2)
        assert len(results) == 2

        # Check the results
        counts = {doc["_id"]: doc["count"] for doc in results}
        assert counts["A"] == 2
        assert counts["B"] == 2


if __name__ == "__main__":
    pytest.main([__file__])
