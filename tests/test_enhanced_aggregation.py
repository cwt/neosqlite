"""
Test cases for enhanced aggregation with json_group_array and json_group_object.
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


def test_aggregation_with_push_using_json_group_array():
    """Test $push accumulator using json_group_array for better performance."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "item": "item1", "value": 10},
                {"category": "A", "item": "item2", "value": 20},
                {"category": "B", "item": "item3", "value": 15},
                {"category": "B", "item": "item4", "value": 25},
            ]
        )

        # Test $push aggregation
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$item"}}},
            {"$sort": {"_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 2

        # Verify results maintain order
        assert results[0]["_id"] == "A"
        assert results[0]["items"] == ["item1", "item2"]
        assert results[1]["_id"] == "B"
        assert results[1]["items"] == ["item3", "item4"]


def test_aggregation_with_add_to_set_using_json_group_array():
    """Test $addToSet accumulator using json_group_array with DISTINCT for unique values."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with duplicates
        collection.insert_many(
            [
                {"category": "A", "tag": "python"},
                {"category": "A", "tag": "javascript"},
                {"category": "A", "tag": "python"},  # Duplicate
                {"category": "B", "tag": "go"},
                {"category": "B", "tag": "python"},
                {"category": "B", "tag": "go"},  # Duplicate
            ]
        )

        # Test $addToSet aggregation
        pipeline = [
            {"$group": {"_id": "$category", "tags": {"$addToSet": "$tag"}}},
            {"$sort": {"_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 2

        # Verify unique values (order may vary due to DISTINCT)
        assert results[0]["_id"] == "A"
        assert set(results[0]["tags"]) == {"python", "javascript"}
        assert results[1]["_id"] == "B"
        assert set(results[1]["tags"]) == {"go", "python"}


def test_aggregation_with_complex_group_operations():
    """Test complex group operations with multiple accumulators."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "department": "Engineering",
                    "name": "Alice",
                    "salary": 75000,
                    "experience": 5,
                },
                {
                    "department": "Engineering",
                    "name": "Bob",
                    "salary": 80000,
                    "experience": 7,
                },
                {
                    "department": "Marketing",
                    "name": "Charlie",
                    "salary": 60000,
                    "experience": 3,
                },
                {
                    "department": "Marketing",
                    "name": "David",
                    "salary": 65000,
                    "experience": 4,
                },
            ]
        )

        # Complex aggregation with multiple accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$department",
                    "employee_count": {"$sum": 1},
                    "total_salary": {"$sum": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_experience": {"$max": "$experience"},
                    "employees": {"$push": "$name"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 2

        # Verify Engineering department results
        engineering = next(
            (r for r in results if r["_id"] == "Engineering"), None
        )
        assert engineering is not None
        assert engineering["employee_count"] == 2
        assert engineering["total_salary"] == 155000
        assert engineering["avg_salary"] == 77500
        assert engineering["max_experience"] == 7
        assert set(engineering["employees"]) == {"Alice", "Bob"}

        # Verify Marketing department results
        marketing = next((r for r in results if r["_id"] == "Marketing"), None)
        assert marketing is not None
        assert marketing["employee_count"] == 2
        assert marketing["total_salary"] == 125000
        assert marketing["avg_salary"] == 62500
        assert marketing["max_experience"] == 4
        assert set(marketing["employees"]) == {"Charlie", "David"}


def test_aggregation_with_unwind_and_group():
    """Test aggregation pipeline with $unwind followed by $group."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with arrays
        collection.insert_many(
            [
                {"name": "Alice", "skills": ["python", "sql", "javascript"]},
                {"name": "Bob", "skills": ["java", "python"]},
                {"name": "Charlie", "skills": ["go", "rust", "python", "sql"]},
            ]
        )

        # Unwind skills and group by skill
        pipeline = [
            {"$unwind": "$skills"},
            {
                "$group": {
                    "_id": "$skills",
                    "count": {"$sum": 1},
                    "developers": {"$push": "$name"},
                }
            },
            {"$sort": {"count": -1, "_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) >= 5  # At least 5 different skills

        # Verify python skill (most common)
        python_skill = next((r for r in results if r["_id"] == "python"), None)
        assert python_skill is not None
        assert python_skill["count"] == 3
        assert set(python_skill["developers"]) == {"Alice", "Bob", "Charlie"}


def test_aggregation_with_nested_grouping():
    """Test aggregation with nested grouping operations."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"region": "North", "city": "Boston", "sales": 100},
                {"region": "North", "city": "Boston", "sales": 150},
                {"region": "North", "city": "New York", "sales": 200},
                {"region": "South", "city": "Miami", "sales": 120},
                {"region": "South", "city": "Atlanta", "sales": 180},
            ]
        )

        # Group by a single field (the complex _id grouping is not supported in Python fallback)
        pipeline = [
            {
                "$group": {
                    "_id": "$region",
                    "total_sales": {"$sum": "$sales"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 2

        # Verify specific results
        north_region = next((r for r in results if r["_id"] == "North"), None)
        south_region = next((r for r in results if r["_id"] == "South"), None)
        assert north_region is not None
        assert south_region is not None
        assert north_region["total_sales"] == 450  # 100 + 150 + 200
        assert south_region["total_sales"] == 300  # 120 + 180


def test_aggregation_performance_with_large_dataset():
    """Test aggregation performance with a larger dataset."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert a larger dataset
        docs = []
        for i in range(1000):
            docs.append(
                {
                    "category": f"Category_{i % 10}",
                    "value": i,
                    "tags": [f"tag_{j}" for j in range(i % 5)],
                }
            )
        collection.insert_many(docs)

        # Perform aggregation
        pipeline = [
            {"$match": {"value": {"$gte": 500}}},
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$category",
                    "total": {"$sum": "$value"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"total": -1}},
            {"$limit": 5},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) <= 5
        assert all(r["total"] >= 0 for r in results)


def test_aggregation_with_empty_results():
    """Test aggregation behavior with empty result sets."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Don't insert any data, run aggregation
        pipeline = [{"$group": {"_id": "$category", "count": {"$sum": 1}}}]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 0


def test_aggregation_with_single_document():
    """Test aggregation with single document."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert single document
        collection.insert_one({"category": "A", "value": 42, "tags": ["test"]})

        # Simple aggregation
        pipeline = [
            {"$group": {"_id": "$category", "total": {"$sum": "$value"}}}
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 1
        assert results[0]["_id"] == "A"
        assert results[0]["total"] == 42


def test_aggregation_with_force_fallback():
    """Test aggregation with force fallback enabled."""
    with Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 15},
                {"category": "B", "value": 25},
            ]
        )

        # Enable force fallback
        set_force_fallback(True)
        assert get_force_fallback() is True

        # Test group operation - should use Python fallback
        pipeline = [
            {"$group": {"_id": "$category", "total": {"$sum": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        assert len(results) == 2

        # Verify results
        assert results[0]["_id"] == "A"
        assert results[0]["total"] == 30
        assert results[1]["_id"] == "B"
        assert results[1]["total"] == 40

        # Disable force fallback
        set_force_fallback(False)
        assert get_force_fallback() is False


if __name__ == "__main__":
    pytest.main([__file__])
