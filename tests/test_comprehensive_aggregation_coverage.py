"""
Comprehensive test to maximize coverage of query_helper.py aggregation logic
"""

import pytest
from neosqlite import Connection


def test_basic_aggregation_coverage():
    """Test basic aggregation scenarios to maximize code coverage."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert diverse test data
        test_documents = [
            {
                "name": "Alice",
                "age": 30,
                "department": "Engineering",
                "salary": 75000,
                "skills": ["python", "javascript"],
                "active": True,
            },
            {
                "name": "Bob",
                "age": 25,
                "department": "Marketing",
                "salary": 60000,
                "skills": ["design", "copywriting"],
                "active": True,
            },
            {
                "name": "Charlie",
                "age": 35,
                "department": "Engineering",
                "salary": 85000,
                "skills": ["python", "go", "rust"],
                "active": False,
            },
            {
                "name": "Diana",
                "age": 28,
                "department": "Engineering",
                "salary": 70000,
                "skills": ["javascript", "typescript"],
                "active": True,
            },
            {
                "name": "Eve",
                "age": 32,
                "department": "Marketing",
                "salary": 65000,
                "skills": ["analytics", "design"],
                "active": True,
            },
        ]

        collection.insert_many(test_documents)

        # Test 1: Simple $match
        results = list(
            collection.aggregate([{"$match": {"department": "Engineering"}}])
        )
        assert len(results) == 3

        # Test 2: $match with operators
        results = list(
            collection.aggregate([{"$match": {"age": {"$gte": 30}}}])
        )
        assert len(results) == 3

        # Test 3: $match with $in
        results = list(
            collection.aggregate(
                [
                    {
                        "$match": {
                            "department": {"$in": ["Engineering", "Marketing"]}
                        }
                    }
                ]
            )
        )
        assert len(results) == 5

        # Test 4: $sort
        results = list(collection.aggregate([{"$sort": {"age": -1}}]))
        assert len(results) == 5
        # First result should be oldest person
        assert results[0]["age"] == 35

        # Test 5: $limit
        results = list(
            collection.aggregate([{"$sort": {"age": 1}}, {"$limit": 2}])
        )
        assert len(results) == 2
        assert results[0]["age"] == 25
        assert results[1]["age"] == 28

        # Test 6: Complex $match with logical operators
        results = list(
            collection.aggregate(
                [
                    {
                        "$match": {
                            "$and": [
                                {"age": {"$gte": 25}},
                                {
                                    "$or": [
                                        {"department": "Engineering"},
                                        {"salary": {"$lt": 65000}},
                                    ]
                                },
                            ]
                        }
                    }
                ]
            )
        )
        assert len(results) >= 1

        # Test 7: $unwind simple
        results = list(collection.aggregate([{"$unwind": "$skills"}]))
        # Should have more results due to unwinding (5 docs * avg ~2.5 skills each)
        assert len(results) > 5

        # Test 8: $group simple
        results = list(
            collection.aggregate(
                [{"$group": {"_id": "$department", "count": {"$sum": 1}}}]
            )
        )
        assert len(results) == 2  # Engineering and Marketing
        dept_counts = {doc["_id"]: doc["count"] for doc in results}
        assert dept_counts["Engineering"] == 3
        assert dept_counts["Marketing"] == 2


def test_unwind_then_group_coverage():
    """Test $unwind followed by $group to hit the specific optimization path."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data that should trigger the specific $unwind+$group optimization
        collection.insert_many(
            [
                {"category": "tech", "items": ["laptop", "phone"]},
                {"category": "tech", "items": ["laptop", "tablet"]},
                {"category": "home", "items": ["sofa", "table", "laptop"]},
            ]
        )

        # This specific pattern should trigger the optimization path at lines 1388-1515:
        # $unwind followed immediately by $group with simple string field references
        pipeline = [
            {"$unwind": "$items"},  # String syntax with $ prefix
            {
                "$group": {
                    "_id": "$items",  # String syntax with $ prefix - this should trigger the path
                    "count": {"$sum": 1},
                }
            },
        ]

        # Execute the pipeline - this should use the SQL optimization path
        results = list(collection.aggregate(pipeline))

        # Verify results
        assert len(results) >= 3  # At least laptop, phone, tablet, sofa, table
        item_counts = {doc["_id"]: doc["count"] for doc in results}
        # Laptop appears in all 3 documents
        assert item_counts["laptop"] == 3


if __name__ == "__main__":
    pytest.main([__file__])
