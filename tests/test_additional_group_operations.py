# coding: utf-8
"""
Test cases for additional $group accumulators: $push and $addToSet
"""
import neosqlite
import pytest


def test_group_stage_with_push_accumulator():
    """Test $group stage with $push accumulator to build arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 30},
            {"category": "B", "value": 40},
            {"category": "A", "value": 50},
        ]
        collection.insert_many(docs)

        # Test group with $push
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have items [10, 30, 50] in order
        assert {"_id": "A", "items": [10, 30, 50]} in result
        # Category B should have items [20, 40] in order
        assert {"_id": "B", "items": [20, 40]} in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_accumulator():
    """Test $group stage with $addToSet accumulator to build unique value arrays"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set"]

        # Insert test data with duplicate values
        docs = [
            {"category": "A", "tag": "python"},
            {"category": "B", "tag": "java"},
            {"category": "A", "tag": "javascript"},
            {"category": "B", "tag": "python"},
            {"category": "A", "tag": "python"},  # Duplicate
            {"category": "B", "tag": "java"},  # Duplicate
            {"category": "A", "tag": "go"},
        ]
        collection.insert_many(docs)

        # Test group with $addToSet
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$addToSet": "$tag"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check that each category has the correct unique tags
        for doc in result:
            if doc["_id"] == "A":
                # Should have python, javascript, go (order may vary due to set behavior)
                assert len(doc["items"]) == 3
                assert set(doc["items"]) == {"python", "javascript", "go"}
            elif doc["_id"] == "B":
                # Should have java, python (order may vary due to set behavior)
                assert len(doc["items"]) == 2
                assert set(doc["items"]) == {"java", "python"}

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_push_and_other_accumulators():
    """Test $group stage with $push accumulator combined with other accumulators"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push_combined"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "B", "value": 20, "name": "second"},
            {"category": "A", "value": 30, "name": "third"},
            {"category": "B", "value": 40, "name": "fourth"},
        ]
        collection.insert_many(docs)

        # Test group with $push and $sum
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "names": {"$push": "$name"},
                    "total": {"$sum": "$value"},
                    "count": {"$count": {}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have names ["first", "third"], total 40, count 2
        assert {
            "_id": "A",
            "names": ["first", "third"],
            "total": 40,
            "count": 2,
        } in result
        # Category B should have names ["second", "fourth"], total 60, count 2
        assert {
            "_id": "B",
            "names": ["second", "fourth"],
            "total": 60,
            "count": 2,
        } in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_and_other_accumulators():
    """Test $group stage with $addToSet accumulator combined with other accumulators"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set_combined"]

        # Insert test data with duplicate values
        docs = [
            {"category": "A", "tag": "python", "value": 10},
            {"category": "B", "tag": "java", "value": 20},
            {"category": "A", "tag": "javascript", "value": 30},
            {"category": "B", "tag": "python", "value": 40},
            {"category": "A", "tag": "python", "value": 50},  # Duplicate tag
        ]
        collection.insert_many(docs)

        # Test group with $addToSet and $sum
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "items": {"$addToSet": "$tag"},
                    "total": {"$sum": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check results
        for doc in result:
            if doc["_id"] == "A":
                # Should have unique tags and sum of values
                assert len(doc["items"]) == 2  # python, javascript
                assert set(doc["items"]) == {"python", "javascript"}
                assert doc["total"] == 90  # 10 + 30 + 50
            elif doc["_id"] == "B":
                # Should have unique tags and sum of values
                assert len(doc["items"]) == 2  # java, python
                assert set(doc["items"]) == {"java", "python"}
                assert doc["total"] == 60  # 20 + 40

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_push_null_values():
    """Test $group stage with $push accumulator handling null values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_push_null"]

        # Insert test data with null values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": None},
            {"category": "A"},  # Missing value field
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $push including null values
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$push": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [10, None, None] (missing field becomes None)
        assert {"_id": "A", "items": [10, None, None]} in result
        # Category B should have [20]
        assert {"_id": "B", "items": [20]} in result

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


def test_group_stage_with_add_to_set_null_values():
    """Test $group stage with $addToSet accumulator handling null values"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_add_to_set_null"]

        # Insert test data with duplicate null values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": None},
            {"category": "A"},  # Missing value field
            {"category": "A", "value": 10},  # Duplicate
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $addToSet including null values
        pipeline = [
            {"$group": {"_id": "$category", "items": {"$addToSet": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Check that each category has the correct unique values (including null)
        for doc in result:
            if doc["_id"] == "A":
                # Should have 10, None (duplicates removed, nulls treated as equal)
                assert len(doc["items"]) == 2
                assert 10 in doc["items"]
                assert None in doc["items"]
            elif doc["_id"] == "B":
                # Should have 20
                assert doc["items"] == [20]

        # Sort by category for consistent ordering
        result.sort(key=lambda x: x["_id"])
        categories = [doc["_id"] for doc in result]
        assert categories == ["A", "B"]


if __name__ == "__main__":
    pytest.main([__file__])
