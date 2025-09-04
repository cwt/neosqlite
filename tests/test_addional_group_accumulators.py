# coding: utf-8
"""
Test cases for missing $group accumulator operators in NeoSQLite
"""
import math
import neosqlite
import pytest


def test_group_stage_with_first_accumulator():
    """Test $group stage with $first accumulator to get first value in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "B", "value": 20, "name": "fourth"},
            {"category": "B", "value": 40, "name": "fifth"},
        ]
        collection.insert_many(docs)

        # Test group with $first
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first value 10 (first document)
        assert {"_id": "A", "first_value": 10} in result
        # Category B should have first value 20 (first document)
        assert {"_id": "B", "first_value": 20} in result


def test_group_stage_with_last_accumulator():
    """Test $group stage with $last accumulator to get last value in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "B", "value": 20, "name": "fourth"},
            {"category": "B", "value": 40, "name": "fifth"},
        ]
        collection.insert_many(docs)

        # Test group with $last
        pipeline = [
            {"$group": {"_id": "$category", "last_value": {"$last": "$value"}}},
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have last value 50 (last document)
        assert {"_id": "A", "last_value": 50} in result
        # Category B should have last value 40 (last document)
        assert {"_id": "B", "last_value": 40} in result


def test_group_stage_with_std_dev_pop_accumulator():
    """Test $group stage with $stdDevPop accumulator for population standard deviation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_pop"]

        # Insert test data
        docs = [
            {"category": "A", "value": 2},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 5},
            {"category": "A", "value": 5},
            {"category": "A", "value": 7},
            {"category": "A", "value": 9},
            {"category": "B", "value": 1},
            {"category": "B", "value": 3},
            {"category": "B", "value": 5},
            {"category": "B", "value": 7},
            {"category": "B", "value": 9},
        ]
        collection.insert_many(docs)

        # Test group with $stdDevPop
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev": {"$stdDevPop": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Verify standard deviation calculations
        for doc in result:
            if doc["_id"] == "A":
                # Population std dev for [2,4,4,4,5,5,7,9] ≈ 2.0
                assert abs(doc["std_dev"] - 2.0) < 0.01
            elif doc["_id"] == "B":
                # Population std dev for [1,3,5,7,9] = 2.828...
                assert abs(doc["std_dev"] - math.sqrt(8)) < 0.01


def test_group_stage_with_std_dev_samp_accumulator():
    """Test $group stage with $stdDevSamp accumulator for sample standard deviation"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_samp"]

        # Insert test data
        docs = [
            {"category": "A", "value": 2},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 4},
            {"category": "A", "value": 5},
            {"category": "A", "value": 5},
            {"category": "A", "value": 7},
            {"category": "A", "value": 9},
            {"category": "B", "value": 1},
            {"category": "B", "value": 3},
            {"category": "B", "value": 5},
            {"category": "B", "value": 7},
            {"category": "B", "value": 9},
        ]
        collection.insert_many(docs)

        # Test group with $stdDevSamp
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev": {"$stdDevSamp": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Verify sample standard deviation calculations
        for doc in result:
            if doc["_id"] == "A":
                # Sample std dev for [2,4,4,4,5,5,7,9] ≈ 2.138
                assert abs(doc["std_dev"] - 2.138) < 0.01
            elif doc["_id"] == "B":
                # Sample std dev for [1,3,5,7,9] ≈ 3.162
                assert abs(doc["std_dev"] - math.sqrt(10)) < 0.01


def test_group_stage_with_merge_objects_accumulator():
    """Test $group stage with $mergeObjects accumulator to merge documents in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_merge_objects"]

        # Insert test data
        docs = [
            {"category": "A", "data1": {"x": 1}, "data2": {"y": 2}},
            {"category": "A", "data1": {"x": 3}, "data2": {"z": 4}},
            {"category": "B", "data1": {"x": 5}, "data2": {"y": 6}},
            {"category": "B", "data1": {"x": 7}, "data2": {"z": 8}},
        ]
        collection.insert_many(docs)

        # Test group with $mergeObjects on data1 field
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "merged_data": {"$mergeObjects": "$data1"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have merged data from data1 fields
        assert {
            "_id": "A",
            "merged_data": {"x": 3},
        } in result  # Last value wins
        # Category B should have merged data from data1 fields
        assert {
            "_id": "B",
            "merged_data": {"x": 7},
        } in result  # Last value wins


def test_group_stage_with_first_and_last_combined():
    """Test $group stage with $first and $last accumulators combined"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_last_combined"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "timestamp": 1},
            {"category": "A", "value": 30, "timestamp": 2},
            {"category": "A", "value": 50, "timestamp": 3},
            {"category": "B", "value": 20, "timestamp": 1},
            {"category": "B", "value": 40, "timestamp": 2},
        ]
        collection.insert_many(docs)

        # Test group with $first and $last combined
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_value": {"$first": "$value"},
                    "last_value": {"$last": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first=10 and last=50
        assert {"_id": "A", "first_value": 10, "last_value": 50} in result
        # Category B should have first=20 and last=40
        assert {"_id": "B", "first_value": 20, "last_value": 40} in result


def test_group_stage_with_std_dev_edge_cases():
    """Test $group stage with $stdDevPop and $stdDevSamp edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_std_dev_edge_cases"]

        # Test with single value (std dev should be 0)
        docs = [
            {"category": "A", "value": 5},
            {"category": "B", "value": 10},
            {"category": "B", "value": 10},
        ]
        collection.insert_many(docs)

        # Test group with both std dev accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "std_dev_pop": {"$stdDevPop": "$value"},
                    "std_dev_samp": {"$stdDevSamp": "$value"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A has only one value, so std dev should be 0
        assert {"_id": "A", "std_dev_pop": 0.0, "std_dev_samp": 0.0} in result
        # Category B has two identical values, so std dev should be 0
        assert result[1]["_id"] == "B"
        assert result[1]["std_dev_pop"] == 0.0
        # For sample std dev with n=2, it should also be 0 when values are identical
        assert result[1]["std_dev_samp"] == 0.0


def test_group_stage_with_merge_objects_complex():
    """Test $group stage with $mergeObjects with more complex nested objects"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_merge_objects_complex"]

        # Insert test data with nested objects
        docs = [
            {"category": "A", "config": {"database": "mysql", "port": 3306}},
            {"category": "A", "config": {"cache": "redis", "port": 6379}},
            {
                "category": "A",
                "config": {"database": "postgresql", "ssl": True},
            },
        ]
        collection.insert_many(docs)

        # Test group with $mergeObjects
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "merged_config": {"$mergeObjects": "$config"},
                }
            },
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 1
        # The merged config should contain all keys, with later values overwriting earlier ones
        expected_config = {
            "database": "postgresql",
            "port": 6379,
            "cache": "redis",
            "ssl": True,
        }
        assert result[0]["merged_config"] == expected_config


if __name__ == "__main__":
    pytest.main([__file__])
