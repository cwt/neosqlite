# coding: utf-8
"""
Test cases for advanced $group accumulator operators in NeoSQLite
"""
import neosqlite
import pytest


def test_group_stage_with_first_n_accumulator():
    """Test $group stage with $firstN accumulator to get first N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $firstN (get first 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_values": {"$firstN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have first 2 values [10, 30]
        assert {"_id": "A", "first_values": [10, 30]} in result
        # Category B should have first 2 values [20, 40]
        assert {"_id": "B", "first_values": [20, 40]} in result


def test_group_stage_with_last_n_accumulator():
    """Test $group stage with $lastN accumulator to get last N values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last_n"]

        # Insert test data in specific order
        docs = [
            {"category": "A", "value": 10, "name": "first"},
            {"category": "A", "value": 30, "name": "second"},
            {"category": "A", "value": 50, "name": "third"},
            {"category": "A", "value": 70, "name": "fourth"},
            {"category": "B", "value": 20, "name": "fifth"},
            {"category": "B", "value": 40, "name": "sixth"},
        ]
        collection.insert_many(docs)

        # Test group with $lastN (get last 2 values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "last_values": {"$lastN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have last 2 values [50, 70]
        assert {"_id": "A", "last_values": [50, 70]} in result
        # Category B should have last 2 values [20, 40]
        assert {"_id": "B", "last_values": [20, 40]} in result


def test_group_stage_with_min_n_accumulator():
    """Test $group stage with $minN accumulator to get N minimum values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_min_n"]

        # Insert test data
        docs = [
            {"category": "A", "value": 70},
            {"category": "A", "value": 30},
            {"category": "A", "value": 50},
            {"category": "A", "value": 10},
            {"category": "B", "value": 40},
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $minN (get 2 minimum values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "min_values": {"$minN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have 2 minimum values [10, 30]
        assert {"_id": "A", "min_values": [10, 30]} in result
        # Category B should have 2 minimum values [20, 40]
        assert {"_id": "B", "min_values": [20, 40]} in result


def test_group_stage_with_max_n_accumulator():
    """Test $group stage with $maxN accumulator to get N maximum values in group"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_max_n"]

        # Insert test data
        docs = [
            {"category": "A", "value": 70},
            {"category": "A", "value": 30},
            {"category": "A", "value": 50},
            {"category": "A", "value": 10},
            {"category": "B", "value": 40},
            {"category": "B", "value": 20},
        ]
        collection.insert_many(docs)

        # Test group with $maxN (get 2 maximum values)
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "max_values": {"$maxN": {"input": "$value", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have 2 maximum values [70, 50]
        assert {"_id": "A", "max_values": [70, 50]} in result
        # Category B should have 2 maximum values [40, 20]
        assert {"_id": "B", "max_values": [40, 20]} in result


def test_group_stage_with_first_n_edge_cases():
    """Test $group stage with $firstN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_first_n_edge_cases"]

        # Test with fewer documents than n
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $firstN where n=5 but only 2/1 documents exist
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_values": {"$firstN": {"input": "$value", "n": 5}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have all values [10, 20]
        assert {"_id": "A", "first_values": [10, 20]} in result
        # Category B should have all values [30]
        assert {"_id": "B", "first_values": [30]} in result


def test_group_stage_with_last_n_edge_cases():
    """Test $group stage with $lastN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_last_n_edge_cases"]

        # Test with fewer documents than n
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $lastN where n=5 but only 2/1 documents exist
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "last_values": {"$lastN": {"input": "$value", "n": 5}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have all values [10, 20]
        assert {"_id": "A", "last_values": [10, 20]} in result
        # Category B should have all values [30]
        assert {"_id": "B", "last_values": [30]} in result


def test_group_stage_with_min_n_edge_cases():
    """Test $group stage with $minN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_min_n_edge_cases"]

        # Test with duplicate values
        docs = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $minN
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "min_values": {"$minN": {"input": "$value", "n": 3}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [10, 10, 20]
        assert {"_id": "A", "min_values": [10, 10, 20]} in result
        # Category B should have [30]
        assert {"_id": "B", "min_values": [30]} in result


def test_group_stage_with_max_n_edge_cases():
    """Test $group stage with $maxN accumulator edge cases"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_max_n_edge_cases"]

        # Test with duplicate values
        docs = [
            {"category": "A", "value": 20},
            {"category": "A", "value": 20},
            {"category": "A", "value": 10},
            {"category": "B", "value": 30},
        ]
        collection.insert_many(docs)

        # Test group with $maxN
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "max_values": {"$maxN": {"input": "$value", "n": 3}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A should have [20, 20, 10]
        assert {"_id": "A", "max_values": [20, 20, 10]} in result
        # Category B should have [30]
        assert {"_id": "B", "max_values": [30]} in result


def test_group_stage_combined_advanced_accumulators():
    """Test $group stage with multiple advanced accumulators combined"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_group_combined_advanced"]

        # Insert test data
        docs = [
            {"category": "A", "value": 10, "score": 100},
            {"category": "A", "value": 30, "score": 200},
            {"category": "A", "value": 50, "score": 150},
            {"category": "A", "value": 70, "score": 300},
            {"category": "B", "value": 20, "score": 250},
            {"category": "B", "value": 40, "score": 180},
        ]
        collection.insert_many(docs)

        # Test group with multiple advanced accumulators
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "first_two_values": {
                        "$firstN": {"input": "$value", "n": 2}
                    },
                    "last_two_scores": {"$lastN": {"input": "$score", "n": 2}},
                    "top_two_scores": {"$maxN": {"input": "$score", "n": 2}},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        result = collection.aggregate(pipeline)

        assert len(result) == 2
        # Category A
        assert result[0]["_id"] == "A"
        assert result[0]["first_two_values"] == [10, 30]
        assert result[0]["last_two_scores"] == [150, 300]
        assert result[0]["top_two_scores"] == [
            300,
            200,
        ]  # Top 2 scores: 300, 200
        # Category B
        assert result[1]["_id"] == "B"
        assert result[1]["first_two_values"] == [20, 40]
        assert result[1]["last_two_scores"] == [250, 180]
        assert result[1]["top_two_scores"] == [
            250,
            180,
        ]  # Top 2 scores: 250, 180


if __name__ == "__main__":
    pytest.main([__file__])
