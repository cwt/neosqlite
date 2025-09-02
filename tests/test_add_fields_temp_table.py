"""
Test for $addFields support in temporary table aggregation.
"""

import pytest
from neosqlite import Connection


def test_add_fields_basic():
    """Test basic $addFields functionality with temporary table aggregation."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test simple field copying with $addFields
        pipeline = [{"$addFields": {"userName": "$name", "userAge": "$age"}}]

        # Check if pipeline can be processed with temporary tables
        from neosqlite.collection.temporary_table_aggregation import (
            can_process_with_temporary_tables,
        )

        assert can_process_with_temporary_tables(pipeline)

        # Process the pipeline
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        processor = TemporaryTableAggregationProcessor(collection)
        results = processor.process_pipeline(pipeline)

        # Verify results
        assert len(results) == 3
        for doc in results:
            assert "userName" in doc
            assert "userAge" in doc
            assert doc["userName"] == doc["name"]
            assert doc["userAge"] == doc["age"]


def test_add_fields_with_match():
    """Test $addFields combined with $match using temporary table aggregation."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Test $match followed by $addFields
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$addFields": {"dept": "$department"}},
        ]

        # Check if pipeline can be processed with temporary tables
        from neosqlite.collection.temporary_table_aggregation import (
            can_process_with_temporary_tables,
        )

        assert can_process_with_temporary_tables(pipeline)

        # Process the pipeline
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        processor = TemporaryTableAggregationProcessor(collection)
        results = processor.process_pipeline(pipeline)

        # Verify results
        assert len(results) == 2
        for doc in results:
            assert doc["department"] == "Engineering"
            assert "dept" in doc
            assert doc["dept"] == "Engineering"


if __name__ == "__main__":
    pytest.main([__file__])
