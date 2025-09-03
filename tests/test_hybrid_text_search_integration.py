"""
Integration tests to verify the hybrid text search implementation.
"""

import pytest
from neosqlite import Connection
from neosqlite.collection.temporary_table_aggregation import (
    can_process_with_temporary_tables,
)


def test_pipeline_with_text_operator_can_be_processed():
    """Test that pipelines with $text operators can be processed with temporary tables."""
    # This pipeline should now be processable with temporary tables
    pipeline = [
        {"$match": {"status": "active"}},  # Regular match
        {"$match": {"$text": {"$search": "python"}}},  # Text search
        {"$sort": {"name": 1}},  # Sort
    ]

    # This should return True now that we allow $text operators
    assert can_process_with_temporary_tables(pipeline) == True


def test_complex_pipeline_with_text_operator():
    """Test a complex pipeline with text search works correctly."""
    with Connection(":memory:") as conn:
        # Create test data
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "Python Developer",
                    "description": "Works with Python programming language",
                    "status": "active",
                    "tags": ["backend", "scripting"],
                },
                {
                    "name": "Java Developer",
                    "description": "Works with Java programming language",
                    "status": "active",
                    "tags": ["backend", "enterprise"],
                },
                {
                    "name": "JavaScript Developer",
                    "description": "Works with JavaScript programming language",
                    "status": "inactive",
                    "tags": ["frontend", "web"],
                },
                {
                    "name": "Python Data Scientist",
                    "description": "Works with Python for data analysis",
                    "status": "active",
                    "tags": ["data", "science"],
                },
            ]
        )

        # Pipeline that should benefit from hybrid processing
        # 1. Filter by status (SQL)
        # 2. Text search (Python)
        # 3. Sort (SQL)
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "python"}}
            },  # Should filter to 2 docs
            {"$sort": {"name": 1}},  # Should sort the results
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 2 documents
        assert len(result) == 2

        # Both should be active status
        assert all(doc["status"] == "active" for doc in result)

        # Both should contain "python" in their description
        assert all("python" in doc["description"].lower() for doc in result)

        # Should be sorted by name
        names = [doc["name"] for doc in result]
        assert names == sorted(names)

        # Specific documents should be found
        assert result[0]["name"] == "Python Data Scientist"
        assert result[1]["name"] == "Python Developer"


if __name__ == "__main__":
    pytest.main([__file__])
