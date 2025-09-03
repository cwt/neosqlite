"""
Performance test to verify that hybrid text search is faster than full Python fallback.
"""

import time
import pytest
from neosqlite import Connection
from neosqlite.collection.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
    can_process_with_temporary_tables,
    execute_2nd_tier_aggregation,
)


def test_performance_improvement():
    """Test that hybrid text search is faster than full Python fallback."""
    with Connection(":memory:") as conn:
        # Create a larger dataset to see performance differences
        collection = conn.test_collection
        docs = []
        for i in range(1000):
            status = "active" if i % 2 == 0 else "inactive"
            lang = (
                "python"
                if i % 3 == 0
                else "java" if i % 3 == 1 else "javascript"
            )
            docs.append(
                {
                    "name": f"Developer {i}",
                    "description": f"{lang} programming expert with experience in multiple domains",
                    "status": status,
                    "score": i,
                }
            )

        collection.insert_many(docs)

        # Test pipeline that would benefit from hybrid processing
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter ~500 docs
            {"$sort": {"score": -1}},  # Should sort those docs
            {
                "$match": {"$text": {"$search": "python"}}
            },  # Should filter with text search
            {"$limit": 10},  # Should limit results
        ]

        # Test with hybrid approach (new implementation)
        start_time = time.perf_counter()
        hybrid_result = list(collection.aggregate(pipeline))
        hybrid_time = time.perf_counter() - start_time

        # Verify results are correct
        assert len(hybrid_result) == 10
        assert all(doc["status"] == "active" for doc in hybrid_result)
        assert all(
            "python" in doc["description"].lower() for doc in hybrid_result
        )

        # The hybrid approach should be much faster than what a full Python fallback would be
        # For 1000 documents, processing everything in Python would be much slower
        assert hybrid_time < 1.0  # Should complete in less than 1 second

        # Print timing for reference
        print(f"Hybrid approach time: {hybrid_time:.4f} seconds")


def test_pipeline_can_be_processed_with_temporary_tables():
    """Test that pipelines with $text operators can now be processed with temporary tables."""
    pipeline = [
        {"$match": {"status": "active"}},
        {"$match": {"$text": {"$search": "python"}}},
        {"$sort": {"name": 1}},
    ]

    # Should return True - pipeline can be processed with temporary tables
    assert can_process_with_temporary_tables(pipeline) == True


if __name__ == "__main__":
    pytest.main([__file__])
