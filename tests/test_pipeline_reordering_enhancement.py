# coding: utf-8
"""
Additional test cases for pipeline reordering optimization
"""
import neosqlite
import pytest


def test_pipeline_reordering_with_complex_logical_operators():
    """Test pipeline reordering with complex logical operators in match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 5}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "tags": [f"tag{j}" for j in range(3)],
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Test pipeline with complex logical operators in match
        pipeline = [
            {"$unwind": "$tags"},
            {
                "$match": {
                    "$or": [
                        {"category": "Category2"},
                        {"status": "active", "score": {"$gt": 50}},
                    ]
                }
            },
            {"$limit": 10},
        ]

        # The optimization should reorder this to put match first
        result = collection.aggregate(pipeline)

        # Should still work correctly
        assert len(result) <= 10

        # All documents should match the criteria
        for doc in result:
            category_match = doc["category"] == "Category2"
            status_score_match = doc["status"] == "active" and doc["score"] > 50
            assert category_match or status_score_match


def test_pipeline_reordering_with_and_operators():
    """Test pipeline reordering with $and operators in match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        docs = []
        for i in range(50):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 3}",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "score": i,
                    "value": i * 2,
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("score")

        # Test pipeline with $and operators
        pipeline = [
            {"$sort": {"value": -1}},
            {
                "$match": {
                    "$and": [
                        {"category": "Category1"},
                        {"status": "active"},
                        {"score": {"$gte": 20}},
                    ]
                }
            },
            {"$limit": 5},
        ]

        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 5

        # All should match all criteria
        for doc in result:
            assert doc["category"] == "Category1"
            assert doc["status"] == "active"
            assert doc["score"] >= 20

        # Should be sorted by value descending
        values = [doc["value"] for doc in result]
        assert values == sorted(values, reverse=True)


def test_cost_based_optimization_favors_early_filtering():
    """Test that cost-based optimization favors pipelines with early filtering"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Create test pipelines
        # Pipeline 1: Expensive operation first, then match
        pipeline1 = [
            {"$unwind": "$items"},  # Expensive
            {"$match": {"category": "A"}},  # Should be moved to front
            {"$sort": {"value": 1}},
            {"$limit": 10},
        ]

        # Pipeline 2: Match first, then expensive operation
        pipeline2 = [
            {"$match": {"category": "A"}},  # Indexed match at front
            {"$unwind": "$items"},  # Expensive but processes fewer docs
            {"$sort": {"value": 1}},
            {"$limit": 10},
        ]

        # Create indexes to make the cost estimation realistic
        collection.create_index("category")

        # Estimate costs
        cost1 = query_helper._estimate_pipeline_cost(pipeline1)
        cost2 = query_helper._estimate_pipeline_cost(pipeline2)

        # Pipeline 2 should have lower cost because it filters early
        assert cost2 < cost1


def test_no_reordering_when_no_indexes():
    """Test that pipelines are not reordered when no indexes are available"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Pipeline with match on non-indexed field
        pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"description": "test"}},  # Non-indexed field
            {"$limit": 5},
        ]

        # Reorder the pipeline
        reordered = query_helper._reorder_pipeline_for_indexes(pipeline)

        # Should remain unchanged since no indexes are used
        assert reordered == pipeline


def test_mixed_indexed_and_non_indexed_fields():
    """Test pipeline reordering with mixed indexed and non-indexed fields"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Insert test data
        docs = []
        for i in range(30):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "description": f"Description {i}",
                    "items": [{"id": j, "value": i * 10 + j} for j in range(5)],
                }
            )
        collection.insert_many(docs)

        # Create index only on category
        collection.create_index("category")

        # Pipeline with match on both indexed and non-indexed fields
        pipeline = [
            {"$unwind": "$items"},
            {"$match": {"category": "Category2", "description": "test"}},
            {"$sort": {"items.value": 1}},
            {"$limit": 10},
        ]

        # The optimization should still move the match to the front
        # even though it contains both indexed and non-indexed fields
        result = collection.aggregate(pipeline)

        # Should work correctly
        assert len(result) <= 10

        # All should match the category (indexed field)
        categories = [doc["category"] for doc in result]
        assert all(cat == "Category2" for cat in categories)


def test_pipeline_with_multiple_match_stages():
    """Test pipeline reordering with multiple match stages"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Insert test data
        docs = []
        for i in range(40):
            docs.append(
                {
                    "name": f"User{i}",
                    "category": f"Category{i % 4}",
                    "status": "active" if i % 3 == 0 else "inactive",
                    "priority": "high" if i % 5 == 0 else "low",
                    "score": i,
                }
            )
        collection.insert_many(docs)

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")
        collection.create_index("priority")

        # Pipeline with multiple match stages
        pipeline = [
            {"$unwind": "$tags"},
            {"$match": {"category": "Category1"}},  # Indexed
            {"$sort": {"score": -1}},
            {"$match": {"status": "active"}},  # Indexed but later in pipeline
            {"$limit": 8},
        ]

        # Reorder the pipeline
        reordered = query_helper._reorder_pipeline_for_indexes(pipeline)

        # Both match stages should be moved to the front
        # The order might vary but both should be before unwind
        match_stages = [stage for stage in reordered if "$match" in stage]
        unwind_stages = [stage for stage in reordered if "$unwind" in stage]

        # All match stages should come before unwind stages
        first_unwind_index = next(
            i for i, stage in enumerate(reordered) if "$unwind" in stage
        )
        last_match_index = max(
            i for i, stage in enumerate(reordered) if "$match" in stage
        )

        assert last_match_index < first_unwind_index


def test_complex_pipeline_cost_estimation():
    """Test cost estimation for complex pipelines with multiple operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]
        query_helper = collection.query_engine.helpers

        # Create indexes
        collection.create_index("category")
        collection.create_index("status")

        # Complex pipeline with multiple operations
        complex_pipeline = [
            {"$unwind": "$items"},
            {"$unwind": "$items.subitems"},
            {"$match": {"category": "A", "status": "active"}},
            {"$group": {"_id": "$items.id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]

        # Simple pipeline with early filtering
        optimized_pipeline = [
            {"$match": {"category": "A", "status": "active"}},
            {"$unwind": "$items"},
            {"$unwind": "$items.subitems"},
            {"$group": {"_id": "$items.id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]

        # Estimate costs
        complex_cost = query_helper._estimate_pipeline_cost(complex_pipeline)
        optimized_cost = query_helper._estimate_pipeline_cost(
            optimized_pipeline
        )

        # The optimized pipeline should have lower cost
        assert optimized_cost < complex_cost


if __name__ == "__main__":
    pytest.main([__file__])
