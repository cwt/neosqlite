"""
Test $facet stage with tier optimization.

Each sub-pipeline in $facet should use Tier-1/Tier-2 optimization when possible,
falling back to Tier-3 Python only when necessary.
"""

import pytest

from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)


class TestFacetTierOptimization:
    """Test class for $facet tier optimization."""

    @pytest.fixture(autouse=True)
    def reset_fallback(self):
        """Reset fallback flag after each test."""
        yield
        set_force_fallback(False)

    @pytest.fixture
    def collection(self, connection):
        """Create a test collection with sample data."""
        coll = connection["test_facet"]
        coll.insert_many(
            [
                {"a": 1, "b": 1, "category": "X", "value": 10},
                {"a": 2, "b": 2, "category": "X", "value": 20},
                {"a": 3, "b": 3, "category": "Y", "value": 30},
                {"a": 4, "b": 4, "category": "Y", "value": 40},
                {"a": 5, "b": 5, "category": "Z", "value": 50},
            ]
        )
        return coll

    def _normalize_result(self, result):
        """Normalize aggregation results for comparison."""
        if isinstance(result, list):
            return sorted(
                [
                    {
                        k: (sorted(v) if isinstance(v, list) else v)
                        for k, v in doc.items()
                    }
                    for doc in result
                ],
                key=lambda x: str(x.get("_id", "")),
            )
        return result

    def test_facet_tier1_vs_tier3(self, collection):
        """Verify $facet sub-pipelines using Tier-1 produce identical results to Tier-3."""
        pipeline = [
            {
                "$facet": {
                    "grouped": [
                        {"$match": {"a": {"$gte": 2}}},
                        {
                            "$group": {
                                "_id": "$category",
                                "sum_a": {"$sum": "$a"},
                            }
                        },
                    ],
                    "sorted": [
                        {"$sort": {"b": -1}},
                        {"$limit": 3},
                    ],
                }
            }
        ]

        # Get Tier-1/Tier-2 results
        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        # Get Tier-3 Python fallback results
        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Results MUST be identical
        assert tier12_result[0]["grouped"] == tier3_result[0]["grouped"]
        assert tier12_result[0]["sorted"] == tier3_result[0]["sorted"]

    def test_facet_with_match_only(self, collection):
        """Verify $facet with simple $match sub-pipeline (Tier-1 supported)."""
        pipeline = [
            {
                "$facet": {
                    "filtered": [
                        {"$match": {"a": {"$gte": 3}}},
                    ]
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier12_result[0]["filtered"] == tier3_result[0]["filtered"]

    def test_facet_with_project(self, collection):
        """Verify $facet with $project sub-pipeline."""
        pipeline = [
            {
                "$facet": {
                    "projected": [
                        {"$project": {"_id": 0, "a": 1, "category": 1}}
                    ]
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Compare without _id
        tier12_docs = [
            {k: v for k, v in doc.items() if k != "_id"}
            for doc in tier12_result[0]["projected"]
        ]
        tier3_docs = [
            {k: v for k, v in doc.items() if k != "_id"}
            for doc in tier3_result[0]["projected"]
        ]
        assert tier12_docs == tier3_docs

    def test_facet_with_skip_limit(self, collection):
        """Verify $facet with $skip and $limit sub-pipeline."""
        pipeline = [
            {
                "$facet": {
                    "paginated": [
                        {"$sort": {"a": 1}},
                        {"$skip": 1},
                        {"$limit": 2},
                    ]
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier12_result[0]["paginated"] == tier3_result[0]["paginated"]

    def test_facet_with_count(self, collection):
        """Verify $facet with $count sub-pipeline."""
        pipeline = [
            {
                "$facet": {
                    "total": [{"$count": "total"}],
                    "filtered": [
                        {"$match": {"category": "X"}},
                        {"$count": "count"},
                    ],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier12_result[0]["total"] == tier3_result[0]["total"]
        assert tier12_result[0]["filtered"] == tier3_result[0]["filtered"]

    def test_facet_empty_collection(self, connection):
        """Verify $facet works with empty collection."""
        collection = connection["test_facet_empty"]
        pipeline = [
            {
                "$facet": {
                    "results": [{"$match": {"a": {"$gte": 0}}}],
                    "count": [{"$count": "total"}],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        assert tier12_result[0]["results"] == tier3_result[0]["results"]
        assert tier12_result[0]["count"] == tier3_result[0]["count"]

    def test_facet_multiple_subpipelines(self, collection):
        """Verify $facet with multiple sub-pipelines."""
        pipeline = [
            {
                "$facet": {
                    "by_category": [
                        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                    ],
                    "high_value": [
                        {"$match": {"value": {"$gte": 30}}},
                        {"$sort": {"value": -1}},
                    ],
                    "stats": [
                        {"$group": {"_id": None, "avg": {"$avg": "$value"}}},
                    ],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Compare by_category (normalize order)
        tier12_by_cat = self._normalize_result(tier12_result[0]["by_category"])
        tier3_by_cat = self._normalize_result(tier3_result[0]["by_category"])
        assert tier12_by_cat == tier3_by_cat

        # Compare high_value
        assert tier12_result[0]["high_value"] == tier3_result[0]["high_value"]

        # Compare stats (with tolerance for float)
        tier12_avg = tier12_result[0]["stats"][0]["avg"]
        tier3_avg = tier3_result[0]["stats"][0]["avg"]
        assert abs(tier12_avg - tier3_avg) < 1e-10
