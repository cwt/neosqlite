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

    def test_facet_tier2_with_unwind(self, collection):
        """Verify $facet with $unwind sub-pipeline forces Tier 2 (temp tables).

        $unwind in sub-pipelines prevents Tier 1 SQL optimization,
        so this tests Tier 2 temporary table handling.
        """
        # Clear fixture data and insert fresh data with arrays
        collection.delete_many({})
        collection.insert_many(
            [
                {"_id": 1, "item": "A", "sizes": ["S", "M", "L"]},
                {"_id": 2, "item": "B", "sizes": ["M", "L"]},
                {"_id": 3, "item": "C", "sizes": ["S"]},
            ]
        )

        pipeline = [
            {
                "$facet": {
                    "unwind_sizes": [
                        {"$unwind": "$sizes"},
                        {"$group": {"_id": "$sizes", "count": {"$sum": 1}}},
                    ],
                    "items": [
                        {"$sort": {"item": 1}},
                    ],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Compare unwind_sizes (normalize order)
        tier12_unwind = self._normalize_result(tier12_result[0]["unwind_sizes"])
        tier3_unwind = self._normalize_result(tier3_result[0]["unwind_sizes"])
        assert tier12_unwind == tier3_unwind

        # Compare items
        assert tier12_result[0]["items"] == tier3_result[0]["items"]

    def test_facet_tier2_complex_pipeline(self, collection):
        """Verify $facet with complex sub-pipelines that force Tier 2.

        This test uses a pipeline structure that Tier 1 SQL can't optimize
        (e.g., $unwind with $lookup), forcing Tier 2 temp table processing.
        """
        # Create a second collection for lookup
        other_coll = collection._database["test_facet_categories"]
        other_coll.insert_many(
            [
                {"_id": "X", "name": "Category X", "multiplier": 2},
                {"_id": "Y", "name": "Category Y", "multiplier": 3},
                {"_id": "Z", "name": "Category Z", "multiplier": 4},
            ]
        )

        # Clear and re-insert main collection data
        collection.delete_many({})
        collection.insert_many(
            [
                {"_id": 1, "value": 10, "category": "X"},
                {"_id": 2, "value": 20, "category": "X"},
                {"_id": 3, "value": 30, "category": "Y"},
            ]
        )

        pipeline = [
            {
                "$facet": {
                    "with_lookup": [
                        {
                            "$lookup": {
                                "from": "test_facet_categories",
                                "localField": "category",
                                "foreignField": "_id",
                                "as": "cat_info",
                            }
                        },
                        {"$unwind": "$cat_info"},
                        {
                            "$project": {
                                "value": 1,
                                "multiplier": "$cat_info.multiplier",
                            }
                        },
                    ],
                    "simple_count": [
                        {"$count": "total"},
                    ],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Compare simple_count
        assert (
            tier12_result[0]["simple_count"] == tier3_result[0]["simple_count"]
        )

        # Compare with_lookup (normalize)
        tier12_lookup = sorted(
            tier12_result[0]["with_lookup"], key=lambda x: x.get("_id", 0)
        )
        tier3_lookup = sorted(
            tier3_result[0]["with_lookup"], key=lambda x: x.get("_id", 0)
        )
        assert tier12_lookup == tier3_lookup

    def test_facet_tier2_with_match_and_group(self, collection):
        """Verify $facet with $match and $group in sub-pipelines at Tier 2."""
        pipeline = [
            {
                "$facet": {
                    "filtered_groups": [
                        {"$match": {"a": {"$gte": 2}}},
                        {
                            "$group": {
                                "_id": "$category",
                                "total_value": {"$sum": "$value"},
                                "avg_a": {"$avg": "$a"},
                            }
                        },
                    ],
                    "all_count": [
                        {"$count": "total"},
                    ],
                }
            }
        ]

        set_force_fallback(False)
        tier12_result = list(collection.aggregate(pipeline))

        set_force_fallback(True)
        tier3_result = list(collection.aggregate(pipeline))

        # Compare all_count
        assert tier12_result[0]["all_count"] == tier3_result[0]["all_count"]

        # Compare filtered_groups (normalize order)
        tier12_groups = self._normalize_result(
            tier12_result[0]["filtered_groups"]
        )
        tier3_groups = self._normalize_result(
            tier3_result[0]["filtered_groups"]
        )
        assert tier12_groups == tier3_groups
