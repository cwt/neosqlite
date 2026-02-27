"""
Integration tests for aggregation expression support.

This module tests complex, real-world aggregation pipelines that combine
multiple expression features across different stages.
"""

import pytest
import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


class TestComplexAggregationPipelines:
    """Test complex multi-stage aggregation pipelines with expressions."""

    @pytest.fixture
    def sales_collection(self):
        """Create a test collection with sales data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.sales
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "date": "2024-01-15",
                    "product": "Widget",
                    "category": "A",
                    "price": 100,
                    "quantity": 5,
                    "discount": 0.1,
                },
                {
                    "_id": 2,
                    "date": "2024-01-16",
                    "product": "Gadget",
                    "category": "B",
                    "price": 200,
                    "quantity": 3,
                    "discount": 0.15,
                },
                {
                    "_id": 3,
                    "date": "2024-01-17",
                    "product": "Widget",
                    "category": "A",
                    "price": 100,
                    "quantity": 8,
                    "discount": 0.05,
                },
                {
                    "_id": 4,
                    "date": "2024-01-18",
                    "product": "Gizmo",
                    "category": "B",
                    "price": 150,
                    "quantity": 4,
                    "discount": 0.2,
                },
                {
                    "_id": 5,
                    "date": "2024-01-19",
                    "product": "Widget",
                    "category": "A",
                    "price": 100,
                    "quantity": 10,
                    "discount": 0.0,
                },
            ]
        )
        yield coll
        conn.close()

    def test_revenue_calculation_pipeline(self, sales_collection):
        """Test pipeline calculating revenue with tax and discount."""
        set_force_fallback(True)
        try:
            pipeline = [
                # Calculate net amount per transaction
                {
                    "$addFields": {
                        "gross": {"$multiply": ["$price", "$quantity"]},
                        "discount_amount": {
                            "$multiply": ["$price", "$quantity", "$discount"]
                        },
                    }
                },
                {
                    "$addFields": {
                        "net": {"$subtract": ["$gross", "$discount_amount"]}
                    }
                },
                # Group by category
                {
                    "$group": {
                        "_id": "$category",
                        "total_gross": {"$sum": "$gross"},
                        "total_discount": {"$sum": "$discount_amount"},
                        "total_net": {"$sum": "$net"},
                        "transaction_count": {"$sum": 1},
                    }
                },
                # Add computed fields
                {
                    "$addFields": {
                        "avg_transaction": {
                            "$divide": ["$total_net", "$transaction_count"]
                        },
                        "discount_rate": {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$total_discount",
                                        "$total_gross",
                                    ]
                                },
                                100,
                            ]
                        },
                    }
                },
                {"$sort": {"total_net": -1}},
            ]
            results = list(sales_collection.aggregate(pipeline))

            assert len(results) == 2
            # Category A should have higher total
            category_a = next(r for r in results if r["_id"] == "A")
            assert (
                category_a["total_gross"] == 2300
            )  # (100*5) + (100*8) + (100*10)
            assert (
                category_a["total_discount"] == 90
            )  # (500*0.1) + (800*0.05) + (1000*0.0)
            assert category_a["total_net"] == 2210
        finally:
            set_force_fallback(False)

    def test_conditional_categorization_pipeline(self, sales_collection):
        """Test pipeline with conditional categorization."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "revenue": {"$multiply": ["$price", "$quantity"]}
                    }
                },
                {
                    "$addFields": {
                        "tier": {
                            "$cond": {
                                "if": {"$gte": ["$revenue", 500]},
                                "then": "high",
                                "else": "standard",
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$tier",
                        "count": {"$sum": 1},
                        "total_revenue": {"$sum": "$revenue"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            results = list(sales_collection.aggregate(pipeline))

            # Product 1: 500 (high), Product 2: 600 (high), Product 3: 800 (high)
            # Product 4: 600 (high), Product 5: 1000 (high)
            # All products have revenue >= 500
            assert len(results) == 1
            high = results[0]
            assert high["_id"] == "high"
            assert high["count"] == 5
            assert high["total_revenue"] == 3500  # 500+600+800+600+1000
        finally:
            set_force_fallback(False)

    def test_root_variable_comparison(self, sales_collection):
        """Test comparing original vs modified documents."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "tax": {"$multiply": ["$price", "$quantity", 0.08]}
                    }
                },
                {
                    "$addFields": {
                        "root_doc": "$$ROOT",
                        "current_total": {
                            "$add": ["$price", "$quantity", "$tax"]
                        },
                    }
                },
                {
                    "$project": {
                        "product": 1,
                        "root_price": "$root_doc.price",
                        "root_quantity": "$root_doc.quantity",
                        "has_tax": {"$gt": ["$tax", 0]},
                        "current_total": 1,
                    }
                },
                {"$limit": 2},
            ]
            results = list(sales_collection.aggregate(pipeline))

            assert len(results) == 2
            # Root price should match the original document's price
            assert results[0]["root_price"] == 100
            assert results[0]["root_quantity"] == 5
            # Current doc has tax
            assert results[0]["has_tax"] is True
        finally:
            set_force_fallback(False)


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.fixture
    def test_collection(self):
        """Create a test collection with edge case data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.edge_cases
        coll.insert_many(
            [
                {"_id": 1, "value": 0, "nullable": None},
                {"_id": 2, "value": 100, "nullable": 50},
                {"_id": 3, "value": -50, "nullable": None},
            ]
        )
        yield coll
        conn.close()

    def test_null_handling_in_expressions(self, test_collection):
        """Test that null values are handled correctly in expressions."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "doubled": {"$multiply": ["$value", 2]},
                        "with_null": {"$add": ["$value", "$nullable"]},
                    }
                }
            ]
            results = list(test_collection.aggregate(pipeline))

            assert len(results) == 3
            # Zero handling
            assert results[0]["doubled"] == 0
            # Null handling - should be None
            assert results[0]["with_null"] is None
            # Normal calculation
            assert results[1]["doubled"] == 200
            assert results[1]["with_null"] == 150
        finally:
            set_force_fallback(False)

    def test_negative_numbers(self, test_collection):
        """Test expressions with negative numbers."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$addFields": {
                        "abs_value": {"$abs": "$value"},
                        "negated": {"$multiply": ["$value", -1]},
                    }
                }
            ]
            results = list(test_collection.aggregate(pipeline))

            assert len(results) == 3
            assert results[2]["abs_value"] == 50  # abs(-50)
            assert results[2]["negated"] == 50  # -(-50)
        finally:
            set_force_fallback(False)


class TestFacetComplexPipelines:
    """Test complex $facet pipelines."""

    @pytest.fixture
    def products_collection(self):
        """Create a test collection with product data."""
        conn = neosqlite.Connection(":memory:")
        coll = conn.products
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Product A",
                    "category": "Electronics",
                    "price": 100,
                    "stock": 50,
                    "rating": 4.5,
                },
                {
                    "_id": 2,
                    "name": "Product B",
                    "category": "Electronics",
                    "price": 200,
                    "stock": 30,
                    "rating": 4.0,
                },
                {
                    "_id": 3,
                    "name": "Product C",
                    "category": "Books",
                    "price": 25,
                    "stock": 100,
                    "rating": 4.8,
                },
                {
                    "_id": 4,
                    "name": "Product D",
                    "category": "Books",
                    "price": 35,
                    "stock": 75,
                    "rating": 3.9,
                },
                {
                    "_id": 5,
                    "name": "Product E",
                    "category": "Electronics",
                    "price": 150,
                    "stock": 20,
                    "rating": 4.2,
                },
            ]
        )
        yield coll
        conn.close()

    def test_facet_with_multiple_analyses(self, products_collection):
        """Test $facet with multiple complex analyses."""
        set_force_fallback(True)
        try:
            pipeline = [
                {
                    "$facet": {
                        # Analysis 1: Category summary
                        "category_summary": [
                            {
                                "$group": {
                                    "_id": "$category",
                                    "count": {"$sum": 1},
                                    "avg_price": {"$avg": "$price"},
                                    "total_stock": {"$sum": "$stock"},
                                }
                            }
                        ],
                        # Analysis 2: Price tiers
                        "price_tiers": [
                            {
                                "$addFields": {
                                    "tier": {
                                        "$cond": {
                                            "if": {"$gte": ["$price", 100]},
                                            "then": "premium",
                                            "else": "budget",
                                        }
                                    }
                                }
                            },
                            {
                                "$group": {
                                    "_id": "$tier",
                                    "products": {"$push": "$name"},
                                    "avg_rating": {"$avg": "$rating"},
                                }
                            },
                        ],
                        # Analysis 3: Top rated
                        "top_rated": [
                            {"$sort": {"rating": -1}},
                            {"$limit": 3},
                            {
                                "$project": {
                                    "name": 1,
                                    "rating": 1,
                                    "price_with_tax": {
                                        "$multiply": ["$price", 1.08]
                                    },
                                }
                            },
                        ],
                    }
                }
            ]
            results = list(products_collection.aggregate(pipeline))

            assert len(results) == 1
            facet = results[0]

            # Check category_summary
            assert "category_summary" in facet
            assert len(facet["category_summary"]) == 2

            # Check price_tiers
            assert "price_tiers" in facet
            assert len(facet["price_tiers"]) == 2

            # Check top_rated
            assert "top_rated" in facet
            assert len(facet["top_rated"]) == 3
            # First should be highest rated
            assert (
                facet["top_rated"][0]["rating"]
                >= facet["top_rated"][1]["rating"]
            )
        finally:
            set_force_fallback(False)
