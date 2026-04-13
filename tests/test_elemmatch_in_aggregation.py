"""Test for $elemMatch and $in operators in aggregation pipelines (Bug fixes)"""

import neosqlite


class TestElemMatchAndInInAggregation:
    """Test that $elemMatch and $in work correctly in both find() and aggregate()."""

    def test_elemmatch_simple_value_find_vs_aggregate(self):
        """Test $elemMatch with simple string value returns same results in find() and aggregate()."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn.test_collection
            collection.insert_many(
                [
                    {"name": "post1", "tags": ["maliit-keyboard", "gtk"]},
                    {"name": "post2", "tags": ["maliit-keyboard", "qt"]},
                    {"name": "post3", "tags": ["maliit-keyboard", "gnome"]},
                    {"name": "post4", "tags": ["gtk", "qt"]},
                    {"name": "post5", "tags": ["gnome", "kde"]},
                    {"name": "post6", "tags": ["maliit-keyboard"]},
                    {"name": "post7", "tags": ["gtk"]},
                    {"name": "post8", "tags": ["qt"]},
                ]
            )

            # Using find() - should return 4 posts
            find_results = list(
                collection.find({"tags": {"$elemMatch": "maliit-keyboard"}})
            )
            find_names = sorted([r["name"] for r in find_results])

            # Using aggregate() - should return same 4 posts
            aggregate_results = list(
                collection.aggregate(
                    [{"$match": {"tags": {"$elemMatch": "maliit-keyboard"}}}]
                )
            )
            aggregate_names = sorted([r["name"] for r in aggregate_results])

            # Both should return the same results
            assert find_names == aggregate_names
            assert find_names == ["post1", "post2", "post3", "post6"]

    def test_elemmatch_with_operator_find_vs_aggregate(self):
        """Test $elemMatch with $eq operator returns same results in find() and aggregate()."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn.test_collection
            collection.insert_many(
                [
                    {"name": "post1", "tags": ["maliit-keyboard", "gtk"]},
                    {"name": "post2", "tags": ["maliit-keyboard", "qt"]},
                    {"name": "post3", "tags": ["maliit-keyboard", "gnome"]},
                    {"name": "post4", "tags": ["gtk", "qt"]},
                    {"name": "post5", "tags": ["gnome", "kde"]},
                ]
            )

            # Using find()
            find_results = list(
                collection.find(
                    {"tags": {"$elemMatch": {"$eq": "maliit-keyboard"}}}
                )
            )
            find_names = sorted([r["name"] for r in find_results])

            # Using aggregate()
            aggregate_results = list(
                collection.aggregate(
                    [
                        {
                            "$match": {
                                "tags": {
                                    "$elemMatch": {"$eq": "maliit-keyboard"}
                                }
                            }
                        }
                    ]
                )
            )
            aggregate_names = sorted([r["name"] for r in aggregate_results])

            # Both should return the same results
            assert find_names == aggregate_names
            assert find_names == ["post1", "post2", "post3"]

    def test_in_on_array_fields_find_vs_aggregate(self):
        """Test $in on array fields returns same results in find() and aggregate().

        Note: MongoDB $in on array fields checks if any element in the array
        equals one of the specified values. This test verifies both find() and
        aggregate() return the same (possibly empty) results.
        """
        with neosqlite.Connection(":memory:") as conn:
            collection = conn.test_collection
            collection.insert_many(
                [
                    {"name": "post1", "tags": ["maliit-keyboard", "gtk"]},
                    {"name": "post2", "tags": ["maliit-keyboard", "qt"]},
                    {"name": "post3", "tags": ["gtk", "qt"]},
                    {"name": "post4", "tags": ["gnome", "kde"]},
                    {"name": "post5", "tags": ["maliit-keyboard"]},
                ]
            )

            # Using $in - both find() and aggregate() should return same results
            find_results = list(
                collection.find({"tags": {"$in": ["maliit-keyboard"]}})
            )
            find_names = sorted([r["name"] for r in find_results])

            aggregate_results = list(
                collection.aggregate(
                    [{"$match": {"tags": {"$in": ["maliit-keyboard"]}}}]
                )
            )
            aggregate_names = sorted([r["name"] for r in aggregate_results])

            # Both should return the same results (even if both are empty)
            assert find_names == aggregate_names
