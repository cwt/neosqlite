"""
Tests for $expr kill switch functionality.

Verifies that the force_python flag properly forces Python evaluation for all operators.
"""

from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestKillSwitch:
    """Test kill switch forces Python evaluation."""

    def test_kill_switch_comparison_operators(self):
        """Test kill switch with comparison operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$eq": ["$a", "$b"]},
            {"$gt": ["$a", 5]},
            {"$lt": ["$a", 10]},
        ]

        for expr in test_exprs:
            # With kill switch on (force_python=True), should return None
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

            # Python evaluation should still work
            test_doc = {"a": 5, "b": 10}
            python_result = evaluator.evaluate_python(expr, test_doc)
            assert isinstance(python_result, bool)

    def test_kill_switch_arithmetic_operators(self):
        """Test kill switch with arithmetic operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$add": ["$a", "$b"]},
            {"$multiply": ["$a", 2]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_conditional_operators(self):
        """Test kill switch with conditional operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$cond": {"if": {"$gt": ["$a", 10]}, "then": 1, "else": 0}},
            {"$ifNull": ["$a", "default"]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_array_operators(self):
        """Test kill switch with array operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$size": ["$items"]},
            {"$in": ["$value", "$allowed"]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_string_operators(self):
        """Test kill switch with string operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$concat": ["$a", "$b"]},
            {"$toLower": ["$text"]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_date_operators(self):
        """Test kill switch with date operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$year": ["$date"]},
            {"$month": ["$date"]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_math_operators(self):
        """Test kill switch with math operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$abs": ["$value"]},
            {"$sqrt": ["$value"]},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_kill_switch_object_operators(self):
        """Test kill switch with object operators."""
        evaluator = ExprEvaluator()
        test_exprs = [
            {"$mergeObjects": [{"a": 1}, {"b": 2}]},
            {"$getField": {"field": "name"}},
        ]

        for expr in test_exprs:
            sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
            assert sql is None
            assert params == []

    def test_tier_selection_respects_kill_switch(self):
        """Test that all tiers respect the kill switch."""
        evaluator = ExprEvaluator()
        expr = {"$eq": ["$a", 5]}

        # Tier 1 with kill switch
        sql1, params1 = evaluator.evaluate(expr, tier=1, force_python=True)
        assert sql1 is None

        # Tier 2 with kill switch
        sql2, params2 = evaluator.evaluate(expr, tier=2, force_python=True)
        assert sql2 is None

        # Tier 3 (always Python)
        sql3, params3 = evaluator.evaluate(expr, tier=3, force_python=False)
        assert sql3 is None

    def test_kill_switch_complex_expression(self):
        """Test kill switch with complex nested expression."""
        evaluator = ExprEvaluator()
        expr = {
            "$and": [
                {"$gt": [{"$add": ["$a", "$b"]}, 10]},
                {"$lt": [{"$multiply": ["$c", "$d"]}, 100]},
            ]
        }

        # With kill switch on
        sql, params = evaluator.evaluate(expr, tier=1, force_python=True)
        assert sql is None
        assert params == []

        # Python evaluation should work
        test_doc = {"a": 5, "b": 6, "c": 10, "d": 5}
        python_result = evaluator.evaluate_python(expr, test_doc)
        assert isinstance(python_result, bool)
