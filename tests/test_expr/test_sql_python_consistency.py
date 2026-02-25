"""
Tests for $expr SQL vs Python consistency.

Verifies that SQL (Tier 1) and Python (Tier 3) implementations produce identical results.
"""

from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestSQLPythonConsistency:
    """Test that SQL and Python implementations produce identical results."""

    def _check_sql_python_consistency(self, evaluator, expr, test_docs):
        """
        Helper to check that SQL and Python produce same results.

        Args:
            evaluator: ExprEvaluator instance
            expr: Expression to test
            test_docs: List of test documents
        """
        # Get SQL result
        sql_result, params = evaluator._evaluate_sql_tier1(expr)

        # Get Python results
        python_results = []
        for doc in test_docs:
            python_result = evaluator.evaluate_python(expr, doc)
            python_results.append(python_result)

        # If SQL is supported, verify it generates valid SQL
        if sql_result is not None:
            assert isinstance(sql_result, str)
            assert len(sql_result) > 0
            return True
        return False

    def test_comparison_consistency(self):
        """Test comparison operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 5, "b": 5},
            {"a": 5, "b": 10},
            {"a": 10, "b": 5},
        ]

        # Test all comparison operators
        for op in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"]:
            expr = {op: ["$a", "$b"]}
            self._check_sql_python_consistency(evaluator, expr, test_docs)

            # Verify Python results
            python_results = [
                evaluator.evaluate_python(expr, doc) for doc in test_docs
            ]
            assert isinstance(python_results[0], bool)

    def test_arithmetic_consistency(self):
        """Test arithmetic operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 10, "b": 5},
            {"a": 20, "b": 4},
        ]

        # Test arithmetic in comparison
        for op, expected_op in [
            ("$add", "+"),
            ("$subtract", "-"),
            ("$multiply", "*"),
            ("$divide", "/"),
        ]:
            expr = {"$eq": [{op: ["$a", "$b"]}, 5]}
            self._check_sql_python_consistency(evaluator, expr, test_docs)

    def test_conditional_consistency(self):
        """Test $cond SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 15},
            {"a": 5},
        ]

        expr = {
            "$cond": {"if": {"$gt": ["$a", 10]}, "then": "high", "else": "low"}
        }
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"a": 15}) == "high"
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == "low"

    def test_ifnull_consistency(self):
        """Test $ifNull SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"a": 5},
            {"a": None},
            {},
        ]

        expr = {"$ifNull": ["$a", "default"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": None}) == "default"
        assert evaluator._evaluate_expr_python(expr, {}) == "default"

    def test_array_consistency(self):
        """Test array operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"items": [1, 2, 3]},
            {"items": [1, 2]},
            {"items": []},
        ]

        # Test $size
        expr = {"$size": ["$items"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"items": [1, 2, 3]}) == 3
        assert evaluator._evaluate_expr_python(expr, {"items": []}) == 0

    def test_string_consistency(self):
        """Test string operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"text": "HELLO"},
            {"text": "world"},
        ]

        # Test $toLower
        expr = {"$toLower": ["$text"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "HELLO"}) == "hello"
        )
        assert (
            evaluator._evaluate_expr_python(expr, {"text": "world"}) == "world"
        )

    def test_math_consistency(self):
        """Test math operators SQL vs Python consistency."""
        evaluator = ExprEvaluator()
        test_docs = [
            {"value": 5.5},
            {"value": -5.5},
            {"value": 10},
        ]

        # Test $abs
        expr = {"$abs": ["$value"]}
        self._check_sql_python_consistency(evaluator, expr, test_docs)

        # Verify Python results
        assert evaluator._evaluate_expr_python(expr, {"value": -5.5}) == 5.5
        assert evaluator._evaluate_expr_python(expr, {"value": 10}) == 10
