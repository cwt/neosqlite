"""
Tests for $expr arithmetic and math operators.

Arithmetic: $add, $subtract, $multiply, $divide, $mod
Math: $abs, $ceil, $floor, $round, $trunc, $pow, $sqrt
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator


class TestArithmeticOperatorsPython:
    """Test arithmetic operators Python evaluation."""

    def test_add_operator(self):
        """Test $add operator."""
        evaluator = ExprEvaluator()
        expr = {"$add": ["$a", "$b", 5]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10, "b": 5})
        assert result == 20

    def test_subtract_operator(self):
        """Test $subtract operator."""
        evaluator = ExprEvaluator()
        expr = {"$subtract": ["$a", "$b"]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10, "b": 3})
        assert result == 7

    def test_multiply_operator(self):
        """Test $multiply operator."""
        evaluator = ExprEvaluator()
        expr = {"$multiply": ["$a", "$b"]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10, "b": 5})
        assert result == 50

    def test_divide_operator(self):
        """Test $divide operator."""
        evaluator = ExprEvaluator()
        expr = {"$divide": ["$a", 2]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result == 5.0

    def test_divide_by_zero(self):
        """Test $divide with zero."""
        evaluator = ExprEvaluator()
        expr = {"$divide": ["$a", 0]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result is None

    def test_mod_operator(self):
        """Test $mod operator."""
        evaluator = ExprEvaluator()
        expr = {"$mod": ["$a", 3]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result == 1

    def test_mod_by_zero(self):
        """Test $mod with zero."""
        evaluator = ExprEvaluator()
        expr = {"$mod": ["$a", 0]}
        result = evaluator._evaluate_expr_python(expr, {"a": 10})
        assert result is None


class TestArithmeticOperatorsSQL:
    """Test arithmetic operators SQL conversion."""

    def test_add_sql(self):
        """Test $add SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$add": ["$a", "$b", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "+" in sql
        assert params == [5]

    def test_subtract_sql(self):
        """Test $subtract SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$subtract": ["$a", 5]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "-" in sql
        assert params == [5]

    def test_multiply_sql(self):
        """Test $multiply SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$multiply": ["$a", 2]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "*" in sql
        assert params == [2]

    def test_divide_sql(self):
        """Test $divide SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$divide": ["$a", 2]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "/" in sql
        assert params == [2]

    def test_mod_sql(self):
        """Test $mod SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$mod": ["$a", 3]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "%" in sql
        assert params == [3]


class TestMathOperatorsPython:
    """Test math operators Python evaluation."""

    def test_abs_operator(self):
        """Test $abs operator."""
        evaluator = ExprEvaluator()
        expr = {"$abs": ["$a"]}

        assert evaluator._evaluate_expr_python(expr, {"a": -5}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": 5}) == 5

    def test_ceil_operator(self):
        """Test $ceil operator."""
        evaluator = ExprEvaluator()
        expr = {"$ceil": ["$a"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5.2}) == 6
        assert evaluator._evaluate_expr_python(expr, {"a": 5.9}) == 6

    def test_floor_operator(self):
        """Test $floor operator."""
        evaluator = ExprEvaluator()
        expr = {"$floor": ["$a"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5.2}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": 5.9}) == 5

    def test_round_operator(self):
        """Test $round operator."""
        evaluator = ExprEvaluator()
        expr = {"$round": ["$a"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5.2}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": 5.6}) == 6

    def test_trunc_operator(self):
        """Test $trunc operator."""
        evaluator = ExprEvaluator()
        expr = {"$trunc": ["$a"]}

        assert evaluator._evaluate_expr_python(expr, {"a": 5.9}) == 5
        assert evaluator._evaluate_expr_python(expr, {"a": -5.9}) == -5

    def test_pow_operator(self):
        """Test $pow operator."""
        evaluator = ExprEvaluator()
        expr = {"$pow": ["$base", 2]}
        result = evaluator._evaluate_expr_python(expr, {"base": 5})
        assert result == 25

    def test_pow_fractional(self):
        """Test $pow with fractional exponent."""
        evaluator = ExprEvaluator()
        expr = {"$pow": ["$base", 0.5]}
        result = evaluator._evaluate_expr_python(expr, {"base": 16})
        assert result == 4.0

    def test_sqrt_operator(self):
        """Test $sqrt operator."""
        evaluator = ExprEvaluator()
        expr = {"$sqrt": ["$value"]}
        result = evaluator._evaluate_expr_python(expr, {"value": 16})
        assert result == 4.0

    def test_sqrt_negative(self):
        """Test $sqrt with negative."""
        evaluator = ExprEvaluator()
        expr = {"$sqrt": ["$value"]}
        result = evaluator._evaluate_expr_python(expr, {"value": -4})
        assert result is None


class TestMathOperatorsSQL:
    """Test math operators SQL conversion."""

    def test_abs_sql(self):
        """Test $abs SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$abs": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "abs(" in sql

    def test_ceil_sql(self):
        """Test $ceil SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$ceil": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "ceil(" in sql

    def test_floor_sql(self):
        """Test $floor SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$floor": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "floor(" in sql

    def test_round_sql(self):
        """Test $round SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$round": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "round(" in sql

    def test_trunc_sql(self):
        """Test $trunc SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$trunc": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "cast(" in sql
        assert "as integer" in sql

    def test_pow_sql(self):
        """Test $pow SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$pow": ["$base", 2]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "pow(" in sql

    def test_sqrt_sql(self):
        """Test $sqrt SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$sqrt": ["$value"]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "sqrt(" in sql


class TestArithmeticIntegration:
    """Integration tests for arithmetic and math operators."""

    def test_add_integration(self):
        """Test $add with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"a": 10, "b": 5, "sum": 15},
                    {"a": 5, "b": 5, "sum": 15},  # Wrong sum
                ]
            )

            expr = {"$expr": {"$ne": [{"$add": ["$a", "$b"]}, "$sum"]}}
            results = list(collection.find(expr))
            assert len(results) == 1

    def test_abs_integration(self):
        """Test $abs with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"value": -5},
                    {"value": 5},
                    {"value": -10},
                ]
            )

            expr = {"$expr": {"$gt": [{"$abs": ["$value"]}, 7]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["value"] == -10
