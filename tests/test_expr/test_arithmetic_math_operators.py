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

    def test_exp_integration(self):
        """Test $exp with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"x": 0},
                    {"x": 1},
                    {"x": 2},
                ]
            )

            # exp(0) = 1 (exact)
            expr = {"$expr": {"$eq": [{"$exp": 0}, 1]}}
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # exp(1) ≈ 2.718 (use range comparison for floating point)
            expr = {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$exp": 1}, 2.717]},
                        {"$lte": [{"$exp": 1}, 2.719]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # Test with field reference: exp(x) > 1 for x > 0
            expr = {"$expr": {"$gt": [{"$exp": "$x"}, 1]}}
            results = list(collection.find(expr))
            assert len(results) == 2  # x=1 and x=2

    def test_degreesToRadians_integration(self):
        """Test $degreesToRadians with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"angle": 0},
                    {"angle": 180},
                    {"angle": 90},
                ]
            )

            # radians(180°) ≈ π (use range comparison)
            expr = {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$degreesToRadians": 180}, 3.1415]},
                        {"$lte": [{"$degreesToRadians": 180}, 3.1417]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # radians(90°) ≈ π/2
            expr = {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$degreesToRadians": 90}, 1.570]},
                        {"$lte": [{"$degreesToRadians": 90}, 1.571]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # Test with field reference
            expr = {"$expr": {"$gt": [{"$degreesToRadians": "$angle"}, 0]}}
            results = list(collection.find(expr))
            assert len(results) == 2  # angle=180 and angle=90

    def test_radiansToDegrees_integration(self):
        """Test $radiansToDegrees with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"angle": 0},
                    {"angle": 3.14159},  # ≈ π
                    {"angle": 1.5708},  # ≈ π/2
                ]
            )

            # degrees(π) ≈ 180 (use range comparison)
            expr = {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$radiansToDegrees": 3.14159}, 179.9]},
                        {"$lte": [{"$radiansToDegrees": 3.14159}, 180.1]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # degrees(π/2) ≈ 90
            expr = {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$radiansToDegrees": 1.5708}, 89.9]},
                        {"$lte": [{"$radiansToDegrees": 1.5708}, 90.1]},
                    ]
                }
            }
            results = list(collection.find(expr))
            assert len(results) == 3  # All documents match

            # Test with field reference
            expr = {"$expr": {"$gt": [{"$radiansToDegrees": "$angle"}, 0]}}
            results = list(collection.find(expr))
            assert len(results) == 2  # angle=π and angle=π/2

    def test_round_two_operands(self):
        """Test $round with two operands (number and precision)."""
        evaluator = ExprEvaluator()

        # Python evaluation
        expr = {"$round": [3.14159, 2]}
        assert evaluator._evaluate_expr_python(expr, {}) == 3.14

        expr = {"$round": [3.14159, 0]}
        assert evaluator._evaluate_expr_python(expr, {}) == 3

        # SQL conversion
        expr = {"$round": [3.14159, 2]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "round(" in sql

    def test_round_integration(self):
        """Test $round with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_many(
                [
                    {"value": 3.14159},
                    {"value": 2.71828},
                    {"value": 1.41421},
                ]
            )

            # Test with precision
            expr = {"$expr": {"$eq": [{"$round": ["$value", 2]}, 3.14]}}
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["value"] == 3.14159
