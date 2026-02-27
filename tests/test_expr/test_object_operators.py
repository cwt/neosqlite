"""
Tests for $expr object operators.

Covers: $mergeObjects, $getField, $setField, $unsetField, $objectToArray
"""

import neosqlite
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.query_helper import set_force_fallback


class TestObjectOperatorsPython:
    """Test object operators Python evaluation."""

    def test_mergeObjects_operator(self):
        """Test $mergeObjects operator."""
        evaluator = ExprEvaluator()
        expr = {"$mergeObjects": [{"obj1": "a"}, {"obj2": "b"}]}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == {"obj1": "a", "obj2": "b"}

    def test_mergeObjects_with_fields(self):
        """Test $mergeObjects with field references."""
        evaluator = ExprEvaluator()
        expr = {"$mergeObjects": ["$obj1", "$obj2"]}
        result = evaluator._evaluate_expr_python(
            expr, {"obj1": {"a": 1}, "obj2": {"b": 2}}
        )
        assert result == {"a": 1, "b": 2}

    def test_mergeObjects_override(self):
        """Test $mergeObjects with override."""
        evaluator = ExprEvaluator()
        expr = {"$mergeObjects": [{"a": 1}, {"a": 2}]}
        result = evaluator._evaluate_expr_python(expr, {})
        assert result == {"a": 2}  # Second object overrides first

    def test_getField_operator(self):
        """Test $getField operator."""
        evaluator = ExprEvaluator()
        expr = {"$getField": {"field": "name"}}
        result = evaluator._evaluate_expr_python(expr, {"name": "John"})
        assert result == "John"

    def test_getField_with_input(self):
        """Test $getField with input."""
        evaluator = ExprEvaluator()
        expr = {"$getField": {"field": "name", "input": "$obj"}}
        result = evaluator._evaluate_expr_python(
            expr, {"obj": {"name": "John"}}
        )
        assert result == "John"

    def test_getField_missing(self):
        """Test $getField with missing field."""
        evaluator = ExprEvaluator()
        expr = {"$getField": {"field": "missing"}}
        result = evaluator._evaluate_expr_python(expr, {"name": "John"})
        assert result is None

    def test_setField_operator(self):
        """Test $setField operator."""
        evaluator = ExprEvaluator()
        expr = {"$setField": {"field": "name", "value": "John"}}
        result = evaluator._evaluate_expr_python(expr, {"age": 30})
        assert result == {"age": 30, "name": "John"}

    def test_setField_with_input(self):
        """Test $setField with input."""
        evaluator = ExprEvaluator()
        expr = {
            "$setField": {"field": "name", "value": "John", "input": "$obj"}
        }
        result = evaluator._evaluate_expr_python(expr, {"obj": {"age": 30}})
        assert result == {"age": 30, "name": "John"}

    def test_setField_override(self):
        """Test $setField override."""
        evaluator = ExprEvaluator()
        expr = {"$setField": {"field": "name", "value": "Jane"}}
        result = evaluator._evaluate_expr_python(expr, {"name": "John"})
        assert result == {"name": "Jane"}

    def test_unsetField_operator(self):
        """Test $unsetField operator."""
        evaluator = ExprEvaluator()
        expr = {"$unsetField": {"field": "name"}}
        result = evaluator._evaluate_expr_python(
            expr, {"name": "John", "age": 30}
        )
        assert result == {"age": 30}
        assert "name" not in result

    def test_unsetField_missing_field(self):
        """Test $unsetField with missing field."""
        evaluator = ExprEvaluator()
        expr = {"$unsetField": {"field": "missing"}}
        result = evaluator._evaluate_expr_python(expr, {"name": "John"})
        assert result == {"name": "John"}  # No change

    def test_unsetField_with_input(self):
        """Test $unsetField with input."""
        evaluator = ExprEvaluator()
        expr = {"$unsetField": {"field": "name", "input": "$obj"}}
        result = evaluator._evaluate_expr_python(
            expr, {"obj": {"name": "John", "age": 30}}
        )
        assert result == {"age": 30}

    def test_objectToArray_operator(self):
        """Test $objectToArray operator."""
        evaluator = ExprEvaluator()
        expr = {"$objectToArray": "$obj"}
        result = evaluator._evaluate_expr_python(
            expr, {"obj": {"a": 1, "b": 2}}
        )
        assert isinstance(result, list)
        assert len(result) == 2
        keys = {item["k"] for item in result}
        assert keys == {"a", "b"}

    def test_objectToArray_empty(self):
        """Test $objectToArray with empty object."""
        evaluator = ExprEvaluator()
        expr = {"$objectToArray": "$obj"}
        result = evaluator._evaluate_expr_python(expr, {"obj": {}})
        assert result == []


class TestObjectOperatorsSQL:
    """Test object operators SQL conversion."""

    def test_mergeObjects_sql_single(self):
        """Test $mergeObjects with single object SQL."""
        evaluator = ExprEvaluator()
        expr = {"$mergeObjects": [{"$getField": {"field": "obj1"}}]}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None

    def test_mergeObjects_sql_multiple(self):
        """Test $mergeObjects with multiple objects SQL."""
        evaluator = ExprEvaluator()
        expr = {
            "$mergeObjects": [
                {"$getField": {"field": "obj1"}},
                {"$getField": {"field": "obj2"}},
            ]
        }
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_patch" in sql

    def test_getField_sql(self):
        """Test $getField SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$getField": {"field": "name"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_extract" in sql
        assert "name" in sql

    def test_getField_with_input_sql(self):
        """Test $getField with input SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$getField": {"field": "name", "input": "$obj"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_extract" in sql

    def test_setField_sql(self):
        """Test $setField SQL conversion."""
        evaluator = ExprEvaluator()
        expr = {"$setField": {"field": "name", "value": "John"}}
        sql, params = evaluator._evaluate_sql_tier1(expr)
        assert sql is not None
        assert "json_set" in sql


class TestObjectIntegration:
    """Integration tests for object operators."""

    def test_getField_integration(self):
        """Test $getField with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"name": "John", "age": 30})

            expr = {
                "$expr": {"$eq": [{"$getField": {"field": "name"}}, "John"]}
            }
            results = list(collection.find(expr))
            assert len(results) == 1
            assert results[0]["name"] == "John"

    def test_unsetField_integration(self):
        """Test $unsetField with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"obj": {"a": 1, "b": 2, "c": 3}})

            set_force_fallback(True)
            try:
                expr = {
                    "$expr": {
                        "$and": [
                            {
                                "$ne": [
                                    {
                                        "$getField": {
                                            "field": "a",
                                            "input": "$obj",
                                        }
                                    },
                                    None,
                                ]
                            },
                            {
                                "$eq": [
                                    {
                                        "$getField": {
                                            "field": "a",
                                            "input": {
                                                "$unsetField": {
                                                    "field": "a",
                                                    "input": "$obj",
                                                }
                                            },
                                        }
                                    },
                                    None,
                                ]
                            },
                        ]
                    }
                }
                results = list(collection.find(expr))
                assert len(results) == 1
            finally:
                set_force_fallback(False)

    def test_objectToArray_integration(self):
        """Test $objectToArray with database."""
        with neosqlite.Connection(":memory:") as conn:
            collection = conn["test"]
            collection.insert_one({"obj": {"a": 1, "b": 2}})

            set_force_fallback(True)
            try:
                expr = {"$expr": {"$isArray": [{"$objectToArray": "$obj"}]}}
                results = list(collection.find(expr))
                assert len(results) == 1
            finally:
                set_force_fallback(False)
