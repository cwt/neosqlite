"""Comprehensive unit tests for PythonEvaluatorsMixin."""

import pytest
import math
import re
from datetime import datetime, timezone, timedelta
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.collection.expr_evaluator.constants import REMOVE_SENTINEL
from neosqlite.objectid import ObjectId

@pytest.fixture
def evaluator():
    return ExprEvaluator()

def eval_py(evaluator, expr, doc):
    return evaluator._evaluate_expr_python(expr, doc)

def test_logical_operators(evaluator):
    doc = {"a": 1}
    assert eval_py(evaluator, {"$nor": [{"$eq": ["$a", 2]}, {"$eq": ["$a", 3]}]}, doc) is True
    assert eval_py(evaluator, {"$nor": [{"$eq": ["$a", 1]}, {"$eq": ["$a", 3]}]}, doc) is False
    assert eval_py(evaluator, {"$and": [{"$eq": ["$a", 1]}, {"$literal": True}]}, doc) is True
    assert eval_py(evaluator, {"$or": [{"$eq": ["$a", 2]}, {"$literal": True}]}, doc) is True
    assert eval_py(evaluator, {"$not": [{"$eq": ["$a", 2]}]}, doc) is True
    
    with pytest.raises(ValueError, match="Unknown logical operator"):
        evaluator._evaluate_logical_python("$invalid", [], {})

def test_comparison_operators(evaluator):
    doc = {"a": 10, "b": 20}
    assert eval_py(evaluator, {"$cmp": ["$a", "$b"]}, doc) == -1
    assert eval_py(evaluator, {"$cmp": ["$b", "$a"]}, doc) == 1
    assert eval_py(evaluator, {"$cmp": ["$a", 10]}, doc) == 0
    
    assert eval_py(evaluator, {"$gt": [None, 10]}, doc) is False
    assert eval_py(evaluator, {"$lte": [10, 10]}, doc) is True
    assert eval_py(evaluator, {"$ne": [10, 20]}, doc) is True
    
    with pytest.raises(ValueError, match="Unknown comparison operator"):
        evaluator._evaluate_comparison_python("$invalid", [1, 2], {})

def test_arithmetic_operators(evaluator):
    doc = {"a": 10, "b": 2, "c": None}
    assert eval_py(evaluator, {"$add": ["$a", 5, 2]}, doc) == 17
    assert eval_py(evaluator, {"$subtract": ["$a", "$b", 1]}, doc) == 7
    assert eval_py(evaluator, {"$multiply": ["$a", "$b", 3]}, doc) == 60
    assert eval_py(evaluator, {"$divide": ["$a", "$b", 2]}, doc) == 2.5
    assert eval_py(evaluator, {"$mod": [10, 3]}, doc) == 1
    
    assert eval_py(evaluator, {"$add": ["$a", "$c"]}, doc) is None
    assert eval_py(evaluator, {"$divide": ["$a", 0]}, doc) is None
    assert eval_py(evaluator, {"$mod": [10, 0]}, doc) is None
    
    with pytest.raises(ValueError, match="Unknown arithmetic operator"):
        evaluator._evaluate_arithmetic_python("$invalid", [1, 2], {})

def test_math_operators(evaluator):
    doc = {"a": 1.5, "b": -1.5}
    assert eval_py(evaluator, {"$ceil": "$a"}, doc) == 2
    assert eval_py(evaluator, {"$floor": "$a"}, doc) == 1
    assert eval_py(evaluator, {"$trunc": "$a"}, doc) == 1
    assert eval_py(evaluator, {"$abs": "$b"}, doc) == 1.5
    
    # $round
    assert eval_py(evaluator, {"$round": [1.234, 2]}, doc) == 1.23
    assert eval_py(evaluator, {"$round": [1.5]}, doc) == 2.0
    assert eval_py(evaluator, {"$round": [1.234, None]}, doc) == 1.0
    
    # Logs
    assert eval_py(evaluator, {"$ln": {"$literal": math.e}}, doc) == 1.0
    assert eval_py(evaluator, {"$log10": 100}, doc) == 2.0
    assert eval_py(evaluator, {"$log2": 8}, doc) == 3.0
    assert eval_py(evaluator, {"$log": [100, 10]}, doc) == 2.0
    assert eval_py(evaluator, {"$exp": 0}, doc) == 1.0
    
    # Sigmoid
    assert eval_py(evaluator, {"$sigmoid": 0}, doc) == 0.5
    assert eval_py(evaluator, {"$sigmoid": {"input": 0}}, doc) == 0.5
    assert eval_py(evaluator, {"$sigmoid": {"input": None, "onNull": {"$literal": 1.0}}}, doc) == 1.0
    assert eval_py(evaluator, {"$sigmoid": [None]}, {"a": None}) is None
    
    # Pow and Sqrt
    assert eval_py(evaluator, {"$pow": [2, 3]}, doc) == 8
    assert eval_py(evaluator, {"$sqrt": 16}, doc) == 4.0
    assert eval_py(evaluator, {"$sqrt": -1}, doc) is None

def test_trig_operators(evaluator):
    assert eval_py(evaluator, {"$sin": 0}, {}) == 0.0
    assert eval_py(evaluator, {"$cos": 0}, {}) == 1.0
    assert eval_py(evaluator, {"$atan2": [0, 1]}, {}) == 0.0
    assert eval_py(evaluator, {"$asin": 2}, {}) is None
    assert eval_py(evaluator, {"$acosh": 0}, {}) is None
    assert eval_py(evaluator, {"$atanh": 1}, {}) is None
    assert eval_py(evaluator, {"$degreesToRadians": 180}, {}) == pytest.approx(math.pi)

def test_conditional_operators(evaluator):
    # $cond
    assert eval_py(evaluator, {"$cond": [{"$literal": True}, "yes", "no"]}, {}) == "yes"
    assert eval_py(evaluator, {"$cond": {"if": {"$literal": True}, "then": "yes", "else": "no"}}, {}) == "yes"
    assert eval_py(evaluator, {"$cond": {"if": {"$literal": False}, "then": "yes"}}, {}) is None
    
    # $ifNull
    assert eval_py(evaluator, {"$ifNull": [None, "fallback"]}, {}) == "fallback"
    assert eval_py(evaluator, {"$ifNull": ["value", "fallback"]}, {}) == "value"

def test_array_operators(evaluator):
    doc = {"arr": [1, 2, 3]}
    assert eval_py(evaluator, {"$size": ["$arr"]}, doc) == 3
    assert eval_py(evaluator, {"$in": [2, "$arr"]}, doc) is True
    assert eval_py(evaluator, {"$isArray": ["$arr"]}, doc) is True
    
    # Aggregation
    assert eval_py(evaluator, {"$sum": ["$arr"]}, doc) == 6
    assert eval_py(evaluator, {"$avg": ["$arr"]}, doc) == 2.0
    assert eval_py(evaluator, {"$min": ["$arr"]}, doc) == 1
    assert eval_py(evaluator, {"$max": ["$arr"]}, doc) == 3
    assert eval_py(evaluator, {"$sum": [["a"]]}, doc) == 0
    assert eval_py(evaluator, {"$avg": [["a"]]}, doc) is None
    
    # Selection
    assert eval_py(evaluator, {"$arrayElemAt": ["$arr", 1]}, doc) == 2
    assert eval_py(evaluator, {"$first": ["$arr"]}, doc) == 1
    assert eval_py(evaluator, {"$last": ["$arr"]}, doc) == 3
    assert eval_py(evaluator, {"$first": [[]]}, doc) is None
    
    # N operators
    assert eval_py(evaluator, {"$firstN": {"input": "$arr", "n": 2}}, doc) == [1, 2]
    assert eval_py(evaluator, {"$lastN": {"input": "$arr", "n": 2}}, doc) == [2, 3]
    assert eval_py(evaluator, {"$maxN": {"input": [3, 1, 2], "n": 2}}, doc) == [3, 2]
    assert eval_py(evaluator, {"$minN": {"input": [3, 1, 2], "n": 2}}, doc) == [1, 2]
    
    # $sortArray
    assert eval_py(evaluator, {"$sortArray": {"input": [3, 1, 2], "sortBy": None}}, doc) == [1, 2, 3]
    assert eval_py(evaluator, {"$sortArray": {"input": [{"x": 2}, {"x": 1}], "sortBy": {"x": 1}}}, doc) == [{"x": 1}, {"x": 2}]
    
    # $slice (existing tests use [array, count, skip])
    assert eval_py(evaluator, {"$slice": [[1, 2, 3, 4, 5], 2, 1]}, doc) == [2, 3]
    
    # $indexOfArray
    assert eval_py(evaluator, {"$indexOfArray": [[1, 2, 3], 2]}, doc) == 1
    assert eval_py(evaluator, {"$indexOfArray": [[1, 2, 3], 5]}, doc) == -1

def test_array_transform_operators(evaluator):
    doc = {"arr": [1, 2, 3]}
    # $filter
    assert eval_py(evaluator, {"$filter": {"input": "$arr", "as": "x", "cond": {"$gt": ["$$x", 1]}}}, doc) == [2, 3]
    # $map
    assert eval_py(evaluator, {"$map": {"input": "$arr", "as": "x", "in": {"$multiply": ["$$x", 2]}}}, doc) == [2, 4, 6]
    # $reduce
    assert eval_py(evaluator, {"$reduce": {"input": "$arr", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}}, doc) == 6

def test_string_operators(evaluator):
    doc = {"s": "hello"}
    assert eval_py(evaluator, {"$concat": ["a", "b", "c"]}, doc) == "abc"
    assert eval_py(evaluator, {"$toLower": "HELLO"}, doc) == "hello"
    assert eval_py(evaluator, {"$toUpper": "hello"}, doc) == "HELLO"
    assert eval_py(evaluator, {"$strLenBytes": "hello"}, doc) == 5
    assert eval_py(evaluator, {"$substr": ["hello", 1, 2]}, doc) == "el"
    assert eval_py(evaluator, {"$trim": {"input": "  abc  "}}, doc) == "abc"
    assert eval_py(evaluator, {"$ltrim": {"input": "  abc  "}}, doc) == "abc  "
    assert eval_py(evaluator, {"$rtrim": {"input": "  abc  "}}, doc) == "  abc"
    assert eval_py(evaluator, {"$indexOfBytes": ["l", "hello"]}, doc) == 2
    assert eval_py(evaluator, {"$regexMatch": {"input": "hello", "regex": "ell"}}, doc) is True
    assert eval_py(evaluator, {"$split": ["a,b,c", ","]}, doc) == ["a", "b", "c"]
    assert eval_py(evaluator, {"$replaceAll": {"input": "hello", "find": "l", "replacement": "x"}}, doc) == "hexxo"
    assert eval_py(evaluator, {"$replaceOne": {"input": "hello", "find": "l", "replacement": "x"}}, doc) == "hexlo"
    assert eval_py(evaluator, {"$strLenCP": "hello"}, doc) == 5
    assert eval_py(evaluator, {"$substrCP": ["hello", 1, 2]}, doc) == "el"
    assert eval_py(evaluator, {"$indexOfCP": ["l", "hello"]}, doc) == 2
    assert eval_py(evaluator, {"$strcasecmp": ["abc", "ABC"]}, doc) == 0
    assert eval_py(evaluator, {"$substrBytes": ["你好", 0, 3]}, doc) == "你"
    
    # Regex find
    res = eval_py(evaluator, {"$regexFind": {"input": "abc123def", "regex": "\\d+"}}, doc)
    assert res["match"] == "123"
    assert res["index"] == 3
    
    res_all = eval_py(evaluator, {"$regexFindAll": {"input": "a1b2", "regex": "\\d"}}, doc)
    assert len(res_all) == 2

def test_date_operators(evaluator):
    dt = datetime(2024, 3, 9, 12, 30, 45, 123000)
    doc = {"d": dt}
    assert eval_py(evaluator, {"$year": "$d"}, doc) == 2024
    assert eval_py(evaluator, {"$month": "$d"}, doc) == 3
    assert eval_py(evaluator, {"$dayOfMonth": "$d"}, doc) == 9
    assert eval_py(evaluator, {"$hour": "$d"}, doc) == 12
    assert eval_py(evaluator, {"$minute": "$d"}, doc) == 30
    assert eval_py(evaluator, {"$second": "$d"}, doc) == 45
    assert eval_py(evaluator, {"$millisecond": "$d"}, doc) == 123
    assert eval_py(evaluator, {"$dayOfWeek": "$d"}, doc) == 7  # Saturday (1=Sunday)
    assert eval_py(evaluator, {"$dayOfYear": "$d"}, doc) == 69
    assert eval_py(evaluator, {"$isoDayOfWeek": "$d"}, doc) == 6  # Saturday (1=Monday)
    
    # Arithmetic
    res_add = eval_py(evaluator, {"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1}}, doc)
    assert res_add.day == 10
    
    res_diff = eval_py(evaluator, {"$dateDiff": {"startDate": dt, "endDate": dt + timedelta(hours=2), "unit": "hour"}}, doc)
    assert res_diff == 2
    
    # Conversion
    res_str = eval_py(evaluator, {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}}, doc)
    assert res_str == "2024-03-09"
    
    res_from_str = eval_py(evaluator, {"$dateFromString": {"dateString": "2024-03-09T12:00:00Z"}}, doc)
    assert res_from_str.year == 2024
    
    res_parts = eval_py(evaluator, {"$dateToParts": {"date": "$d"}}, doc)
    assert res_parts["year"] == 2024
    
    res_from_parts = eval_py(evaluator, {"$dateFromParts": {"year": 2024, "month": 3, "day": 9}}, doc)
    assert res_from_parts.year == 2024
    
    res_trunc = eval_py(evaluator, {"$dateTrunc": {"date": "$d", "unit": "month"}}, doc)
    assert res_trunc.day == 1

def test_object_operators(evaluator):
    doc = {"obj": {"a": 1}}
    assert eval_py(evaluator, {"$mergeObjects": [{"x": 1}, {"y": 2}]}, doc) == {"x": 1, "y": 2}
    assert eval_py(evaluator, {"$getField": {"field": "a", "input": "$obj"}}, doc) == 1
    assert eval_py(evaluator, {"$setField": {"field": "b", "input": "$obj", "value": 2}}, doc) == {"a": 1, "b": 2}
    assert eval_py(evaluator, {"$unsetField": {"field": "a", "input": "$obj"}}, doc) == {}
    assert eval_py(evaluator, {"$objectToArray": "$obj"}, doc) == [{"k": "a", "v": 1}]
    # $let fix
    assert eval_py(evaluator, {"$let": {"vars": {"x": 1}, "in": {"$add": ["$$x", 10]}}}, doc) == 11

def test_type_operators(evaluator):
    doc = {"a": "123"}
    assert eval_py(evaluator, {"$type": "$a"}, doc) == "string"
    assert eval_py(evaluator, {"$toInt": "$a"}, doc) == 123
    assert eval_py(evaluator, {"$toBool": "$a"}, doc) is True
    assert eval_py(evaluator, {"$convert": {"input": "$a", "to": "int"}}, doc) == 123

def test_literal_and_operands(evaluator):
    doc = {"a": 1, "$$v": 2}
    assert eval_py(evaluator, {"$literal": {"$add": [1, 2]}}, doc) == {"$add": [1, 2]}
    assert evaluator._evaluate_operand_python("$a", doc) == 1
    assert evaluator._evaluate_operand_python("$$v", doc) == 2
    assert evaluator._evaluate_operand_python("$$REMOVE", doc) == REMOVE_SENTINEL
    assert evaluator._evaluate_operand_python(42, doc) == 42


def test_literal_python(evaluator):
    assert eval_py(evaluator, {"$literal": {"a": 1}}, {}) == {"a": 1}

def test_logical_operators_ext(evaluator):
    doc = {"a": 1}
    assert evaluator._evaluate_logical_python("$and", [{"$eq": ["$a", 1]}], doc) is True
    assert evaluator._evaluate_logical_python("$or", [{"$eq": ["$a", 2]}], doc) is False
    with pytest.raises(ValueError, match="not requires exactly one operand"):
        evaluator._evaluate_logical_python("$not", [1, 2], {})

def test_cmp_operand_count(evaluator):
    with pytest.raises(ValueError, match="requires exactly 2 operands"):
        evaluator._evaluate_cmp_python([1], {})

def test_arithmetic_divide_by_zero(evaluator):
    assert eval_py(evaluator, {"$divide": [10, 0]}, {}) is None
    assert eval_py(evaluator, {"$divide": [10, 2, 0]}, {}) is None

def test_math_log_operands(evaluator):
    with pytest.raises(ValueError, match="requires exactly 2 operands"):
        evaluator._evaluate_math_python("$log", [10], {})

def test_math_round_operands(evaluator):
    with pytest.raises(ValueError, match="requires 1 or 2 operands"):
        evaluator._evaluate_math_python("$round", [1, 2, 3], {})

def test_math_sigmoid_object_null(evaluator):
    expr = {"$sigmoid": {"input": None, "onNull": 0.5}}
    assert eval_py(evaluator, expr, {}) == 0.5

def test_math_pow_sqrt_errors(evaluator):
    with pytest.raises(ValueError, match="requires exactly 2 operands"):
        evaluator._evaluate_pow_python([1], {})
    assert evaluator._evaluate_pow_python([None, 2], {}) is None
    with pytest.raises(ValueError, match="requires exactly 1 operand"):
        evaluator._evaluate_sqrt_python([1, 2], {})
    assert evaluator._evaluate_sqrt_python([-1], {}) is None

def test_trig_atan2_errors(evaluator):
    with pytest.raises(ValueError, match="requires exactly 2 operands"):
        evaluator._evaluate_trig_python("$atan2", [1], {})
    assert evaluator._evaluate_trig_python("$atan2", [None, 1], {}) is None

def test_array_sum_avg_mixed(evaluator):
    assert eval_py(evaluator, {"$sum": "not-a-list"}, {}) == 0
    assert eval_py(evaluator, {"$avg": "not-a-list"}, {}) is None
    assert eval_py(evaluator, {"$sum": [["a", "b"]]}, {}) == 0
    assert eval_py(evaluator, {"$avg": [["a", "b"]]}, {}) is None

def test_array_elem_at_errors(evaluator):
    assert eval_py(evaluator, {"$arrayElemAt": [[1, 2], 5]}, {}) is None
    assert eval_py(evaluator, {"$arrayElemAt": [1, 0]}, {}) is None

def test_array_first_last_non_list(evaluator):
    # Non-list input returns None
    assert evaluator._evaluate_array_python("$first", 1, {}) is None
    assert evaluator._evaluate_array_python("$last", 1, {}) is None

def test_array_n_operators_errors(evaluator):
    with pytest.raises(ValueError, match="requires input array and n count"):
        evaluator._evaluate_array_python("$firstN", [1], {})
    assert eval_py(evaluator, {"$firstN": {"input": 1, "n": 1}}, {}) == []
    assert eval_py(evaluator, {"$lastN": {"input": [1], "n": -1}}, {}) == []
    with pytest.raises(ValueError, match="requires input array and n count"):
        evaluator._evaluate_array_python("$maxN", [1], {})
    assert eval_py(evaluator, {"$maxN": {"input": 1, "n": 1}}, {}) == []
    assert eval_py(evaluator, {"$maxN": {"input": [1, "a"], "n": 1}}, {}) == []
    with pytest.raises(ValueError, match="requires input array and n count"):
        evaluator._evaluate_array_python("$minN", [1], {})
    assert eval_py(evaluator, {"$minN": {"input": 1, "n": 1}}, {}) == []
    assert eval_py(evaluator, {"$minN": {"input": [1, "a"], "n": 1}}, {}) == []

def test_array_sort_errors(evaluator):
    with pytest.raises(ValueError, match="requires input array"):
        evaluator._evaluate_array_python("$sortArray", [], {})
    assert eval_py(evaluator, {"$sortArray": {"input": 1}}, {}) == []
    assert eval_py(evaluator, {"$sortArray": {"input": [1, "a"], "sortBy": None}}, {}) == [1, "a"]
    assert eval_py(evaluator, {"$sortArray": {"input": [{"x": 1}, {"x": "a"}], "sortBy": {"x": 1}}}, {}) == [{"x": 1}, {"x": "a"}]

def test_array_slice_non_list(evaluator):
    assert eval_py(evaluator, {"$slice": [1, 1]}, {}) == []

def test_date_day_of_week_correction(evaluator):
    # Sunday
    dt = datetime(2026, 3, 8)
    assert eval_py(evaluator, {"$dayOfWeek": "$d"}, {"d": dt}) == 1
    # Monday
    dt = datetime(2026, 3, 9)
    assert eval_py(evaluator, {"$dayOfWeek": "$d"}, {"d": dt}) == 2

def test_type_conversion_ext(evaluator):
    assert eval_py(evaluator, {"$toDouble": "1.5"}, {}) == 1.5
    assert eval_py(evaluator, {"$toDouble": "invalid"}, {}) is None
    assert eval_py(evaluator, {"$toInt": "10"}, {}) == 10
    assert eval_py(evaluator, {"$toInt": "invalid"}, {}) is None
    assert eval_py(evaluator, {"$toLong": "100"}, {}) == 100
    assert eval_py(evaluator, {"$toLong": "invalid"}, {}) is None
    assert eval_py(evaluator, {"$toDecimal": "10.5"}, {}) == Decimal("10.5")
    assert eval_py(evaluator, {"$toDecimal": "invalid"}, {}) is None
    
    oid_str = "507f1f77bcf86cd799439011"
    assert eval_py(evaluator, {"$toObjectId": oid_str}, {}) == ObjectId(oid_str)
    assert eval_py(evaluator, {"$toObjectId": "invalid"}, {}) is None
    
    assert eval_py(evaluator, {"$isNumber": 1}, {}) is True
    assert eval_py(evaluator, {"$isNumber": "1"}, {}) is False
    assert eval_py(evaluator, {"$isNumber": True}, {}) is False
    
    assert eval_py(evaluator, {"$toBool": 0}, {}) is False
    assert eval_py(evaluator, {"$toBool": 1}, {}) is True
    assert eval_py(evaluator, {"$toBool": ""}, {}) is False
    assert eval_py(evaluator, {"$toBool": "a"}, {}) is True
    assert eval_py(evaluator, {"$toBool": None}, {}) is False

def test_unsupported_operators(evaluator):
    with pytest.raises(NotImplementedError):
        eval_py(evaluator, {"$unsupported": 1}, {})
    with pytest.raises(ValueError, match="Unknown comparison operator"):
        evaluator._evaluate_comparison_python("$invalid", [1, 2], {})
    with pytest.raises(ValueError, match="Unknown math operator"):
        evaluator._evaluate_math_python("$invalid", [1], {})
    with pytest.raises(ValueError, match="Unknown trig operator"):
        evaluator._evaluate_trig_python("$invalid", [1], {})
    with pytest.raises(ValueError, match="Unknown angle operator"):
        evaluator._evaluate_angle_python("$invalid", [1], {})
    with pytest.raises(NotImplementedError):
        evaluator._evaluate_array_python("$invalid", [1], {})
