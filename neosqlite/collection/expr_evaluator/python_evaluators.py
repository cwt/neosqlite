"""
Python evaluation methods for NeoSQLite $expr operator.

This module contains the PythonEvaluatorsMixin class which provides
all the _evaluate_*_python methods for evaluating MongoDB $expr expressions
in Python as a fallback when SQL evaluation is not possible.
"""

from __future__ import annotations

import calendar
import logging
import math
import random
import re
import warnings
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List

logger = logging.getLogger(__name__)

# Import from sibling modules
from .constants import REMOVE_SENTINEL
from .type_utils import (
    _convert_to_bindata,
    _convert_to_bool,
    _convert_to_bsonbindata,
    _convert_to_bsonregex,
    _convert_to_date,
    _convert_to_decimal,
    _convert_to_double,
    _convert_to_int,
    _convert_to_long,
    _convert_to_null,
    _convert_to_objectid,
    _convert_to_regex,
    _convert_to_string,
    get_bson_type,
)

if TYPE_CHECKING:
    pass


class PythonEvaluatorsMixin:
    """
    Mixin class providing Python evaluation methods for $expr expressions.

    This mixin provides fallback evaluation capabilities when SQL-based
    evaluation (Tier 1 and Tier 2) is not possible or when the kill switch
    is activated.
    """

    # Type annotations for attributes expected from parent class
    _log2_warned: bool

    def evaluate_python(
        self, expr: Dict[str, Any], document: Dict[str, Any]
    ) -> bool:
        """
        Python fallback evaluation for $expr.

        This ensures identical results to SQL evaluation and provides
        the kill switch functionality.

        Args:
            expr: The $expr expression
            document: Document to evaluate against

        Returns:
            Boolean result of expression evaluation
        """
        result = self._evaluate_expr_python(expr, document)
        # For boolean context, ensure we return a boolean
        if isinstance(result, bool):
            return result
        # For comparison results (like $cmp), convert to boolean context
        return bool(result)

    def _evaluate_expr_python(
        self, expr: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Recursively evaluate expression in Python."""
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError("Invalid $expr expression structure")

        operator, operands = next(iter(expr.items()))

        # Handle different operator types
        match operator:
            case "$and" | "$or" | "$not" | "$nor":
                return self._evaluate_logical_python(
                    operator, operands, document
                )
            case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne":
                return self._evaluate_comparison_python(
                    operator, operands, document
                )
            case "$cmp":
                return self._evaluate_cmp_python(operands, document)
            case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                return self._evaluate_arithmetic_python(
                    operator, operands, document
                )
            case "$abs" | "$ceil" | "$floor" | "$round" | "$trunc":
                return self._evaluate_math_python(operator, operands, document)
            case "$ln" | "$log" | "$log10" | "$log2" | "$exp" | "$sigmoid":
                return self._evaluate_math_python(operator, operands, document)
            case "$pow":
                return self._evaluate_pow_python(operands, document)
            case "$sqrt":
                return self._evaluate_sqrt_python(operands, document)
            case (
                "$sin"
                | "$cos"
                | "$tan"
                | "$asin"
                | "$acos"
                | "$atan"
                | "$atan2"
                | "$sinh"
                | "$cosh"
                | "$tanh"
                | "$asinh"
                | "$acosh"
                | "$atanh"
            ):
                return self._evaluate_trig_python(operator, operands, document)
            case "$degreesToRadians" | "$radiansToDegrees":
                return self._evaluate_angle_python(operator, operands, document)
            case "$cond":
                return self._evaluate_cond_python(operands, document)
            case "$ifNull":
                return self._evaluate_ifNull_python(operands, document)
            case "$switch":
                return self._evaluate_switch_python(operands, document)
            case (
                "$size"
                | "$in"
                | "$isArray"
                | "$arrayElemAt"
                | "$first"
                | "$last"
                | "$firstN"
                | "$lastN"
                | "$maxN"
                | "$minN"
                | "$sortArray"
                | "$slice"
                | "$indexOfArray"
                | "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$setEquals"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
            ):
                return self._evaluate_array_python(operator, operands, document)
            case "$filter" | "$map" | "$reduce":
                return self._evaluate_array_transform_python(
                    operator, operands, document
                )
            case (
                "$concat"
                | "$toLower"
                | "$toUpper"
                | "$strLenBytes"
                | "$substr"
                | "$trim"
                | "$ltrim"
                | "$rtrim"
                | "$indexOfBytes"
                | "$regexMatch"
                | "$regexFind"
                | "$regexFindAll"
                | "$split"
                | "$replaceAll"
                | "$replaceOne"
                | "$strLenCP"
                | "$indexOfCP"
                | "$strcasecmp"
                | "$substrBytes"
                | "$substrCP"
            ):
                return self._evaluate_string_python(
                    operator, operands, document
                )
            case (
                "$year"
                | "$month"
                | "$dayOfMonth"
                | "$hour"
                | "$minute"
                | "$second"
                | "$dayOfWeek"
                | "$dayOfYear"
                | "$week"
                | "$isoDayOfWeek"
                | "$isoWeek"
                | "$millisecond"
            ):
                return self._evaluate_date_python(operator, operands, document)
            case "$dateAdd" | "$dateSubtract" | "$dateDiff":
                return self._evaluate_date_arithmetic_python(
                    operator, operands, document
                )
            case (
                "$dateFromString"
                | "$dateToString"
                | "$dateFromParts"
                | "$dateToParts"
                | "$dateTrunc"
            ):
                return self._evaluate_date_arithmetic_python(
                    operator, operands, document
                )
            case (
                "$mergeObjects"
                | "$getField"
                | "$setField"
                | "$unsetField"
                | "$objectToArray"
                | "$let"
                | "$literal"
                | "$rand"
            ):
                return self._evaluate_object_python(
                    operator, operands, document
                )
            case (
                "$type"
                | "$toString"
                | "$toInt"
                | "$toDouble"
                | "$toBool"
                | "$toLong"
                | "$toDecimal"
                | "$toObjectId"
                | "$isNumber"
                | "$convert"
            ):
                return self._evaluate_type_python(operator, operands, document)
            case "$binarySize" | "$bsonSize":
                return self._evaluate_data_size_python(
                    operator, operands, document
                )
            case "$literal":
                return self._evaluate_literal_python(operands, document)
            case "$function":
                raise NotImplementedError(
                    "The '$function' operator is not supported in NeoSQLite. "
                    "Please use '$expr' with Python expressions, or post-process results in Python."
                )
            case "$accumulator":
                raise NotImplementedError(
                    "The '$accumulator' operator is not supported in NeoSQLite. "
                    "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                    "or post-process results in Python."
                )
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in Python evaluation"
                )

    def _evaluate_logical_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> bool:
        """Evaluate logical operators in Python."""
        if operator == "$not":
            if len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            return not self._evaluate_expr_python(operands[0], document)

        results = [self._evaluate_expr_python(op, document) for op in operands]

        match operator:
            case "$and":
                return all(results)
            case "$or":
                return any(results)
            case "$nor":
                return not any(results)
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

    def _evaluate_comparison_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> bool:
        """Evaluate comparison operators in Python."""
        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        match operator:
            case "$eq":
                return left == right
            case "$ne":
                return left != right
            case "$gt" | "$gte" | "$lt" | "$lte":
                # For ordering comparisons, if any operand is None, return False
                # (MongoDB behavior - null values don't participate in ordering)
                if left is None or right is None:
                    return False
                return (
                    left > right
                    if operator == "$gt"
                    else (
                        left >= right
                        if operator == "$gte"
                        else (
                            left < right if operator == "$lt" else left <= right
                        )
                    )
                )
            case _:
                raise ValueError(f"Unknown comparison operator: {operator}")

    def _evaluate_cmp_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> int:
        """Evaluate $cmp operator in Python."""
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        if left < right:
            return -1
        elif left > right:
            return 1
        else:
            return 0

    def _evaluate_arithmetic_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> float | None:
        """Evaluate arithmetic operators in Python.

        Note: In MongoDB, arithmetic operations with null return null.
        """
        values = [
            self._evaluate_operand_python(op, document) for op in operands
        ]

        # If any operand is None, return None (MongoDB behavior)
        if any(v is None for v in values):
            return None

        match operator:
            case "$add":
                return sum(values)
            case "$subtract":
                return values[0] - sum(values[1:])
            case "$multiply":
                result = 1
                for v in values:
                    result *= v
                return result
            case "$divide":
                result = values[0]
                for v in values[1:]:
                    if v == 0:
                        return None  # Division by zero
                    result /= v
                return result
            case "$mod":
                if len(values) != 2 or values[1] == 0:
                    return None
                return values[0] % values[1]
            case _:
                raise ValueError(f"Unknown arithmetic operator: {operator}")

    def _evaluate_math_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> float | None:
        """Evaluate math operators in Python."""
        # Handle $log with custom base separately (requires 2 operands)
        if operator == "$log":
            if not isinstance(operands, list) or len(operands) != 2:
                raise ValueError(
                    "$log requires exactly 2 operands: [number, base]"
                )
            number = self._evaluate_operand_python(operands[0], document)
            base = self._evaluate_operand_python(operands[1], document)
            if number is None or base is None:
                return None
            if number <= 0 or base <= 1:
                return None
            return math.log(number, base)

        # $sigmoid can be either simple form or object form with onNull
        if operator == "$sigmoid":
            # Object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
            if isinstance(operands, dict):
                # Handled in the operator-specific section below
                pass
            else:
                # Simple format: { $sigmoid: <expr> }
                operands = (
                    [operands] if not isinstance(operands, list) else operands
                )

        # Handle both list and single operand formats for other operators
        if operator not in ("$sigmoid", "$round") and not isinstance(
            operands, list
        ):
            operands = [operands]

        if operator not in ("$sigmoid", "$round") and len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        if operator == "$round" and (len(operands) < 1 or len(operands) > 2):
            raise ValueError(f"{operator} requires 1 or 2 operands")

        if operator not in ("$sigmoid", "$round"):
            value = self._evaluate_operand_python(operands[0], document)
        elif operator == "$round":
            value = self._evaluate_operand_python(operands[0], document)
            precision = (
                self._evaluate_operand_python(operands[1], document)
                if len(operands) > 1
                else 0
            )
        else:
            value = (
                self._evaluate_operand_python(operands[0], document)
                if isinstance(operands, list)
                else None
            )

        match operator:
            case "$abs":
                return abs(value) if value is not None else None
            case "$ceil":
                return math.ceil(value) if value is not None else None
            case "$floor":
                return math.floor(value) if value is not None else None
            case "$round":
                if value is None:
                    return None
                if precision is None:
                    precision = 0
                return round(value, int(precision))
            case "$trunc":
                return int(value) if value is not None else None
            case "$ln":
                # Natural logarithm (base e)
                return (
                    math.log(value) if value is not None and value > 0 else None
                )
            case "$log10":
                # Base-10 logarithm
                return (
                    math.log10(value)
                    if value is not None and value > 0
                    else None
                )
            case "$log2":
                # Base-2 logarithm
                # Warn about NeoSQLite extension (not in MongoDB)
                if not self._log2_warned:
                    warnings.warn(
                        "$log2 is a NeoSQLite extension (not available in MongoDB). "
                        "For MongoDB compatibility, use { $log: [ <number>, 2 ] } instead.",
                        UserWarning,
                        stacklevel=4,
                    )
                    self._log2_warned = True
                return (
                    math.log2(value)
                    if value is not None and value > 0
                    else None
                )
            case "$exp":
                # Exponential function (e^x)
                return math.exp(value) if value is not None else None
            case "$sigmoid":
                # Sigmoid function: 1 / (1 + e^(-x))
                # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
                if isinstance(operands, dict):
                    input_val = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    on_null = operands.get("onNull")
                    if input_val is None:
                        return self._evaluate_operand_python(on_null, document)
                    return 1.0 / (1.0 + math.exp(-input_val))

                # Simple format: operands is a list [expr] or just expr
                if isinstance(operands, list):
                    if not operands:
                        return None
                    input_val = self._evaluate_operand_python(
                        operands[0], document
                    )
                else:
                    input_val = self._evaluate_operand_python(
                        operands, document
                    )

                if input_val is None:
                    return None
                return 1.0 / (1.0 + math.exp(-input_val))
            case _:
                raise ValueError(f"Unknown math operator: {operator}")

    def _evaluate_pow_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> float | None:
        """Evaluate $pow operator in Python."""
        if len(operands) != 2:
            raise ValueError("$pow requires exactly 2 operands")
        base = self._evaluate_operand_python(operands[0], document)
        exponent = self._evaluate_operand_python(operands[1], document)
        if base is None or exponent is None:
            return None
        return pow(base, exponent)

    def _evaluate_sqrt_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> float | None:
        """Evaluate $sqrt operator in Python."""
        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]
        if len(operands) != 1:
            raise ValueError("$sqrt requires exactly 1 operand")
        value = self._evaluate_operand_python(operands[0], document)

        return math.sqrt(value) if value is not None and value >= 0 else None

    def _evaluate_trig_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> float | None:
        """Evaluate trigonometric operators in Python."""

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]

        # Handle $atan2 separately (requires 2 operands)
        if operator == "$atan2":
            if len(operands) != 2:
                raise ValueError("$atan2 requires exactly 2 operands")
            y = self._evaluate_operand_python(operands[0], document)
            x = self._evaluate_operand_python(operands[1], document)
            if y is None or x is None:
                return None
            return math.atan2(y, x)

        # All other trig operators require 1 operand
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        match operator:
            case "$sin":
                return math.sin(value)
            case "$cos":
                return math.cos(value)
            case "$tan":
                return math.tan(value)
            case "$asin":
                return math.asin(value) if -1 <= value <= 1 else None
            case "$acos":
                return math.acos(value) if -1 <= value <= 1 else None
            case "$atan":
                return math.atan(value)
            # Hyperbolic functions
            case "$sinh":
                return math.sinh(value)
            case "$cosh":
                return math.cosh(value)
            case "$tanh":
                return math.tanh(value)
            # Inverse hyperbolic functions
            case "$asinh":
                return math.asinh(value)
            case "$acosh":
                return math.acosh(value) if value >= 1 else None
            case "$atanh":
                return math.atanh(value) if -1 < value < 1 else None
            case _:
                raise ValueError(f"Unknown trig operator: {operator}")

    def _evaluate_angle_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> float | None:
        """Evaluate angle conversion operators in Python."""

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            ops = [operands]
        else:
            ops = operands

        if len(ops) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(ops[0], document)
        if value is None:
            return None

        match operator:
            case "$degreesToRadians":
                return math.radians(value)
            case "$radiansToDegrees":
                return math.degrees(value)
            case _:
                raise ValueError(f"Unknown angle operator: {operator}")

    def _evaluate_cond_python(
        self, operands: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $cond operator in Python."""
        if not isinstance(operands, dict):
            # Handle array format: [condition, true_case, false_case]
            if isinstance(operands, list) and len(operands) == 3:
                condition = self._evaluate_expr_python(operands[0], document)
                if condition:
                    return self._evaluate_operand_python(operands[1], document)
                else:
                    return self._evaluate_operand_python(operands[2], document)
            raise ValueError("$cond requires a dictionary or 3-element array")

        if "if" not in operands or "then" not in operands:
            raise ValueError("$cond requires 'if' and 'then' fields")

        condition = self._evaluate_expr_python(operands["if"], document)
        if condition:
            return self._evaluate_operand_python(operands["then"], document)
        elif "else" in operands:
            return self._evaluate_operand_python(operands["else"], document)
        else:
            return None

    def _evaluate_ifNull_python(
        self, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $ifNull operator in Python."""
        if not isinstance(operands, list) or len(operands) != 2:
            raise ValueError("$ifNull requires exactly 2 operands")

        expr = self._evaluate_operand_python(operands[0], document)
        if expr is not None:
            return expr
        return self._evaluate_operand_python(operands[1], document)

    def _evaluate_switch_python(
        self, operands: Dict[str, Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $switch operator in Python."""
        if not isinstance(operands, dict):
            raise ValueError("$switch requires a dictionary")

        branches = operands.get("branches", [])
        default = operands.get("default")

        for branch in branches:
            if not isinstance(branch, dict):
                continue
            case = branch.get("case")
            then = branch.get("then")
            if case is not None and self._evaluate_expr_python(case, document):
                return self._evaluate_operand_python(then, document)

        if default is not None:
            return self._evaluate_operand_python(default, document)
        return None

    def _evaluate_array_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate array operators in Python."""
        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return len(array)
                return None
            case "$in":
                if len(operands) != 2:
                    raise ValueError("$in requires exactly 2 operands")
                value = self._evaluate_operand_python(operands[0], document)
                array = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    return value in array
                return False
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return isinstance(value, list)
            case "$sum" | "$avg" | "$min" | "$max":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    array_ops = [operands]
                else:
                    array_ops = operands

                if len(array_ops) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array = self._evaluate_operand_python(array_ops[0], document)
                if not isinstance(array, list):
                    return 0 if operator == "$sum" else None

                # Filter numeric values for sum/avg
                nums = [
                    v
                    for v in array
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
                ]

                if not nums:
                    if operator == "$sum":
                        return 0
                    return None

                match operator:
                    case "$sum":
                        return sum(nums)
                    case "$avg":
                        return sum(nums) / len(nums)
                    case "$min":
                        return min(array)  # min/max work on all types
                    case "$max":
                        return max(array)
                    case _:
                        return None
            case "$arrayElemAt":
                if len(operands) != 2:
                    raise ValueError("$arrayElemAt requires exactly 2 operands")
                array = self._evaluate_operand_python(operands[0], document)
                index = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list) and isinstance(index, int):
                    try:
                        return array[index]
                    except IndexError:
                        return None
                return None
            case "$first":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    ops = [operands]
                else:
                    ops = operands
                if len(ops) != 1:
                    raise ValueError("$first requires exactly 1 operand")
                array = self._evaluate_operand_python(ops[0], document)
                if isinstance(array, list) and len(array) > 0:
                    return array[0]
                return None
            case "$last":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    ops = [operands]
                else:
                    ops = operands
                if len(ops) != 1:
                    raise ValueError("$last requires exactly 1 operand")
                array = self._evaluate_operand_python(ops[0], document)
                if isinstance(array, list) and len(array) > 0:
                    return array[-1]
                return None
            case "$firstN":
                # Get first N elements from array
                # MongoDB syntax: { $firstN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$firstN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                return array[: int(n)]
            case "$lastN":
                # Get last N elements from array
                # MongoDB syntax: { $lastN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$lastN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                n_int = int(n)
                if n_int <= 0:
                    return []
                return array[-n_int:] if n_int < len(array) else array
            case "$maxN":
                # Get maximum N elements from array (sorted descending, take first N)
                # MongoDB syntax: { $maxN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$maxN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                # Sort descending and take first N
                try:
                    sorted_array = sorted(array, reverse=True)
                    return sorted_array[: int(n)]
                except (TypeError, ValueError):
                    return []
            case "$minN":
                # Get minimum N elements from array (sorted ascending, take first N)
                # MongoDB syntax: { $minN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$minN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                # Sort ascending and take first N
                try:
                    sorted_array = sorted(array)
                    return sorted_array[: int(n)]
                except (TypeError, ValueError):
                    return []
            case "$sortArray":
                # Sort array elements
                # MongoDB syntax: { $sortArray: { input: <array>, sortBy: { <field>: <direction> } } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    sort_by = operands.get("sortBy")
                elif isinstance(operands, list) and len(operands) >= 1:
                    array_operand = operands[0]
                    sort_by = operands[1] if len(operands) > 1 else None
                else:
                    raise ValueError("$sortArray requires input array")

                array = self._evaluate_operand_python(array_operand, document)

                if not isinstance(array, list):
                    return []

                # If no sortBy specified, sort primitive values
                if sort_by is None:
                    try:
                        return sorted(array)
                    except TypeError:
                        return array

                # Sort by field (for array of objects)
                if isinstance(sort_by, dict):
                    # Get first field and direction
                    sort_field = next(iter(sort_by.keys()))
                    direction = sort_by[sort_field]
                    reverse = direction == -1

                    try:

                        def sort_key(x: Any) -> Any:
                            """
                            Extract the sort field from a dictionary or return the value.
                            """
                            return (
                                x.get(sort_field) if isinstance(x, dict) else x
                            )

                        return sorted(
                            array,
                            key=sort_key,  # type: ignore[arg-type]
                            reverse=reverse,
                        )
                    except (TypeError, AttributeError):
                        return array

                return array
            case "$slice":
                if not isinstance(operands, list) or len(operands) < 2:
                    raise ValueError("$slice requires array and count/position")
                array = self._evaluate_operand_python(operands[0], document)
                count = self._evaluate_operand_python(operands[1], document)

                if not isinstance(array, list):
                    return []
                if len(operands) >= 3:
                    skip = self._evaluate_operand_python(operands[2], document)
                    return array[skip : skip + count]
                elif isinstance(count, int) and count < 0:
                    return array[count:]
                else:
                    return array[:count]
            case "$indexOfArray":
                if len(operands) != 2:
                    raise ValueError(
                        "$indexOfArray requires exactly 2 operands"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                value = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    try:
                        return array.index(value)
                    except ValueError:
                        return -1
                return -1
            case "$setEquals":
                if len(operands) != 2:
                    raise ValueError("$setEquals requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1) == set(set2)
                return False
            case "$setIntersection":
                if len(operands) != 2:
                    raise ValueError(
                        "$setIntersection requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) & set(set2))
                return []
            case "$setUnion":
                if len(operands) != 2:
                    raise ValueError("$setUnion requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) | set(set2))
                return []
            case "$setDifference":
                if len(operands) != 2:
                    raise ValueError(
                        "$setDifference requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) - set(set2))
                return []
            case "$setIsSubset":
                if len(operands) != 2:
                    raise ValueError("$setIsSubset requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1).issubset(set(set2))
                return False
            case "$anyElementTrue":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError(
                        "$anyElementTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return any(array)
                return False
            case "$allElementsTrue":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError(
                        "$allElementsTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return all(array)
                return False
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in Python evaluation"
                )

    def _evaluate_array_transform_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate $filter, $map, $reduce operators in Python.

        These operators use variable scoping:
        - $filter: {input: <array>, as: <var>, cond: <expr>}
        - $map: {input: <array>, as: <var>, in: <expr>}
        - $reduce: {input: <array>, initialValue: <val>, in: <expr>}
        """
        match operator:
            case "$filter":
                if not isinstance(operands, dict):
                    raise ValueError("$filter requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "item")
                cond = operands.get("cond")

                if cond is None:
                    raise ValueError("$filter requires 'cond' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate condition in context
                    if self._evaluate_expr_python(cond, ctx):
                        result.append(item)

                return result

            case "$map":
                if not isinstance(operands, dict):
                    raise ValueError("$map requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "item")
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$map requires 'in' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate expression in context
                    result.append(self._evaluate_operand_python(in_expr, ctx))

                return result

            case "$reduce":
                if not isinstance(operands, dict):
                    raise ValueError("$reduce requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return None

                initial_value = operands.get("initialValue")
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$reduce requires 'in' expression")

                # Evaluate initial value
                acc = self._evaluate_operand_python(initial_value, document)

                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    # $$value is the accumulator, $$this is the current item
                    ctx = dict(document)
                    ctx["$$value"] = acc
                    ctx["$$this"] = item
                    ctx["$$index"] = i

                    # Evaluate expression in context to get new accumulator value
                    acc = self._evaluate_operand_python(in_expr, ctx)

                return acc

            case _:
                raise NotImplementedError(
                    f"Array transform operator {operator} not supported in Python evaluation"
                )

    def _evaluate_string_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate string operators in Python.

        Args:
            operator: The string operator ($toUpper, $toLower, etc.)
            operands: The operand(s). Can be:
                      - A single value for simple cases like {"$toUpper": "$field"}
                      - A list of values for array format
                      - A dict for operators like $trim, $regexMatch
            document: The document to evaluate against
        """
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$toUpper: "$field"} and {$toUpper: ["$field"]}
        # But some operators like $trim, $regexMatch, $replaceAll use dict format
        if operator in (
            "$trim",
            "$ltrim",
            "$rtrim",
            "$regexMatch",
            "$regexFind",
            "$regexFindAll",
            "$replaceAll",
            "$replaceOne",
        ):
            # These operators use dict format, don't normalize
            pass
        elif not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$concat":
                values = [
                    self._evaluate_operand_python(op, document)
                    for op in operands
                ]
                return "".join(str(v) if v is not None else "" for v in values)
            case "$toLower":
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).lower() if value is not None else None
            case "$toUpper":
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).upper() if value is not None else None
            case "$strLenBytes":
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return (
                    len(str(value).encode("utf-8"))
                    if value is not None
                    else None
                )
            case "$substr":
                if len(operands) != 3:
                    raise ValueError("$substr requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$trim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$trim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).strip(str(chars_val))
                return str(input_val).strip()
            case "$ltrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$ltrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).lstrip(str(chars_val))
                return str(input_val).lstrip()
            case "$rtrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$rtrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).rstrip(str(chars_val))
                return str(input_val).rstrip()
            case "$indexOfBytes":
                if len(operands) < 2:
                    raise ValueError(
                        "$indexOfBytes requires string and substring"
                    )
                string = self._evaluate_operand_python(operands[0], document)
                substr = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$regexMatch":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexMatch requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return False

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                return bool(re.search(regex, str(input_val), flags))
            case "$split":
                if len(operands) != 2:
                    raise ValueError("$split requires string and delimiter")
                string = self._evaluate_operand_python(operands[0], document)
                delimiter = self._evaluate_operand_python(operands[1], document)
                if string is None or delimiter is None:
                    return []
                return str(string).split(str(delimiter))
            case "$replaceAll":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    find = self._evaluate_operand_python(
                        operands.get("find"), document
                    )
                    replacement = self._evaluate_operand_python(
                        operands.get("replacement"), document
                    )
                else:
                    # Handle list format
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceAll requires string, find, and replacement"
                        )
                    string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    find = self._evaluate_operand_python(operands[1], document)
                    replacement = self._evaluate_operand_python(
                        operands[2], document
                    )
                if string is None:
                    return None
                return str(string).replace(str(find), str(replacement))
            case "$replaceOne":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    find = self._evaluate_operand_python(
                        operands.get("find"), document
                    )
                    replacement = self._evaluate_operand_python(
                        operands.get("replacement"), document
                    )
                else:
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceOne requires string, find, and replacement"
                        )
                    string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    find = self._evaluate_operand_python(operands[1], document)
                    replacement = self._evaluate_operand_python(
                        operands[2], document
                    )
                if string is None:
                    return None
                # Replace only first occurrence
                return str(string).replace(str(find), str(replacement), 1)
            case "$strLenCP":
                # String length in code points (Unicode characters)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$strLenCP requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                return len(str(value))
            case "$substrCP":
                # Substring by code points (not implemented - use $substr)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 3:
                    raise ValueError("$substrCP requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    # For BMP characters, this is the same as $substr
                    # For full Unicode support, would need proper code point handling
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$indexOfCP":
                # Find substring by code points
                if len(operands) < 2:
                    raise ValueError("$indexOfCP requires string and substring")
                string = self._evaluate_operand_python(operands[0], document)
                substr = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$strcasecmp":
                # Case-insensitive string comparison
                if len(operands) != 2:
                    raise ValueError("$strcasecmp requires exactly 2 operands")
                str1 = self._evaluate_operand_python(operands[0], document)
                str2 = self._evaluate_operand_python(operands[1], document)
                if str1 is None or str2 is None:
                    return None
                # Return -1, 0, or 1 like MongoDB
                s1 = str(str1).lower()
                s2 = str(str2).lower()
                if s1 < s2:
                    return -1
                elif s1 > s2:
                    return 1
                else:
                    return 0
            case "$substrBytes":
                # Substring by bytes (for UTF-8 encoded strings)
                if len(operands) != 3:
                    raise ValueError("$substrBytes requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if string is None or start is None or length is None:
                    return None
                # Encode to UTF-8, slice by bytes, decode back
                encoded = str(string).encode("utf-8")
                sliced = encoded[int(start) : int(start) + int(length)]
                try:
                    return sliced.decode("utf-8")
                except UnicodeDecodeError:
                    # If we cut in the middle of a multi-byte character, return what we can
                    return sliced.decode("utf-8", errors="ignore")
            case "$regexFind":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexFind requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return None

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                match_result = re.search(regex, str(input_val), flags)
                if match_result:
                    result = {
                        "match": match_result.group(),
                        "idx": match_result.start(),
                        "captures": (
                            list(match_result.groups())
                            if match_result.groups()
                            else []
                        ),
                    }
                    return result
                return None
            case "$regexFindAll":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError(
                        "$regexFindAll requires 'input' and 'regex'"
                    )
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return []

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                matches = list(re.finditer(regex, str(input_val), flags))
                all_results: List[Dict[str, Any]] = []
                for match_result in matches:
                    match_obj: Dict[str, Any] = {
                        "match": match_result.group(),
                        "idx": match_result.start(),
                        "captures": (
                            list(match_result.groups())
                            if match_result.groups()
                            else []
                        ),
                    }
                    all_results.append(match_obj)
                return all_results
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in Python evaluation"
                )

    def _evaluate_date_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> int | None:
        """Evaluate date operators in Python.

        MongoDB compatibility: Date operators require the field to be stored as
        BSON Date/datetime type. String dates are NOT automatically converted,
        matching MongoDB's behavior.
        """

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        # MongoDB compatibility: Only accept datetime objects, not strings
        # MongoDB's $year, $month, etc. fail with "can't convert from BSON type string to Date"
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            # Reject string dates to match MongoDB behavior
            raise ValueError(
                f"${operator} requires a date type field, got string. "
                "Store dates as datetime objects, not ISO strings."
            )
        else:
            return None

        # Extract date components
        match operator:
            case "$year":
                return dt.year
            case "$month":
                return dt.month
            case "$dayOfMonth":
                return dt.day
            case "$hour":
                return dt.hour
            case "$minute":
                return dt.minute
            case "$second":
                return dt.second
            case "$millisecond":
                return dt.microsecond // 1000
            case "$dayOfWeek":
                # MongoDB uses 1 (Sunday) to 7 (Saturday)
                # Python's weekday() returns 0 (Monday) to 6 (Sunday)
                return ((dt.weekday() + 1) % 7) + 1
            case "$dayOfYear":
                return dt.timetuple().tm_yday
            case "$week":
                # Week of year (0-53)
                return int(dt.strftime("%U"))
            case "$isoDayOfWeek":
                return dt.isocalendar()[2]  # 1=Monday
            case "$isoWeek":
                return dt.isocalendar()[1]
            case _:
                raise NotImplementedError(
                    f"Date operator {operator} not supported in Python evaluation"
                )

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate $dateAdd, $dateSubtract, $dateDiff operators in Python."""
        match operator:
            case "$dateAdd" | "$dateSubtract":
                # Handle MongoDB dict format: {startDate, amount, unit}
                if isinstance(operands, dict):
                    operands = [
                        operands.get("startDate"),
                        operands.get("amount"),
                        operands.get("unit", "day"),
                    ]

                if len(operands) < 2 or len(operands) > 3:
                    raise ValueError(
                        f"{operator} requires 2-3 operands: [date, amount, unit]"
                    )

                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None

                amount = self._evaluate_operand_python(operands[1], document)
                if amount is None:
                    return None

                unit = operands[2] if len(operands) > 2 else "day"

                # Parse date value
                if isinstance(value, str):
                    try:
                        dt = datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None
                elif isinstance(value, datetime):
                    dt = value
                else:
                    return None

                # Create timedelta based on unit
                if unit == "year":
                    # Handle years separately (not supported by timedelta directly)
                    years = amount if operator == "$dateAdd" else -amount
                    try:
                        new_dt = dt.replace(year=dt.year + int(years))
                        dt = new_dt
                    except ValueError:
                        # Handle Feb 29 edge case
                        new_dt = dt.replace(year=dt.year + int(years), day=28)
                        dt = new_dt
                elif unit == "month":
                    # Handle months separately
                    months = amount if operator == "$dateAdd" else -amount
                    new_month = dt.month + int(months)
                    new_year = dt.year + (new_month - 1) // 12
                    new_month = ((new_month - 1) % 12) + 1
                    try:
                        dt = dt.replace(year=new_year, month=new_month)
                    except ValueError:
                        # Handle day overflow (e.g., Jan 31 + 1 month)
                        last_day = calendar.monthrange(new_year, new_month)[1]
                        dt = dt.replace(
                            year=new_year,
                            month=new_month,
                            day=min(dt.day, last_day),
                        )
                else:
                    # Convert to timedelta
                    delta_kwargs = {
                        f"{unit}s": (
                            amount if operator == "$dateAdd" else -amount
                        )
                    }
                    delta = timedelta(**delta_kwargs)
                    dt = dt + delta

                # Return datetime object (MongoDB compatibility)
                return dt
            case "$dateDiff":
                # Handle MongoDB dict format: {startDate, endDate, unit}
                if isinstance(operands, dict):
                    start_operand = operands.get("startDate")
                    end_operand = operands.get("endDate")
                    unit = operands.get("unit", "day")
                    # Evaluate operands
                    start = self._evaluate_operand_python(
                        start_operand, document
                    )
                    end = self._evaluate_operand_python(end_operand, document)
                else:
                    if len(operands) < 2:
                        raise ValueError(
                            "$dateDiff requires startDate and endDate"
                        )
                    start = self._evaluate_operand_python(operands[0], document)
                    end = self._evaluate_operand_python(operands[1], document)
                    unit = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else "day"
                    )

                if start is None or end is None:
                    return None

                # Parse dates
                if isinstance(start, str):
                    try:
                        start = datetime.fromisoformat(
                            start.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None
                if isinstance(end, str):
                    try:
                        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    except ValueError:
                        return None

                if not isinstance(start, datetime) or not isinstance(
                    end, datetime
                ):
                    return None

                # Calculate difference
                delta = end - start

                match unit:
                    case "year":
                        return end.year - start.year
                    case "month":
                        return (end.year - start.year) * 12 + (
                            end.month - start.month
                        )
                    case "day":
                        return delta.days
                    case "hour":
                        return int(delta.total_seconds() / 3600)
                    case "minute":
                        return int(delta.total_seconds() / 60)
                    case "second":
                        return int(delta.total_seconds())
                    case "millisecond":
                        return int(delta.total_seconds() * 1000)
                    case "week":
                        return delta.days // 7
                    case _:
                        return delta.days
            case "$dateFromString":
                # Handle MongoDB dict format: {dateString, timezone, onError, onNull}
                if isinstance(operands, dict):
                    date_string_operand = operands.get("dateString")
                    timezone = operands.get("timezone")
                    on_error = operands.get("onError")
                    on_null = operands.get("onNull")
                    # Evaluate the dateString operand
                    date_string = self._evaluate_operand_python(
                        date_string_operand, document
                    )
                else:
                    if len(operands) < 1:
                        raise ValueError("$dateFromString requires dateString")
                    date_string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    timezone = (
                        self._evaluate_operand_python(operands[1], document)
                        if len(operands) > 1
                        else None
                    )
                    on_error = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else None
                    )
                    on_null = (
                        self._evaluate_operand_python(operands[3], document)
                        if len(operands) > 3
                        else None
                    )

                if date_string is None:
                    return on_null

                try:
                    # If already a datetime, return it
                    if isinstance(date_string, datetime):
                        return date_string

                    # Parse ISO 8601 date string
                    if isinstance(date_string, str):
                        # Handle various ISO 8601 formats
                        date_string = date_string.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(date_string)

                        # Handle timezone if specified
                        if timezone and dt.tzinfo is None:
                            # Simple timezone handling (e.g., "+05:30")
                            try:
                                from datetime import timezone as tz

                                if str(timezone).startswith("+") or str(
                                    timezone
                                ).startswith("-"):
                                    tz_str = str(timezone)
                                    hours = int(tz_str[1:3])
                                    minutes = (
                                        int(tz_str[4:6])
                                        if len(tz_str) > 4
                                        else 0
                                    )
                                    offset_seconds = hours * 3600 + minutes * 60
                                    if tz_str[0] == "-":
                                        offset_seconds = -offset_seconds
                                    dt = dt.replace(tzinfo=tz.utc)  # Simplified
                            except (ValueError, TypeError, AttributeError) as e:
                                logger.debug(
                                    f"Failed to parse timezone in $dateFromString: {e}"
                                )
                                pass

                        return dt
                    return None
                except Exception as e:
                    logger.debug(f"Failed to evaluate $dateFromString: {e}")
                    return on_error
            case "$dateToString":
                # Handle MongoDB dict format: {format, date, timezone}
                if isinstance(operands, dict):
                    fmt = operands.get("format", "%Y-%m-%d")
                    date_operand = operands.get("date")
                    timezone = operands.get("timezone")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 2:
                        raise ValueError(
                            "$dateToString requires format and date"
                        )
                    fmt = self._evaluate_operand_python(operands[0], document)
                    date_val = self._evaluate_operand_python(
                        operands[1], document
                    )
                    timezone = operands[2] if len(operands) > 2 else None

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Convert MongoDB format to Python strftime format
                # MongoDB uses %Y, %m, %d, %H, %M, %S, %L (milliseconds), %Z (timezone)
                python_fmt = fmt.replace("%L", "%f")[
                    :19
                ]  # %f gives microseconds, we'll truncate

                result = date_val.strftime(python_fmt)

                # Handle milliseconds (%L)
                if "%L" in fmt:
                    ms = date_val.microsecond // 1000
                    result = result.replace(
                        str(date_val.microsecond)[:3].zfill(3), str(ms).zfill(3)
                    )

                return result
            case "$dateFromParts":
                # Handle MongoDB dict format: {year, month, day, hour, minute, second, millisecond, timezone}
                if not isinstance(operands, dict):
                    raise ValueError("$dateFromParts requires a dictionary")

                year = self._evaluate_operand_python(
                    operands.get("year"), document
                )
                month = (
                    self._evaluate_operand_python(
                        operands.get("month"), document
                    )
                    or 1
                )
                day = (
                    self._evaluate_operand_python(operands.get("day"), document)
                    or 1
                )
                hour = (
                    self._evaluate_operand_python(
                        operands.get("hour"), document
                    )
                    or 0
                )
                minute = (
                    self._evaluate_operand_python(
                        operands.get("minute"), document
                    )
                    or 0
                )
                second = (
                    self._evaluate_operand_python(
                        operands.get("second"), document
                    )
                    or 0
                )
                millisecond = (
                    self._evaluate_operand_python(
                        operands.get("millisecond"), document
                    )
                    or 0
                )
                timezone = operands.get("timezone")

                if year is None:
                    return None

                try:
                    dt = datetime(
                        year=int(year),
                        month=int(month),
                        day=int(day),
                        hour=int(hour),
                        minute=int(minute),
                        second=int(second),
                        microsecond=(
                            int(millisecond) * 1000 if millisecond else 0
                        ),
                    )
                    return dt
                except (ValueError, TypeError):
                    return None
            case "$dateToParts":
                # Handle MongoDB dict format: {date, timezone, unit}
                if isinstance(operands, dict):
                    date_operand = operands.get("date")
                    timezone = operands.get("timezone")
                    unit = operands.get("unit")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 1:
                        raise ValueError("$dateToParts requires date")
                    date_val = self._evaluate_operand_python(
                        operands[0], document
                    )
                    timezone = (
                        self._evaluate_operand_python(operands[1], document)
                        if len(operands) > 1
                        else None
                    )
                    unit = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else None
                    )

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Build parts dictionary
                parts = {
                    "year": date_val.year,
                    "month": date_val.month,
                    "day": date_val.day,
                    "hour": date_val.hour,
                    "minute": date_val.minute,
                    "second": date_val.second,
                    "millisecond": date_val.microsecond // 1000,
                }

                # If unit is specified, only return parts up to that unit
                match unit:
                    case "year":
                        return {"year": parts["year"]}
                    case "month":
                        return {"year": parts["year"], "month": parts["month"]}
                    case "day":
                        return {
                            "year": parts["year"],
                            "month": parts["month"],
                            "day": parts["day"],
                        }
                    case "hour":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k in ["year", "month", "day", "hour"]
                        }
                    case "minute":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k in ["year", "month", "day", "hour", "minute"]
                        }
                    case "second":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k
                            in [
                                "year",
                                "month",
                                "day",
                                "hour",
                                "minute",
                                "second",
                            ]
                        }
                    case _:
                        return parts
            case "$dateTrunc":
                # Handle MongoDB dict format: {date, unit, startOfWeek}
                if isinstance(operands, dict):
                    date_operand = operands.get("date")
                    unit = operands.get("unit", "day")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 2:
                        raise ValueError("$dateTrunc requires date and unit")
                    date_val = self._evaluate_operand_python(
                        operands[0], document
                    )
                    unit = self._evaluate_operand_python(operands[1], document)

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Truncate based on unit
                if unit == "year":
                    return date_val.replace(
                        month=1,
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                elif unit == "quarter":
                    # Round down to start of quarter
                    quarter_month = ((date_val.month - 1) // 3) * 3 + 1
                    return date_val.replace(
                        month=quarter_month,
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                elif unit == "month":
                    return date_val.replace(
                        day=1, hour=0, minute=0, second=0, microsecond=0
                    )
                elif unit == "week":
                    # Round down to start of week (Monday by default)
                    days_since_monday = date_val.weekday()
                    return (
                        date_val - timedelta(days=days_since_monday)
                    ).replace(hour=0, minute=0, second=0, microsecond=0)
                elif unit == "day":
                    return date_val.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                elif unit == "hour":
                    return date_val.replace(minute=0, second=0, microsecond=0)
                elif unit == "minute":
                    return date_val.replace(second=0, microsecond=0)
                elif unit == "second":
                    return date_val.replace(microsecond=0)
                else:
                    return date_val
            case _:
                raise NotImplementedError(
                    f"Date arithmetic operator {operator} not supported in Python evaluation"
                )

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate object operators in Python."""
        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list):
                    raise ValueError("$mergeObjects requires a list of objects")
                result: dict[str, Any] = {}
                for obj in operands:
                    obj_val = self._evaluate_operand_python(obj, document)
                    if isinstance(obj_val, dict):
                        result |= obj_val
                return result
            case "$getField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError("$getField requires 'field' specification")
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = document
                if not isinstance(obj, dict):
                    return None
                return obj.get(field)
            case "$setField":
                if not isinstance(operands, dict):
                    raise ValueError("$setField requires a dictionary")
                field = operands.get("field")
                value = operands.get("value")
                input_val = operands.get("input")
                if field is None:
                    raise ValueError("$setField requires 'field'")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    obj = {}
                result = dict(obj)
                result[field] = self._evaluate_operand_python(value, document)
                return result
            case "$unsetField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError(
                        "$unsetField requires 'field' specification"
                    )
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    return None
                result = dict(obj)
                result.pop(field, None)
                return result
            case "$objectToArray":
                # Convert object to array of {k, v} objects
                if isinstance(operands, dict):
                    obj = operands
                else:
                    obj = self._evaluate_operand_python(operands, document)
                if not isinstance(obj, dict):
                    return []
                return [{"k": k, "v": v} for k, v in obj.items()]
            case "$let":
                # MongoDB syntax: { $let: { vars: { <var1>: <expr1>, ... }, in: <expr> } }
                if not isinstance(operands, dict):
                    raise ValueError("$let requires a dictionary")
                vars_spec = operands.get("vars", {})
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$let requires 'in' expression")

                # Create new document context with variables
                new_context = dict(document)
                for var_name, var_expr in vars_spec.items():
                    var_value = self._evaluate_operand_python(
                        var_expr, document
                    )
                    new_context["$$" + var_name] = var_value

                # Evaluate the 'in' expression with new context
                return self._evaluate_expr_python(in_expr, new_context)
            case "$literal":
                # Return the operand as-is without evaluation
                return operands
            case "$rand":
                # Return random number between 0 and 1
                return random.random()
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in Python evaluation"
                )

    def _evaluate_data_size_python(
        self, operator: str, operands: Any, document: Dict[str, Any]
    ) -> int:
        """Evaluate data size operators ($binarySize, $bsonSize) in Python."""
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)

        if operator == "$binarySize":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return len(value)
            # Binary class is a subclass of bytes, so it's already covered.
            # Handle encoded binary objects
            if isinstance(value, dict) and value.get("__neosqlite_binary__"):
                from ...binary import Binary

                try:
                    bin_val = Binary.decode_from_storage(value)
                    return len(bin_val)
                except Exception as e:
                    logger.debug(
                        f"Failed to decode binary for $binarySize: {e}"
                    )
                    pass

            raise TypeError(
                f"$binarySize requires a binary value, got {type(value)}"
            )

        elif operator == "$bsonSize":
            # MongoDB $bsonSize returns the size of the document in BSON bytes.
            # In NeoSQLite, we'll return the size of the JSON representation.
            import json

            # Use simple JSON dump for size calculation (approximates BSON)
            try:
                # Use a basic approach for now
                return len(json.dumps(value).encode("utf-8"))
            except Exception as e:
                # Fallback to string length for non-serializable objects
                logger.debug(
                    f"Failed to calculate $bsonSize using JSON, falling back to string length: {e}"
                )
                return len(str(value).encode("utf-8"))

        raise NotImplementedError(f"Operator {operator} not supported")

    def _evaluate_type_python(
        self, operator: str, operands: List[Any], document: Dict[str, Any]
    ) -> Any:
        """Evaluate type conversion operators in Python."""
        # Handle both list and single operand formats (but not for $convert which needs dict)
        if operator != "$convert" and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$type":
                if len(operands) != 1:
                    raise ValueError("$type requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return get_bson_type(value)
            case "$toString":
                if len(operands) != 1:
                    raise ValueError("$toString requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value) if value is not None else None
            case "$toInt":
                if len(operands) != 1:
                    raise ValueError("$toInt requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDouble":
                if len(operands) != 1:
                    raise ValueError("$toDouble requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return float(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toBool":
                if len(operands) != 1:
                    raise ValueError("$toBool requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return False
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    return len(value) > 0
                return bool(value)
            case "$toLong":
                if len(operands) != 1:
                    raise ValueError("$toLong requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    # Python ints are already 64-bit
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDecimal":
                if len(operands) != 1:
                    raise ValueError("$toDecimal requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    from decimal import Decimal, InvalidOperation

                    return Decimal(str(value)) if value is not None else None
                except (ValueError, TypeError, ImportError, InvalidOperation):
                    return None
            case "$toObjectId":
                if len(operands) != 1:
                    raise ValueError("$toObjectId requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                # Convert hex string to ObjectId
                from neosqlite.objectid import ObjectId

                try:
                    if isinstance(value, str) and len(value) == 24:
                        return ObjectId(value)
                    # For other types, try to create from string representation
                    return ObjectId(str(value))
                except Exception as e:
                    logger.debug(
                        f"Failed to convert to ObjectId in expression: {e}"
                    )
                    return None
            case "$isNumber":
                # Check if value is a number (int or float, but not bool)
                if len(operands) != 1:
                    raise ValueError("$isNumber requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                # In Python, bool is a subclass of int, so we need to check for bool first
                if isinstance(value, bool):
                    return False
                return isinstance(value, (int, float))
            case "$convert":
                # $convert is complex - requires 'to' field
                if not isinstance(operands, dict):
                    raise ValueError("$convert requires a dictionary")
                input_val = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                to_type = operands.get("to")
                on_error = operands.get("onError")
                on_null = operands.get("onNull")

                if input_val is None:
                    return on_null

                # Import required types upfront
                from neosqlite.objectid import ObjectId

                # Map conversion types to named converter methods
                conversion_map = {
                    "int": _convert_to_int,
                    "long": _convert_to_long,
                    "double": _convert_to_double,
                    "decimal": _convert_to_decimal,
                    "string": _convert_to_string,
                    "bool": _convert_to_bool,
                    "objectId": _convert_to_objectid,
                    "binData": _convert_to_bindata,
                    "bsonBinData": _convert_to_bsonbindata,
                    "regex": _convert_to_regex,
                    "bsonRegex": _convert_to_bsonregex,
                    "date": _convert_to_date,
                    "null": _convert_to_null,
                }

                try:
                    converter = conversion_map.get(to_type)
                    if converter:
                        return converter(input_val)
                    return input_val
                except Exception as e:
                    logger.debug(f"Failed to convert type in $convert: {e}")
                    return on_error
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in Python evaluation"
                )

    # Type converter wrapper methods for backward compatibility (used by tests)
    @staticmethod
    def _convert_to_int(value: Any) -> Any:
        """Convert value to int."""
        return _convert_to_int(value)

    @staticmethod
    def _convert_to_long(value: Any) -> Any:
        """Convert value to long (64-bit int)."""
        return _convert_to_long(value)

    @staticmethod
    def _convert_to_double(value: Any) -> Any:
        """Convert value to double (float)."""
        return _convert_to_double(value)

    @staticmethod
    def _convert_to_decimal(value: Any) -> Any:
        """Convert value to decimal (float, as SQLite lacks Decimal128)."""
        return _convert_to_decimal(value)

    @staticmethod
    def _convert_to_string(value: Any) -> Any:
        """Convert value to string."""
        return _convert_to_string(value)

    @staticmethod
    def _convert_to_bool(value: Any) -> Any:
        """Convert value to bool."""
        return _convert_to_bool(value)

    @staticmethod
    def _convert_to_objectid(value: Any) -> Any:
        """Convert value to ObjectId."""
        return _convert_to_objectid(value)

    @staticmethod
    def _convert_to_bindata(value: Any) -> Any:
        """Convert value to Binary (binData)."""
        return _convert_to_bindata(value)

    @staticmethod
    def _convert_to_bsonbindata(value: Any) -> Any:
        """Convert value to Binary (bsonBinData)."""
        return _convert_to_bsonbindata(value)

    @staticmethod
    def _convert_to_regex(value: Any) -> Any:
        """Convert value to regex pattern."""
        return _convert_to_regex(value)

    @staticmethod
    def _convert_to_bsonregex(value: Any) -> Any:
        """Convert value to regex pattern (bsonRegex)."""
        return _convert_to_bsonregex(value)

    @staticmethod
    def _convert_to_date(value: Any) -> Any:
        """Convert value to date."""
        return _convert_to_date(value)

    @staticmethod
    def _convert_to_null(value: Any) -> None:
        """Convert any value to None."""
        return _convert_to_null(value)

    def _get_bson_type(self, value: Any) -> str:
        """Get BSON type name for a value."""
        return get_bson_type(value)

    def _evaluate_literal_python(
        self, operands: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate $literal operator in Python."""
        # $literal just returns its argument as-is (used to escape special characters)
        return self._evaluate_operand_python(operands, document)

    def _evaluate_operand_python(
        self, operand: Any, document: Dict[str, Any]
    ) -> Any:
        """Evaluate an operand in Python context."""
        match operand:
            case str() if operand.startswith("$"):
                # Field reference - navigate document
                field_path = operand[1:]  # Remove $

                # Handle $$variable syntax
                if field_path.startswith("$"):
                    # $$var syntax - check for special variables
                    var_name = "$" + field_path  # Reconstruct $$var
                    if var_name == "$$REMOVE":
                        # Special sentinel for field removal in $project
                        return REMOVE_SENTINEL

                    if var_name == "$$ROOT" or var_name == "$$CURRENT":
                        # If not explicitly in document context, the document itself
                        # is the root/current context
                        return document.get(var_name, document)

                    # Otherwise look up directly in document context
                    return document.get(var_name)

                keys = field_path.split(".")
                value: Any | None = document
                for key in keys:
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        return None
                return value

            case dict():
                # Check if it's an expression (single key starting with $) or literal dict
                if len(operand) == 1:
                    key = next(iter(operand.keys()))
                    if key.startswith("$"):
                        # Nested expression
                        return self._evaluate_expr_python(operand, document)
                # Otherwise, it's a literal dict (e.g., for $mergeObjects)
                return operand

            case _:
                # Literal value
                return operand
