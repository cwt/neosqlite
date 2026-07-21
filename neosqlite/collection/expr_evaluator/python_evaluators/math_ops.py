"""Math, arithmetic, trigonometric, and angle Python evaluators."""

from __future__ import annotations

import math
import warnings
from typing import Any

from .base import BasePythonMixin


class MathPythonMixin(BasePythonMixin):
    """Arithmetic, math, trig, and angle conversion operators."""

    _log2_warned: bool

    def _evaluate_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
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
        self, operator: str, operands: list[Any], document: dict[str, Any]
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
        self, operands: list[Any], document: dict[str, Any]
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
        self, operands: list[Any], document: dict[str, Any]
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
        self, operator: str, operands: list[Any], document: dict[str, Any]
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
        self, operator: str, operands: Any, document: dict[str, Any]
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
