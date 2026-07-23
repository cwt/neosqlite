"""SQL converters for math trig operators."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class MathTrigMixin(BaseSqlMixin):
    _log2_warned: bool

    def _convert_math_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert math operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$exp: 1} and {$exp: [1]}
        # Note: $pow, $log, $sigmoid, and $round can have multiple operands
        if operator not in (
            "$pow",
            "$log",
            "$sigmoid",
            "$round",
        ) and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$pow":
                # Handle $pow separately (requires 2 operands)
                if len(operands) != 2:
                    raise ValueError("$pow requires exactly 2 operands")
                base_sql, base_params = self._convert_operand_to_sql(
                    operands[0]
                )
                exp_sql, exp_params = self._convert_operand_to_sql(operands[1])
                sql = f"pow({base_sql}, {exp_sql})"
                return sql, base_params + exp_params
            case "$log":
                # $log with custom base requires 2 operands: [number, base]
                if len(operands) != 2:
                    raise ValueError(
                        "$log requires exactly 2 operands: [number, base]"
                    )
                number_sql, number_params = self._convert_operand_to_sql(
                    operands[0]
                )
                base_sql, base_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite: log(base, number)
                sql = f"log({base_sql}, {number_sql})"
                return sql, number_params + base_params
            case "$round":
                # $round can have 1 or 2 operands: [number] or [number, precision]
                if len(operands) < 1 or len(operands) > 2:
                    raise ValueError("$round requires 1 or 2 operands")
                number_sql, number_params = self._convert_operand_to_sql(
                    operands[0]
                )
                if len(operands) == 2:
                    precision_sql, precision_params = (
                        self._convert_operand_to_sql(operands[1])
                    )
                    sql = f"round({number_sql}, {precision_sql})"
                    return sql, number_params + precision_params
                else:
                    sql = f"round({number_sql})"
                    return sql, number_params
            case "$sigmoid":
                # Sigmoid function: 1 / (1 + e^(-x))
                # Handle object format: { $sigmoid: { input: <expr>, onNull: <expr> } }
                if isinstance(operands, dict):
                    input_sql, input_params = self._convert_operand_to_sql(
                        operands.get("input")
                    )
                    on_null_value = operands.get("onNull")
                    if on_null_value is not None:
                        on_null_sql, on_null_params = (
                            self._convert_operand_to_sql(on_null_value)
                        )
                        sql = f"(CASE WHEN {input_sql} IS NULL THEN {on_null_sql} ELSE (1.0 / (1.0 + exp(-({input_sql})))) END)"
                        return sql, input_params + on_null_params
                    else:
                        sql = f"(1.0 / (1.0 + exp(-({input_sql}))))"
                        return sql, input_params
                else:
                    # Simple format: { $sigmoid: <expr> } or { $sigmoid: [<expr>] }
                    if not isinstance(operands, list):
                        operands = [operands]
                    if len(operands) != 1:
                        raise ValueError("$sigmoid requires exactly 1 operand")
                    value_sql, value_params = self._convert_operand_to_sql(
                        operands[0]
                    )
                    sql = f"(1.0 / (1.0 + exp(-({value_sql}))))"
                    return sql, value_params
            case _:
                # All other math operators require 1 operand
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")

                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )

                match operator:
                    case "$abs":
                        sql = f"abs({value_sql})"
                    case "$ceil":
                        sql = f"ceil({value_sql})"
                    case "$floor":
                        sql = f"floor({value_sql})"
                    case "$round":
                        sql = f"round({value_sql})"
                    case "$trunc":
                        sql = f"cast({value_sql} as integer)"
                    case "$sqrt":
                        sql = f"sqrt({value_sql})"
                    case "$ln":
                        # Natural logarithm (base e)
                        sql = f"ln({value_sql})"
                    case "$log10":
                        # Base-10 logarithm
                        sql = f"log10({value_sql})"
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
                        sql = f"log2({value_sql})"
                    case "$exp":
                        # Exponential function (e^x)
                        sql = f"exp({value_sql})"
                    case _:
                        raise NotImplementedError(
                            f"Math operator {operator} not supported in SQL tier"
                        )

                return sql, value_params

    def _convert_trig_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert trigonometric and hyperbolic operators to SQL.

        Args:
            operator: The trig operator ($sin, $cos, etc.)
            operands: The operand(s). Can be:
                      - A single value (string, number) for simple cases like {"$sin": "$angle"}
                      - A list of values for array format like {"$sin": ["$angle"]}
        """
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$sin: "$angle"} and {$sin: ["$angle"]}
        if not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$atan2":
                # Handle $atan2 separately (requires 2 operands)
                if len(operands) != 2:
                    raise ValueError("$atan2 requires exactly 2 operands")
                y_sql, y_params = self._convert_operand_to_sql(operands[0])
                x_sql, x_params = self._convert_operand_to_sql(operands[1])
                sql = f"atan2({y_sql}, {x_sql})"
                return sql, y_params + x_params
            case _:
                # All other trig operators require 1 operand
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")

                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )

                # Standard trigonometric functions
                match operator:
                    case "$sin":
                        sql_func = "sin"
                    case "$cos":
                        sql_func = "cos"
                    case "$tan":
                        sql_func = "tan"
                    case "$asin":
                        sql_func = "asin"
                    case "$acos":
                        sql_func = "acos"
                    case "$atan":
                        sql_func = "atan"
                    # Hyperbolic functions
                    case "$sinh":
                        sql_func = "sinh"
                    case "$cosh":
                        sql_func = "cosh"
                    case "$tanh":
                        sql_func = "tanh"
                    # Inverse hyperbolic functions
                    case "$asinh":
                        sql_func = "asinh"
                    case "$acosh":
                        sql_func = "acosh"
                    case "$atanh":
                        sql_func = "atanh"
                    case _:
                        raise NotImplementedError(
                            f"Trig operator {operator} not supported in SQL tier"
                        )

                sql = f"{sql_func}({value_sql})"
                return sql, value_params

    def _convert_angle_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert angle conversion operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$degreesToRadians: 180} and {$degreesToRadians: [180]}
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        match operator:
            case "$degreesToRadians":
                # radians = degrees * pi() / 180
                sql = f"({value_sql} * pi() / 180.0)"
            case "$radiansToDegrees":
                # degrees = radians * 180 / pi()
                sql = f"({value_sql} * 180.0 / pi())"
            case _:
                raise NotImplementedError(
                    f"Angle operator {operator} not supported in SQL tier"
                )

        return sql, value_params
