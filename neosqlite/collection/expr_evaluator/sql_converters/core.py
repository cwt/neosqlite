"""SQL converters for core operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...json_path_utils import build_json_extract_expression

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class CoreMixin(BaseSqlMixin):
    """$expr dispatch, operand-to-SQL conversion, operator mapping."""

    def _convert_expr_to_sql(
        self, expr: dict[str, Any]
    ) -> tuple[str, list[Any]]:
        """
        Convert a $expr expression to SQL.

        Args:
            expr: Expression dictionary

        Returns:
            Tuple of (SQL expression, parameters)

        Raises:
            NotImplementedError: If operator is not supported in SQL
            ValueError: If expression structure is invalid
        """
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError("Invalid $expr expression structure")

        operator, operands = next(iter(expr.items()))

        # Handle different operator types
        match operator:
            case "$and" | "$or" | "$not" | "$nor":
                return self._convert_logical_operator(operator, operands)
            case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne":
                return self._convert_comparison_operator(operator, operands)
            case "$cmp":
                return self._convert_cmp_operator(operands)
            case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                return self._convert_arithmetic_operator(operator, operands)
            case (
                "$pow"
                | "$sqrt"
                | "$ln"
                | "$log"
                | "$log10"
                | "$log2"
                | "$exp"
                | "$sigmoid"
            ):
                return self._convert_math_operator(operator, operands)
            case "$cond":
                return self._convert_cond_operator(operands)
            case "$ifNull":
                return self._convert_ifNull_operator(operands)
            case "$switch":
                return self._convert_switch_operator(operands)
            case (
                "$size"
                | "$in"
                | "$isArray"
                | "$arrayElemAt"
                | "$first"
                | "$last"
                | "$firstN"
                | "$lastN"
                | "$sortArray"
                | "$maxN"
                | "$minN"
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
                return self._convert_array_operator(operator, operands)
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
            ):
                return self._convert_string_operator(operator, operands)
            case "$abs" | "$ceil" | "$floor" | "$round" | "$trunc":
                return self._convert_math_operator(operator, operands)
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
                return self._convert_trig_operator(operator, operands)
            case "$degreesToRadians" | "$radiansToDegrees":
                return self._convert_angle_operator(operator, operands)
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
                return self._convert_date_operator(operator, operands)
            case "$dateAdd" | "$dateSubtract":
                return self._convert_date_arithmetic_operator(
                    operator, operands
                )
            case "$dateDiff":
                return self._convert_date_diff_operator(operands)
            case "$dateToString":
                return self._convert_date_to_string_operator(operands)
            case "$dateTrunc":
                return self._convert_date_trunc_operator(operands)
            case "$dateFromParts":
                return self._convert_date_from_parts_operator(operands)
            case "$dateToParts":
                return self._convert_date_to_parts_operator(operands)
            case "$dateFromString":
                return self._convert_date_from_string_operator(operands)
            case (
                "$mergeObjects"
                | "$getField"
                | "$setField"
                | "$unsetField"
                | "$objectToArray"
                | "$literal"
                | "$rand"
            ):
                return self._convert_object_operator(operator, operands)
            case "$let":
                return self._convert_let_operator(operands)
            case "$filter" | "$map" | "$reduce":
                return self._convert_array_transform_operator(
                    operator, operands
                )
            case (
                "$type"
                | "$toString"
                | "$toInt"
                | "$toDouble"
                | "$toLong"
                | "$toBool"
                | "$toDecimal"
                | "$toObjectId"
                | "$isNumber"
                | "$convert"
            ):
                return self._convert_type_operator(operator, operands)
            case "$binarySize" | "$bsonSize":
                return self._convert_data_size_operator(operator, operands)
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in SQL tier"
                )

    def _convert_operand_to_sql(self, operand: Any) -> tuple[str, list[Any]]:
        """
        Convert an operand to SQL expression.

        Handles:
        - Field references: "$field" → json_extract/jsonb_extract expression
        - Literals: numbers, strings, booleans
        - Nested expressions: {"$operator": [...]}
        """
        # Check for aggregation variables if context is available
        from ..context import _is_aggregation_variable

        if (
            _is_aggregation_variable(operand)
            and hasattr(self, "_current_context")
            and self._current_context is not None
        ):
            # We are inside an aggregator that set the context
            # Use the handle_aggregation_variable method from the parent/mixin
            # which is mixed into ExprEvaluator
            return self._handle_aggregation_variable(
                operand, self._current_context
            )

        match operand:
            case str() if operand.startswith("$"):
                # Field reference
                field_path = operand[1:]  # Remove $
                # Use dynamic json/jsonb prefix based on support
                json_path_expr = build_json_extract_expression(
                    self.data_column, field_path
                )
                # Replace hardcoded "json_extract" with dynamic prefix
                if self.jsonb.jsonb_supported:
                    json_path_expr = json_path_expr.replace(
                        "json_extract", "jsonb_extract"
                    )
                return json_path_expr, []

            case list() | dict():
                # Check if it's an expression (dict with single key starting with $)
                if isinstance(operand, dict) and len(operand) == 1:
                    key = next(iter(operand.keys()))
                    if key.startswith("$"):
                        return self._convert_expr_to_sql(operand)

                # Literal list or dict - convert to JSON for SQL
                from neosqlite.collection.json_helpers import (
                    neosqlite_json_dumps,
                )

                return "json(?)", [neosqlite_json_dumps(operand)]

            case _:
                # Literal value (scalar)
                return "?", [operand]

    def _map_comparison_operator(self, op: str) -> str:
        """Map MongoDB comparison operators to SQL."""
        mapping = {
            "$eq": "=",
            "$gt": ">",
            "$gte": ">=",
            "$lt": "<",
            "$lte": "<=",
            "$ne": "!=",
        }
        return mapping.get(op, op)

    def _map_arithmetic_operator(self, op: str) -> str:
        """Map MongoDB arithmetic operators to SQL."""
        mapping = {
            "$add": "+",
            "$subtract": "-",
            "$multiply": "*",
            "$divide": "/",
            "$mod": "%",
        }
        return mapping.get(op, op)
