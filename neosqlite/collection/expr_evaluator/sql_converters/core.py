"""SQL converters for core operators."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from ...json_path_utils import parse_json_path

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class CoreMixin(BaseSqlMixin):
    """Mixin providing core ``$expr`` SQL dispatch and operand conversion.

    Dispatches MongoDB aggregation expression operators to category-specific
    SQL converter methods inherited via ``BaseSqlMixin``, translates operands
    (field paths, aggregation variables, literals, and nested expressions)
    into parameterized SQL fragments, and maps comparison and arithmetic
    operators to their SQL operators.
    """

    def _convert_expr_to_sql(
        self, expr: dict[str, Any]
    ) -> tuple[str, list[Any]]:
        """Convert a ``$expr`` expression dictionary into a SQL fragment.

        Validates that ``expr`` is a single-key operator dictionary and routes
        the operator and operands to the appropriate category-specific SQL
        converter method.

        Args:
            expr: Single-key dictionary mapping an aggregation operator name to
                its operands.

        Returns:
            tuple[str, list[Any]]: Pair containing the generated SQL expression
            string and its bound parameter list.

        Raises:
            ValueError: If ``expr`` is not a single-key dictionary.
            NotImplementedError: If the operator is not supported in the SQL
                tier.
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
                | "$substrBytes"
                | "$substrCP"
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
            case "$meta":
                return self._convert_meta_operator(operands)
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in SQL tier"
                )

    def _convert_meta_operator(self, operands: Any) -> tuple[str, list[Any]]:
        """Translate ``$meta: "textScore"`` into a stored-score JSON extract.

        Text-search stages persist the FTS5 BM25 relevance score inside each
        document as ``_textScore``. Reading it back is therefore a plain JSON
        extraction with a ``0.0`` fallback for documents that were never
        scored. Keeping this in the SQL converter lets ``$meta`` participate in
        Tier-1 CTE compilation instead of dropping to the temporary-table
        path. Any other ``$meta`` argument raises ``NotImplementedError`` so
        the caller falls back to Python, matching the Python evaluator's
        behavior.

        Args:
            operands: The ``$meta`` argument, expected to be ``"textScore"``.

        Returns:
            tuple[str, list[Any]]: SQL expression and an empty parameter list.

        Raises:
            NotImplementedError: If ``operands`` is not ``"textScore"``.
        """
        if operands != "textScore":
            raise NotImplementedError(
                f"$meta with '{operands}' not supported in SQL tier"
            )
        extract = self.jsonb.function("extract")
        return (
            f"COALESCE({extract}({self.data_column}, '$._textScore'), 0.0)",
            [],
        )

    def _convert_operand_to_sql(self, operand: Any) -> tuple[str, list[Any]]:
        """Convert an operand into a parameterized SQL expression.

        Handles aggregation variables (when ``self._current_context`` is set),
        ``$field`` paths via ``json_extract`` / ``jsonb_extract``, nested
        single-key ``$`` expression dictionaries via ``_convert_expr_to_sql``,
        literal lists/dicts via ``json(?)``, ``datetime`` objects via
        ``json_object('$date', ?)``, and scalar literals as ``?`` placeholders.

        Args:
            operand: Field path, aggregation variable, nested expression dict,
                or literal value to convert.

        Returns:
            tuple[str, list[Any]]: Pair of ``(sql_expression, parameters)``.
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
                return (
                    f"{self.jsonb.function('extract')}("
                    f"{self.data_column}, '{parse_json_path(field_path)}')"
                ), []

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
                if isinstance(operand, datetime):
                    return "json_object('$date', ?)", [operand.isoformat()]
                return "?", [operand]

    def _map_comparison_operator(self, op: str) -> str:
        """Map a MongoDB comparison operator name to its SQL operator.

        Supports ``$eq``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, and ``$ne``;
        unrecognized operators are returned unchanged.

        Args:
            op: MongoDB comparison operator name.

        Returns:
            str: Corresponding SQL comparison operator, or ``op`` if unknown.
        """
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
        """Map a MongoDB arithmetic operator name to its SQL operator.

        Supports ``$add``, ``$subtract``, ``$multiply``, ``$divide``, and
        ``$mod``; unrecognized operators are returned unchanged.

        Args:
            op: MongoDB arithmetic operator name.

        Returns:
            str: Corresponding SQL arithmetic operator, or ``op`` if unknown.
        """
        mapping = {
            "$add": "+",
            "$subtract": "-",
            "$multiply": "*",
            "$divide": "/",
            "$mod": "%",
        }
        return mapping.get(op, op)
