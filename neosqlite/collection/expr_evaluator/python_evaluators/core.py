"""Core Python evaluation: dispatcher, logical/comparison, conditionals, operands."""

from __future__ import annotations

from typing import Any

from ..constants import REMOVE_SENTINEL
from .base import BasePythonMixin


class CorePythonMixin(BasePythonMixin):
    """Entry point, operator dispatch, and basic evaluation helpers."""

    def evaluate_python(
        self, expr: dict[str, Any], document: dict[str, Any]
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
        self, expr: dict[str, Any], document: dict[str, Any]
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
            case "$meta":
                # Handle $meta: "textScore" for FTS relevance scoring
                if operands == "textScore":
                    # Extract stored text score from document
                    return document.get("_textScore", 0.0)
                else:
                    raise NotImplementedError(
                        f"$meta with '{operands}' not supported"
                    )
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
        self, operator: str, operands: list[Any], document: dict[str, Any]
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
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> bool:
        """Evaluate comparison operators in Python."""
        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        # For $eq/$ne, handle missing-field-vs-null distinction.
        # MongoDB: missing field == null literal -> False
        #          missing field == missing field -> True
        #          null field == null literal -> True
        if operator in ("$eq", "$ne"):
            left_missing = self._is_field_missing(operands[0], document)
            right_missing = self._is_field_missing(operands[1], document)
            # Literal null on one side, missing field on the other -> not equal
            if (operands[1] is None and left_missing) or (
                operands[0] is None and right_missing
            ):
                return False if operator == "$eq" else True
            if left_missing and right_missing:
                return True if operator == "$eq" else False
            if left_missing or right_missing:
                return False if operator == "$eq" else True
            return (left == right) if operator == "$eq" else (left != right)

        match operator:
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
        self, operands: list[Any], document: dict[str, Any]
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

    def _evaluate_cond_python(
        self, operands: dict[str, Any], document: dict[str, Any]
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
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate $ifNull operator in Python."""
        if not isinstance(operands, list) or len(operands) != 2:
            raise ValueError("$ifNull requires exactly 2 operands")

        expr = self._evaluate_operand_python(operands[0], document)
        if expr is not None:
            return expr
        return self._evaluate_operand_python(operands[1], document)

    def _evaluate_switch_python(
        self, operands: dict[str, Any], document: dict[str, Any]
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

    def _is_field_missing(self, operand: Any, document: dict[str, Any]) -> bool:
        """Check whether a $field reference points to a missing field.

        Returns True if operand is a field reference ($field) and the
        referenced field does not exist in the document.  Returns False
        for non-field operands (literals, expressions) and for fields
        that exist (even if their value is None).
        """
        if not isinstance(operand, str) or not operand.startswith("$"):
            return False
        if operand.startswith("$$"):
            # Aggregation variable; check if it exists in document
            return operand not in document
        field_path = operand[1:]
        keys = field_path.split(".")
        current: Any = document
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return True
        return False

    def _evaluate_literal_python(
        self, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate $literal operator in Python."""
        # $literal just returns its argument as-is (used to escape special characters)
        return self._evaluate_operand_python(operands, document)

    def _evaluate_operand_python(
        self, operand: Any, document: dict[str, Any]
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

                    # Check if there's a field path after the variable name
                    # e.g., $$comment.comment_author should split into var_name="$$comment" and field_parts=["comment_author"]
                    if "." in var_name:
                        # Split on first dot: "$$comment.comment_author" -> "$$comment" and "comment_author"
                        var_name, field_suffix = var_name.split(".", 1)
                        field_parts = field_suffix.split(".")
                    else:
                        field_parts = []

                    if var_name == "$$REMOVE":
                        # Special sentinel for field removal in $project
                        return REMOVE_SENTINEL

                    if var_name == "$$ROOT" or var_name == "$$CURRENT":
                        # If not explicitly in document context, the document itself
                        # is the root/current context
                        value = document.get(var_name, document)
                    else:
                        # Otherwise look up directly in document context
                        value = document.get(var_name)

                    # Navigate the field path within the variable value
                    for key in field_parts:
                        if isinstance(value, dict):
                            value = value.get(key)
                        else:
                            return None
                    return value

                # Regular field navigation (not a variable)
                keys = field_path.split(".")
                current: Any | None = document
                for key in keys:
                    if isinstance(current, dict):
                        current = current.get(key)
                    else:
                        return None
                return current

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
