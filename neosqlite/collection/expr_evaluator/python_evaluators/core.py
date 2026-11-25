"""Core Python evaluation: dispatch, logic, comparisons, and operands."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..constants import REMOVE_SENTINEL
from .base import BasePythonMixin, _is_mongo_truthy


class CorePythonMixin(BasePythonMixin):
    """Entry point, operator dispatch, and basic evaluation helpers.

    Implements the Tier-3 Python fallback evaluator for ``$expr`` expressions,
    covering operator dispatch, logical and comparison operators, conditional
    expressions, literal escaping, and document field/variable resolution.
    """

    def evaluate_python(
        self, expr: dict[str, Any], document: dict[str, Any]
    ) -> bool:
        """Evaluate an ``$expr`` expression against a document in Python.

        Provides the Tier-3 Python fallback matching SQL evaluation semantics
        and supporting the force-fallback kill switch. Converts non-boolean
        evaluation results to ``bool`` using Python truthiness.

        Args:
            expr: The ``$expr`` expression dictionary to evaluate.
            document: Document dictionary to evaluate the expression against.

        Returns:
            bool: Truthiness of the evaluated expression result.
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
        """Recursively evaluate a single-key ``$expr`` dictionary in Python.

        Dispatches the operator key to the corresponding Python evaluator
        method across logical, comparison, arithmetic, math, trigonometric,
        conditional, array, string, date, object, type, and size operators.

        Args:
            expr: Single-key expression dictionary ``{operator: operands}``.
            document: Document dictionary providing field and variable context.

        Returns:
            Any: Evaluated Python value for the expression.

        Raises:
            ValueError: If ``expr`` is not a single-key dictionary.
            NotImplementedError: If the operator (or ``$meta`` argument) is
                not supported in Python evaluation.
        """
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
            case "$function":
                from ....js import (
                    JsWorkerError,
                    get_js_worker,
                    js_unavailable_reason,
                )

                worker = get_js_worker()
                if worker is None:
                    raise NotImplementedError(
                        "The '$function' operator is not supported in NeoSQLite "
                        f"(no supported JS engine: {js_unavailable_reason()}). "
                        "Please use '$expr' with Python expressions, or post-process results in Python."
                    )
                if not isinstance(operands, dict):
                    raise ValueError(
                        "$function requires a specification object"
                    )
                body = operands.get("body")
                lang = operands.get("lang", "js")
                if lang != "js":
                    raise ValueError(f"Unsupported $function language: {lang}")
                if hasattr(body, "__str__") and not isinstance(body, str):
                    body = str(body)
                if not isinstance(body, str):
                    raise ValueError(
                        "$function body must be a JavaScript string"
                    )
                args = [
                    self._evaluate_operand_python(item, document)
                    for item in (operands.get("args") or [])
                ]
                try:
                    return worker.call(body, args)
                except JsWorkerError as exc:
                    raise ValueError(
                        f"JavaScript error in $function: {exc}"
                    ) from exc
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
        """Evaluate logical ``$expr`` operators in Python.

        Supports ``$not`` (both single-element list and bare-dict forms),
        short-circuiting ``$and`` and ``$or``, and ``$nor``.

        Args:
            operator: Logical operator name (``$and``, ``$or``, ``$not``, or
                ``$nor``).
            operands: Operator operands (list of expressions or operands, or a
                single expression dict for ``$not``).
            document: Document dictionary used for operand evaluation.

        Returns:
            bool: Boolean result of the logical operation.

        Raises:
            ValueError: If ``$not`` has invalid operands or ``operator`` is
                unknown.
        """
        if operator == "$not":
            # Accept both {"$not": [<expr>]} and the documented bare-dict
            # form {"$not": {"$gt": [...]}} (#122), plus field/scalar operands.
            if isinstance(operands, dict):
                return not _is_mongo_truthy(
                    self._evaluate_operand_python(operands, document)
                )
            if not isinstance(operands, list) or len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            return not _is_mongo_truthy(
                self._evaluate_operand_python(operands[0], document)
            )

        # Short-circuit: evaluate lazily so later (possibly invalid) operands
        # do not crash rows already decided (#122)
        if operator == "$and":
            for op in operands:
                if not self._evaluate_operand_python(op, document):
                    return False
            return True
        if operator == "$or":
            for op in operands:
                if self._evaluate_operand_python(op, document):
                    return True
            return False
        if operator == "$nor":
            return not any(
                self._evaluate_operand_python(op, document) for op in operands
            )

        raise ValueError(f"Unknown logical operator: {operator}")

    def _evaluate_comparison_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> bool:
        """Evaluate comparison ``$expr`` operators in Python.

        Evaluates ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, and ``$lte``
        with MongoDB missing-field vs. null semantics for equality and
        cross-type rank ordering for relational comparisons.

        Args:
            operator: Comparison operator name.
            operands: Two-element sequence ``[left, right]`` to compare.
            document: Document dictionary used for operand evaluation.

        Returns:
            bool: Result of the comparison.

        Raises:
            ValueError: If ``operator`` is not a recognized comparison
                operator.
        """
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
                left_missing = self._is_field_missing(operands[0], document)
                right_missing = self._is_field_missing(operands[1], document)

                def _type_rank(v, missing):
                    """Return a rank for cross-type comparison ordering.

                    Ranks missing fields (0) below ``None`` (1), numbers and
                    ``Decimal`` (2), strings (3), dicts (4), lists/tuples (5),
                    other values (6), and booleans (9).

                    Args:
                        v: Evaluated operand value.
                        missing: True if the operand was a missing field.

                    Returns:
                        int: Relative type rank used for relational ordering.
                    """
                    if missing:
                        return 0
                    if v is None:
                        return 1
                    if isinstance(v, bool):
                        return 9
                    if isinstance(v, Decimal):
                        return 2
                    if isinstance(v, (int, float)):
                        return 2
                    if isinstance(v, str):
                        return 3
                    if isinstance(v, dict):
                        return 4
                    if isinstance(v, (list, tuple)):
                        return 5
                    return 6

                lr = _type_rank(left, left_missing)
                rr = _type_rank(right, right_missing)
                if lr != rr:
                    # Different type ranks: use rank ordering.
                    if operator == "$gt":
                        return lr > rr
                    if operator == "$gte":
                        return lr >= rr
                    if operator == "$lt":
                        return lr < rr
                    return lr <= rr
                # Same type rank: compare values.
                if left is None and right is None:
                    # null (including present null) vs null
                    if operator in ("$gte", "$lte", "$eq"):
                        return True
                    return False
                try:
                    if operator == "$gt":
                        return left > right
                    if operator == "$gte":
                        return left >= right
                    if operator == "$lt":
                        return left < right
                    return left <= right
                except (TypeError, ValueError):
                    return False
            case _:
                raise ValueError(f"Unknown comparison operator: {operator}")

    def _evaluate_cmp_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> int:
        """Evaluate the ``$cmp`` operator in Python using BSON sort keys.

        Args:
            operands: Two-element list ``[left, right]`` to compare.
            document: Document dictionary used for operand evaluation.

        Returns:
            int: ``-1`` if left < right, ``1`` if left > right, or ``0`` if
                equal according to ``bson_sort_key()``.

        Raises:
            ValueError: If ``operands`` does not contain exactly 2 elements.
        """
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left = self._evaluate_operand_python(operands[0], document)
        right = self._evaluate_operand_python(operands[1], document)

        from ...type_utils import bson_sort_key

        left_key = bson_sort_key(left)
        right_key = bson_sort_key(right)

        if left_key < right_key:
            return -1
        elif left_key > right_key:
            return 1
        else:
            return 0

    def _evaluate_cond_python(
        self, operands: dict[str, Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate the ``$cond`` conditional operator in Python.

        Accepts either a 3-element array ``[if, then, else]`` or a dictionary
        with ``"if"``, ``"then"``, and optional ``"else"`` keys (returning
        None when ``"else"`` is omitted and the condition is falsy).

        Args:
            operands: Conditional specification as a dict or 3-element list.
            document: Document dictionary used for evaluation.

        Returns:
            Any: Evaluated result of the selected branch, or None if falsy
                without an ``"else"`` branch.

        Raises:
            ValueError: If ``operands`` is neither a valid dict nor a
                3-element list, or lacks ``"if"`` or ``"then"``.
        """
        if not isinstance(operands, dict):
            # Handle array format: [condition, true_case, false_case]
            if isinstance(operands, list) and len(operands) == 3:
                condition = _is_mongo_truthy(
                    self._evaluate_operand_python(operands[0], document)
                )
                if condition:
                    return self._evaluate_operand_python(operands[1], document)
                else:
                    return self._evaluate_operand_python(operands[2], document)
            raise ValueError("$cond requires a dictionary or 3-element array")

        if "if" not in operands or "then" not in operands:
            raise ValueError("$cond requires 'if' and 'then' fields")

        condition = _is_mongo_truthy(
            self._evaluate_operand_python(operands["if"], document)
        )
        if condition:
            return self._evaluate_operand_python(operands["then"], document)
        elif "else" in operands:
            return self._evaluate_operand_python(operands["else"], document)
        else:
            return None

    def _evaluate_ifNull_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate the ``$ifNull`` null-coalescing operator in Python.

        Args:
            operands: Two-element list ``[expression, replacement]``.
            document: Document dictionary used for operand evaluation.

        Returns:
            Any: Evaluated ``operands[0]`` if it is not None, otherwise
                evaluated ``operands[1]``.

        Raises:
            ValueError: If ``operands`` is not a 2-element list.
        """
        if not isinstance(operands, list) or len(operands) != 2:
            raise ValueError("$ifNull requires exactly 2 operands")

        expr = self._evaluate_operand_python(operands[0], document)
        if expr is not None:
            return expr
        return self._evaluate_operand_python(operands[1], document)

    def _evaluate_switch_python(
        self, operands: dict[str, Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate the ``$switch`` multi-branch operator in Python.

        Evaluates each dictionary entry in ``branches`` in order and returns
        the ``then`` operand of the first branch whose ``case`` evaluates
        truthy, falling back to ``default`` or None.

        Args:
            operands: Dictionary containing ``branches`` and optional
                ``default`` expression.
            document: Document dictionary used for evaluation.

        Returns:
            Any: Evaluated ``then`` or ``default`` operand, or None if no
                branch matched and ``default`` is None.

        Raises:
            ValueError: If ``operands`` is not a dictionary.
        """
        if not isinstance(operands, dict):
            raise ValueError("$switch requires a dictionary")

        branches = operands.get("branches", [])
        default = operands.get("default")

        for branch in branches:
            if not isinstance(branch, dict):
                continue
            case = branch.get("case")
            then = branch.get("then")
            if case is not None and _is_mongo_truthy(
                self._evaluate_operand_python(case, document)
            ):
                return self._evaluate_operand_python(then, document)

        if default is not None:
            return self._evaluate_operand_python(default, document)
        return None

    def _is_field_missing(self, operand: Any, document: dict[str, Any]) -> bool:
        """Check whether a ``$field`` reference points to a missing field.

        Returns True if ``operand`` is a field or variable reference starting
        with ``$`` and the referenced key or path does not exist in
        ``document``. Returns False for non-field operands (literals,
        expressions) and for fields that exist even if their value is None.

        Args:
            operand: Expression operand to inspect.
            document: Document dictionary to traverse.

        Returns:
            bool: True if ``operand`` references a missing field or variable,
                False otherwise.
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

    def _evaluate_operand_python(
        self, operand: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate an expression operand in Python context.

        Resolves ``$field`` paths and ``$$variable`` references (including
        ``$$REMOVE``, ``$$ROOT``, and ``$$CURRENT``), evaluates nested
        single-key ``$`` expression dicts, and returns literal values
        unchanged.

        Args:
            operand: Operand value, field/variable path, or expression dict.
            document: Document dictionary providing fields and variables.

        Returns:
            Any: Resolved field value, evaluated subexpression, sentinel, or
                literal value (or None when traversing through non-dicts).
        """
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
