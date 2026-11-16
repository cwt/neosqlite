"""Shared base for Python evaluator mixins (attributes + cross-mixin stubs)."""

from __future__ import annotations

from typing import Any


class BasePythonMixin:
    """Attribute annotations and method stubs shared across domain mixins.

    Real implementations live in ``CorePythonMixin`` and the domain mixins;
    stubs here let static analysis resolve cross-mixin calls via MRO.
    """

    _log2_warned: bool

    def _evaluate_operand_python(
        self, operand: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate an operand expression or literal against a document.

        Args:
            operand: Field path, ``$$`` variable, nested expression dict, or
                literal value.
            document: Document dictionary providing field and variable state.

        Returns:
            Any: Evaluated Python value for the operand.
        """
        ...

    def _evaluate_expr_python(
        self, expr: dict[str, Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate a single-operator ``$expr`` dictionary in Python.

        Args:
            expr: Single-key dictionary mapping an operator name to operands.
            document: Document dictionary to evaluate the expression against.

        Returns:
            Any: Evaluated Python result of the expression.
        """
        ...

    def _evaluate_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate arithmetic operators in Python.

        Handles ``$add``, ``$subtract``, ``$multiply``, ``$divide``, and
        ``$mod``.

        Args:
            operator: Arithmetic operator name.
            operands: List of operand expressions to combine.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Numeric result, or ``None`` for null operands or division by
            zero.
        """
        ...

    def _evaluate_math_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate scalar mathematical operators in Python.

        Handles ``$abs``, ``$ceil``, ``$floor``, ``$round``, ``$trunc``,
        ``$ln``, ``$log``, ``$log10``, ``$log2``, ``$exp``, and ``$sigmoid``.

        Args:
            operator: Math operator name.
            operands: Operand list, single expression, or ``$sigmoid`` dict.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Computed numeric value, or ``None`` for null/out-of-domain
            inputs.
        """
        ...

    def _evaluate_pow_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate the ``$pow`` exponentiation operator in Python.

        Args:
            operands: Two-element list ``[base, exponent]``.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Result of raising base to exponent, or ``None`` if null.
        """
        ...

    def _evaluate_sqrt_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate the ``$sqrt`` square-root operator in Python.

        Args:
            operands: Single operand or one-element list for the radicand.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Non-negative square root, or ``None`` if null or negative.
        """
        ...

    def _evaluate_trig_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate trigonometric and hyperbolic operators in Python.

        Handles ``$sin``, ``$cos``, ``$tan``, ``$asin``, ``$acos``,
        ``$atan``, ``$atan2``, ``$sinh``, ``$cosh``, ``$tanh``, ``$asinh``,
        ``$acosh``, and ``$atanh``.

        Args:
            operator: Trigonometric or hyperbolic operator name.
            operands: Single operand, or two operands for ``$atan2``.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Computed float result, or ``None`` if null or out of domain.
        """
        ...

    def _evaluate_angle_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate angle unit conversion operators in Python.

        Handles ``$degreesToRadians`` and ``$radiansToDegrees``.

        Args:
            operator: Angle conversion operator name.
            operands: Single operand or one-element list containing the angle.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Converted angle in radians or degrees, or ``None`` if null.
        """
        ...

    def _evaluate_array_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate array and set aggregation operators in Python.

        Args:
            operator: Array or set operator name (such as ``$size``, ``$in``,
                ``$slice``, or ``$setUnion``).
            operands: Operand list or operator-specific specification.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Evaluated array, scalar, or boolean result.
        """
        ...

    def _evaluate_array_transform_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate variable-scoping array transform operators in Python.

        Handles ``$filter``, ``$map``, and ``$reduce``.

        Args:
            operator: Array transform operator name.
            operands: Specification dictionary containing ``input`` and
                transform options.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Transformed list or reduced value.
        """
        ...

    def _evaluate_string_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate string and regular-expression operators in Python.

        Args:
            operator: String operator name (such as ``$concat``, ``$trim``,
                or ``$regexMatch``).
            operands: Single operand, operand list, or specification dict.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Resulting string, integer, list, boolean, or match dict.
        """
        ...

    def _evaluate_date_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate date component extraction operators in Python.

        Handles ``$year``, ``$month``, ``$dayOfMonth``, ``$hour``,
        ``$minute``, ``$second``, ``$millisecond``, ``$dayOfWeek``,
        ``$dayOfYear``, ``$week``, ``$isoDayOfWeek``, and ``$isoWeek``.

        Args:
            operator: Date extraction operator name.
            operands: Single date expression or one-element list.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Extracted integer date component, or ``None`` for null or
            non-datetime values.
        """
        ...

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate date arithmetic and conversion operators in Python.

        Handles ``$dateAdd``, ``$dateSubtract``, ``$dateDiff``,
        ``$dateFromString``, ``$dateToString``, ``$dateFromParts``,
        ``$dateToParts``, and ``$dateTrunc``.

        Args:
            operator: Date arithmetic or formatting operator name.
            operands: Operator specification dictionary or operand list.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Resulting datetime, numeric difference, formatted string, or
            date-parts dictionary.
        """
        ...

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate object, variable-binding, and literal operators in Python.

        Handles ``$mergeObjects``, ``$getField``, ``$setField``,
        ``$unsetField``, ``$objectToArray``, ``$let``, ``$literal``, and
        ``$rand``.

        Args:
            operator: Object or utility operator name.
            operands: Operator specification (dict, list, or literal value).
            document: Document dictionary to evaluate against.

        Returns:
            Any: Resulting document, field value, key-value list, or float.
        """
        ...

    def _evaluate_type_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate BSON type inspection and conversion operators in Python.

        Handles ``$type``, ``$isNumber``, ``$convert``, and ``$to*``
        conversion operators.

        Args:
            operator: Type inspection or conversion operator name.
            operands: Single operand, operand list, or ``$convert`` dict.
            document: Document dictionary to evaluate against.

        Returns:
            Any: BSON type string, boolean check, or converted Python value.
        """
        ...

    def _evaluate_data_size_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate binary and BSON byte-size operators in Python.

        Handles ``$binarySize`` and ``$bsonSize``.

        Args:
            operator: Data-size operator name.
            operands: Single expression or one-element list to measure.
            document: Document dictionary to evaluate against.

        Returns:
            Any: Measured size in bytes as an integer.
        """
        ...
