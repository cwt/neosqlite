"""Base class for SQL converter mixins.

Declares shared method signatures that individual mixins call across
category boundaries (e.g. _convert_operand_to_sql is defined in CoreMixin
but used by StringMixin, MathTrigMixin, etc.).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ...jsonb_support import JSONBContext


class BaseSqlMixin:
    """Shared type annotations and cross-mixin stubs for SQL converters.

    Declares instance attributes and properties supplied by ``ExprEvaluator``
    along with stub signatures for converter and helper methods implemented in
    sibling mixin classes so static type checkers can resolve cross-mixin
    calls.

    Attributes:
        data_column: SQL column or expression holding the document JSON
            payload.
        jsonb: Connection-scoped JSONB capability context.
        _current_context: Active ``AggregationContext`` during nested
            expression evaluation, or ``None``.
        _slice_counter: Counter used to generate unique aliases in ``$slice``.
        _reduce_counter: Counter used to generate unique CTE names in
            ``$reduce``.
        _je_counter: Counter used to generate unique ``json_each`` aliases.
    """

    # Attributes supplied by ExprEvaluator
    data_column: str
    jsonb: JSONBContext
    _current_context: Any  # AggregationContext | None
    _slice_counter: int
    _reduce_counter: int
    _je_counter: int

    # Properties supplied by ExprEvaluator (accessed as self.json_each_function, etc.)
    @property
    def json_each_function(self) -> str:  # type: ignore[empty-body]
        """Return the active SQLite table-valued ``json_each`` function name.

        Returns:
            str: ``"jsonb_each"`` when supported, otherwise ``"json_each"``.
        """
        ...

    @property
    def json_group_array_function(self) -> str:  # type: ignore[empty-body]
        """Return the active SQLite ``json_group_array`` function name.

        Returns:
            str: ``"jsonb_group_array"`` when JSONB is supported, otherwise
                ``"json_group_array"``.
        """
        ...

    @property
    def json_function_prefix(self) -> str:  # type: ignore[empty-body]
        """Return the active SQLite JSON function prefix.

        Returns:
            str: ``"jsonb"`` when JSONB is supported, otherwise ``"json"``.
        """
        ...

    # Cross-mixin method stubs (real implementations in CoreMixin)
    def _convert_operand_to_sql(  # type: ignore[empty-body]
        self, operand: Any
    ) -> tuple[str, list[Any]]:
        """Convert an operand to a SQL expression and parameter list.

        Handles ``$``-prefixed field references, nested expression dicts,
        aggregation variables when ``_current_context`` is active, and
        scalar or JSON literals.

        Args:
            operand: Field reference, nested operator expression, aggregation
                variable, or literal value.

        Returns:
            tuple[str, list[Any]]: SQL expression fragment and bound parameter
                list.
        """
        ...

    def _convert_expr_to_sql(  # type: ignore[empty-body]
        self, expr: dict[str, Any]
    ) -> tuple[str, list[Any]]:
        """Dispatch a single-key ``$expr`` operator dictionary to SQL.

        Args:
            expr: Single-key MongoDB aggregation expression dictionary.

        Returns:
            tuple[str, list[Any]]: SQL expression fragment and bound parameter
                list.
        """
        ...

    # Cross-mixin method stubs (real implementations in various mixins)
    def _convert_logical_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert logical operators (``$and``, ``$or``, ``$not``, ``$nor``).

        Args:
            op: Logical operator name starting with ``"$"``.
            operands: List of operand expressions, or a single expression for
                ``$not``.

        Returns:
            tuple[str, list[Any]]: SQL boolean expression and bound parameters.
        """
        ...

    def _convert_comparison_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert comparison operators (``$eq``, ``$ne``, ``$gt``, etc.).

        Args:
            op: Comparison operator name (e.g., ``"$eq"``, ``"$gt"``).
            operands: Two-element list of left and right operands.

        Returns:
            tuple[str, list[Any]]: SQL comparison expression and bound
                parameters.
        """
        ...

    def _convert_cmp_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$cmp`` into a three-way SQL ``CASE`` expression.

        Args:
            operands: Two-element list of operands to compare.

        Returns:
            tuple[str, list[Any]]: SQL expression returning ``-1``, ``0``, or
                ``1``, and its bound parameters.
        """
        ...

    def _convert_arithmetic_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert basic arithmetic operators to SQL.

        Handles ``$add``, ``$subtract``, ``$multiply``, ``$divide``, and
        ``$mod``.

        Args:
            op: Arithmetic operator name starting with ``"$"``.
            operands: List of at least two operand expressions.

        Returns:
            tuple[str, list[Any]]: SQL arithmetic expression and bound
                parameters.
        """
        ...

    def _convert_cond_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$cond`` into a SQL ``CASE`` expression.

        Args:
            operands: Either a 3-element ``[if, then, else]`` list or a dict
                with ``"if"``, ``"then"``, and optional ``"else"`` keys.

        Returns:
            tuple[str, list[Any]]: SQL ``CASE`` expression and bound
                parameters.
        """
        ...

    def _convert_ifNull_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$ifNull`` into a SQL ``COALESCE`` call.

        Args:
            operands: Two-element list ``[expr, replacement]``.

        Returns:
            tuple[str, list[Any]]: SQL ``COALESCE`` expression and bound
                parameters.
        """
        ...

    def _convert_switch_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$switch`` into a multi-branch SQL ``CASE`` statement.

        Args:
            operands: Dictionary with a non-empty ``"branches"`` list and an
                optional ``"default"`` expression.

        Returns:
            tuple[str, list[Any]]: SQL ``CASE`` expression and bound
                parameters.
        """
        ...

    def _convert_array_transform_operator(  # type: ignore[empty-body]
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$filter``, ``$map``, and ``$reduce`` to SQL.

        Args:
            operator: Transform operator name (``"$filter"``, ``"$map"``, or
                ``"$reduce"``).
            operands: Specification dictionary with ``"input"``, optional
                ``"as"``, and operator-specific expression keys.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_array_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert MongoDB array and set operators to SQL.

        Args:
            op: Array or set operator name starting with ``"$"``.
            operands: Operator argument, list of arguments, or specification
                dict.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_set_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert MongoDB array set operators to SQL.

        Handles ``$setEquals``, ``$setIntersection``, ``$setUnion``,
        ``$setDifference``, ``$setIsSubset``, ``$anyElementTrue``, and
        ``$allElementsTrue``.

        Args:
            op: Set operator name starting with ``"$"``.
            operands: Operand expression or list of operand expressions.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_string_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert MongoDB string aggregation operators to SQL.

        Args:
            op: String operator name (e.g., ``"$concat"``, ``"$trim"``).
            operands: Operand expression, argument list, or specification dict.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_math_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert math operators (``$abs``, ``$pow``, ``$sqrt``, etc.) to SQL.

        Args:
            op: Math operator name starting with ``"$"``.
            operands: Single operand, argument list, or ``$sigmoid`` dict.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_trig_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert trigonometric and hyperbolic operators to SQL.

        Args:
            op: Trigonometric operator name (e.g., ``"$sin"``, ``"$atan2"``).
            operands: Single operand or argument list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_angle_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$degreesToRadians`` and ``$radiansToDegrees`` to SQL.

        Args:
            op: Angle conversion operator name starting with ``"$"``.
            operands: Single operand or one-element list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert date component extraction operators to SQL ``strftime``.

        Handles ``$year``, ``$month``, ``$dayOfMonth``, ``$hour``,
        ``$minute``, ``$second``, ``$dayOfWeek``, ``$dayOfYear``, ``$week``,
        ``$isoDayOfWeek``, ``$isoWeek``, and ``$millisecond``.

        Args:
            op: Date extraction operator name starting with ``"$"``.
            operands: Single date operand or one-element list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_arithmetic_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateAdd`` and ``$dateSubtract`` operators to SQL.

        Args:
            op: Date arithmetic operator (``"$dateAdd"`` or
                ``"$dateSubtract"``).
            operands: Specification dict or ``[date, amount, unit]`` list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_diff_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateDiff`` to a SQL date difference expression.

        Args:
            operands: Specification dict or ``[date1, date2, unit]`` list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_to_string_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateToString`` to a SQLite ``strftime`` expression.

        Args:
            operands: Specification dict or ``[format, date, timezone]`` list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_trunc_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateTrunc`` to a SQLite ``strftime`` expression.

        Args:
            operands: Specification dict or ``[date, unit]`` list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_from_parts_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateFromParts`` to SQLite ISO-8601 date construction.

        Args:
            operands: Dictionary specifying ``"year"`` and optional calendar
                component fields.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_to_parts_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateToParts`` to a SQLite ``json_object`` expression.

        Args:
            operands: Specification dict or list containing the date operand.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_date_from_string_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$dateFromString`` to SQLite ``strftime`` parsing.

        Args:
            operands: Specification dict or list containing ``dateString`` and
                optional options.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_object_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert MongoDB object operators to SQL expressions.

        Handles ``$mergeObjects``, ``$getField``, ``$setField``,
        ``$unsetField``, ``$objectToArray``, ``$literal``, and ``$rand``.

        Args:
            op: Object operator name starting with ``"$"``.
            operands: Operator arguments (list, dict, or literal value).

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_let_operator(  # type: ignore[empty-body]
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$let`` by evaluating and inlining scoped variables.

        Args:
            operands: Dictionary with optional ``"vars"`` mapping and required
                ``"in"`` expression.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameters for the
                ``in`` body.
        """
        ...

    def _convert_data_size_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert ``$binarySize`` and ``$bsonSize`` operators to SQL.

        Args:
            op: Size operator name (``"$binarySize"`` or ``"$bsonSize"``).
            operands: Single operand expression or one-element list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _convert_type_operator(  # type: ignore[empty-body]
        self, op: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert MongoDB type inspection and conversion operators to SQL.

        Args:
            op: Type operator name (e.g., ``"$toInt"``, ``"$type"``).
            operands: Single operand or one-element list.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...

    def _get_operator_return_type(  # type: ignore[empty-body]
        self, operator: str
    ) -> str | None:
        """Infer the BSON return type category of a MongoDB operator.

        Args:
            operator: MongoDB aggregation operator name starting with ``"$"``.

        Returns:
            str | None: BSON type category (``"number"``, ``"bool"``,
                ``"string"``, ``"array"``, or ``"object"``), or ``None`` if
                unknown or ambiguous.
        """
        ...

    def _get_literal_bson_type(  # type: ignore[empty-body]
        self, value: Any
    ) -> str | None:
        """Return the BSON type category for a literal Python value.

        Args:
            value: Literal value or expression node to classify.

        Returns:
            str | None: BSON type category (``"null"``, ``"bool"``,
                ``"number"``, ``"string"``, ``"array"``, or ``"object"``), or
                ``None`` for field references, expressions, or unknown types.
        """
        ...

    def _map_comparison_operator(  # type: ignore[empty-body]
        self, op: str
    ) -> str:
        """Map a MongoDB comparison operator name to its SQL operator symbol.

        Args:
            op: MongoDB comparison operator (e.g., ``"$eq"``, ``"$gte"``).

        Returns:
            str: Equivalent SQL operator symbol (e.g., ``"="``, ``">="``), or
                ``op`` if unmapped.
        """
        ...

    def _map_arithmetic_operator(  # type: ignore[empty-body]
        self, op: str
    ) -> str:
        """Map a MongoDB arithmetic operator name to its SQL operator symbol.

        Args:
            op: MongoDB arithmetic operator (e.g., ``"$add"``, ``"$mod"``).

        Returns:
            str: Equivalent SQL operator symbol (e.g., ``"+"``, ``"%"``), or
                ``op`` if unmapped.
        """
        ...

    def _build_pattern_with_options(  # type: ignore[empty-body]
        self, regex: str, options: str
    ) -> str:
        """Prepend inline regex flag modifiers derived from option characters.

        Args:
            regex: Regular expression pattern string.
            options: Regex flag characters (supporting ``i``, ``m``, ``s``,
                ``x`` case-insensitively).

        Returns:
            str: Pattern prefixed with ``(?...)`` flags when present, otherwise
                ``regex`` unchanged.
        """
        ...

    def _handle_aggregation_variable(  # type: ignore[empty-body]
        self, operand: Any, ctx: Any
    ) -> tuple[str, list[Any]]:
        """Resolve a ``$$`` aggregation variable reference to a SQL expression.

        Args:
            operand: Aggregation variable reference string starting with
                ``"$$"``.
            ctx: Active ``AggregationContext`` storing column and variable
                bindings.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameter list.
        """
        ...
