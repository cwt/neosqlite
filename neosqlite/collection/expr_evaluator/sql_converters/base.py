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
    """Shared annotations for all SQL converter mixins."""

    # Attributes supplied by ExprEvaluator
    data_column: str
    jsonb: "JSONBContext"
    _current_context: Any  # AggregationContext | None

    # Properties supplied by ExprEvaluator (accessed as self.json_each_function, etc.)
    @property
    def json_each_function(self) -> str: ...  # type: ignore[empty-body]
    @property
    def json_group_array_function(self) -> str: ...  # type: ignore[empty-body]
    @property
    def json_function_prefix(self) -> str: ...  # type: ignore[empty-body]

    # Cross-mixin method stubs (real implementations in CoreMixin)
    def _convert_operand_to_sql(self, operand: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_expr_to_sql(self, expr: dict[str, Any]) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]

    # Cross-mixin method stubs (real implementations in various mixins)
    def _convert_logical_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_comparison_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_cmp_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_arithmetic_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_cond_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_ifNull_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_switch_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_array_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_set_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_string_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_math_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_trig_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_angle_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_date_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_date_arithmetic_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_date_diff_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_object_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_let_operator(self, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_data_size_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
    def _convert_type_operator(self, op: str, operands: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]

    def _get_operator_return_type(self, operator: str) -> str | None: ...  # type: ignore[empty-body]
    def _get_literal_bson_type(self, value: Any) -> str | None: ...  # type: ignore[empty-body]
    def _map_comparison_operator(self, op: str) -> str: ...  # type: ignore[empty-body]
    def _map_arithmetic_operator(self, op: str) -> str: ...  # type: ignore[empty-body]
    def _build_pattern_with_options(self, regex: str, options: str) -> str: ...  # type: ignore[empty-body]
    def _handle_aggregation_variable(self, operand: Any, ctx: Any) -> tuple[str, list[Any]]: ...  # type: ignore[empty-body]
