"""Shared base for Python evaluator mixins (attributes + cross-mixin stubs)."""

from __future__ import annotations

from typing import Any


class BasePythonMixin:
    """
    Attribute annotations and method stubs shared across domain mixins.

    Real implementations live in CorePythonMixin and the domain mixins;
    stubs here let static analysis resolve cross-mixin calls via MRO.
    """

    _log2_warned: bool

    def _evaluate_operand_python(
        self, operand: Any, document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_expr_python(
        self, expr: dict[str, Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_math_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_pow_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_sqrt_python(
        self, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_trig_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_angle_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_array_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_array_transform_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_string_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_date_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_type_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any: ...

    def _evaluate_data_size_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any: ...
