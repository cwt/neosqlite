"""
SQL converters for expression evaluation.

Composes domain-specific mixins into SqlConvertersMixin, which provides
all _convert_* methods for translating MongoDB $expr operators to SQL.

This package was split from the monolithic sql_converters.py (1990 lines)
into 13 focused modules mirroring the python_evaluators/ structure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...jsonb_support import JSONBContext
    from ..context import AggregationContext

from .arithmetic import ArithmeticMixin
from .array import ArrayMixin
from .comparison import ComparisonMixin
from .conditional import ConditionalMixin
from .core import CoreMixin
from .data_size import DataSizeMixin
from .date import DateMixin
from .logical import LogicalMixin
from .math_trig import MathTrigMixin
from .object import ObjectMixin
from .set import SetMixin
from .string import StringMixin
from .type_converters import TypeConvertersMixin

__all__ = ["SqlConvertersMixin"]


class SqlConvertersMixin(
    CoreMixin,
    LogicalMixin,
    ComparisonMixin,
    ArithmeticMixin,
    ConditionalMixin,
    ArrayMixin,
    SetMixin,
    StringMixin,
    MathTrigMixin,
    DateMixin,
    ObjectMixin,
    TypeConvertersMixin,
    DataSizeMixin,
):
    """
    Mixin class providing SQL conversion methods for expression evaluation.

    This class is designed to be composed into ExprEvaluator and provides
    all the _convert_* methods for converting MongoDB operators to SQL.

    Required attributes from parent class:
        - data_column: Name of the JSON data column
        - json_function_prefix: 'json' or 'jsonb' based on support
        - json_each_function: 'json_each' or 'jsonb_each'
        - json_group_array_function: 'json_group_array' or 'jsonb_group_array'
        - jsonb: JSONBContext with JSONB capability flags
        - _log2_warned: Boolean tracking if $log2 warning was issued
        - _current_context: Optional AggregationContext for variable scoping
    """

    # Type annotations for simple attributes expected from parent class
    data_column: str
    jsonb: "JSONBContext"
    _log2_warned: bool
    _current_context: "AggregationContext | None"
