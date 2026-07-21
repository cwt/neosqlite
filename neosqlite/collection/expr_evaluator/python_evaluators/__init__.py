"""Python evaluation methods for NeoSQLite $expr operator.

Provides PythonEvaluatorsMixin, composed from domain-specific mixins, as the
fallback when SQL evaluation is not possible or the kill switch is activated.
"""

from __future__ import annotations

from .array_ops import ArrayPythonMixin
from .core import CorePythonMixin
from .date_ops import DatePythonMixin
from .math_ops import MathPythonMixin
from .object_ops import ObjectPythonMixin
from .string_ops import StringPythonMixin
from .type_ops import TypePythonMixin

__all__ = ["PythonEvaluatorsMixin"]


class PythonEvaluatorsMixin(
    MathPythonMixin,
    ArrayPythonMixin,
    StringPythonMixin,
    DatePythonMixin,
    ObjectPythonMixin,
    TypePythonMixin,
    CorePythonMixin,
):
    """
    Mixin class providing Python evaluation methods for $expr expressions.

    This mixin provides fallback evaluation capabilities when SQL-based
    evaluation (Tier 1 and Tier 2) is not possible or when the kill switch
    is activated.
    """

    _log2_warned: bool
