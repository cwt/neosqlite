"""Type conversion and BSON type Python evaluators."""

from __future__ import annotations

import logging
from typing import Any

from ..type_utils import (
    _convert_to_bindata,
    _convert_to_bool,
    _convert_to_bsonbindata,
    _convert_to_bsonregex,
    _convert_to_date,
    _convert_to_decimal,
    _convert_to_double,
    _convert_to_int,
    _convert_to_long,
    _convert_to_null,
    _convert_to_objectid,
    _convert_to_regex,
    _convert_to_string,
    get_bson_type,
)

logger = logging.getLogger(__name__)

from .base import BasePythonMixin


class TypePythonMixin(BasePythonMixin):
    """Type inspection/conversion operators and thin converter wrappers."""

    def _evaluate_type_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate type conversion operators in Python."""
        # Handle both list and single operand formats (but not for $convert which needs dict)
        if operator != "$convert" and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$type":
                if len(operands) != 1:
                    raise ValueError("$type requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return get_bson_type(value)
            case "$toString":
                if len(operands) != 1:
                    raise ValueError("$toString requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value) if value is not None else None
            case "$toInt":
                if len(operands) != 1:
                    raise ValueError("$toInt requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDouble":
                if len(operands) != 1:
                    raise ValueError("$toDouble requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    return float(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toBool":
                if len(operands) != 1:
                    raise ValueError("$toBool requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return False
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    return bool(value)
                return bool(value)
            case "$toLong":
                if len(operands) != 1:
                    raise ValueError("$toLong requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    # Python ints are already 64-bit
                    return int(value) if value is not None else None
                except (ValueError, TypeError):
                    return None
            case "$toDecimal":
                if len(operands) != 1:
                    raise ValueError("$toDecimal requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                try:
                    from decimal import Decimal, InvalidOperation

                    return Decimal(str(value)) if value is not None else None
                except (ValueError, TypeError, ImportError, InvalidOperation):
                    return None
            case "$toObjectId":
                if len(operands) != 1:
                    raise ValueError("$toObjectId requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                # Convert hex string to ObjectId
                from neosqlite.objectid import ObjectId

                try:
                    if isinstance(value, str) and len(value) == 24:
                        return ObjectId(value)
                    # For other types, try to create from string representation
                    return ObjectId(str(value))
                except Exception as e:
                    logger.debug(
                        f"Failed to convert to ObjectId in expression: {e}"
                    )
                    return None
            case "$isNumber":
                # Check if value is a number (int or float, but not bool)
                if len(operands) != 1:
                    raise ValueError("$isNumber requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                # In Python, bool is a subclass of int, so we need to check for bool first
                if isinstance(value, bool):
                    return False
                return isinstance(value, (int, float))
            case "$convert":
                # $convert is complex - requires 'to' field
                if not isinstance(operands, dict):
                    raise ValueError("$convert requires a dictionary")
                input_val = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                to_type = operands.get("to")
                on_error = operands.get("onError")
                on_null = operands.get("onNull")

                if input_val is None:
                    return on_null

                # Import required types upfront
                from neosqlite.objectid import ObjectId

                # Map conversion types to named converter methods
                conversion_map = {
                    "int": _convert_to_int,
                    "long": _convert_to_long,
                    "double": _convert_to_double,
                    "decimal": _convert_to_decimal,
                    "string": _convert_to_string,
                    "bool": _convert_to_bool,
                    "objectId": _convert_to_objectid,
                    "binData": _convert_to_bindata,
                    "bsonBinData": _convert_to_bsonbindata,
                    "regex": _convert_to_regex,
                    "bsonRegex": _convert_to_bsonregex,
                    "date": _convert_to_date,
                    "null": _convert_to_null,
                }

                try:
                    converter = conversion_map.get(to_type)
                    if converter:
                        return converter(input_val)
                    return input_val
                except Exception as e:
                    logger.debug(f"Failed to convert type in $convert: {e}")
                    return on_error
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in Python evaluation"
                )

    # Type converter wrapper methods for backward compatibility (used by tests)
    def _convert_to_int(value: Any) -> Any:
        """Convert value to int."""
        return _convert_to_int(value)

    @staticmethod
    def _convert_to_long(value: Any) -> Any:
        """Convert value to long (64-bit int)."""
        return _convert_to_long(value)

    @staticmethod
    def _convert_to_double(value: Any) -> Any:
        """Convert value to double (float)."""
        return _convert_to_double(value)

    @staticmethod
    def _convert_to_decimal(value: Any) -> Any:
        """Convert value to decimal (float, as SQLite lacks Decimal128)."""
        return _convert_to_decimal(value)

    @staticmethod
    def _convert_to_string(value: Any) -> Any:
        """Convert value to string."""
        return _convert_to_string(value)

    @staticmethod
    def _convert_to_bool(value: Any) -> Any:
        """Convert value to bool."""
        return _convert_to_bool(value)

    @staticmethod
    def _convert_to_objectid(value: Any) -> Any:
        """Convert value to ObjectId."""
        return _convert_to_objectid(value)

    @staticmethod
    def _convert_to_bindata(value: Any) -> Any:
        """Convert value to Binary (binData)."""
        return _convert_to_bindata(value)

    @staticmethod
    def _convert_to_bsonbindata(value: Any) -> Any:
        """Convert value to Binary (bsonBinData)."""
        return _convert_to_bsonbindata(value)

    @staticmethod
    def _convert_to_regex(value: Any) -> Any:
        """Convert value to regex pattern."""
        return _convert_to_regex(value)

    @staticmethod
    def _convert_to_bsonregex(value: Any) -> Any:
        """Convert value to regex pattern (bsonRegex)."""
        return _convert_to_bsonregex(value)

    @staticmethod
    def _convert_to_date(value: Any) -> Any:
        """Convert value to date."""
        return _convert_to_date(value)

    @staticmethod
    def _convert_to_null(value: Any) -> None:
        """Convert any value to None."""
        return _convert_to_null(value)

    def _get_bson_type(self, value: Any) -> str:
        """Get BSON type name for a value."""
        return get_bson_type(value)
