"""Object, let/literal/rand, and data-size Python evaluators."""

from __future__ import annotations

import json
import logging
import random
from typing import Any

logger = logging.getLogger(__name__)

from .base import BasePythonMixin


class ObjectPythonMixin(BasePythonMixin):
    """Python evaluators for object, variable, literal, and size operators.

    Implements Python-tier evaluation for MongoDB aggregation object
    operators (``$mergeObjects``, ``$getField``, ``$setField``,
    ``$unsetField``, ``$objectToArray``), binding/utility operators
    (``$let``, ``$literal``, ``$rand``), and data-size operators
    (``$binarySize``, ``$bsonSize``). Nested operand and expression evaluation
    is delegated to ``BasePythonMixin`` helpers.
    """

    def _evaluate_object_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate object, variable, and utility operators in Python.

        Handles ``$mergeObjects``, ``$getField``, ``$setField``,
        ``$unsetField``, ``$objectToArray``, ``$let``, ``$literal``, and
        ``$rand`` against an in-memory document dictionary.

        Args:
            operator: MongoDB aggregation operator name.
            operands: Raw operator operand list, dict, or literal value.
            document: Current document dictionary used as context.

        Returns:
            Any: Result produced by the evaluated aggregation operator.

        Raises:
            ValueError: If ``operands`` lacks required fields or has an
                invalid type for the operator.
            NotImplementedError: If ``operator`` is not supported here.
        """
        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list):
                    raise ValueError("$mergeObjects requires a list of objects")
                result: dict[str, Any] = {}
                for obj in operands:
                    obj_val = self._evaluate_operand_python(obj, document)
                    if isinstance(obj_val, dict):
                        result |= obj_val
                return result
            case "$getField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError("$getField requires 'field' specification")
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = document
                if not isinstance(obj, dict):
                    return None
                return obj.get(field)
            case "$setField":
                if not isinstance(operands, dict):
                    raise ValueError("$setField requires a dictionary")
                field = operands.get("field")
                value = operands.get("value")
                input_val = operands.get("input")
                if field is None:
                    raise ValueError("$setField requires 'field'")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    obj = {}
                result = dict(obj)
                result[field] = self._evaluate_operand_python(value, document)
                return result
            case "$unsetField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError(
                        "$unsetField requires 'field' specification"
                    )
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    obj = self._evaluate_operand_python(input_val, document)
                else:
                    obj = dict(document)
                if not isinstance(obj, dict):
                    return None
                result = dict(obj)
                result.pop(field, None)
                return result
            case "$objectToArray":
                # Convert object to array of {k, v} objects
                if isinstance(operands, dict):
                    obj = operands
                else:
                    obj = self._evaluate_operand_python(operands, document)
                if not isinstance(obj, dict):
                    return []
                return [{"k": k, "v": v} for k, v in obj.items()]
            case "$let":
                # MongoDB syntax: { $let: { vars: { <var1>: <expr1>, ... }, in: <expr> } }
                if not isinstance(operands, dict):
                    raise ValueError("$let requires a dictionary")
                vars_spec = operands.get("vars", {})
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$let requires 'in' expression")

                # Create new document context with variables
                new_context = dict(document)
                for var_name, var_expr in vars_spec.items():
                    var_value = self._evaluate_operand_python(
                        var_expr, document
                    )
                    new_context["$$" + var_name] = var_value

                # Evaluate the 'in' expression with new context
                return self._evaluate_expr_python(in_expr, new_context)
            case "$literal":
                # Return the operand as-is without evaluation
                return operands
            case "$rand":
                # Return random number between 0 and 1
                return random.random()
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in Python evaluation"
                )

    def _evaluate_data_size_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> int:
        """Evaluate data size operators ($binarySize, $bsonSize) in Python.

        For ``$binarySize``, measures raw bytes-like values or decodes stored
        NeoSQLite binary dicts. For ``$bsonSize``, approximates BSON size
        via UTF-8 JSON encoding with a string fallback.

        Args:
            operator: Either ``"$binarySize"`` or ``"$bsonSize"``.
            operands: Single expression or one-element list to measure.
            document: Current document dictionary used as context.

        Returns:
            int: Byte length of the binary or serialized value.

        Raises:
            ValueError: If ``operands`` does not have exactly one element.
            TypeError: If ``$binarySize`` evaluates to a non-binary value.
            NotImplementedError: If ``operator`` is not supported.
        """
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)

        if operator == "$binarySize":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return len(value)
            # Binary class is a subclass of bytes, so it's already covered.
            # Handle encoded binary objects
            if isinstance(value, dict) and value.get("__neosqlite_binary__"):
                from ....binary import Binary

                try:
                    bin_val = Binary.decode_from_storage(value)
                    return len(bin_val)
                except Exception as e:
                    logger.debug(
                        f"Failed to decode binary for $binarySize: {e}"
                    )
                    pass

            raise TypeError(
                f"$binarySize requires a binary value, got {type(value)}"
            )

        elif operator == "$bsonSize":
            # MongoDB $bsonSize returns the size of the document in BSON bytes.
            # In NeoSQLite, we'll return the size of the JSON representation.
            # Use simple JSON dump for size calculation (approximates BSON)
            try:
                # Use a basic approach for now
                return len(json.dumps(value).encode("utf-8"))
            except Exception as e:
                # Fallback to string length for non-serializable objects
                logger.debug(
                    f"Failed to calculate $bsonSize using JSON, falling back to string length: {e}"
                )
                return len(str(value).encode("utf-8"))

        raise NotImplementedError(f"Operator {operator} not supported")
