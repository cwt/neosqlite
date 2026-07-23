"""Array and array-transform Python evaluators."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .base import BasePythonMixin

# BSON comparison order groups (lower = earlier in sort).
# Order: Null < Numbers < Strings < Objects < Arrays <
#        BinData < ObjectId < Boolean < Date < Timestamp < Regex
_BSON_TYPE_ORDER: dict[type | str, int] = {
    type(None): 0,
    int: 1,
    float: 1,
    str: 2,
    dict: 3,
    list: 4,
    bytes: 5,
    bytearray: 5,
    bool: 6,
    datetime: 7,
}


def _bson_sort_key(value: Any) -> tuple[int, Any]:
    """Return a sort key tuple that encodes BSON comparison order.

    The first element is the BSON type group (0..9), the second is the
    value itself for same-type comparison.  Unknown types fall at the end.
    """
    if value is None:
        return (0, None)
    # bool is a subclass of int; check it first.
    if isinstance(value, bool):
        return (6, value)
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, dict):
        return (3, str(value))
    if isinstance(value, list):
        return (4, str(value))
    if isinstance(value, (bytes, bytearray)):
        return (5, value)
    if isinstance(value, datetime):
        return (7, value)
    # From neosqlite.objectid / neosqlite.binary if available
    type_name = type(value).__name__
    if type_name == "ObjectId":
        return (5, str(value))
    if type_name == "Binary":
        return (5, bytes(value) if hasattr(value, "__bytes__") else str(value))
    # Fallback: unknown types sort last
    return (99, str(value))


def _bson_sort(array: list[Any]) -> list[Any]:
    """Sort a list using MongoDB BSON comparison order."""
    try:
        return sorted(array, key=_bson_sort_key)
    except TypeError:
        # Fallback: if two values of the same type still can't be compared,
        # return original order.
        return array


class ArrayPythonMixin(BasePythonMixin):
    """Array, set, and transform ($filter/$map/$reduce) operators."""

    def _evaluate_array_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate array operators in Python."""
        # Normalize operands for operators that accept single values
        if operator in (
            "$size",
            "$isArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
        ) and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return len(array)
                return None
            case "$in":
                if len(operands) != 2:
                    raise ValueError("$in requires exactly 2 operands")
                value = self._evaluate_operand_python(operands[0], document)
                array = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    return value in array
                return False
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return isinstance(value, list)
            case "$sum" | "$avg" | "$min" | "$max":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    array_ops = [operands]
                else:
                    array_ops = operands

                if len(array_ops) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array = self._evaluate_operand_python(array_ops[0], document)
                if not isinstance(array, list):
                    return 0 if operator == "$sum" else None

                # Filter numeric values for sum/avg
                nums = [
                    v
                    for v in array
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
                ]

                if not nums:
                    if operator == "$sum":
                        return 0
                    return None

                match operator:
                    case "$sum":
                        return sum(nums)
                    case "$avg":
                        return sum(nums) / len(nums)
                    case "$min":
                        return min(array)  # min/max work on all types
                    case "$max":
                        return max(array)
                    case _:
                        return None
            case "$arrayElemAt":
                if len(operands) != 2:
                    raise ValueError("$arrayElemAt requires exactly 2 operands")
                array = self._evaluate_operand_python(operands[0], document)
                index = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list) and isinstance(index, int):
                    try:
                        return array[index]
                    except IndexError:
                        return None
                return None
            case "$first":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    ops = [operands]
                else:
                    ops = operands
                if len(ops) != 1:
                    raise ValueError("$first requires exactly 1 operand")
                array = self._evaluate_operand_python(ops[0], document)
                if isinstance(array, list) and array:
                    return array[0]
                return None
            case "$last":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    ops = [operands]
                else:
                    ops = operands
                if len(ops) != 1:
                    raise ValueError("$last requires exactly 1 operand")
                array = self._evaluate_operand_python(ops[0], document)
                if isinstance(array, list) and array:
                    return array[-1]
                return None
            case "$firstN":
                # Get first N elements from array
                # MongoDB syntax: { $firstN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$firstN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                return array[: int(n)]
            case "$lastN":
                # Get last N elements from array
                # MongoDB syntax: { $lastN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$lastN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                n_int = int(n)
                if n_int <= 0:
                    return []
                return array[-n_int:] if n_int < len(array) else array
            case "$maxN":
                # Get maximum N elements from array (sorted descending, take first N)
                # MongoDB syntax: { $maxN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$maxN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                # Sort descending and take first N
                try:
                    sorted_array = sorted(array, reverse=True)
                    return sorted_array[: int(n)]
                except (TypeError, ValueError):
                    return []
            case "$minN":
                # Get minimum N elements from array (sorted ascending, take first N)
                # MongoDB syntax: { $minN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$minN requires input array and n count")

                array = self._evaluate_operand_python(array_operand, document)
                n = self._evaluate_operand_python(n_operand, document)

                if not isinstance(array, list) or n is None:
                    return []

                # Sort ascending and take first N
                try:
                    sorted_array = sorted(array)
                    return sorted_array[: int(n)]
                except (TypeError, ValueError):
                    return []
            case "$sortArray":
                # Sort array elements
                # MongoDB syntax: { $sortArray: { input: <array>, sortBy: { <field>: <direction> } } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    sort_by = operands.get("sortBy")
                elif isinstance(operands, list) and len(operands) >= 1:
                    array_operand = operands[0]
                    sort_by = operands[1] if len(operands) > 1 else None
                else:
                    raise ValueError("$sortArray requires input array")

                array = self._evaluate_operand_python(array_operand, document)

                if not isinstance(array, list):
                    return []

                # If no sortBy specified, sort by value using BSON comparison order.
                # BSON order: Null < Numbers < Strings < Objects < Arrays <
                #             BinData < ObjectId < Boolean < Date < Timestamp < Regex
                if sort_by is None:
                    return _bson_sort(array)

                # Sort by field (for array of objects)
                if isinstance(sort_by, dict):
                    # Get first field and direction
                    sort_field = next(iter(sort_by.keys()))
                    direction = sort_by[sort_field]
                    reverse = direction == -1

                    try:

                        def sort_key(x: Any) -> Any:
                            """
                            Extract the sort field from a dictionary or return the value.
                            """
                            return (
                                x.get(sort_field) if isinstance(x, dict) else x
                            )

                        return sorted(
                            array,
                            key=sort_key,  # type: ignore[arg-type]
                            reverse=reverse,
                        )
                    except (TypeError, AttributeError):
                        return array

                return array
            case "$slice":
                if not isinstance(operands, list) or len(operands) < 2:
                    raise ValueError("$slice requires array and count/position")
                array = self._evaluate_operand_python(operands[0], document)
                count = self._evaluate_operand_python(operands[1], document)

                if not isinstance(array, list):
                    return []
                if len(operands) >= 3:
                    skip = self._evaluate_operand_python(operands[2], document)
                    return array[skip : skip + count]
                elif isinstance(count, int) and count < 0:
                    return array[count:]
                else:
                    return array[:count]
            case "$indexOfArray":
                if len(operands) != 2:
                    raise ValueError(
                        "$indexOfArray requires exactly 2 operands"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                value = self._evaluate_operand_python(operands[1], document)
                if isinstance(array, list):
                    try:
                        return array.index(value)
                    except ValueError:
                        return -1
                return -1
            case "$setEquals":
                if len(operands) != 2:
                    raise ValueError("$setEquals requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1) == set(set2)
                return False
            case "$setIntersection":
                if len(operands) != 2:
                    raise ValueError(
                        "$setIntersection requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) & set(set2))
                return []
            case "$setUnion":
                if len(operands) != 2:
                    raise ValueError("$setUnion requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) | set(set2))
                return []
            case "$setDifference":
                if len(operands) != 2:
                    raise ValueError(
                        "$setDifference requires exactly 2 operands"
                    )
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return list(set(set1) - set(set2))
                return []
            case "$setIsSubset":
                if len(operands) != 2:
                    raise ValueError("$setIsSubset requires exactly 2 operands")
                set1 = self._evaluate_operand_python(operands[0], document)
                set2 = self._evaluate_operand_python(operands[1], document)
                if isinstance(set1, list) and isinstance(set2, list):
                    return set(set1).issubset(set(set2))
                return False
            case "$anyElementTrue":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError(
                        "$anyElementTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return any(array)
                return False
            case "$allElementsTrue":
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError(
                        "$allElementsTrue requires exactly 1 operand"
                    )
                array = self._evaluate_operand_python(operands[0], document)
                if isinstance(array, list):
                    return all(array)
                return False
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in Python evaluation"
                )

    def _evaluate_array_transform_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate $filter, $map, $reduce operators in Python.

        These operators use variable scoping:
        - $filter: {input: <array>, as: <var>, cond: <expr>}
        - $map: {input: <array>, as: <var>, in: <expr>}
        - $reduce: {input: <array>, initialValue: <val>, in: <expr>}
        """
        match operator:
            case "$filter":
                if not isinstance(operands, dict):
                    raise ValueError("$filter requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "this")  # MongoDB default
                cond = operands.get("cond")

                if cond is None:
                    raise ValueError("$filter requires 'cond' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate condition in context
                    if self._evaluate_expr_python(cond, ctx):
                        result.append(item)

                return result

            case "$map":
                if not isinstance(operands, dict):
                    raise ValueError("$map requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return []

                as_var = operands.get("as", "this")  # MongoDB default
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$map requires 'in' expression")

                result = []
                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    ctx = dict(document)
                    ctx[f"$${as_var}"] = item
                    ctx[f"$${as_var}Index"] = i

                    # Evaluate expression in context
                    result.append(self._evaluate_operand_python(in_expr, ctx))

                return result

            case "$reduce":
                if not isinstance(operands, dict):
                    raise ValueError("$reduce requires a dictionary")

                input_array = self._evaluate_operand_python(
                    operands.get("input"), document
                )
                if not isinstance(input_array, list):
                    return None

                initial_value = operands.get("initialValue")
                in_expr = operands.get("in")

                if in_expr is None:
                    raise ValueError("$reduce requires 'in' expression")

                # Evaluate initial value
                acc = self._evaluate_operand_python(initial_value, document)

                for i, item in enumerate(input_array):
                    # Create context with variable bindings
                    # $$value is the accumulator, $$this is the current item
                    ctx = dict(document)
                    ctx["$$value"] = acc
                    ctx["$$this"] = item
                    ctx["$$index"] = i

                    # Evaluate expression in context to get new accumulator value
                    acc = self._evaluate_operand_python(in_expr, ctx)

                return acc

            case _:
                raise NotImplementedError(
                    f"Array transform operator {operator} not supported in Python evaluation"
                )
