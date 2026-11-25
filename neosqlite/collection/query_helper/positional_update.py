"""Positional update operations for array elements."""

import operator
import re
from typing import Any

from neosqlite.query_operators import (
    _bson_values_equal,
    _matches_single_bson_type,
    _range_element_matches,
)

from ..type_utils import bson_sort_key
from .utils import _validate_bit_field_value, _validate_inc_mul_field_value


def _is_non_positional_op_supported(op: str) -> bool:
    return op in {
        "$set",
        "$unset",
        "$inc",
        "$mul",
        "$min",
        "$max",
        "$currentDate",
        "$bit",
    }


def _apply_value_op(
    container: dict[str, Any] | list[Any],
    key: str | int,
    exists: bool,
    current_val: Any,
    value: Any,
    op: str,
    field_path: str,
) -> None:
    """Apply an update operator (``$set``, ``$unset``, ``$inc``, etc.) to one slot."""
    if op in ("$set", "$currentDate"):
        if isinstance(container, list) and isinstance(key, int):
            container[key] = value
        elif isinstance(container, dict) and isinstance(key, str):
            container[key] = value
        return
    if op == "$unset":
        if isinstance(container, list) and isinstance(key, int):
            container[key] = None
        elif isinstance(container, dict) and isinstance(key, str):
            container.pop(key, None)
        return
    if op == "$inc":
        _validate_inc_mul_field_value(field_path, current_val, "$inc")
        new_val = (0 if current_val is None else current_val) + value
    elif op == "$mul":
        _validate_inc_mul_field_value(field_path, current_val, "$mul")
        new_val = (0 if current_val is None else current_val) * value
    elif op == "$min":
        if (
            not exists
            or current_val is None
            or bson_sort_key(value) < bson_sort_key(current_val)
        ):
            new_val = value
        else:
            return
    elif op == "$max":
        if (
            not exists
            or current_val is None
            or bson_sort_key(value) > bson_sort_key(current_val)
        ):
            new_val = value
        else:
            return
    elif op == "$bit":
        _validate_bit_field_value(field_path, current_val)
        new_val = 0 if current_val is None else current_val
        if "and" in value:
            new_val &= value["and"]
        if "or" in value:
            new_val |= value["or"]
        if "xor" in value:
            new_val ^= value["xor"]
    else:
        return

    if isinstance(container, list) and isinstance(key, int):
        container[key] = new_val
    elif isinstance(container, dict) and isinstance(key, str):
        container[key] = new_val


def _apply_nested_field_op(
    doc: dict[str, Any], field_path: str, value: Any, op: str = "$set"
) -> None:
    """Apply an update operator at a nested dot-notation path (dict or list)."""
    parts = field_path.split(".")
    current: Any = doc
    for part in parts[:-1]:
        if isinstance(current, list):
            if not (part.isascii() and part.isdigit()):
                return
            idx = int(part)
            if op == "$unset":
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return
            else:
                if idx >= len(current):
                    current.extend([None] * (idx + 1 - len(current)))
                if current[idx] is None:
                    current[idx] = {}
                current = current[idx]
        elif isinstance(current, dict):
            if part not in current:
                if op == "$unset":
                    return
                current[part] = {}
            current = current[part]
        else:
            return

    last = parts[-1]
    if isinstance(current, list):
        if last.isascii() and last.isdigit():
            idx = int(last)
            if op == "$unset":
                if 0 <= idx < len(current):
                    _apply_value_op(
                        current, idx, True, current[idx], value, op, field_path
                    )
            else:
                if idx >= len(current):
                    current.extend([None] * (idx + 1 - len(current)))
                _apply_value_op(
                    current, idx, True, current[idx], value, op, field_path
                )
    elif isinstance(current, dict):
        exists = last in current
        cur_val = current.get(last) if exists else None
        _apply_value_op(current, last, exists, cur_val, value, op, field_path)


def _extract_array_constraints(
    filter_doc: dict[str, Any] | None,
    base: str,
    structural_leaf: str | None,
) -> list[tuple[str | None, Any]]:
    """Collect element predicates for array ``base`` from ``filter_doc`` (including ``$and``)."""
    if not filter_doc:
        return []

    branches: list[dict[str, Any]] = []
    stack = [filter_doc]
    while stack:
        node = stack.pop(0)
        if not isinstance(node, dict):
            continue
        branches.append(node)
        and_list = node.get("$and")
        if isinstance(and_list, list):
            stack.extend(sub for sub in and_list if isinstance(sub, dict))

    constraints: list[tuple[str | None, Any]] = []
    base_parts = base.split(".")
    for branch in branches:
        found_in_branch = False
        if structural_leaf:
            full_key = f"{base}.{structural_leaf}"
            if full_key in branch:
                constraints.append((structural_leaf, branch[full_key]))
                found_in_branch = True

        for k, v in branch.items():
            if k.startswith(base + "."):
                if structural_leaf and k == f"{base}.{structural_leaf}":
                    continue
                constraints.append((k[len(base) + 1 :], v))
                found_in_branch = True

        if not found_in_branch and base in branch:
            cond = branch[base]
            if isinstance(cond, dict) and "$elemMatch" in cond:
                em = cond["$elemMatch"]
                constraints.append((structural_leaf, em))
                extra = {k: v for k, v in cond.items() if k != "$elemMatch"}
                if extra:
                    constraints.append((structural_leaf, extra))
            else:
                constraints.append((structural_leaf, cond))
            found_in_branch = True

        if not found_in_branch and len(base_parts) > 1:
            nested = _resolve_filter_value(branch, base_parts)
            if nested is not None:
                if isinstance(nested, dict) and "$elemMatch" in nested:
                    em = nested["$elemMatch"]
                    constraints.append((structural_leaf, em))
                    extra = {
                        k: v for k, v in nested.items() if k != "$elemMatch"
                    }
                    if extra:
                        constraints.append((structural_leaf, extra))
                else:
                    constraints.append((structural_leaf, nested))

    return constraints


def _elem_matches_constraints(
    elem: Any, constraints: list[tuple[str | None, Any]]
) -> bool:
    """Evaluate all extracted element constraints against ``elem``."""
    for leaf, cond in constraints:
        target = elem
        if leaf:
            if not isinstance(elem, dict):
                return False
            missing = False
            for lg in leaf.split("."):
                if isinstance(target, dict) and lg in target:
                    target = target[lg]
                elif isinstance(target, list) and lg.isascii() and lg.isdigit():
                    idx = int(lg)
                    if 0 <= idx < len(target):
                        target = target[idx]
                    else:
                        missing = True
                        break
                else:
                    missing = True
                    break
            if missing:
                if isinstance(cond, dict) and cond.get("$exists") is False:
                    continue
                return False
        if isinstance(cond, dict):
            if not _matches_filter(target, cond):
                return False
        elif not _bson_values_equal(target, cond):
            return False
    return True


def _apply_positional_update(
    doc: dict[str, Any],
    field_path: str,
    value: Any,
    array_filters: list[dict[str, Any]] | None = None,
    filter_doc: dict[str, Any] | None = None,
    *,
    op: str = "$set",
) -> bool:
    """Apply an update to array elements using positional operators.

    Supports ``$`` (first matching element), ``$[]`` (all elements), and
    ``$[identifier]`` (elements matching ``array_filters``), as well as
    ordinary dot-notation field paths without positional operators, across
    ``$set``, ``$unset``, ``$inc``, ``$mul``, ``$min``, ``$max``,
    ``$currentDate``, and ``$bit``.

    Args:
        doc: Document dictionary to mutate in place.
        field_path: Dot-separated field path optionally containing
            positional operator segments.
        value: Value or operand for the update operator.
        array_filters: Optional list of filter dictionaries keyed by
            identifier for ``$[identifier]`` operators.
        filter_doc: Optional query filter document used to identify the
            matching element for the ``$`` positional operator.
        op: Update operator name (defaults to ``"$set"``).

    Returns:
        bool: ``True`` if an update was applied, ``False`` otherwise.
    """
    if not field_path:
        return False

    # Parse the field path into parts
    parts = field_path.split(".")

    # Check for positional operators
    has_positional = any(
        p == "$" or p == "$[]" or p.startswith("$[") for p in parts
    )

    if not has_positional:
        # No positional operator, simple nested update
        _apply_nested_field_op(doc, field_path, value, op)
        return True

    # Canonical "$" form: <path.to.array>.<optional leaf>.<tail...>
    # Resolve the matched element from the query filter up-front (#99).
    if "$" in parts:
        di = parts.index("$")
        if parts[di] == "$" and di >= 1:
            applied = _apply_dollar_update(
                doc, parts, di, value, filter_doc, op=op, field_path=field_path
            )
            if applied is not None:
                return applied
            # fall through to legacy recursion for exotic shapes

    # Find the array field and positional operator
    return _apply_positional_recursive(
        doc,
        parts,
        0,
        value,
        array_filters,
        filter_doc,
        op=op,
        field_path=field_path,
    )


def _apply_dollar_update(
    doc: dict[str, Any],
    parts: list[str],
    dollar_index: int,
    value: Any,
    filter_doc: dict[str, Any] | None,
    *,
    op: str = "$set",
    field_path: str = "",
) -> bool | None:
    """Apply a filter-driven ``$`` positional update on a target array.

    Locates the target array in ``doc`` along ``parts[:dollar_index]``,
    resolves the matching condition from ``filter_doc``, and updates the
    first matching element (or the first element when no filter is given).
    """
    arr_segs = parts[:dollar_index]
    tail = parts[dollar_index + 1 :]

    def _get_at(d: Any, segs: list[str]) -> Any:
        cur = d
        for sg in segs:
            if not isinstance(cur, dict) or sg not in cur:
                return None
            cur = cur[sg]
        return cur

    # Locate the ARRAY within the prefix. The prefix may end with the
    # array itself ("scores") or name a leaf inside its elements
    # ("students.name") — try the deepest split that yields a list (#99).
    arr: Any = None
    structural_leaf: str | None = None
    for split in range(len(arr_segs), 0, -1):
        cand = _get_at(doc, arr_segs[:split])
        if isinstance(cand, list):
            arr = cand
            rest = arr_segs[split:]
            structural_leaf = ".".join(rest) if rest else None
            break
    if arr is None:
        return None  # shape not understood by this fast path

    base = ".".join(arr_segs)
    constraints = _extract_array_constraints(filter_doc, base, structural_leaf)

    def _apply_elem(i: int, elem: Any) -> bool:
        if tail:
            if isinstance(elem, dict):
                _apply_nested_field_op(elem, ".".join(tail), value, op)
                return True
            return False
        _apply_value_op(arr, i, True, elem, value, op, field_path)
        return True

    if not constraints:
        if filter_doc:
            # Query exists but constrains nothing on this array (#99)
            return False
        # Legacy: no query at all — update the first element
        if arr:
            return _apply_elem(0, arr[0])
        return False

    for i, elem in enumerate(arr):
        if _elem_matches_constraints(elem, constraints):
            return _apply_elem(i, elem)
    return False


def _apply_positional_recursive(
    doc: Any,
    parts: list[str],
    index: int,
    value: Any,
    array_filters: list[dict[str, Any]] | None = None,
    filter_doc: dict[str, Any] | None = None,
    parent_array: list[Any] | None = None,  # Track parent array for $ operator
    *,
    op: str = "$set",
    field_path: str = "",
) -> bool:
    """Recursively apply positional update through nested structures."""
    if index >= len(parts):
        return False

    current_part = parts[index]
    is_last = index == len(parts) - 1

    # Handle $[] - all array elements (check BEFORE $[identifier] since $[] also starts with $[)
    if current_part == "$[]":
        arr = doc if parent_array is None else parent_array
        if not isinstance(arr, list):
            return False

        for i, elem in enumerate(arr):
            if is_last:
                _apply_value_op(arr, i, True, elem, value, op, field_path)
            else:
                if isinstance(elem, dict):
                    _apply_positional_recursive(
                        elem,
                        parts,
                        index + 1,
                        value,
                        array_filters,
                        filter_doc,
                        None,
                        op=op,
                        field_path=field_path,
                    )

        return True

    # Handle $[identifier] - filtered array element
    elif current_part.startswith("$[") and current_part.endswith("]"):
        identifier = current_part[2:-1]

        filter_spec = None
        if array_filters:
            for af in array_filters:
                if identifier in af:
                    filter_spec = af[identifier]
                    break

        if filter_spec is None:
            return False

        arr = doc if parent_array is None else parent_array
        if not isinstance(arr, list):
            return False

        for i, elem in enumerate(arr):
            if _matches_filter(elem, filter_spec):
                if is_last:
                    _apply_value_op(arr, i, True, elem, value, op, field_path)
                else:
                    if isinstance(elem, dict):
                        _apply_positional_recursive(
                            elem,
                            parts,
                            index + 1,
                            value,
                            array_filters,
                            filter_doc,
                            None,
                            op=op,
                            field_path=field_path,
                        )

        return True

    # Handle $ - first matching array element
    elif current_part == "$":
        arr = parent_array if parent_array is not None else doc
        if not isinstance(arr, list):
            return False

        base = ".".join(parts[:index]) if index > 0 else ""
        constraints = (
            _extract_array_constraints(filter_doc, base, None) if base else []
        )

        def _apply_to(i: int, elem: Any) -> bool:
            if is_last:
                _apply_value_op(arr, i, True, elem, value, op, field_path)
                return True
            if isinstance(elem, dict):
                return _apply_positional_recursive(
                    elem,
                    parts,
                    index + 1,
                    value,
                    array_filters,
                    filter_doc,
                    None,
                    op=op,
                    field_path=field_path,
                )
            return False

        if not constraints:
            if filter_doc:
                return False
            return bool(arr) and _apply_to(0, arr[0])

        matched = False
        for i, elem in enumerate(arr):
            if not matched and _elem_matches_constraints(elem, constraints):
                matched = _apply_to(i, elem)
        return matched

    # Regular field access
    else:
        if not isinstance(doc, dict):
            return False

        if current_part not in doc:
            if is_last and op != "$unset":
                _apply_value_op(
                    doc, current_part, False, None, value, op, field_path
                )
                return True
            return False

        if is_last:
            _apply_value_op(
                doc,
                current_part,
                True,
                doc[current_part],
                value,
                op,
                field_path,
            )
            return True
        else:
            next_val = doc[current_part]
            next_is_positional = (
                index + 1 < len(parts) and parts[index + 1] in ("$", "$[]")
            ) or (index + 1 < len(parts) and parts[index + 1].startswith("$["))
            if next_is_positional:
                return _apply_positional_recursive(
                    next_val,
                    parts,
                    index + 1,
                    value,
                    array_filters,
                    filter_doc,
                    next_val,
                    op=op,
                    field_path=field_path,
                )
            else:
                return _apply_positional_recursive(
                    next_val,
                    parts,
                    index + 1,
                    value,
                    array_filters,
                    filter_doc,
                    None,
                    op=op,
                    field_path=field_path,
                )


def _resolve_filter_value(
    filter_doc: dict[str, Any] | None, segments: list[str]
) -> Any:
    """Find the query filter that applies to the given path prefix (#99)."""
    if not filter_doc:
        return None
    name = ".".join(segments)
    if name in filter_doc:
        return filter_doc[name]
    current: Any = filter_doc
    for seg in segments:
        if isinstance(current, dict) and seg in current:
            current = current[seg]
        else:
            return None
    return current


def _matches_filter(elem: Any, filter_spec: dict[str, Any] | Any) -> bool:
    """Check if an array element matches a filter specification."""
    # Handle scalar filter (direct BSON equality check)
    if not isinstance(filter_spec, dict):
        return _bson_values_equal(elem, filter_spec)

    # Handle scalar element with dict filter (apply query operators)
    if not isinstance(elem, dict):
        return _matches_query_operators(elem, filter_spec)

    # Handle dict element with direct query operators (e.g. {$type: "object"})
    if all(k.startswith("$") for k in filter_spec.keys()):
        return _matches_query_operators(elem, filter_spec)

    # Handle dict element with field filter
    from neosqlite.query_operators import _field_exists, _get_nested_field

    for key, expected_value in filter_spec.items():
        exists = _field_exists(key, elem)
        if not exists:
            if (
                isinstance(expected_value, dict)
                and expected_value.get("$exists") is False
            ):
                continue
            return False
        elem_val = _get_nested_field(key, elem)
        if isinstance(expected_value, dict):
            if not _matches_query_operators(elem_val, expected_value):
                return False
        elif not _bson_values_equal(elem_val, expected_value):
            return False

    return True


def _matches_query_operators(value: Any, operators: dict[str, Any]) -> bool:
    """Check if a value matches a dictionary of query operators."""
    for op, expected in operators.items():
        match op:
            case "$eq":
                if not _bson_values_equal(value, expected):
                    return False
            case "$gt":
                if not _range_element_matches(value, expected, operator.gt):
                    return False
            case "$gte":
                if not _range_element_matches(value, expected, operator.ge):
                    return False
            case "$lt":
                if not _range_element_matches(value, expected, operator.lt):
                    return False
            case "$lte":
                if not _range_element_matches(value, expected, operator.le):
                    return False
            case "$ne":
                if _bson_values_equal(value, expected):
                    return False
            case "$in":
                if not isinstance(expected, (list, tuple)) or not any(
                    _bson_values_equal(value, item) for item in expected
                ):
                    return False
            case "$nin":
                if isinstance(expected, (list, tuple)) and any(
                    _bson_values_equal(value, item) for item in expected
                ):
                    return False
            case "$exists":
                # Elements always exist when extracted from an array
                if not expected:
                    return False
            case "$size":
                if not (
                    isinstance(value, (list, tuple)) and len(value) == expected
                ):
                    return False
            case "$regex":
                flags = 0
                if "$options" in operators:
                    opts = operators["$options"]
                    if "i" in opts:
                        flags |= re.IGNORECASE
                    if "m" in opts:
                        flags |= re.MULTILINE
                    if "s" in opts:
                        flags |= re.DOTALL
                pattern = (
                    expected.pattern
                    if hasattr(expected, "pattern")
                    else str(expected)
                )
                if not isinstance(value, str) or not re.search(
                    pattern, value, flags
                ):
                    return False
            case "$options":
                continue
            case "$type":
                specs = expected if isinstance(expected, list) else [expected]
                if not any(
                    _matches_single_bson_type(value, spec) for spec in specs
                ):
                    return False
            case "$elemMatch":
                if not isinstance(expected, dict):
                    return False
                if isinstance(value, list):
                    if not any(_matches_filter(el, expected) for el in value):
                        return False
                elif not _matches_filter(value, expected):
                    return False
            case "$and":
                if not isinstance(expected, list) or not all(
                    _matches_filter(value, sub) for sub in expected
                ):
                    return False
            case "$or":
                if not isinstance(expected, list) or not any(
                    _matches_filter(value, sub) for sub in expected
                ):
                    return False
            case "$nor":
                if not isinstance(expected, list) or any(
                    _matches_filter(value, sub) for sub in expected
                ):
                    return False
            case "$not":
                if _matches_filter(value, expected):
                    return False
            case _ if op.startswith("$"):
                # Unknown operators must fail loudly: silently matching
                # everything caused arrayFilters to over-update (#100)
                raise ValueError(f"Unsupported operator '{op}' in array filter")
    return True


def _set_nested_field(doc: dict[str, Any], field_path: str, value: Any) -> None:
    """Set a nested field value using dot notation (supporting numeric list indices)."""
    _apply_nested_field_op(doc, field_path, value, "$set")
