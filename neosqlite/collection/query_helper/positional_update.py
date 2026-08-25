"""Positional update operations for array elements."""

from typing import Any


def _apply_positional_update(
    doc: dict[str, Any],
    field_path: str,
    value: Any,
    array_filters: list[dict[str, Any]] | None = None,
    filter_doc: dict[str, Any] | None = None,
) -> bool:
    """
    Apply an update to array elements using positional operators.

    Supports:
    - $: First matching array element
    - $[]: All array elements
    - $[identifier]: Filtered array elements (requires arrayFilters)

    Args:
        doc: The document to update
        field_path: The field path containing positional operator(s)
        value: The value to set
        array_filters: Optional list of filter documents for $[identifier]
        filter_doc: The query filter document (for $ operator)

    Returns:
        bool: True if update was applied, False otherwise
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
        # No positional operator, simple nested set
        _set_nested_field(doc, field_path, value)
        return True

    # Canonical "$" form: <path.to.array>.<optional leaf>.<tail...>
    # Resolve the matched element from the query filter up-front (#99).
    if "$" in parts:
        di = parts.index("$")
        if parts[di] == "$" and di >= 1:
            applied = _apply_dollar_update(doc, parts, di, value, filter_doc)
            if applied is not None:
                return applied
            # fall through to legacy recursion for exotic shapes

    # Find the array field and positional operator
    return _apply_positional_recursive(
        doc, parts, 0, value, array_filters, filter_doc
    )


def _apply_dollar_update(
    doc: dict[str, Any],
    parts: list[str],
    dollar_index: int,
    value: Any,
    filter_doc: dict[str, Any] | None,
) -> bool | None:
    """Filter-driven "$" update; returns None to defer to legacy path."""
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

    # Resolve the query condition for this array: exact prefix key first,
    # then any query key extending the prefix ("students.name": x).
    base = ".".join(arr_segs)
    field_filter: Any = None
    leaf: str | None = structural_leaf
    if filter_doc:
        if leaf:
            full_key = f"{base}.{leaf}"
            if full_key in filter_doc:
                field_filter = filter_doc[full_key]
        if field_filter is None:
            for k, v in filter_doc.items():
                if k.startswith(base + "."):
                    field_filter = v
                    if leaf is None:
                        leaf = k[len(base) + 1 :]
                    break
        if field_filter is None and not leaf and base in filter_doc:
            # Whole-array condition: {"scores": 90} matches any element == 90
            field_filter = filter_doc[base]

    def _pred(elem: Any) -> bool:
        if field_filter is None:
            return False
        target = elem
        if leaf:
            if not isinstance(elem, dict):
                return False
            target = elem
            for lg in leaf.split("."):
                if not isinstance(target, dict) or lg not in target:
                    return False
                target = target[lg]
        if isinstance(field_filter, dict):
            return (
                _matches_query_operators(target, field_filter)
                if not isinstance(target, dict)
                else _matches_filter(target, field_filter)
            )
        return target == field_filter

    if field_filter is None:
        if filter_doc:
            # Query exists but constrains nothing on this array (#99)
            return False
        # Legacy: no query at all — update the first element
        if arr:
            if tail:
                if isinstance(arr[0], dict):
                    _set_nested_field(arr[0], ".".join(tail), value)
                    return True
                return False
            arr[0] = value
            return True
        return False

    for i, elem in enumerate(arr):
        if _pred(elem):
            if tail:
                if isinstance(elem, dict):
                    _set_nested_field(elem, ".".join(tail), value)
                    return True
                return False
            arr[i] = value
            return True
    return False


def _apply_positional_recursive(
    doc: Any,
    parts: list[str],
    index: int,
    value: Any,
    array_filters: list[dict[str, Any]] | None = None,
    filter_doc: dict[str, Any] | None = None,
    parent_array: list[Any] | None = None,  # Track parent array for $ operator
) -> bool:
    """
    Recursively apply positional update through nested structures.

    Args:
        doc: Current document or sub-document
        parts: Field path parts
        index: Current part index
        value: Value to set
        array_filters: Filter documents for $[identifier]
        filter_doc: Query filter for $ operator
        parent_array: Parent array (for $ operator to know which array to update)

    Returns:
        bool: True if update was applied
    """
    if index >= len(parts):
        return False

    current_part = parts[index]
    is_last = index == len(parts) - 1

    # Handle $[] - all array elements (check BEFORE $[identifier] since $[] also starts with $[)
    if current_part == "$[]":
        # The array should be in doc (we're already at the array level)
        arr = doc if parent_array is None else parent_array
        if not isinstance(arr, list):
            return False

        # Update all elements
        for i, elem in enumerate(arr):
            if is_last:
                arr[i] = value
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
                    )

        return True

    # Handle $[identifier] - filtered array element
    elif current_part.startswith("$[") and current_part.endswith("]"):
        identifier = current_part[2:-1]

        # Find the matching filter
        filter_spec = None
        if array_filters:
            for af in array_filters:
                if identifier in af:
                    filter_spec = af[identifier]
                    break

        # If no filter found for this identifier, don't update anything
        if filter_spec is None:
            return False

        # The array should be in doc (we're already at the array level)
        arr = doc if parent_array is None else parent_array
        if not isinstance(arr, list):
            return False

        # Apply filter to find matching elements
        for i, elem in enumerate(arr):
            if _matches_filter(elem, filter_spec):
                if is_last:
                    arr[i] = value
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
                        )

        return True

    # Handle $ - first matching array element
    elif current_part == "$":
        # Use parent_array if available, otherwise doc should be the array
        arr = parent_array if parent_array is not None else doc
        if not isinstance(arr, list):
            return False

        # Resolve the filter that identified this array. The query may use
        # a dotted path ("a.scores": 90) whose immediate parent segment has
        # no top-level entry — search by the full prefix (#99).
        field_filter = (
            _resolve_filter_value(filter_doc, parts[:index])
            if index > 0
            else None
        )

        def _apply_to(i: int, elem: Any) -> bool:
            if is_last:
                arr[i] = value
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
                )
            return False

        if field_filter is None:
            if filter_doc:
                # A query exists but constrains nothing on this array:
                # MongoDB errors here; silently writing element 0 corrupts
                # unrelated documents.
                return False
            # No query at all — legacy behavior: update first element
            return bool(arr) and _apply_to(0, arr[0])

        matched = False
        for i, elem in enumerate(arr):
            if not matched and (
                _matches_filter(elem, field_filter)
                if isinstance(field_filter, dict)
                else elem == field_filter
            ):
                matched = _apply_to(i, elem)
        return matched

    # Regular field access
    else:
        if not isinstance(doc, dict):
            return False

        if current_part not in doc:
            # Create the nested structure if it doesn't exist and this is the last part
            if is_last:
                doc[current_part] = value
                return True
            return False

        if is_last:
            doc[current_part] = value
            return True
        else:
            next_val = doc[current_part]
            # If next part is positional, pass the array as parent_array
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
                )


def _resolve_filter_value(
    filter_doc: dict[str, Any] | None, segments: list[str]
) -> Any:
    """Find the query filter that applies to the given path prefix (#99).

    Looks for a flat dotted key first ("a.scores"), then walks nested
    structures. Returns None when the filter says nothing about the path.
    """
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


def _matches_filter(elem: Any, filter_spec: dict[str, Any]) -> bool:
    """
    Check if an array element matches a filter specification.

    Args:
        elem: The array element to check
        filter_spec: The filter specification (can be a dict with operators or a scalar value)

    Returns:
        bool: True if element matches the filter
    """
    # Handle scalar filter (direct equality check)
    if not isinstance(filter_spec, dict):
        return elem == filter_spec

    # Handle scalar element with dict filter (apply query operators)
    if not isinstance(elem, dict):
        # Apply query operators to scalar value
        return _matches_query_operators(elem, filter_spec)

    # Handle dict element with dict filter
    for key, expected_value in filter_spec.items():
        if key not in elem:
            return False
        if isinstance(expected_value, dict):
            # Handle query operators in filter
            if not _matches_query_operators(elem[key], expected_value):
                return False
        elif elem[key] != expected_value:
            return False

    return True


def _matches_query_operators(value: Any, operators: dict[str, Any]) -> bool:
    """
    Check if a value matches query operators.

    Args:
        value: The value to check
        operators: Dictionary of query operators

    Returns:
        bool: True if value matches all operators
    """
    for op, expected in operators.items():
        match op:
            case "$eq":
                if value != expected:
                    return False
            case "$gt":
                if not (value > expected):
                    return False
            case "$gte":
                if not (value >= expected):
                    return False
            case "$lt":
                if not (value < expected):
                    return False
            case "$lte":
                if not (value <= expected):
                    return False
            case "$ne":
                if value == expected:
                    return False
            case "$in":
                if value not in expected:
                    return False
            case "$nin":
                if value in expected:
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
            case "$type":
                from ..type_utils import get_bson_type

                names = expected if isinstance(expected, list) else [expected]
                aliases = {"long": "int", "double": "int"}
                wanted = {aliases.get(n, n) for n in names}
                if get_bson_type(value) not in wanted and not (
                    "number" in names
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                ):
                    return False
            case _ if op.startswith("$"):
                # Unknown operators must fail loudly: silently matching
                # everything caused arrayFilters to over-update (#100)
                raise ValueError(f"Unsupported operator '{op}' in array filter")
    return True


def _set_nested_field(doc: dict[str, Any], field_path: str, value: Any) -> None:
    """
    Set a nested field value using dot notation.

    Args:
        doc: The document to update
        field_path: Dot-notation field path (e.g., "a.b.c")
        value: The value to set
    """
    parts = field_path.split(".")
    current = doc

    for i, part in enumerate(parts[:-1]):
        if part not in current:
            current[part] = {}
        current = current[part]

    current[parts[-1]] = value
