"""Positional update operations for array elements."""

import re
from typing import Any


def _apply_positional_update(
    doc: dict[str, Any],
    field_path: str,
    value: Any,
    array_filters: list[dict[str, Any]] | None = None,
    filter_doc: dict[str, Any] | None = None,
) -> bool:
    """Apply an update to array elements using positional operators.

    Supports ``$`` (first matching element), ``$[]`` (all elements), and
    ``$[identifier]`` (elements matching ``array_filters``), as well as
    ordinary dot-notation field paths without positional operators.

    Args:
        doc: Document dictionary to mutate in place.
        field_path: Dot-separated field path optionally containing
            positional operator segments.
        value: Value to assign at the resolved target location.
        array_filters: Optional list of filter dictionaries keyed by
            identifier for ``$[identifier]`` operators.
        filter_doc: Optional query filter document used to identify the
            matching element for the ``$`` positional operator.

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
    """Apply a filter-driven ``$`` positional update on a target array.

    Locates the target array in ``doc`` along ``parts[:dollar_index]``,
    resolves the matching condition from ``filter_doc``, and updates the
    first matching element (or the first element when no filter is given).

    Args:
        doc: Document dictionary to mutate in place.
        parts: Split field-path segments.
        dollar_index: Index of the ``"$"`` segment within ``parts``.
        value: Value to assign at the matched element or nested tail path.
        filter_doc: Optional query filter document constraining the update.

    Returns:
        bool | None: ``True`` if an element was updated, ``False`` if no
            matching element could be updated, or ``None`` if the path
            shape should fall back to ``_apply_positional_recursive``.
    """
    arr_segs = parts[:dollar_index]
    tail = parts[dollar_index + 1 :]

    def _get_at(d: Any, segs: list[str]) -> Any:
        """Walk nested dictionaries along ``segs`` and return the value.

        Args:
            d: Root value or dictionary to traverse.
            segs: Sequence of dictionary keys to follow.

        Returns:
            Any: Nested value if all keys exist in dicts, else ``None``.
        """
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
        """Return whether an array element matches the resolved filter.

        Args:
            elem: Candidate element from the target array.

        Returns:
            bool: ``True`` if ``elem`` satisfies ``field_filter`` at
                ``leaf``, ``False`` otherwise.
        """
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
    """Recursively apply positional update through nested structures.

    Traverses ``parts`` starting at ``index`` and handles ``$[]``,
    ``$[identifier]``, ``$``, and regular dictionary key segments.

    Args:
        doc: Current document, sub-document, or array being traversed.
        parts: List of field path segments.
        index: Current segment index in ``parts``.
        value: Value to assign at the target location.
        array_filters: Optional list of filter dictionaries for
            ``$[identifier]`` operators.
        filter_doc: Optional query filter dictionary for ``$`` operators.
        parent_array: Parent list passed when descending into a positional
            operator segment.

    Returns:
        bool: ``True`` if the update succeeded, ``False`` otherwise.
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
            """Assign ``value`` or recurse into ``arr[i]`` for ``$``.

            Args:
                i: Index of the matched element in ``arr``.
                elem: Element at ``arr[i]``.

            Returns:
                bool: ``True`` if the element or nested field was updated.
            """
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

    Looks for a flat dotted key first (e.g., ``"a.scores"``), then walks
    nested dictionary structures along ``segments``. Returns ``None`` when
    ``filter_doc`` does not constrain the path.

    Args:
        filter_doc: Query filter dictionary or ``None``.
        segments: Path segments preceding the positional operator.

    Returns:
        Any: Matching filter value or operator dictionary, or ``None`` if
            no constraint applies to the path.
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


def _matches_filter(elem: Any, filter_spec: dict[str, Any] | Any) -> bool:
    """Check if an array element matches a filter specification.

    Supports scalar equality, operator-only dictionaries (keys starting
    with ``"$"``), and field-level filter dictionaries on sub-documents.

    Args:
        elem: Array element to evaluate.
        filter_spec: Filter dictionary with operators/field criteria or a
            scalar value for equality matching.

    Returns:
        bool: ``True`` if ``elem`` matches ``filter_spec``, ``False``
            otherwise.
    """
    # Handle scalar filter (direct equality check)
    if not isinstance(filter_spec, dict):
        return elem == filter_spec

    # Handle scalar element with dict filter (apply query operators)
    if not isinstance(elem, dict):
        # Apply query operators to scalar value
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
            # Handle query operators in filter
            if not _matches_query_operators(elem_val, expected_value):
                return False
        elif elem_val != expected_value:
            return False

    return True


def _matches_query_operators(value: Any, operators: dict[str, Any]) -> bool:
    """Check if a value matches a dictionary of query operators.

    Evaluates ``$eq``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$ne``,
    ``$in``, ``$nin``, ``$exists``, ``$size``, ``$regex``/``$options``, and
    ``$type`` against ``value``.

    Args:
        value: Value to test against the operators.
        operators: Dictionary mapping MongoDB operator names to expected
            values.

    Returns:
        bool: ``True`` if ``value`` satisfies every operator in
            ``operators``, ``False`` otherwise.

    Raises:
        ValueError: If an unsupported ``$``-prefixed operator is present.
    """
    for op, expected in operators.items():
        match op:
            case "$eq":
                if value != expected:
                    return False
            case "$gt":
                if value is None or expected is None or not (value > expected):
                    return False
            case "$gte":
                if value is None or expected is None or not (value >= expected):
                    return False
            case "$lt":
                if value is None or expected is None or not (value < expected):
                    return False
            case "$lte":
                if value is None or expected is None or not (value <= expected):
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
    """Set a nested field value using dot notation.

    Creates intermediate dictionaries for any missing path segments before
    assigning ``value`` to the final key.

    Args:
        doc: Document dictionary to update in place.
        field_path: Dot-notation field path (e.g., ``"a.b.c"``).
        value: Value to set at the target field.
    """
    parts = field_path.split(".")
    current = doc

    for i, part in enumerate(parts[:-1]):
        if part not in current:
            current[part] = {}
        current = current[part]

    current[parts[-1]] = value
