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

    # Find the array field and positional operator
    return _apply_positional_recursive(
        doc, parts, 0, value, array_filters, filter_doc
    )


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

        # Find first matching element using filter_doc
        matched = False
        for i, elem in enumerate(arr):
            if not matched:
                # Check if this element matches the filter
                # Extract the filter for this array field from filter_doc
                # For "scores.$", filter_doc would be {"scores": 90} or {"_id": 1}
                if filter_doc:
                    # Try to get the filter value for the array field
                    # Look back in parts to find the field name
                    if index > 0:
                        field_name = parts[index - 1]
                        field_filter = filter_doc.get(field_name)
                        if field_filter is not None:
                            # There's a filter for this field, check if element matches
                            if (
                                _matches_filter(elem, field_filter)
                                if isinstance(field_filter, dict)
                                else elem == field_filter
                            ):
                                if is_last:
                                    arr[i] = value
                                    matched = True
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
                                        matched = True
                        else:
                            # No filter for this array field, update first element (MongoDB behavior)
                            if is_last:
                                arr[i] = value
                                matched = True
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
                                    matched = True
                    else:
                        # index <= 0, no field name to look back to, update first element
                        if is_last:
                            arr[i] = value
                            matched = True
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
                                matched = True
                else:
                    # No filter, just update first element
                    if is_last:
                        arr[i] = value
                        matched = True
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
                            matched = True

        return True

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
            # Add more operators as needed
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
