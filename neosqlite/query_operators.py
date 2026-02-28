from .exceptions import MalformedQueryException
from typing import Any, Dict, List
import re


def _get_nested_field(field: str, document: Dict[str, Any]) -> Any:
    """
    Get a nested field value from a document using dot notation.

    Args:
        field (str): The field path using dot notation (e.g., "profile.age").
        document (Dict[str, Any]): The document to get the field value from.

    Returns:
        Any: The field value, or None if the field doesn't exist.
    """
    if "." not in field:
        return document.get(field, None)

    # Handle nested fields
    doc_value: Any = document
    for path in field.split("."):
        if not isinstance(doc_value, dict) or path not in doc_value:
            return None
        doc_value = doc_value.get(path, None)
    return doc_value


# Query operators
def _eq(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the equals operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value equals the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value == value
    except (TypeError, AttributeError):
        return False


def _gt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the greater than operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is greater than the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value > value
    except TypeError:
        return False


def _lt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the less than operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is less than the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value < value
    except TypeError:
        return False


def _gte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the greater than or equal to operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is greater than or equal to the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value >= value
    except TypeError:
        return False


def _lte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the less than or equal to operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is less than or equal to the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value <= value
    except TypeError:
        return False


def _all(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Check if all elements in an array field match the provided value.

    Args:
        field (str): The document field to compare.
        value (List[Any]): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if all elements in the array field match the given value, False otherwise.
    """
    try:
        a = set(value)
    except TypeError:
        raise MalformedQueryException("'$all' must accept an iterable")
    try:
        doc_value = _get_nested_field(field, document)
        b = set(doc_value if isinstance(doc_value, list) else [])
    except TypeError:
        return False
    else:
        return a.issubset(b)


def _in(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Check if a field value is present in the provided list.

    Args:
        field (str): The document field to compare.
        value (List[Any]): The list to check against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is present in the list, False otherwise.
    """
    if not isinstance(value, list):
        raise MalformedQueryException("$in must be followed by an array")

    doc_value = _get_nested_field(field, document)

    # If the field value is a list, check if any element is in the provided list
    if isinstance(doc_value, list):
        return any(item in value for item in doc_value)
    else:
        # If the field value is not a list, check if it's in the provided list
        return doc_value in value


def _ne(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the not equal operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is not equal to the given value, False otherwise.
    """
    doc_value = _get_nested_field(field, document)
    return doc_value != value


def _nin(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Check if a field value is not present in the provided list.

    Args:
        field (str): The document field to compare.
        value (List[Any]): The list to check against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is not present in the list, False otherwise.
    """
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$nin' must accept an iterable")
    doc_value = _get_nested_field(field, document)
    return doc_value not in values


def _mod(field: str, value: List[int], document: Dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the modulo operator.

    Args:
        field (str): The document field to compare.
        value (List[int]): The divisor and remainder to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value modulo the divisor equals the remainder, False otherwise.
    """
    try:
        divisor, remainder = list(map(int, value))
    except (TypeError, ValueError):
        raise MalformedQueryException(
            "'$mod' must accept an iterable: [divisor, remainder]"
        )
    try:
        val = document.get(field)
        if val is None:
            return False
        return int(val) % divisor == remainder
    except (TypeError, ValueError):
        return False


def _exists(field: str, value: bool, document: Dict[str, Any]) -> bool:
    """
    Check if a field exists in the document.

    Args:
        field (str): The document field to check.
        value (bool): True if the field must exist, False if it must not exist.
        document (Dict[str, Any]): The document to check the field in.

    Returns:
        bool: True if the field exists (if value is True), or does not exist (if value is False), False otherwise.
    """
    if not isinstance(value, bool):
        raise MalformedQueryException("'$exists' must be supplied a boolean")

    # Handle nested fields
    if "." in field:
        doc_value: Any = document
        field_parts = field.split(".")
        for i, path in enumerate(field_parts):
            if not isinstance(doc_value, dict) or path not in doc_value:
                # Field doesn't exist
                return not value if value else True
            if i == len(field_parts) - 1:
                # We've reached the final field
                return value
            doc_value = doc_value.get(path, None)
        return not value if value else True
    else:
        return (field in document) if value else (field not in document)


def _regex(field: str, value: str, document: Dict[str, Any]) -> bool:
    """
    Match a field value against a regular expression.

    Args:
        field (str): The document field to compare.
        value (str): The regular expression to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value matches the regular expression, False otherwise.
    """
    try:
        return re.search(value, document.get(field, "")) is not None
    except (TypeError, re.error):
        return False


def _elemMatch(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Check if a field value matches all criteria in a provided dictionary or simple value.

    Args:
        field (str): The document field to compare.
        value (Any): Either a simple value to match directly, a dictionary of query
                     operators (e.g., {"$gte": 90}), or a dictionary of field-value
                     pairs for arrays of objects.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value matches the criteria, False otherwise.
    """
    field_val = document.get(field)
    if not isinstance(field_val, list):
        return False

    # If value is a dictionary, check if it contains query operators or field-value pairs
    if isinstance(value, dict):
        # Check if the dict contains query operators (keys starting with $)
        has_query_operators = any(k.startswith("$") for k in value.keys())

        if has_query_operators:
            # Handle query operators like {"$gte": 90}
            # Apply the operators to each array element
            for elem in field_val:
                if _apply_query_operators(value, elem):
                    return True
            return False
        else:
            # Handle field-value pairs for arrays of objects
            # e.g., {"scores": {"$elemMatch": {"subject": "math", "score": {"$gt": 80}}}}
            for elem in field_val:
                if isinstance(elem, dict) and all(
                    _eq(k, v, elem) for k, v in value.items()
                ):
                    return True
            return False
    else:
        # If value is not a dictionary, check for simple equality match
        # This handles cases like {"tags": {"$elemMatch": "c"}} where array contains simple values
        for elem in field_val:
            if elem == value:
                return True
        return False


def _apply_query_operators(operators: Dict[str, Any], value: Any) -> bool:
    """
    Apply query operators to a single value.

    Args:
        operators: Dictionary of query operators (e.g., {"$gte": 90})
        value: The value to test against

    Returns:
        bool: True if all operators match, False otherwise
    """
    for op, operand in operators.items():
        # Create a temporary document with the value
        temp_doc = {"_temp": value}

        # Get the operator function
        try:
            op_func = globals().get(f"_{op.replace('$', '')}")
            if op_func is None:
                # Try to import from the module
                import neosqlite.query_operators as qo

                op_func = getattr(qo, f"_{op.replace('$', '')}", None)

            if op_func is None:
                # Operator not found, return False
                return False

            # Call the operator function
            if not op_func("_temp", operand, temp_doc):
                return False
        except Exception:
            return False

    return True


def _size(field: str, value: int, document: Dict[str, Any]) -> bool:
    """
    Check if the size of an array field matches a specified value.

    Args:
        field (str): The document field to compare.
        value (int): The size to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the size of the array field matches the specified value, False otherwise.
    """
    field_val = _get_nested_field(field, document)
    if not isinstance(field_val, list):
        return False
    return len(field_val) == value


def _contains(field: str, value: str, document: Dict[str, Any]) -> bool:
    """
    Check if a field value contains a specified substring.

    Args:
        field (str): The document field to compare.
        value (str): The substring to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value contains the specified substring, False otherwise.
    """
    try:
        field_val = document.get(field)
        if field_val is None:
            return False
        # Convert both values to strings and do a case-insensitive comparison
        return str(value).lower() in str(field_val).lower()
    except (TypeError, AttributeError):
        return False


def _type(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Check if field is of specified type.

    Args:
        field (str): The document field to check.
        value (Any): The type to check against (as a number or type object).
        document (Dict[str, Any]): The document to check the field value from.

    Returns:
        bool: True if the field is of the specified type, False otherwise.
    """
    doc_value = _get_nested_field(field, document)

    # MongoDB type mapping
    type_mapping = {
        1: float,
        2: str,
        3: dict,
        4: list,
        8: bool,
        10: type(None),
        16: int,
        18: int,
        19: int,
    }

    # If value is a number, get the corresponding type from mapping
    # Otherwise, use the value directly as a type
    if isinstance(value, int):
        expected_type = type_mapping.get(value)
        if expected_type is None:
            return False
    else:
        expected_type = value

    return isinstance(doc_value, expected_type)
