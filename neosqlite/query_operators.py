from .exceptions import MalformedQueryException
from typing import Any, Dict, List
import re


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
        return document.get(field, None) == value
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
        return document.get(field, None) > value
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
        return document.get(field, None) < value
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
        return document.get(field, None) >= value
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
        return document.get(field, None) <= value
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
        b = set(document.get(field, []))
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
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$in' must accept an iterable")
    return document.get(field, None) in values


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
    return document.get(field, None) != value


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
    return document.get(field, None) not in values


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
        val = document.get(field, None)
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
    if value not in (True, False):
        raise MalformedQueryException("'$exists' must be supplied a boolean")
    if value:
        return field in document
    else:
        return field not in document


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


def _elemMatch(
    field: str, value: Dict[str, Any], document: Dict[str, Any]
) -> bool:
    """
    Check if a field value matches all criteria in a provided dictionary.

    Args:
        field (str): The document field to compare.
        value (Dict[str, Any]): A dictionary of field-value pairs to compare against.
        document (Dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value matches all criteria in the provided dictionary, False otherwise.
    """
    field_val = document.get(field)
    if not isinstance(field_val, list):
        return False
    for elem in field_val:
        if isinstance(elem, dict) and all(
            _eq(k, v, elem) for k, v in value.items()
        ):
            return True
    return False


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
    field_val = document.get(field)
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
