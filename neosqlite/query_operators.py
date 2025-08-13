import re
from typing import Any, Dict, List

from .exceptions import MalformedQueryException


# Query operators
def _eq(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) == value
    except (TypeError, AttributeError):
        return False


def _gt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) > value
    except TypeError:
        return False


def _lt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) < value
    except TypeError:
        return False


def _gte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) >= value
    except TypeError:
        return False


def _lte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) <= value
    except TypeError:
        return False


def _all(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
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
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$in' must accept an iterable")
    return document.get(field, None) in values


def _ne(field: str, value: Any, document: Dict[str, Any]) -> bool:
    return document.get(field, None) != value


def _nin(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$nin' must accept an iterable")
    return document.get(field, None) not in values


def _mod(field: str, value: List[int], document: Dict[str, Any]) -> bool:
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
    if value not in (True, False):
        raise MalformedQueryException("'$exists' must be supplied a boolean")
    if value:
        return field in document
    else:
        return field not in document


def _regex(field: str, value: str, document: Dict[str, Any]) -> bool:
    try:
        return re.search(value, document.get(field, "")) is not None
    except (TypeError, re.error):
        return False


def _elemMatch(
    field: str, value: Dict[str, Any], document: Dict[str, Any]
) -> bool:
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
    field_val = document.get(field)
    if not isinstance(field_val, list):
        return False
    return len(field_val) == value


def _contains(field: str, value: str, document: Dict[str, Any]) -> bool:
    try:
        field_val = document.get(field)
        if field_val is None:
            return False
        # Convert both values to strings and do a case-insensitive comparison
        return str(value).lower() in str(field_val).lower()
    except (TypeError, AttributeError):
        return False
