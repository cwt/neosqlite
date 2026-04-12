import logging
import re
from typing import Any

from .exceptions import MalformedQueryException

logger = logging.getLogger(__name__)


def _get_nested_field(field: str, document: dict[str, Any]) -> Any:
    """
    Get a nested field value from a document using dot notation.

    Args:
        field (str): The field path using dot notation (e.g., "profile.age").
        document (dict[str, Any]): The document to get the field value from.

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


def _get_int_value(field: str, document: dict[str, Any]) -> int | None:
    """
    Get field value and convert to int, returning None if not possible.

    Args:
        field (str): The document field to get.
        document (dict[str, Any]): The document to get the value from.

    Returns:
        int | None: The integer value, or None if conversion fails.
    """
    doc_value = _get_nested_field(field, document)

    if doc_value is None:
        return None

    if isinstance(doc_value, bool):
        return None  # Booleans are not valid for bitwise operations

    try:
        return int(doc_value)
    except (TypeError, ValueError) as e:
        logger.debug(f"{e=}")
        return None


def _convert_to_bitmask(value: Any) -> int | None:
    """
    Convert a value to a bitmask using pattern matching.

    Handles:
    - int: Direct integer bitmask
    - list/tuple: Array of bit positions
    - Other iterables: Iterable of bit positions
    - Other: Try to convert to int

    Args:
        value: The value to convert.

    Returns:
        int | None: The bitmask, or None if conversion fails.
    """
    match value:
        case int():
            return value
        case list() | tuple() as items:
            try:
                bitmask = 0
                for bit_pos in items:
                    bitmask |= 1 << int(bit_pos)
                return bitmask
            except (TypeError, ValueError) as e:
                logger.debug(f"{e=}")
                return None
        case _ if hasattr(value, "__iter__") and not isinstance(
            value, (str, bytes)
        ):
            try:
                bitmask = 0
                for bit_pos in value:
                    bitmask |= 1 << int(bit_pos)
                return bitmask
            except (TypeError, ValueError) as e:
                logger.debug(f"{e=}")
                return None
        case _:
            try:
                return int(value)
            except (TypeError, ValueError) as e:
                logger.debug(f"{e=}")
                return None


# Query operators
def _eq(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the equals operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value equals the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value == value
    except (TypeError, AttributeError) as e:
        logger.debug(f"Equality comparison failed for field '{field}': {e}")
        return False


def _gt(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the greater than operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is greater than the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value > value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _lt(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the less than operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is less than the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value < value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _gte(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the greater than or equal to operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is greater than or equal to the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value >= value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _lte(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the less than or equal to operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is less than or equal to the given value, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        return doc_value <= value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _all(field: str, value: list[Any], document: dict[str, Any]) -> bool:
    """
    Check if all elements in an array field match the provided value.

    Args:
        field (str): The document field to compare.
        value (list[Any]): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if all elements in the array field match the given value, False otherwise.
    """
    try:
        a = set(value)
    except TypeError as e:
        logger.debug(
            f"Operator evaluation failed for '$all' field '{field}': {e}"
        )
        raise MalformedQueryException("'$all' must accept an iterable")
    try:
        doc_value = _get_nested_field(field, document)
        b = set(doc_value if isinstance(doc_value, list) else [])
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False
    else:
        return a.issubset(b)


def _in(field: str, value: list[Any], document: dict[str, Any]) -> bool:
    """
    Check if a field value is present in the provided list.

    Args:
        field (str): The document field to compare.
        value (list[Any]): The list to check against.
        document (dict[str, Any]): The document to compare the field value from.

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


def _ne(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the not equal operator.

    Args:
        field (str): The document field to compare.
        value (Any): The value to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is not equal to the given value, False otherwise.
    """
    doc_value = _get_nested_field(field, document)
    return doc_value != value


def _nin(field: str, value: list[Any], document: dict[str, Any]) -> bool:
    """
    Check if a field value is not present in the provided list.

    Args:
        field (str): The document field to compare.
        value (list[Any]): The list to check against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value is not present in the list, False otherwise.
    """
    try:
        values = iter(value)
    except TypeError as e:
        logger.debug(
            f"Operator evaluation failed for '$nin' field '{field}': {e}"
        )
        raise MalformedQueryException("'$nin' must accept an iterable")
    doc_value = _get_nested_field(field, document)
    return doc_value not in values


def _mod(field: str, value: list[int], document: dict[str, Any]) -> bool:
    """
    Compare a field value with a given value using the modulo operator.

    Args:
        field (str): The document field to compare.
        value (list[int]): The divisor and remainder to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value modulo the divisor equals the remainder, False otherwise.
    """
    try:
        divisor, remainder = list(map(int, value))
    except (TypeError, ValueError) as e:
        logger.debug(
            f"Operator evaluation failed for '$mod' field '{field}': {e}"
        )
        raise MalformedQueryException(
            "'$mod' must accept an iterable: [divisor, remainder]"
        )
    try:
        val = document.get(field)
        if val is None:
            return False
        return int(val) % divisor == remainder
    except (TypeError, ValueError) as e:
        logger.debug(f"Modulo comparison failed for field '{field}': {e}")
        return False


def _exists(field: str, value: bool, document: dict[str, Any]) -> bool:
    """
    Check if a field exists in the document.

    Args:
        field (str): The document field to check.
        value (bool): True if the field must exist, False if it must not exist.
        document (dict[str, Any]): The document to check the field in.

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
                return not value
            if i == len(field_parts) - 1:
                # We've reached the final field
                return value
            doc_value = doc_value.get(path, None)
        # Unreachable: loop always returns via lines 333 or 336
        return not value
    else:
        return (field in document) if value else (field not in document)


def _regex(
    field: str, value: Any, document: dict[str, Any], options: str = ""
) -> bool:
    """
    Match a field value against a regular expression.

    Args:
        field (str): The document field to compare.
        value (Any): The regular expression to compare against (str or re.Pattern).
        document (dict[str, Any]): The document to compare the field value from.
        options (str): Optional regex flags (i, m, x, s).

    Returns:
        bool: True if the field value matches the regular expression, False otherwise.
    """
    flags = 0
    if options:
        if "i" in options.lower():
            flags |= re.IGNORECASE
        if "m" in options.lower():
            flags |= re.MULTILINE
        if "x" in options.lower():
            flags |= re.VERBOSE
        if "s" in options.lower():
            flags |= re.DOTALL

    try:
        doc_val = _get_nested_field(field, document)
        if doc_val is None:
            doc_val = ""

        # If value is already a compiled pattern, flags are ignored in re.search
        if isinstance(value, re.Pattern):
            return value.search(str(doc_val)) is not None

        return re.search(value, str(doc_val), flags) is not None
    except (TypeError, re.error) as e:
        logger.debug(f"Regex matching failed for field '{field}': {e}")
        return False


def _elemMatch(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if a field value matches all criteria in a provided dictionary or simple value.

    Args:
        field (str): The document field to compare.
        value (Any): Either a simple value to match directly, a dictionary of query
                     operators (e.g., {"$gte": 90}), or a dictionary of field-value
                     pairs for arrays of objects.
        document (dict[str, Any]): The document to compare the field value from.

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
                if not isinstance(elem, dict):
                    continue
                match_all = True
                for k, v in value.items():
                    if isinstance(v, dict) and any(
                        sk.startswith("$") for sk in v.keys()
                    ):
                        # Handle query operators like {"$gt": 80} for fields
                        field_val_in_elem = _get_nested_field(k, elem)
                        if not _apply_query_operators(v, field_val_in_elem):
                            match_all = False
                            break
                    else:
                        # Handle literal field-value pairs
                        if not _eq(k, v, elem):
                            match_all = False
                            break
                if match_all:
                    return True
            return False
    else:
        # If value is not a dictionary, check for simple equality match
        # This handles cases like {"tags": {"$elemMatch": "c"}} where array contains simple values
        for elem in field_val:
            if elem == value:
                return True
        return False


def _apply_query_operators(operators: dict[str, Any], value: Any) -> bool:
    """
    Apply query operators to a single value.

    Args:
        operators: Dictionary of query operators (e.g., {"$gte": 90})
        value: The value to test against

    Returns:
        bool: True if all operators match, False otherwise
    """
    # Extract $options for $regex if present
    options = operators.get("$options", "")
    if options and "$regex" not in operators:
        raise MalformedQueryException("Can't use $options without $regex")

    for op, operand in operators.items():
        if op == "$options":
            # $options is handled together with $regex
            continue

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
            if op == "$regex":
                if not op_func("_temp", operand, temp_doc, options=options):
                    return False
            else:
                if not op_func("_temp", operand, temp_doc):
                    return False
        except Exception as e:
            logger.debug(f"{e=}")
            return False

    return True


def _size(field: str, value: int, document: dict[str, Any]) -> bool:
    """
    Check if the size of an array field matches a specified value.

    Args:
        field (str): The document field to compare.
        value (int): The size to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the size of the array field matches the specified value, False otherwise.
    """
    field_val = _get_nested_field(field, document)
    if not isinstance(field_val, list):
        return False
    return len(field_val) == value


def _contains(field: str, value: str, document: dict[str, Any]) -> bool:
    """
    Check if a field value contains a specified substring.

    Args:
        field (str): The document field to compare.
        value (str): The substring to compare against.
        document (dict[str, Any]): The document to compare the field value from.

    Returns:
        bool: True if the field value contains the specified substring, False otherwise.
    """
    try:
        field_val = document.get(field)
        if field_val is None:
            return False
        # Convert both values to strings and do a case-insensitive comparison
        return str(value).lower() in str(field_val).lower()
    except (TypeError, AttributeError) as e:
        logger.debug(
            f"Contains operator evaluation failed for field '{field}': {e}"
        )
        return False


def _type(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if field is of specified type.

    Args:
        field (str): The document field to check.
        value (Any): The type to check against (as a number or type object).
        document (dict[str, Any]): The document to check the field value from.

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


def _bits_all_clear(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if all specified bits are clear (0) in a numeric field.
    MongoDB $bitsAllClear operator.

    Args:
        field (str): The document field to check.
        value (Any): Bitmask as integer, BinData, or array of bit positions.
        document (dict[str, Any]): The document to check.

    Returns:
        bool: True if all specified bits are clear, False otherwise.
    """
    int_value = _get_int_value(field, document)
    if int_value is None:
        return False

    bitmask = _convert_to_bitmask(value)
    if bitmask is None:
        return False

    # Check if all specified bits are clear (result of AND should be 0)
    return (int_value & bitmask) == 0


def _bits_all_set(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if all specified bits are set (1) in a numeric field.
    MongoDB $bitsAllSet operator.

    Args:
        field (str): The document field to check.
        value (Any): Bitmask as integer, BinData, or array of bit positions.
        document (dict[str, Any]): The document to check.

    Returns:
        bool: True if all specified bits are set, False otherwise.
    """
    int_value = _get_int_value(field, document)
    if int_value is None:
        return False

    bitmask = _convert_to_bitmask(value)
    if bitmask is None:
        return False

    # Check if all specified bits are set
    return (int_value & bitmask) == bitmask


def _bits_any_clear(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if any of the specified bits are clear (0) in a numeric field.
    MongoDB $bitsAnyClear operator.

    Args:
        field (str): The document field to check.
        value (Any): Bitmask as integer, BinData, or array of bit positions.
        document (dict[str, Any]): The document to check.

    Returns:
        bool: True if any of the specified bits are clear, False otherwise.
    """
    int_value = _get_int_value(field, document)
    if int_value is None:
        return False

    bitmask = _convert_to_bitmask(value)
    if bitmask is None:
        return False

    # Check if any of the specified bits are clear
    # Invert the value and check if any of the specified bits are set
    return ((~int_value) & bitmask) != 0


def _bits_any_set(field: str, value: Any, document: dict[str, Any]) -> bool:
    """
    Check if any of the specified bits are set (1) in a numeric field.
    MongoDB $bitsAnySet operator.

    Args:
        field (str): The document field to check.
        value (Any): Bitmask as integer, BinData, or array of bit positions.
        document (dict[str, Any]): The document to check.

    Returns:
        bool: True if any of the specified bits are set, False otherwise.
    """
    int_value = _get_int_value(field, document)
    if int_value is None:
        return False

    bitmask = _convert_to_bitmask(value)
    if bitmask is None:
        return False

    # Check if any of the specified bits are set
    return (int_value & bitmask) != 0


# Aliases for operator lookup (to match MongoDB camelCase naming)
_bitsAllClear = _bits_all_clear
_bitsAllSet = _bits_all_set
_bitsAnyClear = _bits_any_clear
_bitsAnySet = _bits_any_set
