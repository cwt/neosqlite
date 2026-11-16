import logging
import re
from datetime import datetime
from typing import Any

from .binary import Binary
from .exceptions import MalformedQueryException
from .objectid import ObjectId

logger = logging.getLogger(__name__)


def _get_nested_field(field: str, document: dict[str, Any]) -> Any:
    """Get a nested field value from a document using dot notation.

    Args:
        field: The field path using dot notation (e.g., ``"profile.age"``).
        document: The document dictionary to inspect.

    Returns:
        Any: The field value, or None if any path segment is missing or
            non-dictionary.
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


def _field_exists(field: str, document: dict[str, Any]) -> bool:
    """Check if a field exists in the document, handling dot notation.

    Distinguishes missing keys from keys explicitly set to ``None``.

    Args:
        field: The field path using dot notation (e.g., ``"profile.age"``).
        document: The document dictionary to inspect.

    Returns:
        bool: True if the field path exists in ``document``, False otherwise.
    """
    if "." not in field:
        return field in document
    doc_value: Any = document
    field_parts = field.split(".")
    for i, path in enumerate(field_parts):
        if not isinstance(doc_value, dict) or path not in doc_value:
            return False
        if i == len(field_parts) - 1:
            return True
        doc_value = doc_value[path]
    return False


def _get_int_value(field: str, document: dict[str, Any]) -> int | None:
    """Get a field value and convert it to ``int`` if valid for bitwise ops.

    Rejects ``None`` and ``bool`` values and returns ``None`` when ``int()``
    conversion raises ``TypeError`` or ``ValueError``.

    Args:
        field: The document field path to extract.
        document: The document dictionary to read from.

    Returns:
        int | None: The converted integer value, or None if unavailable or
            invalid.
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
    """Convert a bitmask specification to an integer using pattern matching.

    Handles:
    - ``int``: Direct integer bitmask.
    - ``list`` or ``tuple``: Sequence of bit positions (``1 << pos``).
    - Other non-str/non-bytes iterables: Bit positions (``1 << pos``).
    - Other values: Converted via ``int(value)``.

    Args:
        value: Bitmask value or sequence of bit positions to convert.

    Returns:
        int | None: Computed integer bitmask, or None if conversion fails.
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
    """Compare a field value with a target value using ``$eq`` semantics.

    Matches when both values are lists and equal, when the field value is a
    list containing ``value``, or when scalar equality holds.

    Args:
        field: The document field path to compare.
        value: The target value to compare against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field matches ``value``, False otherwise.
    """
    try:
        doc_value = _get_nested_field(field, document)
        # Both are arrays: exact array equality
        if isinstance(doc_value, list) and isinstance(value, list):
            return doc_value == value
        # Doc is array, value is scalar: check if value is in array
        if isinstance(doc_value, list):
            return value in doc_value
        # Scalar semantics: exact equality
        return doc_value == value
    except (TypeError, AttributeError) as e:
        logger.debug(f"Equality comparison failed for field '{field}': {e}")
        return False


def _gt(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Compare a field value against a target using ``$gt`` semantics.

    Returns False when ``value`` is None or on boolean/incompatible-type
    mismatches. When the field is a list, matches if any non-boolean numeric
    element satisfies ``x > value``.

    Args:
        field: The document field path to compare.
        value: The threshold value to compare against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field (or an array element) is greater than
            ``value``, False otherwise.
    """
    try:
        if value is None:
            return False
        doc_value = _get_nested_field(field, document)
        # MongoDB array semantics: match if ANY element satisfies
        if isinstance(doc_value, list):
            return any(
                isinstance(x, (int, float))
                and not isinstance(x, bool)
                and x > value
                for x in doc_value
            )
        if isinstance(doc_value, bool) != isinstance(value, bool):
            return False
        return doc_value > value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _lt(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Compare a field value against a target using ``$lt`` semantics.

    Returns False when ``value`` is None or on boolean/incompatible-type
    mismatches. When the field is a list, matches if any non-boolean numeric
    element satisfies ``x < value``.

    Args:
        field: The document field path to compare.
        value: The threshold value to compare against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field (or an array element) is less than ``value``,
            False otherwise.
    """
    try:
        if value is None:
            return False
        doc_value = _get_nested_field(field, document)
        # MongoDB array semantics: match if ANY element satisfies
        if isinstance(doc_value, list):
            return any(
                isinstance(x, (int, float))
                and not isinstance(x, bool)
                and x < value
                for x in doc_value
            )
        if isinstance(doc_value, bool) != isinstance(value, bool):
            return False
        return doc_value < value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _gte(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Compare a field value against a target using ``$gte`` semantics.

    When ``value`` is None, matches if the field value is None. When the field
    is a list, matches if any non-boolean numeric element satisfies
    ``x >= value``.

    Args:
        field: The document field path to compare.
        value: The threshold value to compare against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field (or an array element) is greater than or
            equal to ``value``, False otherwise.
    """
    try:
        if value is None:
            doc_value = _get_nested_field(field, document)
            return doc_value is None
        doc_value = _get_nested_field(field, document)
        # MongoDB array semantics: match if ANY element satisfies
        if isinstance(doc_value, list):
            return any(
                isinstance(x, (int, float))
                and not isinstance(x, bool)
                and x >= value
                for x in doc_value
            )
        if isinstance(doc_value, bool) != isinstance(value, bool):
            return False
        return doc_value >= value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _lte(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Compare a field value against a target using ``$lte`` semantics.

    When ``value`` is None, matches if the field value is None. When the field
    is a list, matches if any non-boolean numeric element satisfies
    ``x <= value``.

    Args:
        field: The document field path to compare.
        value: The threshold value to compare against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field (or an array element) is less than or equal to
            ``value``, False otherwise.
    """
    try:
        if value is None:
            doc_value = _get_nested_field(field, document)
            return doc_value is None
        doc_value = _get_nested_field(field, document)
        # MongoDB array semantics: match if ANY element satisfies
        if isinstance(doc_value, list):
            return any(
                isinstance(x, (int, float))
                and not isinstance(x, bool)
                and x <= value
                for x in doc_value
            )
        if isinstance(doc_value, bool) != isinstance(value, bool):
            return False
        return doc_value <= value
    except TypeError as e:
        logger.debug(f"Operator evaluation failed due to TypeError: {e}")
        return False


def _all(field: str, value: list[Any], document: dict[str, Any]) -> bool:
    """Check if an array field contains all elements from ``value``.

    Args:
        field: The document field path to inspect.
        value: Iterable of required elements.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field is a list containing all items in ``value``,
            False otherwise.

    Raises:
        MalformedQueryException: If ``value`` is not iterable.
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
    """Check if a field value (or any array element) is in ``value``.

    Args:
        field: The document field path to inspect.
        value: List of candidate values to match against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field value (or any array element) is in ``value``,
            False otherwise.

    Raises:
        MalformedQueryException: If ``value`` is not a list.
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
    """Compare a field value with a target using ``$ne`` semantics.

    Missing fields match ``$ne`` unless ``value`` is None. For array fields,
    matches when ``value`` is not contained in the array; for scalars, matches
    when ``doc_value != value``.

    Args:
        field: The document field path to compare.
        value: The value to compare for inequality.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field satisfies ``$ne`` against ``value``, False
            otherwise.
    """
    if not _field_exists(field, document):
        # Missing fields match $ne for any non-null value; {$ne: null} does not
        # match missing fields (MongoDB treats missing as null).
        return value is not None
    doc_value = _get_nested_field(field, document)
    # MongoDB array semantics: check if value is NOT in array
    if isinstance(doc_value, list):
        return value not in doc_value
    # Scalar semantics: not equal
    return doc_value != value


def _nin(field: str, value: list[Any], document: dict[str, Any]) -> bool:
    """Check if a field value (or any array element) is not in ``value``.

    Args:
        field: The document field path to inspect.
        value: Iterable of disallowed values to check against.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field value (or all array elements) are absent from
            ``value``, False otherwise.

    Raises:
        MalformedQueryException: If ``value`` is not iterable.
    """
    try:
        values = iter(value)
    except TypeError as e:
        logger.debug(
            f"Operator evaluation failed for '$nin' field '{field}': {e}"
        )
        raise MalformedQueryException("'$nin' must accept an iterable")
    doc_value = _get_nested_field(field, document)

    # If the field value is a list, check if none of the elements are in the provided list
    if isinstance(doc_value, list):
        return not any(item in value for item in doc_value)
    else:
        # If the field value is not a list, check if it's not in the provided list
        return doc_value not in values


def _mod(field: str, value: list[int], document: dict[str, Any]) -> bool:
    """Evaluate the modulo operator ``$mod`` against a numeric or array field.

    Coerces ``value`` into ``[divisor, remainder]`` integers and checks whether
    ``int(doc_value) % divisor == remainder`` (or any numeric element when the
    field is an array).

    Args:
        field: The document field path to inspect.
        value: Two-element sequence ``[divisor, remainder]``.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field satisfies the modulo condition, False
            otherwise.

    Raises:
        MalformedQueryException: If ``value`` is not a two-integer pair or
            ``divisor`` is 0.
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
    if divisor == 0:
        raise MalformedQueryException("'$mod' divisor must not be 0")
    try:
        doc_value = _get_nested_field(field, document)
        # $mod only works on scalar values, not arrays
        if isinstance(doc_value, list):
            # MongoDB: $mod on arrays matches if ANY element satisfies
            return any(
                isinstance(x, (int, float)) and int(x) % divisor == remainder
                for x in doc_value
            )
        if doc_value is None:
            return False
        return int(doc_value) % divisor == remainder
    except (TypeError, ValueError) as e:
        logger.debug(f"Modulo comparison failed for field '{field}': {e}")
        return False


def _exists(field: str, value: bool, document: dict[str, Any]) -> bool:
    """Check whether a field exists (or does not exist) in a document.

    Args:
        field: The document field path to check.
        value: True to require field existence, False to require absence.
        document: The document dictionary to inspect.

    Returns:
        bool: True if field existence matches ``value``, False otherwise.

    Raises:
        MalformedQueryException: If ``value`` is not a ``bool``.
    """
    if not isinstance(value, bool):
        raise MalformedQueryException("'$exists' must be supplied a boolean")

    exists = _field_exists(field, document)
    return exists if value else not exists


def _regex(
    field: str, value: Any, document: dict[str, Any], options: str = ""
) -> bool:
    """Match a field value against a regular expression.

    Missing or None field values never match. When ``value`` is already a
    compiled ``re.Pattern``, ``options`` flags are ignored; otherwise ``i``,
    ``m``, ``x``, and ``s`` options are translated to Python ``re`` flags.

    Args:
        field: The document field path to search.
        value: Regular expression string or compiled ``re.Pattern``.
        document: The document dictionary containing the field.
        options: Optional regex flag characters (``i``, ``m``, ``x``, ``s``).
            Defaults to ``""``.

    Returns:
        bool: True if ``str(doc_val)`` matches the pattern, False otherwise.
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
            # MongoDB never matches a missing field with $regex (#161)
            return False

        # If value is already a compiled pattern, flags are ignored in re.search
        if isinstance(value, re.Pattern):
            return value.search(str(doc_val)) is not None

        return re.search(value, str(doc_val), flags) is not None
    except (TypeError, re.error) as e:
        logger.debug(f"Regex matching failed for field '{field}': {e}")
        return False


def _elemMatch(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Check if an array field has an element matching ``value`` criteria.

    Supports a dictionary of query operators (such as ``{"$gte": 90}``), a
    dictionary of field conditions for arrays of objects, or a scalar equality
    value.

    Args:
        field: The document field path holding the array.
        value: Query operator dict, subdocument criteria dict, or scalar value
            to match against array elements.
        document: The document dictionary containing the field.

    Returns:
        bool: True if ``field`` is a list with at least one matching element,
            False otherwise.
    """
    field_val = _get_nested_field(field, document)
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
    """Apply a dictionary of query operators to a single value.

    Resolves each ``$`` operator to its corresponding helper function in this
    module and evaluates all conditions against ``{"_temp": value}``,
    forwarding ``$options`` to ``$regex``.

    Args:
        operators: Dictionary of query operators (e.g., ``{"$gte": 90}``).
        value: The candidate value to test against all operators.

    Returns:
        bool: True if all operators match ``value``, False if any operator
            fails, is unknown, or raises a non-query exception.

    Raises:
        MalformedQueryException: If ``$options`` is provided without
            ``$regex``, or if a sub-operator raises it.
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
        except MalformedQueryException:
            # Malformed sub-queries must surface like top-level ones (#163)
            raise
        except Exception as e:
            logger.debug(f"{e=}")
            return False

    return True


def _size(field: str, value: int, document: dict[str, Any]) -> bool:
    """Check if the length of an array field equals ``value``.

    Args:
        field: The document field path to inspect.
        value: Expected array length.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field is a list with length equal to ``value``,
            False otherwise.
    """
    field_val = _get_nested_field(field, document)
    if not isinstance(field_val, list):
        return False
    return len(field_val) == value


def _contains(field: str, value: str, document: dict[str, Any]) -> bool:
    """Check if a field's string representation contains ``value``.

    Performs a case-insensitive substring check on ``str(field_val)`` and
    returns False when the field is None or missing.

    Args:
        field: The document field path to inspect.
        value: Substring to search for (case-insensitive).
        document: The document dictionary containing the field.

    Returns:
        bool: True if ``str(value).lower()`` is in ``str(field_val).lower()``,
            False otherwise.
    """
    try:
        field_val = _get_nested_field(field, document)
        if field_val is None:
            return False
        # Convert both values to strings and do a case-insensitive comparison
        return str(value).lower() in str(field_val).lower()
    except (TypeError, AttributeError) as e:
        logger.debug(
            f"Contains operator evaluation failed for field '{field}': {e}"
        )
        return False


def _matches_single_bson_type(doc_value: Any, spec: Any) -> bool:
    """Return True if a single Python value satisfies a BSON ``$type`` spec.

    Supports numeric BSON type codes (1, 2, 3, 4, 5, 7, 8, 9, 10, 16, 18, 19),
    case-insensitive BSON type alias strings, and Python ``type`` objects,
    distinguishing ``bool`` from ``int`` and NeoSQLite wrapper dicts
    (ObjectId, Binary, ``$date``) from plain objects.

    Args:
        doc_value: Python value extracted from a document field.
        spec: BSON type code, string alias, or Python ``type`` object.

    Returns:
        bool: True if ``doc_value`` matches ``spec``, False otherwise.
    """
    numeric_codes = {
        1: "double",
        2: "string",
        3: "object",
        4: "array",
        5: "binData",
        7: "objectId",
        8: "bool",
        9: "date",
        10: "null",
        16: "int",
        18: "long",
        19: "decimal",
    }

    target: str | type | None = None
    if isinstance(spec, int) and not isinstance(spec, bool):
        target = numeric_codes.get(spec)
        if target is None:
            return False
    elif isinstance(spec, str):
        target = spec.lower()
    elif isinstance(spec, type):
        target = spec
    else:
        return False

    if isinstance(target, str):
        match target:
            case "double":
                return isinstance(doc_value, float)
            case "string":
                return isinstance(doc_value, str)
            case "object":
                return isinstance(doc_value, dict) and not (
                    doc_value.get("__neosqlite_objectid__")
                    or doc_value.get("__neosqlite_binary__")
                    or "$date" in doc_value
                )
            case "array":
                return isinstance(doc_value, list)
            case "bindata":
                return isinstance(doc_value, (Binary, bytes))
            case "objectid":
                return isinstance(doc_value, ObjectId) or (
                    isinstance(doc_value, dict)
                    and doc_value.get("__neosqlite_objectid__") == 1
                )
            case "bool" | "boolean":
                return isinstance(doc_value, bool)
            case "date":
                return isinstance(doc_value, datetime) or (
                    isinstance(doc_value, dict) and "$date" in doc_value
                )
            case "null":
                return doc_value is None
            case "int" | "integer" | "long":
                return isinstance(doc_value, int) and not isinstance(
                    doc_value, bool
                )
            case "decimal" | "number":
                return isinstance(doc_value, (int, float)) and not isinstance(
                    doc_value, bool
                )
            case _:
                return False

    # Python type object passed directly (e.g. {"$type": str})
    if target is int:
        return isinstance(doc_value, int) and not isinstance(doc_value, bool)
    if target is float:
        return isinstance(doc_value, float)
    if target is dict:
        return isinstance(doc_value, dict) and not (
            doc_value.get("__neosqlite_objectid__")
            or doc_value.get("__neosqlite_binary__")
            or "$date" in doc_value
        )
    return isinstance(doc_value, target)


def _type(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Check if an existing field matches a BSON ``$type`` specification.

    Returns False if the field does not exist. Supports either a single BSON
    type spec or a ``list``/``tuple`` of specs (matching if any matches).

    Args:
        field: The document field path to inspect.
        value: BSON type spec (number, string alias, or Python type), or a
            list/tuple of specs.
        document: The document dictionary containing the field.

    Returns:
        bool: True if ``field`` exists and matches ``value``, False otherwise.
    """
    if not _field_exists(field, document):
        return False
    doc_value = _get_nested_field(field, document)

    if isinstance(value, (list, tuple)):
        return any(_matches_single_bson_type(doc_value, item) for item in value)
    return _matches_single_bson_type(doc_value, value)


def _bits_all_clear(field: str, value: Any, document: dict[str, Any]) -> bool:
    """Check if all specified bits are clear (0) in a numeric field.

    Implements the MongoDB ``$bitsAllClear`` operator by checking
    ``(int_value & bitmask) == 0``.

    Args:
        field: The document field path to inspect.
        value: Bitmask as an integer or iterable of bit positions.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field converts to an integer and all bits in
            ``bitmask`` are clear, False otherwise.
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
    """Check if all specified bits are set (1) in a numeric field.

    Implements the MongoDB ``$bitsAllSet`` operator by checking
    ``(int_value & bitmask) == bitmask``.

    Args:
        field: The document field path to inspect.
        value: Bitmask as an integer or iterable of bit positions.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field converts to an integer and all bits in
            ``bitmask`` are set, False otherwise.
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
    """Check if any specified bits are clear (0) in a numeric field.

    Implements the MongoDB ``$bitsAnyClear`` operator by checking
    ``((~int_value) & bitmask) != 0``.

    Args:
        field: The document field path to inspect.
        value: Bitmask as an integer or iterable of bit positions.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field converts to an integer and at least one bit in
            ``bitmask`` is clear, False otherwise.
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
    """Check if any specified bits are set (1) in a numeric field.

    Implements the MongoDB ``$bitsAnySet`` operator by checking
    ``(int_value & bitmask) != 0``.

    Args:
        field: The document field path to inspect.
        value: Bitmask as an integer or iterable of bit positions.
        document: The document dictionary containing the field.

    Returns:
        bool: True if the field converts to an integer and at least one bit in
            ``bitmask`` is set, False otherwise.
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
