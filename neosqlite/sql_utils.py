import re


def validate_identifier(identifier: str) -> str:
    """Validate that a SQL identifier is non-empty and safe.

    Ensures ``identifier`` matches ``^[A-Za-z_][A-Za-z0-9_]*$`` and returns
    it unquoted, which is suitable for building composite names (such as
    ``idx_<table_name>_<field>``) or binding ``sqlite_master`` lookup
    parameters.

    Args:
        identifier: Table, index, trigger, or column identifier to validate.

    Returns:
        str: The validated raw identifier string.

    Raises:
        ValueError: If the identifier is empty or contains unsafe characters.
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    # Only allow safe alphanumeric + underscore identifiers
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(
            f"Invalid identifier '{identifier}': must contain only "
            f"alphanumeric characters and underscores, and must not start with a digit"
        )

    return identifier


def quote_identifier(identifier: str) -> str:
    """Safely validate and double-quote a SQL identifier.

    Validates identifiers against ``^[A-Za-z_][A-Za-z0-9_]*$`` and wraps the
    validated name in double quotes so SQL reserved keywords (such as
    ``order``, ``group``, or ``select``) cannot trigger syntax errors. For
    table names, prefer ``quote_table_name()``.

    Args:
        identifier: SQL identifier (table, index, trigger, or column name) to
            validate and quote.

    Returns:
        str: The double-quoted identifier string (``"<identifier>"``).

    Raises:
        ValueError: If the identifier is empty or contains unsafe characters.
    """
    return f'"{validate_identifier(identifier)}"'


def quote_table_name(name: str) -> str:
    """Safely validate and double-quote a SQL table name.

    In NeoSQLite, table names correspond to collection names and are validated
    and quoted using ``quote_identifier()``.

    Args:
        name: The table or collection name to validate and quote.

    Returns:
        str: The double-quoted table name string (``"<name>"``).

    Raises:
        ValueError: If the table name is empty or contains unsafe characters.
    """
    return quote_identifier(name)
