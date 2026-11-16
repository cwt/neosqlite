import re


def quote_identifier(identifier: str) -> str:
    """Safely validate and return a SQL identifier.

    Only allows identifiers matching ``^[A-Za-z_][A-Za-z0-9_]*$`` and raises
    ``ValueError`` for empty or unsafe identifiers. For table names, prefer
    ``quote_table_name()``.

    Args:
        identifier: SQL identifier (table or column name) to validate.

    Returns:
        str: The validated identifier string.

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


def quote_table_name(name: str) -> str:
    """Safely validate and return a SQL table name.

    In NeoSQLite, table names correspond to collection names and are validated
    using ``quote_identifier()``.

    Args:
        name: The table or collection name to validate.

    Returns:
        str: The validated table name string.

    Raises:
        ValueError: If the table name is empty or contains unsafe characters.
    """
    return quote_identifier(name)
