import re


def quote_identifier(identifier: str) -> str:
    """
    Safely quote a SQL identifier (like a table or column name).
    Only allows safe identifiers matching ^[A-Za-z_][A-Za-z0-9_]*$.
    For table names, use quote_table_name() instead.

    Args:
        identifier (str): The identifier to quote.

    Returns:
        str: The identifier (validated to be safe).

    Raises:
        ValueError: If the identifier contains unsafe characters.
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    # Only allow safe alphanumeric + underscore identifiers
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError(
            f"Invalid identifier '{identifier}': must contain only "
            f"alphanumeric characters and underscores, and must not start with a digit"
        )

    return identifier


def quote_table_name(name: str) -> str:
    """
    Safely quote a table name. In NeoSQLite, table names are the same as collection names.
    """
    return quote_identifier(name)
