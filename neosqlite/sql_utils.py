import re


def quote_identifier(identifier: str) -> str:
    """
    Safely quote a SQL identifier (like a table or column name) only if necessary.
    If the identifier is already a safe alphanumeric string, it's returned as-is.

    Args:
        identifier (str): The identifier to quote.

    Returns:
        str: The quoted or unquoted identifier.
    """
    if not identifier:
        return '""'

    # Check if it's already safe (alphanumeric and underscores, not starting with a digit)
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        return identifier

    # Otherwise, quote it using [] and escape ]
    safe_id = identifier.replace("]", "]]")
    return f"[{safe_id}]"


def quote_table_name(name: str) -> str:
    """
    Safely quote a table name. In NeoSQLite, table names are the same as collection names.
    """
    return quote_identifier(name)
