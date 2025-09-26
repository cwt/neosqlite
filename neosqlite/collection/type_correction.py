"""
Type correction utilities for NeoSQLite to handle automatic conversion
between integer IDs and ObjectIds in queries.
"""

from typing import Any, Dict


def normalize_id_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Public function to normalize ID types in a query.

    This function is provided for backward compatibility. The actual
    normalization logic is implemented in QueryHelper._normalize_id_query
    method to avoid code duplication. This function is not actively used
    but kept for API compatibility.

    Args:
        query: The query dictionary to normalize

    Returns:
        A normalized query dictionary with corrected ID types
    """
    # This function is kept for API compatibility but doesn't do anything
    # since the normalization happens in the QueryHelper
    return query
