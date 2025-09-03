"""
Unified SQL translation framework for NeoSQLite.

This module provides a unified approach to translating MongoDB-style queries
into SQL statements that can be used both for direct execution and for
temporary table generation.
"""

from typing import Any, Dict, List, Tuple, Optional, Union
from ..cursor import DESCENDING


def _empty_result() -> Tuple[str, List[Any]]:
    """Return an empty result tuple for fallback cases."""
    return "", []


def _text_search_result() -> Tuple[None, List[Any]]:
    """Return a text search result tuple for fallback cases."""
    return None, []


class SQLFieldAccessor:
    """Handles field access patterns for different contexts."""

    def __init__(self, data_column: str = "data", id_column: str = "id"):
        self.data_column = data_column
        self.id_column = id_column

    def get_field_access(self, field: str, context: str = "direct") -> str:
        """
        Generate field access SQL based on field name and context.

        Args:
            field: The field name to access
            context: The context ('direct', 'temp_table', etc.)

        Returns:
            SQL expression for accessing the field
        """
        if field == "_id":
            # Special handling for _id field
            return self.id_column
        else:
            # Use json_extract for regular fields
            return f"json_extract({self.data_column}, '$.{field}')"


class SQLOperatorTranslator:
    """Translates MongoDB operators to SQL expressions."""

    def __init__(self, field_accessor: Optional[SQLFieldAccessor] = None):
        self.field_accessor = field_accessor or SQLFieldAccessor()

    def translate_operator(
        self, field_access: str, operator: str, value: Any
    ) -> Tuple[Optional[str], List[Any]]:
        """
        Translate a MongoDB operator to SQL.

        Args:
            field_access: The SQL expression for accessing the field
            operator: The MongoDB operator ($eq, $gt, etc.)
            value: The value to compare against

        Returns:
            Tuple of (SQL expression, parameters) or (None, []) if unsupported
        """
        params: List[Any] = []

        # Serialize Binary objects for SQL comparisons using compact format
        if isinstance(value, bytes) and hasattr(value, "encode_for_storage"):
            from .json_helpers import neosqlite_json_dumps_for_sql

            value = neosqlite_json_dumps_for_sql(value)

        match operator:
            case "$eq":
                sql = f"{field_access} = ?"
                params = [value]
            case "$gt":
                sql = f"{field_access} > ?"
                params = [value]
            case "$lt":
                sql = f"{field_access} < ?"
                params = [value]
            case "$gte":
                sql = f"{field_access} >= ?"
                params = [value]
            case "$lte":
                sql = f"{field_access} <= ?"
                params = [value]
            case "$ne":
                sql = f"{field_access} != ?"
                params = [value]
            case "$in":
                if isinstance(value, (list, tuple)):
                    placeholders = ", ".join("?" for _ in value)
                    sql = f"{field_access} IN ({placeholders})"
                    params = list(value)
                else:
                    # Invalid format for $in
                    return None, []
            case "$nin":
                if isinstance(value, (list, tuple)):
                    placeholders = ", ".join("?" for _ in value)
                    sql = f"{field_access} NOT IN ({placeholders})"
                    params = list(value)
                else:
                    # Invalid format for $nin
                    return None, []
            case "$exists":
                # Handle boolean value for $exists
                if value is True:
                    sql = f"{field_access} IS NOT NULL"
                    params = []
                elif value is False:
                    sql = f"{field_access} IS NULL"
                    params = []
                else:
                    # Invalid value for $exists
                    return None, []
            case "$mod":
                # Handle [divisor, remainder] array
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    divisor, remainder = value
                    sql = f"{field_access} % ? = ?"
                    params = [divisor, remainder]
                else:
                    # Invalid format for $mod
                    return None, []
            case "$size":
                # Handle array size comparison
                if isinstance(value, int):
                    sql = f"json_array_length({field_access}) = ?"
                    params = [value]
                else:
                    # Invalid value for $size
                    return None, []
            case "$contains":
                # Handle case-insensitive substring search
                # Convert value to string to match Python implementation behavior
                str_value = str(value)
                sql = f"lower({field_access}) LIKE ?"
                params = [f"%{str_value.lower()}%"]
            case _:
                # Unsupported operator
                return None, []

        return sql, params


class SQLClauseBuilder:
    """Builds SQL clauses with reusable components."""

    def __init__(
        self,
        field_accessor: Optional[SQLFieldAccessor] = None,
        operator_translator: Optional[SQLOperatorTranslator] = None,
    ):
        self.field_accessor = field_accessor or SQLFieldAccessor()
        self.operator_translator = (
            operator_translator or SQLOperatorTranslator()
        )

    def _build_logical_condition(
        self,
        operator: str,
        conditions: List[Dict[str, Any]],
        context: str = "direct",
    ) -> Tuple[Optional[str], List[Any]]:
        """
        Build a logical condition ($and, $or, $nor, $not).

        Args:
            operator: The logical operator
            conditions: List of condition dictionaries
            context: The context for field access

        Returns:
            Tuple of (SQL expression, parameters) or (None, []) if unsupported
        """
        if not isinstance(conditions, list):
            return None, []

        clauses: List[str] = []
        params: List[Any] = []

        for condition in conditions:
            if isinstance(condition, dict):
                # Recursively build condition
                clause, clause_params = self.build_where_clause(
                    condition, context, is_nested=True
                )
                if clause is None:
                    return None, []  # Unsupported condition, fallback to Python
                if clause:
                    # Remove "WHERE " prefix if present
                    if clause.startswith("WHERE "):
                        clause = clause[6:]
                    clauses.append(f"({clause})")
                    params.extend(clause_params)
                else:
                    # Empty condition
                    return None, []
            else:
                # Invalid condition format
                return None, []

        if not clauses:
            return _empty_result()

        match operator:
            case "$and":
                sql = " AND ".join(clauses)
            case "$or":
                sql = " OR ".join(clauses)
            case "$nor":
                sql = "NOT (" + " OR ".join(clauses) + ")"
            case _:
                # Unsupported logical operator
                return None, []

        return sql, params

    def build_where_clause(
        self,
        query: Dict[str, Any],
        context: str = "direct",
        is_nested: bool = False,
    ) -> Tuple[Optional[str], List[Any]]:
        """
        Build a WHERE clause from a MongoDB-style query.

        Args:
            query: The MongoDB-style query
            context: The context for field access
            is_nested: Whether this is a nested condition within a logical operator

        Returns:
            Tuple of (WHERE clause, parameters) or (None, []) if unsupported
        """
        clauses: List[str] = []
        params: List[Any] = []

        for field, value in query.items():
            if field in ("$and", "$or", "$nor"):
                # Handle logical operators directly
                sql, clause_params = self._build_logical_condition(
                    field, value, context
                )
                if sql is None:
                    return (
                        _empty_result()
                    )  # Unsupported condition, fallback to Python
                if sql:  # Only add if not empty
                    clauses.append(sql)
                    params.extend(clause_params)
            elif field == "$not":
                # Handle $not logical operator (applies to single condition)
                if isinstance(value, dict):
                    not_clause, not_params = self.build_where_clause(
                        value, context, is_nested=True
                    )
                    if not_clause is None:
                        return (
                            _empty_result()
                        )  # Unsupported condition, fallback to Python
                    if not_clause:
                        # Remove "WHERE " prefix if present
                        if not_clause.startswith("WHERE "):
                            not_clause = not_clause[6:]
                        clauses.append(f"NOT ({not_clause})")
                        params.extend(not_params)
                    else:
                        return _empty_result()  # Empty condition
                else:
                    return _empty_result()  # Invalid format for $not
            else:
                # Regular field condition
                # Get field access expression
                field_access = self.field_accessor.get_field_access(
                    field, context
                )

                if isinstance(value, dict):
                    # Handle query operators like $eq, $gt, $lt, etc.
                    for operator, op_val in value.items():
                        sql, clause_params = (
                            self.operator_translator.translate_operator(
                                field_access, operator, op_val
                            )
                        )
                        if sql is None:
                            return (
                                None,
                                [],
                            )  # Unsupported operator, fallback to Python
                        clauses.append(sql)
                        params.extend(clause_params)
                else:
                    # Simple equality check
                    clauses.append(f"{field_access} = ?")
                    params.append(value)

        if not clauses:
            return _empty_result()

        where_clause = " AND ".join(clauses)
        # Only add "WHERE" prefix if this is not a nested condition
        if not is_nested:
            where_clause = "WHERE " + where_clause

        return where_clause, params

    def build_order_by_clause(
        self, sort_spec: Dict[str, Any], context: str = "direct"
    ) -> str:
        """
        Build an ORDER BY clause from a sort specification.

        Args:
            sort_spec: The sort specification
            context: The context for field access

        Returns:
            ORDER BY clause
        """
        if not sort_spec:
            return ""

        order_parts = []
        for field, direction in sort_spec.items():
            field_access = self.field_accessor.get_field_access(field, context)
            order_parts.append(
                f"{field_access} {'DESC' if direction == DESCENDING else 'ASC'}"
            )

        return "ORDER BY " + ", ".join(order_parts)

    def build_limit_offset_clause(
        self, limit_value: Optional[int] = None, skip_value: int = 0
    ) -> str:
        """
        Build LIMIT and OFFSET clauses.

        Args:
            limit_value: The limit value
            skip_value: The skip value

        Returns:
            LIMIT and OFFSET clauses
        """
        limit_clause = ""
        if limit_value is not None:
            if skip_value > 0:
                limit_clause = f"LIMIT {limit_value} OFFSET {skip_value}"
            else:
                limit_clause = f"LIMIT {limit_value}"
        elif skip_value > 0:
            # SQLite requires LIMIT when using OFFSET
            limit_clause = f"LIMIT -1 OFFSET {skip_value}"

        return limit_clause


class SQLTranslator:
    """
    Unified SQL translator that can be used for both direct SQL generation
    and temporary table generation.
    """

    def __init__(
        self,
        table_name: Optional[str] = None,
        data_column: str = "data",
        id_column: str = "id",
    ):
        self.table_name = table_name or "collection"
        self.data_column = data_column
        self.id_column = id_column

        # Initialize components
        self.field_accessor = SQLFieldAccessor(data_column, id_column)
        self.operator_translator = SQLOperatorTranslator()
        self.clause_builder = SQLClauseBuilder(
            self.field_accessor, self.operator_translator
        )

    def translate_match(
        self, match_spec: Dict[str, Any], context: str = "direct"
    ) -> Tuple[Optional[str], List[Any]]:
        """
        Translate a $match stage to SQL WHERE clause.

        Args:
            match_spec: The $match specification
            context: The context for field access

        Returns:
            Tuple of (WHERE clause, parameters) or (None, []) for text search
        """
        # Handle text search queries separately
        if "$text" in match_spec:
            return (
                _text_search_result()
            )  # Special handling required, return None to fallback

        # Check for nested $text operators in logical operators
        if self._contains_text_operator(match_spec):
            return (
                _text_search_result()
            )  # Special handling required, return None to fallback

        return self.clause_builder.build_where_clause(match_spec, context)

    def _contains_text_operator(self, query: Dict[str, Any]) -> bool:
        """
        Check if a query contains any $text operators, including nested in logical operators.

        Args:
            query: The query to check

        Returns:
            True if the query contains $text operators, False otherwise
        """
        for field, value in query.items():
            if field in ("$and", "$or", "$nor"):
                # Check each condition in logical operators
                if isinstance(value, list):
                    for condition in value:
                        if isinstance(
                            condition, dict
                        ) and self._contains_text_operator(condition):
                            return True
            elif field == "$not":
                # Check the condition in $not operator
                if isinstance(value, dict) and self._contains_text_operator(
                    value
                ):
                    return True
            elif field == "$text":
                # Found a $text operator
                return True
        return False

    def translate_sort(
        self, sort_spec: Dict[str, Any], context: str = "direct"
    ) -> str:
        """
        Translate a $sort stage to SQL ORDER BY clause.

        Args:
            sort_spec: The $sort specification
            context: The context for field access

        Returns:
            ORDER BY clause
        """
        return self.clause_builder.build_order_by_clause(sort_spec, context)

    def translate_skip_limit(
        self, limit_value: Optional[int] = None, skip_value: int = 0
    ) -> str:
        """
        Translate $skip and $limit stages to SQL LIMIT/OFFSET clauses.

        Args:
            limit_value: The limit value
            skip_value: The skip value

        Returns:
            LIMIT and OFFSET clauses
        """
        return self.clause_builder.build_limit_offset_clause(
            limit_value, skip_value
        )

    def translate_field_access(
        self, field: str, context: str = "direct"
    ) -> str:
        """
        Translate field access for a given field and context.

        Args:
            field: The field name
            context: The context for field access

        Returns:
            SQL expression for accessing the field
        """
        return self.field_accessor.get_field_access(field, context)

    def translate_sort_skip_limit(
        self,
        sort_spec: Dict[str, Any] | None,
        skip_value: int = 0,
        limit_value: int | None = None,
        context: str = "direct",
    ) -> Tuple[str, str, str]:
        """
        Translate sort/skip/limit stages to SQL clauses.

        Args:
            sort_spec: The $sort specification
            skip_value: The skip value
            limit_value: The limit value
            context: The context for field access

        Returns:
            Tuple of (ORDER BY clause, LIMIT clause, OFFSET clause)
        """
        # Build ORDER BY clause
        order_by = self.translate_sort(sort_spec, context) if sort_spec else ""

        # Build LIMIT/OFFSET clause
        limit_offset = self.translate_skip_limit(limit_value, skip_value)

        return order_by, limit_offset, ""
