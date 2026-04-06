"""
Tier 2: Temporary table-based expression evaluation for complex $expr queries.

This module implements the second tier of the 3-tier $expr evaluation architecture:
- Uses temporary tables for complex expressions that can't be evaluated in a single query
- Leverages SQLite JSON/JSONB native functions for optimal performance
- Ensures data is converted to JSON format when moving to Python space

Tier 2 is used when:
- Expressions are too complex for Tier 1 (single SQL WHERE clause)
- Multiple intermediate calculations are needed
- Aggregation or grouping is required within the expression
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

from .._sqlite import sqlite3
from .json_path_utils import parse_json_path
from .jsonb_support import supports_jsonb
from .query_helper.translation_cache import TranslationCache


class TempTableExprEvaluator:
    """
    Tier 2 evaluator using temporary tables for complex $expr expressions.

    This evaluator:
    1. Creates temporary tables to store intermediate results
    2. Uses SQLite JSON/JSONB functions for efficient processing
    3. Converts to JSON format when exporting to Python space
    """

    def __init__(
        self,
        db_connection,
        data_column: str = "data",
        translation_cache_size: int | None = 100,
    ):
        """
        Initialize the temporary table evaluator.

        Args:
            db_connection: SQLite database connection
            data_column: Name of the column containing JSON data (default: "data")
            translation_cache_size: Size of translation cache (None=default, 0=disabled)
        """
        self.db = db_connection
        self.data_column = data_column
        self._jsonb_supported = supports_jsonb(db_connection)
        self._temp_tables: List[str] = []
        if translation_cache_size is None:
            translation_cache_size = 100
        self._translation_cache = TranslationCache(
            max_size=translation_cache_size
        )

    @property
    def json_function_prefix(self) -> str:
        """Get the appropriate JSON function prefix (json or jsonb)."""
        return "jsonb" if self._jsonb_supported else "json"

    def is_cache_enabled(self) -> bool:
        """Check if translation cache is enabled."""
        return self._translation_cache.is_enabled()

    def get_cache_stats(self) -> dict:
        """Get cache statistics."""
        return self._translation_cache.get_stats()

    def clear_cache(self) -> None:
        """Clear the translation cache."""
        self._translation_cache.clear()

    def cache_size(self) -> int:
        """Get current cache size."""
        return len(self._translation_cache)

    def cache_contains(self, expr: dict) -> bool:
        """Check if expression is in cache."""
        key = self._make_expr_key(expr)
        return self._translation_cache.contains(key)

    def evict_from_cache(self, expr: dict) -> bool:
        """Evict expression from cache."""
        key = self._make_expr_key(expr)
        return self._translation_cache.evict(key)

    def resize_cache(self, new_size: int) -> None:
        """Resize the cache."""
        self._translation_cache.resize(new_size)

    def dump_cache(self) -> list:
        """Dump cache contents for debugging."""
        return self._translation_cache.dump()

    def evaluate(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        filter_expr: Dict[str, Any] | None = None,
    ) -> Tuple[str | None, List[Any], List[str]]:
        """
        Evaluate a $expr expression using temporary tables.

        Args:
            expr: The $expr expression dictionary
            collection_name: Name of the collection table
            filter_expr: Optional additional filter to apply

        Returns:
            Tuple of (SQL query, parameters, table_names) or (None, [], []) for Python fallback
        """
        try:
            # Analyze expression complexity
            complexity = self._analyze_complexity(expr)

            # If too simple for Tier 2, let Tier 1 handle it
            if complexity < 2:
                return None, [], []

            # If too complex for Tier 2, fall back to Tier 3 (Python)
            if complexity > 10:
                return None, [], []

            # Try cache first
            cache_key = self._make_expr_key(expr)
            cached = self._translation_cache.get(cache_key)
            if cached is not None:
                # Build query with cached translation
                return self._build_from_cache(expr, collection_name, cached)

            # Build the evaluation query
            result = self._build_tier2_query(expr, collection_name, filter_expr)
            if result[0] is not None:
                # Cache the translation: WHERE clause template and fields to extract
                sql_query, params, temp_tables = result
                fields = self._extract_field_references(expr)
                # Extract WHERE clause from the full query for caching
                where_clause = self._extract_where_clause(sql_query)
                # Store with temp_table placeholder for substitution on cache hit
                # FIX: self._temp_tables[-1] already includes the prefix
                where_clause_template = where_clause.replace(
                    temp_tables[-1], "{temp_table}"
                )
                self._translation_cache.put(
                    cache_key, where_clause_template, tuple(fields)
                )
            return result

        except (NotImplementedError, ValueError) as e:
            logger.debug(
                f"Tier 2 evaluation failed, falling back to Python: {e}"
            )
            # Fall back to Python evaluation
            return None, [], []
        # Cleanup handled by Cursor

    def _analyze_complexity(self, expr: Dict[str, Any]) -> int:
        """
        Analyze expression complexity to determine if Tier 2 is appropriate.

        Complexity scoring:
        - Base expression: 1 point
        - Each nested operator: +1 point
        - Each arithmetic operator: +1 point
        - Each conditional operator: +2 points
        - Each array operator: +2 points
        - Each subquery/correlated expression: +3 points

        Returns:
            int: Complexity score (1-2: Tier 1, 3-10: Tier 2, 11+: Tier 3)
        """
        if not isinstance(expr, dict):
            return 0

        score = 1  # Base score

        for operator, operands in expr.items():
            match operator:
                case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                    score += 1
                    if isinstance(operands, list):
                        for op in operands:
                            if isinstance(op, dict):
                                score += self._analyze_complexity(op)
                case "$cond" | "$switch":
                    score += 2
                    if isinstance(operands, dict):
                        if "if" in operands:
                            score += self._analyze_complexity(operands["if"])
                        if "then" in operands:
                            if isinstance(operands["then"], dict):
                                score += self._analyze_complexity(
                                    operands["then"]
                                )
                case "$and" | "$or" | "$nor":
                    if isinstance(operands, list):
                        for op in operands:
                            if isinstance(op, dict):
                                score += self._analyze_complexity(op)
                case "$not":
                    if isinstance(operands, list) and len(operands) > 0:
                        if isinstance(operands[0], dict):
                            score += self._analyze_complexity(operands[0])
                case "$size" | "$in" | "$arrayElemAt":
                    score += 2
                case "$concat" | "$substr" | "$trim":
                    score += 1

        return score

    def _make_expr_key(self, expr: Dict[str, Any]) -> str:
        """
        Create a cache key from expression structure.

        Uses TranslationCache._extract_structure to create a hashable key
        that preserves field references ($field) but parameterizes literal values.
        """
        structure = self._translation_cache._extract_structure(expr)
        return str(structure)

    def _build_from_cache(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        cached: tuple[str, tuple[str, ...]],
    ) -> Tuple[str, List[Any], List[str]]:
        """
        Build query from cached translation.

        Args:
            expr: The original expression (for extracting actual parameter values)
            collection_name: Collection table name
            cached: Tuple of (where_clause_template, field_list)

        Returns:
            Tuple of (SQL query, parameters, table_names)
        """
        where_clause_template, field_list = cached

        # Generate unique temp table name
        temp_table = f"temp_expr_{uuid.uuid4().hex[:8]}"
        self._temp_tables.append(temp_table)

        # Create temp table with cached fields
        self._create_temp_table(temp_table, collection_name, list(field_list))

        # Build WHERE clause by substituting temp table name into template
        where_clause = where_clause_template.replace("{temp_table}", temp_table)

        # Extract parameter values from expression
        params = self._extract_param_values_from_expr(expr)

        # Build SELECT with json() conversion for Python-space data
        if self._jsonb_supported:
            select_data = f"json({collection_name}.{self.data_column}) as data"
        else:
            select_data = f"{collection_name}.data as data"

        # Build the final query
        query = f"""
            SELECT {temp_table}.id, {temp_table}._id, {select_data}
            FROM {collection_name}
            JOIN {temp_table} ON {collection_name}.id = {temp_table}.id
            WHERE {where_clause}
        """

        return query.strip(), params, [temp_table]

    def _extract_where_clause(self, full_query: str) -> str:
        """Extract WHERE clause from a full query string."""
        if "WHERE" in full_query:
            # In our generated queries, WHERE is always at the end of the main SELECT
            parts = full_query.rsplit("WHERE", 1)
            return parts[1].strip()
        return ""

    def _extract_param_values_from_expr(
        self, expr: Dict[str, Any]
    ) -> List[Any]:
        """
        Extract actual parameter values from expression for cached query.
        Must follow the exact same traversal order as _convert_expr_to_temp_sql.
        """
        values = []

        def extract_from_expr(e: Any) -> None:
            if not isinstance(e, dict) or len(e) != 1:
                return

            operator, operands = next(iter(e.items()))
            match operator:
                case "$and" | "$or" | "$nor":
                    if isinstance(operands, list):
                        for op in operands:
                            extract_from_expr(op)
                case "$not":
                    if isinstance(operands, list) and len(operands) > 0:
                        extract_from_expr(operands[0])
                case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne" | "$cmp":
                    if isinstance(operands, list):
                        for op in operands:
                            extract_from_operand(op)
                case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                    if isinstance(operands, list):
                        for op in operands:
                            extract_from_operand(op)
                case "$cond":
                    if isinstance(operands, dict):
                        if "if" in operands:
                            extract_from_expr(operands["if"])
                        if "then" in operands:
                            extract_from_operand(operands["then"])
                        if "else" in operands:
                            extract_from_operand(operands["else"])
                case "$abs" | "$ceil" | "$floor" | "$round":
                    if isinstance(operands, list) and len(operands) > 0:
                        extract_from_operand(operands[0])

        def extract_from_operand(op: Any) -> None:
            if isinstance(op, str) and op.startswith("$"):
                # Field reference - no parameter
                pass
            elif isinstance(op, dict):
                # Nested expression
                extract_from_expr(op)
            else:
                # Literal value - parameter!
                values.append(op)

        extract_from_expr(expr)
        return values

    def _build_tier2_query(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        filter_expr: Dict[str, Any] | None = None,
    ) -> Tuple[str, List[Any], List[str]]:
        """
        Build a Tier 2 query using temporary tables.

        Strategy:
        1. Create temp table with document IDs and intermediate calculations
        2. Populate temp table with JSON-extracted values
        3. Query main table joined with temp table using calculated values

        Args:
            expr: The $expr expression
            collection_name: Collection table name
            filter_expr: Optional additional filter

        Returns:
            Tuple of (SQL query, parameters, table_names)
        """
        # Generate unique temp table name
        temp_table = f"temp_expr_{uuid.uuid4().hex[:8]}"
        self._temp_tables.append(temp_table)

        # Extract all field references from expression
        fields = self._extract_field_references(expr)

        # Create temp table to store intermediate results
        self._create_temp_table(temp_table, collection_name, fields)

        # Build the main query
        sql, params = self._build_main_query(
            expr, collection_name, temp_table, filter_expr
        )
        return sql, params, [temp_table]

    def _create_temp_table(
        self, temp_table: str, collection_name: str, fields: List[str]
    ) -> None:
        """
        Create a temporary table with extracted field values.

        Args:
            temp_table: Name of the temporary table
            collection_name: Source collection name
            fields: List of field paths to extract
        """
        # Build column definitions for extracted fields
        select_exprs = ["id"]

        # Check if _id column exists
        try:
            self.db.execute(f"SELECT _id FROM {collection_name} LIMIT 0")
            has_id = True
        except sqlite3.OperationalError as e:
            logger.debug(
                f"Collection '{collection_name}' does not have _id column: {e}"
            )
            has_id = False

        if has_id:
            select_exprs.append("_id")

        for field in fields:
            # Sanitize field name for SQL column
            col_name = self._sanitize_field_name(field)

            # Build JSON extract expression
            if self._jsonb_supported:
                extract_expr = f"jsonb_extract({self.data_column}, '{parse_json_path(field)}') as {col_name}"
            else:
                extract_expr = f"json_extract({self.data_column}, '{parse_json_path(field)}') as {col_name}"

            select_exprs.append(extract_expr)

        # Create temp table with SELECT (faster than CREATE + INSERT)
        select_sql = f"SELECT {', '.join(select_exprs)} FROM {collection_name}"
        create_sql = (
            f"CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} AS {select_sql}"
        )

        self.db.execute(create_sql)

        # Create index on temp table for faster joins
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{temp_table}_id ON {temp_table}(id)"
        )

    def _build_main_query(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        temp_table: str,
        filter_expr: Dict[str, Any] | None = None,
    ) -> Tuple[str, List[Any]]:
        """
        Build the main query that uses the temporary table.

        Args:
            expr: The $expr expression
            collection_name: Collection table name
            temp_table: Temporary table name
            filter_expr: Optional additional filter

        Returns:
            Tuple of (SQL query, parameters)
        """
        # Build WHERE clause from expression using temp table columns
        where_clause, params = self._build_expr_where_from_temp(
            expr, temp_table
        )

        # Build SELECT with json() conversion for Python-space data
        if self._jsonb_supported:
            # Use json() to convert jsonb to regular JSON for Python space
            select_data = f"json({collection_name}.{self.data_column}) as data"
        else:
            select_data = f"{collection_name}.data as data"

        # Build the final query
        query = f"""
            SELECT {temp_table}.id, {temp_table}._id, {select_data}
            FROM {collection_name}
            JOIN {temp_table} ON {collection_name}.id = {temp_table}.id
            WHERE {where_clause}
        """

        return query.strip(), params

    def _build_expr_where_from_temp(self, expr: Dict[str, Any], temp_table: str) -> Tuple[str, List[Any]]:
        """
        Build WHERE clause using temporary table columns.

        Args:
            expr: The $expr expression
            temp_table: Temporary table name

        Returns:
            Tuple of (WHERE clause, parameters)
        """
        sql, params = self._convert_expr_to_temp_sql(expr, temp_table)
        
        # MongoDB $expr truthiness: NOT (null, 0, false, undefined).
        # In SQLite, we use COALESCE and != 0 to return 1 for truthy and 0 for falsy.
        truthy_sql = f"COALESCE(({sql}), 0) != 0"
        return truthy_sql, params

    def _convert_expr_to_temp_sql(
        self, expr: Dict[str, Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """
        Convert expression to SQL using temporary table columns.

        Args:
            expr: The $expr expression
            temp_table: Temporary table name

        Returns:
            Tuple of (SQL expression, parameters)
        """
        if not isinstance(expr, dict) or len(expr) != 1:
            raise ValueError("Invalid $expr expression structure")

        operator, operands = next(iter(expr.items()))

        # Handle different operator types
        match operator:
            case "$and" | "$or" | "$not" | "$nor":
                return self._convert_logical_to_temp_sql(
                    operator, operands, temp_table
                )
            case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne":
                return self._convert_comparison_to_temp_sql(
                    operator, operands, temp_table
                )
            case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                return self._convert_arithmetic_to_temp_sql(
                    operator, operands, temp_table
                )
            case "$cond":
                return self._convert_cond_to_temp_sql(operands, temp_table)
            case "$cmp":
                return self._convert_cmp_to_temp_sql(operands, temp_table)
            case "$abs" | "$ceil" | "$floor" | "$round":
                return self._convert_math_to_temp_sql(
                    operator, operands, temp_table
                )
            case _:
                raise NotImplementedError(
                    f"Operator {operator} not supported in Tier 2"
                )

    def _convert_logical_to_temp_sql(
        self, operator: str, operands: List[Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert logical operators to SQL using temp table."""
        if not isinstance(operands, list):
            raise ValueError(f"{operator} requires a list of expressions")

        if operator == "$not":
            if len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            inner_sql, inner_params = self._convert_expr_to_temp_sql(
                operands[0], temp_table
            )
            return f"NOT ({inner_sql})", inner_params

        if len(operands) < 2:
            raise ValueError(f"{operator} requires at least 2 operands")

        sql_parts = []
        all_params = []

        for operand in operands:
            operand_sql, operand_params = self._convert_expr_to_temp_sql(
                operand, temp_table
            )
            sql_parts.append(f"({operand_sql})")
            all_params.extend(operand_params)

        match operator:
            case "$and":
                sql = " AND ".join(sql_parts)
            case "$or":
                sql = " OR ".join(sql_parts)
            case "$nor":
                sql = f"NOT ({' OR '.join(sql_parts)})"
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

        return sql, all_params

    def _convert_comparison_to_temp_sql(
        self, operator: str, operands: List[Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert comparison operators to SQL using temp table."""
        if len(operands) != 2:
            raise ValueError(f"{operator} requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_temp_sql(
            operands[0], temp_table
        )
        right_sql, right_params = self._convert_operand_to_temp_sql(
            operands[1], temp_table
        )

        op_mapping = {
            "$eq": "=",
            "$gt": ">",
            "$gte": ">=",
            "$lt": "<",
            "$lte": "<=",
            "$ne": "!=",
        }
        sql_operator = op_mapping.get(operator, operator)

        return (
            f"{left_sql} {sql_operator} {right_sql}",
            left_params + right_params,
        )

    def _convert_arithmetic_to_temp_sql(
        self, operator: str, operands: List[Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert arithmetic operators to SQL using temp table."""
        if len(operands) < 2:
            raise ValueError(f"{operator} requires at least 2 operands")

        sql_parts = []
        all_params = []

        for operand in operands:
            operand_sql, operand_params = self._convert_operand_to_temp_sql(
                operand, temp_table
            )
            sql_parts.append(operand_sql)
            all_params.extend(operand_params)

        op_mapping = {
            "$add": "+",
            "$subtract": "-",
            "$multiply": "*",
            "$divide": "/",
            "$mod": "%",
        }
        sql_operator = op_mapping.get(operator, operator)
        sql = f"({f' {sql_operator} '.join(sql_parts)})"

        return sql, all_params

    def _convert_cond_to_temp_sql(
        self, operands: Dict[str, Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert $cond operator to SQL CASE statement using temp table."""
        if not isinstance(operands, dict):
            raise ValueError("$cond requires a dictionary")

        if "if" not in operands or "then" not in operands:
            raise ValueError("$cond requires 'if' and 'then' fields")

        condition_sql, condition_params = self._convert_expr_to_temp_sql(
            operands["if"], temp_table
        )
        then_sql, then_params = self._convert_operand_to_temp_sql(
            operands["then"], temp_table
        )

        if "else" in operands:
            else_sql, else_params = self._convert_operand_to_temp_sql(
                operands["else"], temp_table
            )
        else:
            else_sql, else_params = "NULL", []

        sql = f"CASE WHEN {condition_sql} THEN {then_sql} ELSE {else_sql} END"

        return sql, condition_params + then_params + else_params

    def _convert_cmp_to_temp_sql(
        self, operands: List[Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert $cmp operator to SQL CASE statement using temp table."""
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_temp_sql(
            operands[0], temp_table
        )
        right_sql, right_params = self._convert_operand_to_temp_sql(
            operands[1], temp_table
        )

        sql = f"(CASE WHEN {left_sql} < {right_sql} THEN -1 WHEN {left_sql} > {right_sql} THEN 1 ELSE 0 END)"
        return sql, left_params + right_params

    def _convert_math_to_temp_sql(
        self, operator: str, operands: List[Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """Convert math operators to SQL using temp table."""
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_temp_sql(
            operands[0], temp_table
        )

        op_mapping = {
            "$abs": "abs",
            "$ceil": "ceil",
            "$floor": "floor",
            "$round": "round",
        }
        sql_func = op_mapping.get(operator)

        if sql_func is None:
            raise NotImplementedError(
                f"Math operator {operator} not supported in Tier 2"
            )

        return f"{sql_func}({value_sql})", value_params

    def _convert_operand_to_temp_sql(
        self, operand: Any, temp_table: str
    ) -> Tuple[str, List[Any]]:
        """
        Convert operand to SQL using temporary table columns.

        Args:
            operand: Operand to convert
            temp_table: Temporary table name

        Returns:
            Tuple of (SQL expression, parameters)
        """
        if isinstance(operand, str) and operand.startswith("$"):
            # Field reference - use temp table column
            field_path = operand[1:]  # Remove $
            col_name = self._sanitize_field_name(field_path)
            return f"{temp_table}.{col_name}", []

        elif isinstance(operand, dict):
            # Nested expression
            return self._convert_expr_to_temp_sql(operand, temp_table)

        else:
            # Literal value
            return "?", [operand]

    def _extract_field_references(self, expr: Dict[str, Any]) -> List[str]:
        """
        Extract all unique field references from an expression.

        Args:
            expr: The $expr expression

        Returns:
            List of unique field paths
        """
        fields = []

        if isinstance(expr, dict):
            for operator, operands in expr.items():
                if operator.startswith("$"):
                    fields.extend(
                        self._extract_field_references_from_operand(operands)
                    )

        # Return unique fields while preserving order
        unique_fields = []
        seen = set()
        for f in fields:
            if f not in seen:
                unique_fields.append(f)
                seen.add(f)
        return unique_fields

    def _extract_field_references_from_operand(self, operand: Any) -> List[str]:
        """Extract field references from an operand."""
        fields = []

        if isinstance(operand, str) and operand.startswith("$"):
            fields.append(operand[1:])
        elif isinstance(operand, list):
            for item in operand:
                fields.extend(self._extract_field_references_from_operand(item))
        elif isinstance(operand, dict):
            for op, val in operand.items():
                fields.extend(self._extract_field_references_from_operand(val))

        return fields

    def _sanitize_field_name(self, field_path: str) -> str:
        """
        Sanitize a field path for use as a SQL column name.

        Args:
            field_path: Field path (e.g., "stats.wins")

        Returns:
            Sanitized column name (e.g., "stats_wins")
        """
        # Replace dots with underscores
        col_name = field_path.replace(".", "_")
        # Replace brackets (for array indices)
        col_name = col_name.replace("[", "_").replace("]", "")
        # Ensure valid SQL identifier
        if col_name and not col_name[0].isalpha():
            col_name = "f_" + col_name
        return col_name

    def cleanup_temp_tables(self) -> None:
        """Clean up all temporary tables created by this evaluator."""
        for table in self._temp_tables:
            try:
                self.db.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary table {table} during cleanup: {e}"
                )
                pass  # Ignore cleanup errors
        self._temp_tables.clear()
