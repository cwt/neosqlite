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
import sqlite3
import uuid
from typing import Any, Dict, List, Tuple, Optional
from .jsonb_support import supports_jsonb


class TempTableExprEvaluator:
    """
    Tier 2 evaluator using temporary tables for complex $expr expressions.

    This evaluator:
    1. Creates temporary tables to store intermediate results
    2. Uses SQLite JSON/JSONB functions for efficient processing
    3. Converts to JSON format when exporting to Python space
    """

    def __init__(self, db_connection, data_column: str = "data"):
        """
        Initialize the temporary table evaluator.

        Args:
            db_connection: SQLite database connection
            data_column: Name of the column containing JSON data (default: "data")
        """
        self.db = db_connection
        self.data_column = data_column
        self._jsonb_supported = supports_jsonb(db_connection)
        self._temp_tables: List[str] = []

    @property
    def json_function_prefix(self) -> str:
        """Get the appropriate JSON function prefix (json or jsonb)."""
        return "jsonb" if self._jsonb_supported else "json"

    def evaluate(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        filter_expr: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], List[Any]]:
        """
        Evaluate a $expr expression using temporary tables.

        Args:
            expr: The $expr expression dictionary
            collection_name: Name of the collection table
            filter_expr: Optional additional filter to apply

        Returns:
            Tuple of (SELECT query with temp tables, parameters) or (None, []) for Python fallback
        """
        try:
            # Analyze expression complexity
            complexity = self._analyze_complexity(expr)

            # If too simple for Tier 2, let Tier 1 handle it
            if complexity < 2:
                return None, []

            # If too complex for Tier 2, fall back to Tier 3 (Python)
            if complexity > 10:
                return None, []

            # Build the evaluation query
            return self._build_tier2_query(expr, collection_name, filter_expr)

        except (NotImplementedError, ValueError):
            # Fall back to Python evaluation
            return None, []
        finally:
            # Clean up temporary tables after use
            self._cleanup_temp_tables()

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
            if operator in (
                "$add",
                "$subtract",
                "$multiply",
                "$divide",
                "$mod",
            ):
                score += 1
                if isinstance(operands, list):
                    for op in operands:
                        if isinstance(op, dict):
                            score += self._analyze_complexity(op)
            elif operator in ("$cond", "$switch"):
                score += 2
                if isinstance(operands, dict):
                    if "if" in operands:
                        score += self._analyze_complexity(operands["if"])
                    if "then" in operands:
                        if isinstance(operands["then"], dict):
                            score += self._analyze_complexity(operands["then"])
            elif operator in ("$and", "$or", "$nor"):
                if isinstance(operands, list):
                    for op in operands:
                        if isinstance(op, dict):
                            score += self._analyze_complexity(op)
            elif operator == "$not":
                if isinstance(operands, list) and len(operands) > 0:
                    if isinstance(operands[0], dict):
                        score += self._analyze_complexity(operands[0])
            elif operator in ("$size", "$in", "$arrayElemAt"):
                score += 2
            elif operator in ("$concat", "$substr", "$trim"):
                score += 1

        return score

    def _build_tier2_query(
        self,
        expr: Dict[str, Any],
        collection_name: str,
        filter_expr: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[Any]]:
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
            Tuple of (SQL query, parameters)
        """
        # Generate unique temp table name
        temp_table = f"temp_expr_{uuid.uuid4().hex[:8]}"
        self._temp_tables.append(temp_table)

        # Extract all field references from expression
        fields = self._extract_field_references(expr)

        # Create temp table to store intermediate results
        self._create_temp_table(temp_table, collection_name, fields)

        # Build the main query
        return self._build_main_query(
            expr, collection_name, temp_table, filter_expr
        )

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
        except sqlite3.OperationalError:
            has_id = False

        if has_id:
            select_exprs.append("_id")

        for field in fields:
            # Sanitize field name for SQL column
            col_name = self._sanitize_field_name(field)

            # Build JSON extract expression
            if self._jsonb_supported:
                extract_expr = f"jsonb_extract({self.data_column}, '$.{field}') as {col_name}"
            else:
                extract_expr = f"json_extract({self.data_column}, '$.{field}') as {col_name}"

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
        filter_expr: Optional[Dict[str, Any]] = None,
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
            select_data = f"json({temp_table}.{self.data_column}) as data"
        else:
            select_data = f"{temp_table}.data as data"

        # Build the final query
        query = f"""
            SELECT {temp_table}.id, {temp_table}._id, {select_data}
            FROM {collection_name}
            JOIN {temp_table} ON {collection_name}.id = {temp_table}.id
            WHERE {where_clause}
        """

        return query.strip(), params

    def _build_expr_where_from_temp(
        self, expr: Dict[str, Any], temp_table: str
    ) -> Tuple[str, List[Any]]:
        """
        Build WHERE clause using temporary table columns.

        Args:
            expr: The $expr expression
            temp_table: Temporary table name

        Returns:
            Tuple of (WHERE clause, parameters)
        """
        sql, params = self._convert_expr_to_temp_sql(expr, temp_table)
        return sql, params

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
        if operator in ("$and", "$or", "$not", "$nor"):
            return self._convert_logical_to_temp_sql(
                operator, operands, temp_table
            )
        elif operator in ("$gt", "$gte", "$lt", "$lte", "$eq", "$ne"):
            return self._convert_comparison_to_temp_sql(
                operator, operands, temp_table
            )
        elif operator in ("$add", "$subtract", "$multiply", "$divide", "$mod"):
            return self._convert_arithmetic_to_temp_sql(
                operator, operands, temp_table
            )
        elif operator == "$cond":
            return self._convert_cond_to_temp_sql(operands, temp_table)
        elif operator == "$cmp":
            return self._convert_cmp_to_temp_sql(operands, temp_table)
        elif operator in ("$abs", "$ceil", "$floor", "$round"):
            return self._convert_math_to_temp_sql(
                operator, operands, temp_table
            )
        else:
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

        if operator == "$and":
            sql = " AND ".join(sql_parts)
        elif operator == "$or":
            sql = " OR ".join(sql_parts)
        elif operator == "$nor":
            sql = f"NOT ({' OR '.join(sql_parts)})"
        else:
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
        Extract all field references from an expression.

        Args:
            expr: The $expr expression

        Returns:
            List of field paths
        """
        fields = []

        if isinstance(expr, dict):
            for operator, operands in expr.items():
                if operator.startswith("$"):
                    fields.extend(
                        self._extract_field_references_from_operand(operands)
                    )

        return fields

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

    def _cleanup_temp_tables(self) -> None:
        """Clean up all temporary tables."""
        for table_name in self._temp_tables:
            try:
                self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass  # Ignore cleanup errors
        self._temp_tables.clear()
