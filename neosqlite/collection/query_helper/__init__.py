from ..jsonb_support import (
    supports_jsonb,
    supports_jsonb_each,
    _get_json_each_function,
)
from typing import Any, Dict, List

# Import sqlite3 for type hints and potential direct usage
from ..._sqlite import sqlite3 as sqlite3  # noqa: F401

# Import helper functions
from .helpers import (
    _get_integer_id_for_oid,
    _get_json_error_position,
    _validate_json_document,
)

# Import the centralized ID normalization function
from ..type_correction import normalize_id_query_for_db

# Import utility functions
from .utils import (
    _convert_bytes_to_binary as _convert_bytes_to_binary,
    _get_json_function as _get_json_function,
    _get_json_function_prefix,
    _is_numeric_value as _is_numeric_value,
    _validate_inc_mul_field_value as _validate_inc_mul_field_value,
    get_force_fallback as get_force_fallback,
    set_force_fallback as set_force_fallback,
)

# Import mixin modules
from .crud_operations import CRUDOperationsMixin
from .update_operations import UpdateOperationsMixin
from .query_builder import QueryBuilderMixin
from .aggregation import AggregationMixin
from .query_optimizer import QueryOptimizerMixin


class QueryHelper(
    CRUDOperationsMixin,
    UpdateOperationsMixin,
    QueryBuilderMixin,
    AggregationMixin,
    QueryOptimizerMixin,
):
    """
    A helper class for the QueryEngine that provides methods for building queries,
    performing updates, and processing aggregation pipelines.

    This class contains the core logic for translating MongoDB-like queries and
    operations into SQL statements that can be executed against the SQLite database.
    It handles both simple operations that can be done directly with SQL JSON
    functions and complex operations that require Python-based processing.

    The class is composed of several mixins:
    - CRUDOperationsMixin: Insert, replace, delete operations
    - UpdateOperationsMixin: Update operations (SQL and Python-based)
    - QueryBuilderMixin: WHERE clause building and query application
    - AggregationMixin: Aggregation pipeline processing
    - QueryOptimizerMixin: Query optimization and cost estimation
    """

    def __init__(self, collection):
        """
        Initialize the QueryHelper with a collection.

        Args:
            collection: The collection instance this QueryHelper will operate on.
        """
        self.collection = collection
        # Access debug flag from the database connection if available
        self.debug = (
            getattr(collection.database, "debug", False)
            if hasattr(collection, "database")
            else False
        )
        # Check if JSONB is supported for this connection
        self._jsonb_supported = supports_jsonb(collection.db)
        # Check if jsonb_each is supported (requires SQLite 3.51.0+)
        self._jsonb_each_supported = supports_jsonb_each(collection.db)
        # Cache the function prefix for performance
        self._json_function_prefix = _get_json_function_prefix(
            self._jsonb_supported
        )
        # Cache the correct json_each function name
        self._json_each_function = _get_json_each_function(
            self._jsonb_supported, self._jsonb_each_supported
        )

    def _normalize_id_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize ID types in a query dictionary to correct common mismatches.

        This method delegates to the centralized normalize_id_query_for_db function
        to ensure consistent ID handling across all NeoSQLite components.

        Args:
            query: The query dictionary to process

        Returns:
            A new query dictionary with corrected ID types
        """
        return normalize_id_query_for_db(query)

    def _get_integer_id_for_oid(self, oid: Any) -> int:
        """
        Get the integer ID for a given ObjectId or other ID type.

        Args:
            oid: The ID value (can be ObjectId, int, str, etc.)

        Returns:
            int: The integer ID from the database
        """
        return _get_integer_id_for_oid(self.collection, oid)

    def _validate_json_document(self, json_str: str) -> bool:
        """
        Validate JSON document using SQLite's json_valid function.

        Args:
            json_str: The JSON string to validate

        Returns:
            bool: True if valid, False otherwise
        """
        return _validate_json_document(self.collection.db, json_str)

    def _get_json_error_position(self, json_str: str) -> int:
        """
        Get position of JSON error using json_error_position().

        Args:
            json_str: The JSON string to check

        Returns:
            int: Position of error, or -1 if valid/not supported
        """
        return _get_json_error_position(self.collection.db, json_str)

    def _build_expr_where_clause(
        self, query: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """
        Build a SQL WHERE clause for $expr queries using the 3-tier approach.
        Also handles other query fields combined with $expr.

        Tier Selection Logic:
        - Tier 1 (Simple): Direct SQL WHERE with json_extract/jsonb_extract
        - Tier 2 (Complex): Temporary tables with pre-computed field extractions
        - Tier 3 (Fallback): Python evaluation for unsupported operations

        Args:
            query: Query dictionary containing $expr and potentially other fields

        Returns:
            Tuple of (SQL WHERE clause, parameters) or None for Python fallback
        """
        if "$expr" not in query:
            return None

        expr = query["$expr"]
        if not isinstance(expr, dict):
            return None

        # Import here to avoid circular imports
        from ..expr_evaluator import ExprEvaluator
        from ..expr_temp_table import TempTableExprEvaluator

        # Create evaluator instances with database connection for JSONB support detection
        data_column = (
            self.collection.query_engine._data_column
            if hasattr(self.collection.query_engine, "_data_column")
            else "data"
        )
        tier1_evaluator = ExprEvaluator(data_column, self.collection.db)

        # Determine complexity tier based on expression analysis
        tier = self._analyze_expr_complexity(expr)

        # Check for force fallback (kill switch)
        force_python = get_force_fallback()

        # Tier selection with kill switch awareness
        if force_python or tier >= 3:
            # Tier 3: Python fallback (kill switch or too complex)
            return None

        elif tier == 2:
            # Tier 2: Try temporary tables approach
            tier2_evaluator = TempTableExprEvaluator(
                self.collection.db,
                (
                    self.collection.query_engine._data_column
                    if hasattr(self.collection.query_engine, "_data_column")
                    else "data"
                ),
            )

            # Build full query with temp tables
            other_fields_result = self._build_other_fields_clause(query, expr)
            if other_fields_result is None:
                # Can't handle other fields in SQL, fall back to Python
                return None
            other_fields_clause, other_params = other_fields_result

            if other_fields_clause is None:
                # Can't handle other fields in SQL, fall back to Python
                return None

            # Get the main query from Tier 2 evaluator
            tier2_result = tier2_evaluator.evaluate(
                expr,
                self.collection.name,
                None,  # Filter expr not used yet
            )
            if tier2_result[0] is None:
                # Tier 2 failed, fall back to Tier 1
                pass
            else:
                main_query, params = tier2_result
                # Success - return the full query
                # Note: Tier 2 returns a full SELECT query, not just WHERE clause
                # This needs special handling in the cursor
                # For now, fall back to returning None to use Python evaluation
                # TODO: Implement proper cursor support for Tier 2 queries
                pass

            # If Tier 2 fails, fall back to Tier 1
            sql_expr, params = tier1_evaluator.evaluate(
                expr, tier=1, force_python=False
            )
            if sql_expr is None:
                return None

            # Build WHERE clause with other fields
            return self._combine_expr_with_other_fields(
                sql_expr, params, query, expr
            )

        else:
            # Tier 1: Direct SQL evaluation
            sql_expr, params = tier1_evaluator.evaluate(
                expr, tier=1, force_python=False
            )

            if sql_expr is None:
                # Python fallback - return None to force Python filtering
                return None

            # Build WHERE clause with other fields
            return self._combine_expr_with_other_fields(
                sql_expr, params, query, expr
            )

    def _analyze_expr_complexity(self, expr: Dict[str, Any]) -> int:
        """
        Analyze expression complexity to determine appropriate tier.

        Complexity scoring:
        - Base expression: 1 point
        - Each nested operator: +1 point
        - Arithmetic operators: +1 point each
        - Conditional operators: +2 points each
        - Array operators: +2 points each
        - Type conversion: +1 point each

        Tier thresholds:
        - 1-2: Tier 1 (simple SQL WHERE)
        - 3-8: Tier 2 (temporary tables)
        - 9+: Tier 3 (Python fallback)

        Args:
            expr: The $expr expression

        Returns:
            int: Complexity score
        """
        if not isinstance(expr, dict) or len(expr) != 1:
            return 0

        score = 1  # Base score

        for operator, operands in expr.items():
            # Recurse into operands for all operators
            if isinstance(operands, list):
                for op in operands:
                    if isinstance(op, dict):
                        score += self._analyze_expr_complexity(op)
            elif isinstance(operands, dict):
                score += self._analyze_expr_complexity(operands)

            # Add operator-specific complexity
            match operator:
                case "$add" | "$subtract" | "$multiply" | "$divide" | "$mod":
                    score += 1
                case "$cond" | "$switch":
                    score += 2
                case "$size" | "$in" | "$arrayElemAt" | "$first" | "$last":
                    score += 2
                case "$filter" | "$map" | "$reduce":
                    # Array transformation operators are complex
                    score += 3
                case "$concat" | "$toLower" | "$toUpper" | "$substr":
                    score += 1
                case "$abs" | "$ceil" | "$floor" | "$round" | "$trunc":
                    score += 1
                case "$dateAdd" | "$dateSubtract" | "$dateDiff":
                    score += 1
                case "$regexFind" | "$regexFindAll":
                    # Regex operations require Python evaluation
                    score += 2
                case "$cmp":
                    score += 1
                case "$ifNull" | "$type" | "$toString" | "$toInt":
                    score += 1
                # Comparison and logical operators don't add extra complexity
                # (their complexity comes from their operands which are already counted)

        return score

    def _combine_expr_with_other_fields(
        self,
        sql_expr: str,
        params: List[Any],
        query: Dict[str, Any],
        expr: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Combine $expr SQL with other query fields.

        Args:
            sql_expr: The $expr SQL expression
            params: SQL parameters
            query: Full query dictionary
            expr: The $expr expression

        Returns:
            Tuple of (WHERE clause, parameters) or None for Python fallback
        """
        # Build WHERE clause starting with $expr
        where_parts = [f"({sql_expr})"]
        all_params: List[Any] = list(params)

        # Process other query fields (excluding $expr)
        for field, value in query.items():
            if field == "$expr":
                continue

            # Handle logical operators - fall back to Python
            if field in ("$and", "$or", "$nor", "$not"):
                return None

            # Build clause for regular fields
            field_result = self._build_field_clause(field, value)
            if field_result is None:
                # If any field can't be handled in SQL, fall back to Python
                return None
            field_clause, field_params = field_result
            where_parts.append(field_clause)
            all_params.extend(field_params)

        return f"WHERE {' AND '.join(where_parts)}", all_params
