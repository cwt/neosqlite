from typing import Any

# Import sqlite3 for type hints and potential direct usage
from ..._sqlite import sqlite3 as sqlite3  # noqa: F401
from ..jsonb_support import (
    _get_json_each_function,
    supports_jsonb,
    supports_jsonb_each,
)

# Import the centralized ID normalization function
from ..type_correction import normalize_id_query_for_db
from .aggregation import AggregationMixin

# Import mixin modules
from .crud_operations import CRUDOperationsMixin

# Import helper functions
from .helpers import (
    _get_integer_id_for_oid,
    _get_json_error_position,
    _validate_json_document,
)
from .query_builder import QueryBuilderMixin
from .query_optimizer import QueryOptimizerMixin
from .translation_cache import TranslationCache  # noqa: F401
from .update_operations import UpdateOperationsMixin

# Import utility functions
from .utils import (
    _convert_bytes_to_binary as _convert_bytes_to_binary,
)
from .utils import (
    _get_json_function as _get_json_function,
)
from .utils import (
    _get_json_function_prefix,
)
from .utils import (
    _is_numeric_value as _is_numeric_value,
)
from .utils import (
    _supports_returning_clause as _supports_returning_clause,
)
from .utils import (
    _validate_inc_mul_field_value as _validate_inc_mul_field_value,
)
from .utils import (
    get_force_fallback as get_force_fallback,
)
from .utils import (
    set_force_fallback as set_force_fallback,
)


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
        # Initialize Tier-2 evaluator for complex $expr queries
        # Import here to avoid circular imports
        from ..expr_temp_table import TempTableExprEvaluator

        # Get cache size from database connection (defaults to 100)
        cache_size = (
            getattr(collection.database, "_translation_cache_size", 100)
            if hasattr(collection, "database")
            else 100
        )
        self.tier2_evaluator = TempTableExprEvaluator(
            collection.db,
            data_column=(
                collection.query_engine._data_column
                if hasattr(collection, "query_engine")
                and hasattr(collection.query_engine, "_data_column")
                else "data"
            ),
            translation_cache_size=cache_size,
        )

    def cleanup(self) -> None:
        """Clean up resources used by the QueryHelper."""
        if hasattr(self, "tier2_evaluator"):
            self.tier2_evaluator.cleanup_temp_tables()

    def _normalize_id_query(self, query: dict[str, Any]) -> dict[str, Any]:
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
        self, query: dict[str, Any]
    ) -> tuple[str, list[Any], list[str]] | None:
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
            Tuple of (SQL WHERE clause, parameters, tables) or None for Python fallback
        """
        if "$expr" not in query:
            return None

        expr = query["$expr"]
        if not isinstance(expr, dict):
            return None

        # Import here to avoid circular imports
        from ..expr_evaluator import ExprEvaluator

        # Create evaluator instance for Tier 1
        tier1_evaluator = ExprEvaluator(
            (
                self.collection.query_engine._data_column
                if hasattr(self.collection.query_engine, "_data_column")
                else "data"
            ),
            self.collection.db,
        )

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
            # Get the main query from Tier 2 evaluator
            tier2_result = self.tier2_evaluator.evaluate(
                expr,
                self.collection.name,
                None,  # Filter expr not used yet
            )
            if tier2_result[0] is not None:
                # Success - return the full query with cleanup tables
                return tier2_result

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

    def _build_other_fields_clause(
        self, query: dict[str, Any], expr: dict[str, Any]
    ) -> tuple[str, list[Any]] | None:
        """Helper to build WHERE clause for non-$expr fields."""
        where_parts = []
        all_params = []
        for field, value in query.items():
            if field == "$expr":
                continue
            if field in ("$and", "$or", "$nor", "$not"):
                return None
            field_result = self._build_field_clause(field, value)
            if field_result is None:
                return None
            field_clause, field_params = field_result
            where_parts.append(field_clause)
            all_params.extend(field_params)

        if not where_parts:
            return "", []
        return " AND ".join(where_parts), all_params

    def _analyze_expr_complexity(self, expr: dict[str, Any]) -> int:
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
        params: list[Any],
        query: dict[str, Any],
        expr: dict[str, Any],
    ) -> tuple[str, list[Any], list[str]] | None:
        """
        Combine $expr SQL with other query fields.

        Args:
            sql_expr: The $expr SQL expression
            params: SQL parameters
            query: Full query dictionary
            expr: The $expr expression

        Returns:
            Tuple of (WHERE clause, parameters, tables) or None for Python fallback
        """
        # Build WHERE clause starting with $expr
        # MongoDB $expr truthiness: NOT (null, 0, false, undefined).
        # In SQLite, we use COALESCE and != 0 to return 1 for truthy and 0 for falsy.
        # This handles strings, numbers, and nulls correctly while evaluating once.
        truthy_expr = f"COALESCE(({sql_expr}), 0) != 0"
        where_parts = [f"({truthy_expr})"]
        all_params: list[Any] = list(params)

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

        return f"WHERE {' AND '.join(where_parts)}", all_params, []
