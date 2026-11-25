"""Tier 2 temporary table-based expression evaluation for ``$expr`` queries.

Implements the second tier of the 3-tier ``$expr`` evaluation architecture:

- Creates temporary tables for complex expressions that benefit from
  materializing extracted JSON field values.
- Leverages SQLite JSON/JSONB native functions for query execution.
- Converts JSONB data back to JSON text when returning rows to Python.

Tier 2 handles expressions whose complexity score falls between 2 and 10 and
that do not trigger Tier 2 semantic-hazard checks.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

from .._sqlite import sqlite3
from ..sql_utils import quote_identifier, quote_table_name
from .json_path_utils import parse_json_path
from .jsonb_support import JSONBContext
from .query_helper.translation_cache import TranslationCache


class TempTableExprEvaluator:
    """Tier 2 evaluator using temporary tables for complex ``$expr`` queries.

    Materializes referenced document fields into a SQLite temporary table,
    evaluates the expression against those columns using SQLite JSON/JSONB
    functions, and caches compiled ``WHERE`` templates when no extra filter
    expression is supplied.
    """

    def __init__(
        self,
        db_connection,
        data_column: str = "data",
        translation_cache_size: int | None = 100,
    ):
        """Initialize the temporary table evaluator.

        Args:
            db_connection: SQLite database connection.
            data_column: Name of the column containing JSON data. Defaults to
                ``"data"``.
            translation_cache_size: Maximum number of entries in the
                translation cache. ``None`` defaults to 100; ``0`` disables
                caching.
        """
        self.db = db_connection
        self.data_column = data_column
        self.jsonb = JSONBContext.from_db(db_connection)
        self._temp_tables: list[str] = []
        if translation_cache_size is None:
            translation_cache_size = 100
        self._translation_cache = TranslationCache(
            max_size=translation_cache_size
        )

    @property
    def json_function_prefix(self) -> str:
        """Get the active JSON function prefix for the connection.

        Returns:
            str: ``"jsonb"`` when JSONB is supported, otherwise ``"json"``.
        """
        return self.jsonb.json_function_prefix

    def is_cache_enabled(self) -> bool:
        """Check whether the expression translation cache is enabled.

        Returns:
            bool: ``True`` if the cache maximum size is greater than zero.
        """
        return self._translation_cache.is_enabled()

    def get_cache_stats(self) -> dict:
        """Get translation cache statistics and entry metadata.

        Returns:
            dict: Dictionary containing cache size, hits, misses, hit rate,
                and entry previews.
        """
        return self._translation_cache.get_stats()

    def clear_cache(self) -> None:
        """Clear the translation cache."""
        self._translation_cache.clear()

    def cache_size(self) -> int:
        """Get the number of entries currently stored in the cache.

        Returns:
            int: Current translation cache entry count.
        """
        return len(self._translation_cache)

    def cache_contains(self, expr: dict) -> bool:
        """Check whether a compiled translation for ``expr`` is cached.

        Args:
            expr: ``$expr`` expression dictionary to look up.

        Returns:
            bool: ``True`` if the expression key exists in the cache.
        """
        key = self._make_expr_key(expr)
        return self._translation_cache.contains(key)

    def evict_from_cache(self, expr: dict) -> bool:
        """Evict a cached translation for ``expr`` if present.

        Args:
            expr: ``$expr`` expression dictionary whose entry to remove.

        Returns:
            bool: ``True`` if an entry was evicted, ``False`` otherwise.
        """
        key = self._make_expr_key(expr)
        return self._translation_cache.evict(key)

    def resize_cache(self, new_size: int) -> None:
        """Resize the translation cache, evicting oldest entries if needed.

        Args:
            new_size: New maximum entry capacity for the cache.
        """
        self._translation_cache.resize(new_size)

    def dump_cache(self) -> list:
        """Dump cached entries sorted by hit count for debugging.

        Returns:
            list: List of dictionaries describing cached entries.
        """
        return self._translation_cache.dump()

    def evaluate(
        self,
        expr: dict[str, Any],
        collection_name: str,
        filter_expr: dict[str, Any] | None = None,
    ) -> tuple[str | None, list[Any], list[str]]:
        """Evaluate a ``$expr`` expression using a temporary table.

        Checks complexity bounds (``2 <= complexity <= 10``) and Tier 2
        semantic hazards before building or reusing a cached SQL query joined
        against a materialized temporary table.

        Args:
            expr: The ``$expr`` expression dictionary.
            collection_name: Name of the target collection table.
            filter_expr: Optional additional equality filter dictionary to
                AND into the ``WHERE`` clause.

        Returns:
            tuple[str | None, list[Any], list[str]]: Tuple containing
                ``(sql_query, params, temp_tables)`` on success, or
                ``(None, [], [])`` when falling back to Tier 1 or Tier 3.
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

            # Reject expressions with Tier-2 semantic mismatches or unsafe temp columns
            # so Tier 1 ExprEvaluator / Tier 3 handles them with full MongoDB parity (#116, #121)
            if self._has_tier2_semantics_hazard(expr):
                return None, [], []

            # Try cache first (only when no extra filter_expr is passed)
            cache_key = self._make_expr_key(expr) if not filter_expr else None
            if cache_key is not None:
                cached = self._translation_cache.get(cache_key)
                if cached is not None:
                    # Build query with cached translation
                    result = self._build_from_cache(
                        expr, collection_name, cached
                    )
                    self._temp_tables.clear()
                    return result

            # Build the evaluation query
            result = self._build_tier2_query(expr, collection_name, filter_expr)
            if result[0] is not None and cache_key is not None:
                # Cache the translation: WHERE clause template plus the exact
                # field list and parameters. Keys canonicalize literal values,
                # so a cache hit implies an identical expression.
                sql_query, params, temp_tables = result
                fields = self._extract_field_references(expr)
                # Extract WHERE clause from the full query for caching
                where_clause = self._extract_where_clause(sql_query)
                # Store with temp_table placeholder for substitution on cache hit
                # FIX: self._temp_tables[-1] already includes the prefix
                where_clause_template = where_clause.replace(
                    quote_table_name(temp_tables[-1]), "{temp_table}"
                )
                self._translation_cache.put(
                    cache_key,
                    where_clause_template,
                    (tuple(fields), tuple(params)),
                )
            self._temp_tables.clear()
            return result

        except (NotImplementedError, ValueError) as e:
            logger.debug(
                f"Tier 2 evaluation failed, falling back to Python: {e}"
            )
            # Drop any temp table created before the failure — previously
            # these accumulated in sqlite_temp_master until connection
            # close (#126)
            self._drop_tracked_tables()
            return None, [], []
        except Exception:
            self._drop_tracked_tables()
            raise

    _SQLITE_RESERVED_COLS = frozenset(
        {
            "id",
            "_id",
            "select",
            "from",
            "where",
            "group",
            "order",
            "by",
            "having",
            "table",
            "index",
            "values",
            "and",
            "or",
            "not",
            "null",
            "case",
            "when",
            "then",
            "else",
            "end",
            "as",
            "on",
            "in",
            "is",
            "join",
            "like",
            "limit",
            "offset",
            "set",
            "update",
            "delete",
            "insert",
            "create",
            "drop",
            "alter",
            "union",
            "distinct",
            "all",
            "exists",
            "between",
            "default",
            "check",
            "unique",
            "primary",
            "key",
            "foreign",
            "references",
            "constraint",
        }
    )

    def _has_tier2_semantics_hazard(self, expr: dict[str, Any]) -> bool:
        """Check if ``expr`` has Tier 2 semantic hazards or unsafe columns.

        Returns ``True`` when Tier 2 temp-table evaluation has semantic
        mismatches (#116, #121), no field references, colliding or reserved
        temp-table column names, or unsupported null/bool/complex comparisons,
        so evaluation delegates to Tier 1 ``ExprEvaluator`` or Tier 3 Python.

        Args:
            expr: The ``$expr`` expression dictionary to inspect.

        Returns:
            bool: ``True`` if Tier 2 evaluation should be skipped.
        """
        try:
            fields = self._extract_field_references(expr)
        except Exception:
            return True

        # Without field references there is nothing to materialize in a temp table;
        # Tier 1 can evaluate constant expressions directly without join overhead.
        if not fields:
            return True

        seen_cols: set[str] = set()
        for field in fields:
            if not field or field.startswith("$"):
                return True
            col_name = self._sanitize_field_name(field)
            if (
                not col_name
                or not col_name.replace("_", "").isalnum()
                or col_name.lower() in self._SQLITE_RESERVED_COLS
                or col_name in seen_cols
            ):
                return True
            seen_cols.add(col_name)

        def _is_safe_nonnull_scalar(val: Any) -> bool:
            """Return whether ``val`` is a non-null, non-bool literal scalar.

            Args:
                val: Operand value to inspect.

            Returns:
                bool: ``True`` if ``val`` is an ``int``, ``float``, or
                    non-``$`` ``str``.
            """
            if val is None or isinstance(val, (bool, dict, list)):
                return False
            if isinstance(val, str) and val.startswith("$"):
                return False
            return isinstance(val, (int, float, str))

        def _walk(node: Any) -> bool:
            """Recursively check AST nodes for Tier 2 semantic hazards.

            Args:
                node: Expression node (dict, list, or scalar) to traverse.

            Returns:
                bool: ``True`` if any hazard is found in ``node``.
            """
            if isinstance(node, list):
                return any(_walk(item) for item in node)
            if not isinstance(node, dict):
                return False
            for op, operands in node.items():
                if op in ("$round", "$ne"):
                    return True
                if op == "$eq" and isinstance(operands, list):
                    if len(operands) != 2:
                        return True
                    left, right = operands
                    if left is None or right is None:
                        return True
                    if isinstance(left, (bool, dict, list)) or isinstance(
                        right, (bool, dict, list)
                    ):
                        return True
                    if not (
                        _is_safe_nonnull_scalar(left)
                        or _is_safe_nonnull_scalar(right)
                    ):
                        return True
                if op in ("$gt", "$gte", "$lt", "$lte", "$cmp") and isinstance(
                    operands, list
                ):
                    for arg in operands:
                        if arg is None or isinstance(arg, (bool, dict, list)):
                            if arg is None or isinstance(arg, bool):
                                return True
                if _walk(operands):
                    return True
            return False

        return _walk(expr)

    def _drop_tracked_tables(self) -> None:
        """Drop and forget every temp table this evaluator created."""
        for table in self._temp_tables:
            try:
                self.db.execute(
                    f"DROP TABLE IF EXISTS {quote_table_name(table)}"
                )
            except Exception as drop_error:  # pragma: no cover
                logger.debug(
                    f"Failed to drop temp table '{table}': {drop_error}"
                )
        self._temp_tables.clear()
        # Cleanup handled by Cursor

    def _analyze_complexity(self, expr: dict[str, Any]) -> int:
        """Analyze expression complexity to choose an evaluation tier.

        Complexity scoring:

        - Non-dict expression: 0 points
        - Base dict expression: 1 point
        - Arithmetic operator (``$add``, ``$subtract``, ``$multiply``,
          ``$divide``, ``$mod``): +1 point plus nested dict operand scores
        - Conditional operator (``$cond``, ``$switch``): +2 points plus
          nested ``if`` and dict ``then`` scores
        - Logical operators (``$and``, ``$or``, ``$nor``, ``$not``): sum of
          nested dict operand scores
        - Array operators (``$size``, ``$in``, ``$arrayElemAt``): +2 points
        - String operators (``$concat``, ``$substr``, ``$trim``): +1 point

        Args:
            expr: The ``$expr`` expression dictionary to score.

        Returns:
            int: Complexity score (<2: Tier 1, 2-10: Tier 2, >10: Tier 3).
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
                case "$gt" | "$gte" | "$lt" | "$lte" | "$eq" | "$ne" | "$cmp":
                    if isinstance(operands, list):
                        for op in operands:
                            if isinstance(op, dict):
                                score += self._analyze_complexity(op)
                case "$not":
                    not_ops = (
                        [operands] if isinstance(operands, dict) else operands
                    )
                    if isinstance(not_ops, list) and not_ops:
                        if isinstance(not_ops[0], dict):
                            score += self._analyze_complexity(not_ops[0])
                case "$size" | "$in" | "$arrayElemAt":
                    score += 2
                case "$concat" | "$substr" | "$trim":
                    score += 1

        return score

    def _make_expr_key(self, expr: dict[str, Any]) -> str:
        """Create a cache key from an expression structure.

        Delegates to ``TranslationCache._extract_structure`` to build a
        canonicalized hashable tuple preserving field references and literal
        values, then formats it as a string key.

        Args:
            expr: The ``$expr`` expression dictionary to key.

        Returns:
            str: String representation of the canonicalized expression key.
        """
        structure = self._translation_cache._extract_structure(expr)
        return str(structure)

    def _build_from_cache(
        self,
        expr: dict[str, Any],
        collection_name: str,
        cached: tuple[str, tuple[Any, ...]],
    ) -> tuple[str, list[Any], list[str]]:
        """Build a Tier 2 query from a cached translation entry.

        Args:
            expr: Original expression dictionary (unused; kept for signature
                stability).
            collection_name: Collection table name.
            cached: Tuple ``(where_clause_template, cached_values)`` where
                ``cached_values`` holds ``(field_list, param_values)`` from
                ``TranslationCache``.

        Returns:
            tuple[str, list[Any], list[str]]: Tuple containing
                ``(sql_query, params, temp_tables)`` ready for execution.
        """
        where_clause_template, (field_list, param_values) = cached

        # Generate unique temp table name
        temp_table = f"temp_expr_{uuid.uuid4().hex[:8]}"
        self._temp_tables.append(temp_table)

        # Create temp table with cached fields
        self._create_temp_table(temp_table, collection_name, list(field_list))

        # Build WHERE clause by substituting temp table name into template
        quoted_coll = quote_table_name(collection_name)
        quoted_temp = quote_table_name(temp_table)
        where_clause = where_clause_template.replace(
            "{temp_table}", quoted_temp
        )

        # Parameters are cached verbatim from the identical expression that
        # populated the cache — no re-extraction needed.
        params = list(param_values)

        # Build SELECT with JSON text conversion for Python-space data
        select_data = self.jsonb.data_column(
            f"{quoted_coll}.{self.data_column}"
        )
        select_data = f"{select_data} as data"

        # Build the final query
        query = f"""
            SELECT {quoted_temp}.id, {quoted_temp}._id, {select_data}
            FROM {quoted_coll}
            JOIN {quoted_temp} ON {quoted_coll}.id = {quoted_temp}.id
            WHERE {where_clause}
        """

        return query.strip(), params, [temp_table]

    def _extract_where_clause(self, full_query: str) -> str:
        """Extract the trailing ``WHERE`` clause from a full query string.

        Args:
            full_query: Complete SQL ``SELECT`` query string.

        Returns:
            str: Predicate text following the final ``WHERE``, or ``""`` if
                no ``WHERE`` keyword is present.
        """
        if "WHERE" in full_query:
            # In our generated queries, WHERE is always at the end of the main SELECT
            parts = full_query.rsplit("WHERE", 1)
            return parts[1].strip()
        return ""

    def _build_tier2_query(
        self,
        expr: dict[str, Any],
        collection_name: str,
        filter_expr: dict[str, Any] | None = None,
    ) -> tuple[str, list[Any], list[str]]:
        """Build a Tier 2 query using a newly created temporary table.

        Strategy:

        1. Compile the main query first so unsupported operators fail before
           allocating a temporary table.
        2. Create a temporary table with document IDs and JSON-extracted
           field values.
        3. Return the query joined against the populated temporary table.

        Args:
            expr: The ``$expr`` expression dictionary.
            collection_name: Collection table name.
            filter_expr: Optional additional equality filter dictionary.

        Returns:
            tuple[str, list[Any], list[str]]: Tuple containing
                ``(sql_query, params, temp_tables)``.
        """
        # Generate unique temp table name
        temp_table = f"temp_expr_{uuid.uuid4().hex[:8]}"

        # Extract all field references from expression
        fields = self._extract_field_references(expr)

        # Build the main query before creating the temp table so unsupported
        # expressions fail fast without allocating/dropping a phantom table.
        sql, params = self._build_main_query(
            expr, collection_name, temp_table, filter_expr
        )

        # Create temp table to store intermediate results
        self._temp_tables.append(temp_table)
        self._create_temp_table(temp_table, collection_name, fields)

        return sql, params, [temp_table]

    def _create_temp_table(
        self, temp_table: str, collection_name: str, fields: list[str]
    ) -> None:
        """Create and index a temporary table with extracted field values.

        Args:
            temp_table: Name of the temporary table to create.
            collection_name: Source collection table name.
            fields: List of field paths to extract into sanitized columns.
        """
        # Build column definitions for extracted fields
        select_exprs = ["id"]

        # Check if _id column exists
        quoted_coll = quote_table_name(collection_name)
        quoted_temp = quote_table_name(temp_table)
        try:
            self.db.execute(f"SELECT _id FROM {quoted_coll} LIMIT 0")
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
            extract_expr = (
                f"{self.jsonb.function('extract')}("
                f"{self.data_column}, '{parse_json_path(field)}') as {quote_identifier(col_name)}"
            )

            select_exprs.append(extract_expr)

        # Create temp table with SELECT (faster than CREATE + INSERT)
        select_sql = f"SELECT {', '.join(select_exprs)} FROM {quoted_coll}"
        create_sql = f"CREATE TEMPORARY TABLE IF NOT EXISTS {quoted_temp} AS {select_sql}"

        self.db.execute(create_sql)

        # Create index on temp table for faster joins
        idx_name = quote_identifier(f"idx_{temp_table}_id")
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {quoted_temp}(id)"
        )

    def _build_main_query(
        self,
        expr: dict[str, Any],
        collection_name: str,
        temp_table: str,
        filter_expr: dict[str, Any] | None = None,
    ) -> tuple[str, list[Any]]:
        """Build the main ``SELECT`` query joined with the temporary table.

        Args:
            expr: The ``$expr`` expression dictionary.
            collection_name: Collection table name.
            temp_table: Temporary table name.
            filter_expr: Optional additional equality filter dictionary.

        Returns:
            tuple[str, list[Any]]: ``(sql_query, params)``.
        """
        # Build WHERE clause from expression using temp table columns
        where_clause, params = self._build_expr_where_from_temp(
            expr, temp_table
        )
        quoted_coll = quote_table_name(collection_name)
        quoted_temp = quote_table_name(temp_table)

        # Build SELECT with JSON text conversion for Python-space data
        select_data = self.jsonb.data_column(
            f"{quoted_coll}.{self.data_column}"
        )
        select_data = f"{select_data} as data"

        # Honor the optional extra filter (AND-ed into the WHERE) (#146)
        if filter_expr:
            extra_conds, extra_params = [], []
            for fkey, fval in filter_expr.items():
                extract = (
                    f"{self.jsonb.json_function_prefix}_extract("
                    f"{quoted_coll}.{self.data_column}, "
                    f"'{parse_json_path(fkey)}')"
                )
                extra_conds.append(f"{extract} = ?")
                extra_params.append(fval)
            where_clause = f"{where_clause} AND {' AND '.join(extra_conds)}"
            params = list(params) + extra_params

        # Build the final query
        query = f"""
            SELECT {quoted_temp}.id, {quoted_temp}._id, {select_data}
            FROM {quoted_coll}
            JOIN {quoted_temp} ON {quoted_coll}.id = {quoted_temp}.id
            WHERE {where_clause}
        """

        return query.strip(), params

    def _build_expr_where_from_temp(
        self, expr: dict[str, Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Build a truthy SQL ``WHERE`` predicate using temp-table columns.

        Wraps the compiled expression in ``COALESCE((...), 0) != 0`` to match
        MongoDB ``$expr`` truthiness.

        Args:
            expr: The ``$expr`` expression dictionary.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(where_clause, params)``.
        """
        sql, params = self._convert_expr_to_temp_sql(expr, temp_table)

        # MongoDB $expr truthiness: NOT (null, 0, false, undefined).
        # In SQLite, we use COALESCE and != 0 to return 1 for truthy and 0 for falsy.
        truthy_sql = f"COALESCE(({sql}), 0) != 0"
        return truthy_sql, params

    def _convert_expr_to_temp_sql(
        self, expr: dict[str, Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert a ``$expr`` dictionary into SQL using temp-table columns.

        Args:
            expr: Single-key ``$expr`` expression dictionary.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.

        Raises:
            ValueError: If ``expr`` is not a single-key dictionary.
            NotImplementedError: If the operator is not supported in Tier 2.
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
        self, operator: str, operands: list[Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert logical operators to SQL using temporary table columns.

        Supports ``$and``, ``$or``, ``$not``, and ``$nor``.

        Args:
            operator: Logical operator name.
            operands: List of nested expression operands.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.

        Raises:
            ValueError: If operand shape or count is invalid for ``operator``.
        """
        if operator == "$not" and not isinstance(operands, list):
            operands = [operands]

        if not isinstance(operands, list):
            raise ValueError(f"{operator} requires a list of expressions")

        if operator == "$not":
            if len(operands) != 1:
                raise ValueError("$not requires exactly one operand")
            inner_sql, inner_params = self._convert_expr_to_temp_sql(
                operands[0], temp_table
            )
            return f"NOT (COALESCE(({inner_sql}), 0))", inner_params

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
                coalesced = " OR ".join(f"COALESCE({p}, 0)" for p in sql_parts)
                sql = f"NOT (COALESCE({coalesced}, 0))"
            case _:
                raise ValueError(f"Unknown logical operator: {operator}")

        return sql, all_params

    def _convert_comparison_to_temp_sql(
        self, operator: str, operands: list[Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert comparison operators to SQL using temporary table columns.

        Supports ``$eq``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, and ``$ne``.

        Args:
            operator: Comparison operator name.
            operands: Two-element list of operands to compare.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.

        Raises:
            ValueError: If ``operands`` does not have length 2.
        """
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
        self, operator: str, operands: list[Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert arithmetic operators to SQL using temporary table columns.

        Supports ``$add``, ``$subtract``, ``$multiply``, ``$divide``, and
        ``$mod``.

        Args:
            operator: Arithmetic operator name.
            operands: List of at least two operands.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.

        Raises:
            ValueError: If fewer than two operands are provided.
        """
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

        if operator == "$mod":
            mod_id = getattr(self, "_mod_counter", 0) + 1
            self._mod_counter = mod_id
            acc_sql = sql_parts[0]
            for idx, part in enumerate(sql_parts[1:]):
                cte = f"_tmod_{mod_id}_{idx}"
                acc_sql = (
                    f"(WITH {cte}(l, r) AS (SELECT {acc_sql}, {part}) "
                    f"SELECT l - r * CAST(l / r AS INTEGER) FROM {cte})"
                )
            return acc_sql, all_params

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
        self, operands: dict[str, Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert ``$cond`` operator to a SQL ``CASE`` expression.

        Args:
            operands: Dictionary with ``if``, ``then``, and optional ``else``
                keys.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_case_expression, params)``.

        Raises:
            ValueError: If ``operands`` is not a dict or lacks ``if``/``then``.
        """
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
        self, operands: list[Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert ``$cmp`` operator to a SQL ``CASE`` expression.

        Replicates parameter bindings four times because each operand appears
        in four ``WHEN`` branches.

        Args:
            operands: Two-element list of operands to compare.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_case_expression, params)``.

        Raises:
            ValueError: If ``operands`` does not have length 2.
        """
        if len(operands) != 2:
            raise ValueError("$cmp requires exactly 2 operands")

        left_sql, left_params = self._convert_operand_to_temp_sql(
            operands[0], temp_table
        )
        right_sql, right_params = self._convert_operand_to_temp_sql(
            operands[1], temp_table
        )

        sql = (
            f"(CASE "
            f"WHEN ({left_sql}) IS NULL AND ({right_sql}) IS NULL THEN 0 "
            f"WHEN ({left_sql}) IS NULL THEN -1 "
            f"WHEN ({right_sql}) IS NULL THEN 1 "
            f"WHEN ({left_sql}) < ({right_sql}) THEN -1 "
            f"WHEN ({left_sql}) > ({right_sql}) THEN 1 "
            f"ELSE 0 END)"
        )
        return sql, (left_params + right_params) * 4

    def _convert_math_to_temp_sql(
        self, operator: str, operands: list[Any], temp_table: str
    ) -> tuple[str, list[Any]]:
        """Convert unary math operators to SQL using temporary table columns.

        Supports ``$abs``, ``$ceil``, ``$floor``, and ``$round``.

        Args:
            operator: Math operator name.
            operands: Single-element list containing the numeric operand.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.

        Raises:
            ValueError: If ``operands`` does not have length 1.
            NotImplementedError: If ``operator`` is not supported in Tier 2.
        """
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
    ) -> tuple[str, list[Any]]:
        """Convert an operand to SQL using temporary table columns.

        Translates ``$field`` references into qualified temp-table columns,
        recurses on nested expression dicts, and binds literals as ``?``.

        Args:
            operand: Field reference, nested expression dict, or literal.
            temp_table: Temporary table name.

        Returns:
            tuple[str, list[Any]]: ``(sql_expression, params)``.
        """
        if isinstance(operand, str) and operand.startswith("$"):
            # Field reference - use temp table column
            field_path = operand[1:]  # Remove $
            col_name = self._sanitize_field_name(field_path)
            return (
                f"{quote_table_name(temp_table)}.{quote_identifier(col_name)}",
                [],
            )

        elif isinstance(operand, dict):
            # Nested expression
            return self._convert_expr_to_temp_sql(operand, temp_table)

        else:
            # Literal value
            return "?", [operand]

    def _extract_field_references(self, expr: dict[str, Any]) -> list[str]:
        """Extract all unique field references from an expression.

        Args:
            expr: The ``$expr`` expression dictionary.

        Returns:
            list[str]: Unique field paths in first-seen order without leading
                ``$``.
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

    def _extract_field_references_from_operand(self, operand: Any) -> list[str]:
        """Recursively collect field references from an operand node.

        Args:
            operand: String field reference, list, dict, or literal operand.

        Returns:
            list[str]: Collected field paths without leading ``$`` (may
                contain duplicates).
        """
        fields = []

        if isinstance(operand, str) and operand.startswith("$"):
            fields.append(operand[1:])
        elif isinstance(operand, list):
            for item in operand:
                fields.extend(self._extract_field_references_from_operand(item))
        elif isinstance(operand, dict):
            for val in operand.values():
                fields.extend(self._extract_field_references_from_operand(val))

        return fields

    def _sanitize_field_name(self, field_path: str) -> str:
        """Sanitize a field path for use as a SQL column name.

        Replaces dots and brackets with underscores and prefixes ``f_`` when
        the result does not start with an alphabetic character.

        Args:
            field_path: Field path (e.g., ``"stats.wins"``).

        Returns:
            str: Sanitized column name (e.g., ``"stats_wins"``).
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
                self.db.execute(
                    f"DROP TABLE IF EXISTS {quote_table_name(table)}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary table {table} during cleanup: {e}"
                )
                pass  # Ignore cleanup errors
        self._temp_tables.clear()
