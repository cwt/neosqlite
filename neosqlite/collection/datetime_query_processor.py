"""Datetime query processor for NeoSQLite with three-tier fallback mechanism.

This module provides a three-tier approach for handling datetime queries:

1. SQL tier: Direct SQL processing with ``json_*`` functions.
2. Temp table tier: Temporary table approach for complex queries.
3. Python tier: Pure Python processing as a fallback.

The SQL and temporary table tiers always use ``json_*`` functions rather than
``jsonb_*`` because datetime comparisons require string values, while
``jsonb_*`` returns binary payloads. This mirrors the text-search pattern of
using ``json_*`` functions only.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any

from ..sql_utils import quote_table_name

logger = logging.getLogger(__name__)
from .jsonb_support import JSONBContext
from .query_helper import QueryHelper
from .sql_translator_unified import SQLFieldAccessor, SQLTranslator
from .temporary_table_aggregation import (
    aggregation_pipeline_context,
)


class DateTimeQueryProcessor:
    """Process datetime queries using a three-tier fallback mechanism.

    Executes datetime queries across three ordered tiers:

    1. SQL tier: Direct SQL processing using ``json_*`` functions.
    2. Temp table tier: Temporary table processing for complex queries.
    3. Python tier: Pure Python evaluation as a fallback.

    The SQL and temporary table tiers always use ``json_*`` functions rather
    than ``jsonb_*`` so datetime comparisons operate on JSON strings instead
    of binary JSONB payloads.
    """

    def __init__(
        self, collection, query_engine=None, use_global_kill_switch=False
    ):
        """Initialize the DateTimeQueryProcessor with a collection.

        Args:
            collection: The NeoSQLite collection to run datetime queries on.
            query_engine: Optional QueryEngine instance providing helpers.
            use_global_kill_switch: If True, use the global kill switch;
                otherwise track a per-instance local kill switch.
        """
        self.collection = collection
        self.db = collection.db
        self.query_engine = query_engine
        self.helpers = (
            query_engine.helpers if query_engine else QueryHelper(collection)
        )
        # Initialize JSONB capabilities
        self.jsonb = JSONBContext.from_db(self.db)
        self.sql_translator = SQLTranslator(
            collection.name,
            "data",
            "id",
            db_connection=collection.db,
            jsonb_context=self.jsonb,
        )
        # Configuration for kill switch behavior - DEFAULT TO LOCAL for better isolation
        self._use_global_kill_switch = use_global_kill_switch
        # Local kill switch state (isolated to this instance)
        self._local_kill_switch = False

    def set_kill_switch(self, enabled: bool):
        """Set the kill switch to force fallback to Python implementation.

        Behavior depends on initialization configuration:

        - ``use_global_kill_switch=True``: Updates the global kill switch
          (affects the entire application).
        - ``use_global_kill_switch=False``: Updates only the local kill
          switch for this processor instance.

        Args:
            enabled: If True, force fallback to the Python implementation.
        """
        if self._use_global_kill_switch:
            # Import locally to avoid circular imports and minimize global state touch
            from .query_helper import set_force_fallback

            set_force_fallback(enabled)
        else:
            self._local_kill_switch = enabled

    def is_kill_switch_enabled(self) -> bool:
        """Check whether the kill switch is currently enabled.

        Checks the global kill switch when ``use_global_kill_switch=True``,
        otherwise checks the processor instance's local kill switch.

        Returns:
            bool: True if the kill switch is enabled, False otherwise.
        """
        if self._use_global_kill_switch:
            # Import locally to avoid circular imports and minimize global state touch
            from .query_helper import get_force_fallback

            return get_force_fallback()
        else:
            return self._local_kill_switch

    def process_datetime_query(
        self, query: dict[str, Any], use_kill_switch: bool | None = None
    ) -> list[dict[str, Any]]:
        """Process a datetime query using the three-tier fallback mechanism.

        Returns an empty list immediately if the query contains no datetime
        operations. Otherwise attempts SQL tier, temporary table tier, and
        finally pure Python evaluation while notifying tier-change listeners.

        Args:
            query: MongoDB-style query dictionary with datetime operations.
            use_kill_switch: Optional override for the configured kill switch.

        Returns:
            list[dict[str, Any]]: List of matching documents.
        """
        # Check if kill switch is enabled (configured setting or parameter override)
        force_python = (
            use_kill_switch
            if use_kill_switch is not None
            else self.is_kill_switch_enabled()
        )

        # First, check if query contains datetime operations
        if not self._contains_datetime_operations(query):
            # If not datetime query, return empty list
            return []

        qe = self.query_engine or getattr(self.collection, "query_engine", None)

        # Try SQL tier first
        if not force_python:
            try:
                result = self._process_with_sql_tier(query)
                if result is not None:
                    if qe is not None and hasattr(qe, "_notify_tier_change"):
                        qe._notify_tier_change("tier1", [query])
                    return result
            except Exception as e:
                # If SQL/temp table tier fails, fall through to next tier
                logger.debug(f"{e=}")
                pass

        # Try temporary table tier
        if not force_python:
            try:
                result = self._process_with_temp_table_tier(query)
                if result is not None:
                    if qe is not None and hasattr(qe, "_notify_tier_change"):
                        qe._notify_tier_change("tier2", [query])
                    return result
            except Exception as e:
                # If temp table tier fails, fall through to Python tier
                logger.debug(
                    f"Temp table tier failed in datetime processor: {e}"
                )
                pass

        # Fallback to Python tier
        if qe is not None and hasattr(qe, "_notify_tier_change"):
            qe._notify_tier_change("tier3", [query])
        return self._process_with_python_tier(query)

    def _contains_datetime_operations(self, query: dict[str, Any]) -> bool:
        """Check whether a query contains datetime operations.

        Recursively inspects logical operators (``$and``, ``$or``, ``$nor``,
        ``$not``), comparison operators, ``$in``/``$nin``, ``$type`` date
        specifiers, ``$regex`` patterns, and plain field values.

        Args:
            query: MongoDB-style query dictionary.

        Returns:
            bool: True if the query contains datetime operations, else False.
        """
        for field, value in query.items():
            if field in ("$and", "$or", "$nor"):
                if isinstance(value, list):
                    for condition in value:
                        if isinstance(
                            condition, dict
                        ) and self._contains_datetime_operations(condition):
                            return True
            elif field == "$not":
                if isinstance(
                    value, dict
                ) and self._contains_datetime_operations(value):
                    return True
            elif isinstance(value, dict):
                # Check for datetime-related operators
                for operator, op_value in value.items():
                    if operator in ("$gte", "$gt", "$lte", "$lt", "$eq", "$ne"):
                        # Check if the value is a datetime object or datetime string
                        if self._is_datetime_value(op_value):
                            return True
                    elif operator in ("$in", "$nin"):
                        # For $in and $nin, check if any value in the list is a datetime
                        if isinstance(op_value, list):
                            if any(
                                self._is_datetime_value(item)
                                for item in op_value
                            ):
                                return True
                    elif operator == "$type":
                        # Check if looking for date type
                        if op_value in (
                            9,
                            "date",
                            "Date",
                        ):  # 9 is date type in MongoDB
                            return True
                    elif operator == "$regex":
                        # Check if it's a datetime regex pattern
                        if self._is_datetime_regex(op_value):
                            return True
            else:
                if self._is_datetime_value(value):
                    return True
        return False

    def _is_datetime_value(self, value: Any) -> bool:
        """Check if a value is a datetime object or datetime string.

        Args:
            value: Value to inspect for datetime semantics.

        Returns:
            bool: True if ``value`` is datetime-related, False otherwise.
        """
        from .datetime_utils import is_datetime_value

        return is_datetime_value(value)

    def _is_datetime_regex(self, pattern: str) -> bool:
        """Check if a regex pattern is likely to be datetime-related.

        Args:
            pattern: Regex pattern or datetime-like string to inspect.

        Returns:
            bool: True if ``pattern`` appears datetime-related, else False.
        """
        from .datetime_utils import is_datetime_regex

        return is_datetime_regex(pattern)

    def _process_with_sql_tier(
        self, query: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Process a datetime query using the SQL tier with ``json_*`` calls.

        Translates ``query`` with a ``SQLFieldAccessor`` configured for
        ``jsonb_supported=False`` so datetime strings are compared directly,
        then executes the generated ``SELECT`` against SQLite.

        Args:
            query: MongoDB-style query dictionary.

        Returns:
            list[dict[str, Any]] | None: Matching documents if SQL tier
            succeeded, or ``None`` if disabled, untranslatable, or failed.
        """
        # Check if we should force Python fallback using the configured setting
        if self.is_kill_switch_enabled():
            return None

        # Create a custom SQLFieldAccessor that ensures json_* functions are used
        # for datetime queries as required (as done for $text FTS queries)
        field_accessor = SQLFieldAccessor(
            data_column="data",
            id_column="id",
            jsonb_supported=False,  # Force use of json_* functions for datetime
        )

        # Create a custom translator with the field accessor that uses json_* functions
        from .sql_translator_unified import (
            SQLClauseBuilder,
            SQLOperatorTranslator,
        )

        operator_translator = SQLOperatorTranslator(field_accessor)
        clause_builder = SQLClauseBuilder(field_accessor, operator_translator)

        # Translate the match query to WHERE clause using json_* functions only
        where_clause, params = clause_builder.build_where_clause(
            query, query_param=query
        )

        # If translation failed, return None to trigger fallback
        if where_clause is None:
            return None

        # Build the SQL query using json_* functions for datetime comparison
        cmd = (
            f"SELECT id, _id, {self.jsonb.data_column()} as data "
            f"FROM {quote_table_name(self.collection.name)} {where_clause}"
        )

        try:
            cursor = self.db.execute(cmd, params)
            results = [
                self.collection._load_with_stored_id(row[0], row[2], row[1])
                for row in cursor.fetchall()
            ]
            return results
        except Exception as e:
            # If SQL execution fails, return None to trigger fallback
            logger.debug(f"SQL execution failed in datetime processor: {e}")
            return None

    def _process_with_temp_table_tier(
        self, query: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Process a datetime query using the temporary table tier.

        Populates a base temporary table, translates ``query`` using
        ``json_*`` functions (``jsonb_supported=False``), materializes a
        filtered temporary table, and loads the resulting documents.

        Args:
            query: MongoDB-style query dictionary.

        Returns:
            list[dict[str, Any]] | None: Matching documents if temporary table
            processing succeeded, or ``None`` to trigger Python fallback.
        """
        # Check if we should force Python fallback using the configured setting
        if self.is_kill_switch_enabled():
            return None

        # Use a more robust approach with temporary table for complex datetime queries
        try:
            # Create a unique pipeline ID for this operation
            query_str = str(sorted(query.items()))
            pipeline_id = f"datetime_{hashlib.sha256(query_str.encode()).hexdigest()[:8]}_{uuid.uuid4().hex[:4]}"

            with aggregation_pipeline_context(
                self.db, pipeline_id
            ) as create_temp:
                # Create base temp table with all documents
                base_stage = {"_base": True}
                temp_table = create_temp(
                    base_stage,
                    f"SELECT id, _id, {self.jsonb.data_column()} as data "
                    f"FROM {quote_table_name(self.collection.name)}",
                )

                # To ensure we use json_* functions for datetime queries,
                # we need to create a custom clause builder that forces json_* usage
                from .sql_translator_unified import (
                    SQLClauseBuilder,
                    SQLFieldAccessor,
                    SQLOperatorTranslator,
                )

                field_accessor = SQLFieldAccessor(
                    data_column="data",
                    id_column="id",
                    jsonb_supported=False,  # Force use of json_* functions
                )
                operator_translator = SQLOperatorTranslator(field_accessor)
                clause_builder = SQLClauseBuilder(
                    field_accessor, operator_translator
                )

                # Translate the match query to WHERE clause using json_* functions only
                where_clause, params = clause_builder.build_where_clause(
                    query, context="temp_table", query_param=query
                )

                if where_clause is None:
                    # If we can't translate the query, return None to trigger Python fallback
                    return None

                # Create filtered temporary table using json_* functions
                filtered_stage = {"$datetime_filter": query}
                result_table = create_temp(
                    filtered_stage,
                    f"SELECT * FROM {temp_table} {where_clause}",
                    params,
                )

                # Retrieve results from the filtered table
                cursor = self.db.execute(
                    f"SELECT id, _id, {self.jsonb.data_column()} as data "
                    f"FROM {result_table}"
                )
                results = [
                    self.collection._load_with_stored_id(row[0], row[2], row[1])
                    for row in cursor.fetchall()
                ]
                return results

        except Exception as e:
            # If temp table processing fails, return None to trigger Python fallback
            logger.debug(
                f"Temp table processing failed in datetime processor: {e}"
            )
            return None

    def _process_with_python_tier(
        self, query: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Process a datetime query using pure Python evaluation.

        Loads documents directly from SQLite when ``_load_with_stored_id`` is
        available (avoiding spurious Tier-1 cursor events) or falls back to
        ``find({})``, then filters them with ``helpers._apply_query``.

        Args:
            query: MongoDB-style query dictionary.

        Returns:
            list[dict[str, Any]]: List of matching documents.
        """
        # Read rows directly from SQLite for real Collections so executing
        # Tier-3 fallback does not emit a spurious Tier-1 cursor event.
        if hasattr(self.collection, "_load_with_stored_id"):
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)}"
            )
            cursor = self.db.execute(cmd)
            all_docs = [
                self.collection._load_with_stored_id(row[0], row[2], row[1])
                for row in cursor.fetchall()
            ]
        else:
            all_docs = list(self.collection.find({}))
        return [
            doc for doc in all_docs if self.helpers._apply_query(query, doc)
        ]
