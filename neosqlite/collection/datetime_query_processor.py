"""
Datetime query processor for NeoSQLite with three-tier fallback mechanism.

This module provides a three-tier approach for handling datetime queries:
1. SQL tier: Direct SQL processing with json_* functions
2. Temp table tier: Temporary table approach for complex queries
3. Python tier: Pure Python processing as fallback

The SQL and temp table queries always use json_* functions not jsonb_* because
we need to compare the datetime string, querying with jsonb_* will get byte instead of string.
This extends the existing text search functionality to also use only json_* functions.
"""

from typing import Any, Dict, List, Optional
from .jsonb_support import supports_jsonb
from .sql_translator_unified import SQLTranslator, SQLFieldAccessor
from .temporary_table_aggregation import (
    aggregation_pipeline_context,
)
from .query_helper import QueryHelper


class DateTimeQueryProcessor:
    """
    Process datetime queries using a three-tier fallback mechanism.

    The three-tier approach:
    1. SQL tier: Direct SQL processing with json_* functions
    2. Temp table tier: Temporary table approach for complex queries
    3. Python tier: Pure Python processing as fallback

    The SQL and temp table queries always use json_* functions not jsonb_* because
    we need to compare the datetime string, querying with jsonb_* will get byte instead of string.
    """

    def __init__(
        self, collection, query_engine=None, use_global_kill_switch=False
    ):
        """
        Initialize the DateTimeQueryProcessor with a collection.

        Args:
            collection: The NeoSQLite collection to process datetime queries on
            query_engine: Optional QueryEngine instance for accessing helpers
            use_global_kill_switch: If True, use the global kill switch; if False, use local kill switch only
        """
        self.collection = collection
        self.db = collection.db
        self.query_engine = query_engine
        self.helpers = (
            query_engine.helpers if query_engine else QueryHelper(collection)
        )
        self.sql_translator = SQLTranslator(collection.name, "data", "id")
        # Check if JSONB is supported for this connection
        self._jsonb_supported = supports_jsonb(self.db)
        # Configuration for kill switch behavior - DEFAULT TO LOCAL for better isolation
        self._use_global_kill_switch = use_global_kill_switch
        # Local kill switch state (isolated to this instance)
        self._local_kill_switch = False

    def set_kill_switch(self, enabled: bool):
        """
        Set the kill switch to force fallback to Python implementation.
        Behavior depends on initialization setting:
        - If use_global_kill_switch=True: Sets the global kill switch (affects entire app)
        - If use_global_kill_switch=False: Sets local kill switch (affects only this instance)

        Args:
            enabled: If True, forces fallback to Python implementation
        """
        if self._use_global_kill_switch:
            # Import locally to avoid circular imports and minimize global state touch
            from .query_helper import set_force_fallback

            set_force_fallback(enabled)
        else:
            self._local_kill_switch = enabled

    def is_kill_switch_enabled(self) -> bool:
        """
        Check if the kill switch is enabled.
        Behavior depends on initialization setting:
        - If use_global_kill_switch=True: Checks global kill switch
        - If use_global_kill_switch=False: Checks local kill switch

        Returns:
            True if kill switch is enabled, False otherwise
        """
        if self._use_global_kill_switch:
            # Import locally to avoid circular imports and minimize global state touch
            from .query_helper import get_force_fallback

            return get_force_fallback()
        else:
            return self._local_kill_switch

    def process_datetime_query(
        self, query: Dict[str, Any], use_kill_switch: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Process a datetime query using the three-tier fallback mechanism.

        Args:
            query: MongoDB-style query dictionary containing datetime operations
            use_kill_switch: Optional override for kill switch setting

        Returns:
            List of matching documents
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

        # Try SQL tier first
        if not force_python:
            try:
                result = self._process_with_sql_tier(query)
                if result is not None:
                    return result
            except Exception:
                # If SQL tier fails, fall through to next tier
                pass

        # Try temporary table tier
        if not force_python:
            try:
                result = self._process_with_temp_table_tier(query)
                if result is not None:
                    return result
            except Exception:
                # If temp table tier fails, fall through to Python tier
                pass

        # Fallback to Python tier
        return self._process_with_python_tier(query)

    def _contains_datetime_operations(self, query: Dict[str, Any]) -> bool:
        """
        Check if a query contains datetime operations.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            True if query contains datetime operations, False otherwise
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
        return False

    def _is_datetime_value(self, value: Any) -> bool:
        """
        Check if a value is a datetime object or datetime string.

        Args:
            value: Value to check

        Returns:
            True if value is datetime-related, False otherwise
        """
        from .datetime_utils import is_datetime_value

        return is_datetime_value(value)

    def _is_datetime_regex(self, pattern: str) -> bool:
        """
        Check if a pattern is likely to be datetime-related.

        Args:
            pattern: Pattern string (could be a regex pattern or a datetime string)

        Returns:
            True if pattern is likely datetime-related, False otherwise
        """
        import re
        from .datetime_utils import is_datetime_value

        # If it's not a string, return False
        if not isinstance(pattern, str):
            return False

        # Check if the pattern itself looks like a datetime value
        # This handles cases where people might use exact datetime strings
        if is_datetime_value(pattern):
            return True

        # Check if the pattern contains common datetime-related regex patterns
        # Common datetime-related patterns (for regex patterns)
        datetime_indicators = [
            r"\\d{4}-\\d{2}-\\d{2}",  # Date format: \d{4}-\d{2}-\d{2}
            r"\\d{2}/\\d{2}/\\d{4}",  # US date format: \d{2}/\d{2}/\d{4}
            r"\\d{4}/\\d{2}/\\d{2}",  # Alternative date format: \d{4}/\d{2}/\d{2}
            r"\\d{2}-\\d{2}-\\d{4}",  # Common date format: \d{2}-\d{2}-\d{4}
            r"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",  # Datetime format: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}
        ]

        for indicator in datetime_indicators:
            if re.search(indicator, pattern):
                return True

        return False

    def _process_with_sql_tier(
        self, query: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Process datetime query using SQL tier with json_* functions.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            List of matching documents if successful, None otherwise
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
            SQLOperatorTranslator,
            SQLClauseBuilder,
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
        cmd = f"SELECT id, data FROM {self.collection.name} {where_clause}"

        try:
            cursor = self.db.execute(cmd, params)
            results = []
            for row in cursor.fetchall():
                doc = self.collection._load(row[0], row[1])
                results.append(doc)
            return results
        except Exception:
            # If SQL execution fails, return None to trigger fallback
            return None

    def _process_with_temp_table_tier(
        self, query: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Process datetime query using temporary table approach.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            List of matching documents if successful, None otherwise
        """
        # Check if we should force Python fallback using the configured setting
        if self.is_kill_switch_enabled():
            return None

        # Use a more robust approach with temporary table for complex datetime queries
        try:
            # Create a unique pipeline ID for this operation
            import hashlib
            import uuid

            query_str = str(sorted(query.items()))
            pipeline_id = f"datetime_{hashlib.sha256(query_str.encode()).hexdigest()[:8]}_{uuid.uuid4().hex[:4]}"

            with aggregation_pipeline_context(
                self.db, pipeline_id
            ) as create_temp:
                # Create base temp table with all documents
                base_stage = {"_base": True}
                temp_table = create_temp(
                    base_stage, f"SELECT id, data FROM {self.collection.name}"
                )

                # To ensure we use json_* functions for datetime queries,
                # we need to create a custom clause builder that forces json_* usage
                from .sql_translator_unified import (
                    SQLFieldAccessor,
                    SQLOperatorTranslator,
                    SQLClauseBuilder,
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
                cursor = self.db.execute(f"SELECT id, data FROM {result_table}")
                results = []
                for row in cursor.fetchall():
                    doc = self.collection._load(row[0], row[1])
                    results.append(doc)
                return results

        except Exception:
            # If temp table processing fails, return None to trigger Python fallback
            return None

    def _process_with_python_tier(
        self, query: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Process datetime query using pure Python implementation.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            List of matching documents
        """
        # Fetch all documents and apply the query using Python
        all_docs = list(self.collection.find({}))
        results = []

        for doc in all_docs:
            if self.helpers._apply_query(query, doc):
                results.append(doc)

        return results


class EnhancedDateTimeQueryProcessor(DateTimeQueryProcessor):
    """
    Enhanced datetime query processor with additional datetime-specific query operators.
    """

    def __init__(self, collection, query_engine=None):
        """
        Initialize the EnhancedDateTimeQueryProcessor with a collection.

        Args:
            collection: The NeoSQLite collection to process datetime queries on
            query_engine: Optional QueryEngine instance for accessing helpers
        """
        super().__init__(collection, query_engine)

    def _apply_datetime_query(
        self, query: Dict[str, Any], document: Dict[str, Any]
    ) -> bool:
        """
        Apply datetime-specific query operations to a document.

        Args:
            query: MongoDB-style query dictionary with datetime operations
            document: Document to check against the query

        Returns:
            True if document matches query, False otherwise
        """
        # This method extends the base functionality to handle complex datetime operations
        # For now, we'll use the base query helper but could add more sophisticated datetime handling
        return self.helpers._apply_query(query, document)

    def process_complex_datetime_query(
        self, query: Dict[str, Any], use_kill_switch: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Process complex datetime queries with additional datetime-specific logic.

        Args:
            query: MongoDB-style query dictionary containing complex datetime operations
            use_kill_switch: Optional override for kill switch setting

        Returns:
            List of matching documents
        """
        # Check if kill switch is enabled (configured setting or parameter override)
        force_python = (
            use_kill_switch
            if use_kill_switch is not None
            else self.is_kill_switch_enabled()
        )

        # For complex datetime queries, we might have nested conditions or date ranges
        if not self._contains_datetime_operations(query):
            # If not datetime query, return empty list or process normally
            return []

        # Try SQL tier first
        if not force_python:
            try:
                result = self._process_with_sql_tier(query)
                if result is not None:
                    return result
            except Exception:
                # If SQL tier fails, fall through to next tier
                pass

        # Try temporary table tier
        if not force_python:
            try:
                result = self._process_with_temp_table_tier(query)
                if result is not None:
                    return result
            except Exception:
                # If temp table tier fails, fall through to Python tier
                pass

        # Fallback to enhanced Python tier
        return self._process_with_enhanced_python_tier(query)

    def _process_with_enhanced_python_tier(
        self, query: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Process datetime query using enhanced pure Python implementation.

        Args:
            query: MongoDB-style query dictionary

        Returns:
            List of matching documents
        """
        # Fetch all documents and apply the enhanced datetime query using Python
        all_docs = list(self.collection.find({}))
        results = []

        for doc in all_docs:
            if self._apply_datetime_query(query, doc):
                results.append(doc)

        return results
