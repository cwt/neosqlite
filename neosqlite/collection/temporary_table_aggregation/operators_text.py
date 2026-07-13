from __future__ import annotations

import hashlib
import logging
import warnings
from typing import Any, Callable

from ...sql_utils import quote_table_name
from ..jsonb_support import (
    _get_json_tree_function,
)
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)


class OperatorsTextMixin(OperatorsBaseMixin):
    def _matches_text_search(
        self, document: dict[str, Any], search_term: str
    ) -> bool:
        """
        Apply Python-based text search to a document.

        This method uses the unified_text_search function to determine if a document
        matches a given search term. It's used as a fallback when text search cannot
        be efficiently handled with SQL queries, particularly in cases involving
        unwound elements or complex text search operations.

        Args:
            document (dict[str, Any]): The document to search in
            search_term (str): The term to search for

        Returns:
            bool: True if the document matches the text search, False otherwise
        """

        from neosqlite.collection.text_search import unified_text_search

        return unified_text_search(document, search_term)

    def _batch_insert_documents(
        self, table_name: str, documents: list[tuple]
    ) -> None:
        """
        Insert multiple documents into a temporary table efficiently.

        This method provides an optimized way to insert multiple documents into a
        temporary table by using a single INSERT statement with multiple value sets.
        It's used primarily in the text search processing where documents need to be
        filtered and inserted into a result table.

        Args:
            table_name (str): The name of the table to insert into
            documents (list[tuple]): List of (id, data) tuples to insert
        """
        if not documents:
            return

        placeholders = ",".join(["(?,?)"] * len(documents))
        query = f"INSERT INTO {table_name} (id, data) VALUES {placeholders}"
        flat_params = [item for doc_tuple in documents for item in doc_tuple]
        self.db.execute(query, flat_params)

    def _process_text_search_stage(
        self,
        create_temp: Callable,
        current_table: str,
        match_spec: dict[str, Any],
    ) -> str:
        """
        Process a $text search stage using FTS5 on temporary table.

        This method creates an FTS5 virtual table on the temporary data and uses
        SQLite's FTS5 for efficient text search. The tokenizer configuration is
        detected from the existing FTS index on the collection to ensure consistent
        behavior.

        Note:
            When $text is used after $unwind (or other stages that create temp tables),
            the search operates on the unwound elements in the temp table, not on the
            original collection documents. This differs from MongoDB's semantics where
            $text always uses the collection-level text index on original documents.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            match_spec (dict[str, Any]): The $match stage specification containing the
                                        $text operator with a $search term

        Returns:
            str: Name of the newly created temporary table with text search results

        Raises:
            ValueError: If the $text operator specification is invalid or the search
                        term is not a string
        """
        # Warn about NeoSQLite extension (different semantics from MongoDB)
        # Only warn if $text is used AFTER $unwind (i.e., on a temp table with unwound data)
        if self._has_unwind_in_pipeline and not self._text_on_temp_table_warned:
            warnings.warn(
                "$text search after $unwind is a NeoSQLite extension using FTS5 on "
                "temporary tables, which searches unwound elements directly. "
                "This differs from MongoDB where $text can only be the first stage. "
                "For MongoDB compatibility, place the $text stage at the beginning of the pipeline.",
                UserWarning,
                stacklevel=4,
            )
            self._text_on_temp_table_warned = True

        # Extract and validate search term
        if "$text" not in match_spec or "$search" not in match_spec["$text"]:
            raise ValueError("Invalid $text operator specification")

        search_term = match_spec["$text"]["$search"]
        if not isinstance(search_term, str):
            raise ValueError("$text search term must be a string")

        # Detect tokenizer from existing FTS index on the collection
        tokenizer_clause = self._detect_fts_tokenizer()

        # Generate deterministic table names
        fts_table_name = f"temp_text_fts_{hashlib.sha256(str(match_spec).encode()).hexdigest()[:8]}"
        result_table_name = f"temp_text_filtered_{hashlib.sha256(str(match_spec).encode()).hexdigest()[:8]}"

        # Step 1: Create FTS5 virtual table with detected tokenizer
        # We need to extract text content from the JSON data for indexing
        # After $unwind, the unwound field contains the element (e.g., {"text": "...", ...})
        # We try multiple paths to find text content and concatenate them
        self.db.execute(f"""
            CREATE VIRTUAL TABLE {fts_table_name} USING fts5(src_rowid, id, content {tokenizer_clause})
        """)

        # Step 2: Populate FTS5 table with text content from current table
        # Use json_tree/jsonb_tree to recursively extract ALL string values from
        # the JSON object at any depth, then concatenate them for FTS indexing.
        # This handles any unwound object/array structure without hardcoded paths.
        json_tree_func = _get_json_tree_function(
            self._jsonb_supported, self._jsonb_each_supported
        )

        self.db.execute(f"""
            INSERT INTO {fts_table_name}(src_rowid, id, content)
            SELECT c.rowid, c.id,
                   GROUP_CONCAT(t.value, ' ') as content
            FROM {current_table} c, {json_tree_func}(c.data) t
            WHERE t.type = 'text'
            GROUP BY c.rowid
        """)

        # Step 3: Query FTS5 and create result table with matching documents
        # Join on source rowid to get exact matching rows
        # Also preserve _id column for proper sorting support
        # Store bm25 score in JSON data for $meta: textScore support
        json_set_func = f"{self._json_function_prefix}_set"
        self.db.execute(f"DROP TABLE IF EXISTS {result_table_name}")
        self.db.execute(
            f"""
            CREATE TEMP TABLE {result_table_name} AS
            SELECT c.id, c._id,
                   json({json_set_func}(c.data, '$._textScore', -bm25({fts_table_name}))) as data
            FROM {current_table} c
            INNER JOIN {fts_table_name} f ON c.rowid = f.src_rowid
            WHERE {fts_table_name} MATCH ?
        """,
            [search_term],
        )

        # Clean up FTS table
        self.db.execute(f"DROP TABLE IF EXISTS {fts_table_name}")

        return result_table_name

    def _detect_fts_tokenizer(self) -> str:
        """
        Detect the tokenizer configuration from existing FTS indexes on the collection.

        Returns:
            str: The tokenizer clause for FTS5 (e.g., ", tokenize=porter" or "")
        """
        # Query sqlite_master to find FTS tables for this collection
        fts_table_pattern = f"{quote_table_name(self.collection.name)}_%_fts"
        cursor = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (fts_table_pattern,),
        )

        for (sql,) in cursor.fetchall():
            if sql:
                # Parse tokenizer from CREATE VIRTUAL TABLE ... USING FTS5(..., tokenize=xxx)
                # Example: "CREATE VIRTUAL TABLE test USING FTS5(content, tokenize=porter)"
                import re

                match = re.search(r"tokenize\s*=\s*(\w+)", sql, re.IGNORECASE)
                if match:
                    tokenizer = match.group(1)
                    return f", tokenize={tokenizer}"

        # Default: no tokenizer specified
        return ""
