"""Text search operator mixin for temporary-table aggregation pipelines."""

from __future__ import annotations

import hashlib
import logging
import re
import warnings
from collections.abc import Callable
from typing import Any

from ...sql_utils import quote_table_name
from ..query_helper.utils import sanitize_fts_query
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)


class OperatorsTextMixin(OperatorsBaseMixin):
    """Mixin implementing $text search stages using SQLite FTS5 tables.

    Creates transient FTS5 virtual tables over temporary aggregation table
    rows, matches the collection's existing FTS tokenizer when present, and
    attaches BM25 ``_textScore`` metadata to matching documents.
    """

    def _process_text_search_stage(
        self,
        create_temp: Callable,
        current_table: str,
        match_spec: dict[str, Any],
    ) -> str:
        """Process a $text search stage using FTS5 on a temporary table.

        Creates a transient FTS5 virtual table populated with text extracted
        recursively from ``current_table`` and queries it with SQLite FTS5.
        The tokenizer configuration is detected from any existing FTS index on
        the collection so tokenization matches collection-level searches.

        When ``$text`` follows ``$unwind`` or other stages that create
        temporary tables, the search runs over the unwound rows rather than
        the original collection documents (a NeoSQLite extension differing
        from MongoDB, where ``$text`` must be the first stage).

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            match_spec: The ``$match`` stage specification containing a
                ``$text`` operator with a ``$search`` string.

        Returns:
            str: Name of the newly created temporary table containing matching
                rows with ``_textScore`` attached.

        Raises:
            ValueError: If the ``$text`` operator specification is missing
                ``$search`` or the search term is not a string.
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

        sanitized_term = sanitize_fts_query(search_term)
        json_set_func = f"{self.jsonb.json_function_prefix}_set"
        if not sanitized_term:
            select_sql = f"""
                SELECT c.id, c._id,
                       json({json_set_func}(c.data, '$._textScore', 0.0)) as data
                FROM {current_table} c
                WHERE 0
            """
            return create_temp({"$text": match_spec}, select_sql, params=[])

        # Detect tokenizer from existing FTS index on the collection
        tokenizer_clause = self._detect_fts_tokenizer()

        # Generate deterministic table names
        fts_table_name = f"temp_text_fts_{hashlib.sha256(str(match_spec).encode()).hexdigest()[:8]}"

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
        json_tree_func = self.jsonb.json_tree_function

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
        #
        # The result table is created via create_temp so the aggregation
        # context owns its lifecycle; previously it leaked per distinct
        # search term until connection close (#125).
        select_sql = f"""
            SELECT c.id, c._id,
                   json({json_set_func}(c.data, '$._textScore', -bm25({fts_table_name}))) as data
            FROM {current_table} c
            INNER JOIN {fts_table_name} f ON c.rowid = f.src_rowid
            WHERE {fts_table_name} MATCH ?
        """

        try:
            # create_temp supports bound params and records ownership, so
            # the context drops this table with the rest of the pipeline.
            result_table = create_temp(
                {"$text": match_spec}, select_sql, params=[sanitized_term]
            )
        finally:
            self.db.execute(f"DROP TABLE IF EXISTS {fts_table_name}")

        return result_table

    def _detect_fts_tokenizer(self) -> str:
        """Detect the tokenizer configured on the collection's FTS5 index.

        Inspects ``sqlite_master`` for FTS tables belonging to this collection,
        skipping tables whose names match another collection with a longer
        underscore-prefixed name.

        Returns:
            str: Tokenizer clause for FTS5 creation (for example,
                ``", tokenize=porter"``), or ``""`` when none is configured.
        """
        escaped_name = quote_table_name(self.collection.name).replace(
            "_", "\\_"
        )
        fts_table_pattern = f"{escaped_name}\\_%\\_fts"
        cursor = self.db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE ? ESCAPE '\\'",
            (fts_table_pattern,),
        )

        all_tables_cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        all_tables = {row[0] for row in all_tables_cursor.fetchall()}

        suffix_len = len("_fts")
        for name, sql in cursor.fetchall():
            base_name = name[:-suffix_len]
            is_longer_match = False
            for tbl in all_tables:
                if tbl != self.collection.name and tbl.startswith(
                    self.collection.name + "_"
                ):
                    if base_name.startswith(tbl + "_"):
                        is_longer_match = True
                        break
            if is_longer_match:
                continue

            if sql:
                # Parse tokenizer from CREATE VIRTUAL TABLE ... USING FTS5(..., tokenize=xxx)
                # Example: "CREATE VIRTUAL TABLE test USING FTS5(content, tokenize=porter)"
                match = re.search(r"tokenize\s*=\s*(\w+)", sql, re.IGNORECASE)
                if match:
                    tokenizer = match.group(1)
                    return f", tokenize={tokenizer}"

        # Default: no tokenizer specified
        return ""
