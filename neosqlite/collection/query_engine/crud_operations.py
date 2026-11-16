"""CRUD operations for the QueryEngine."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client_session import ClientSession

logger = logging.getLogger(__name__)

from neosqlite.binary import Binary
from neosqlite.collection.json_helpers import neosqlite_json_dumps_for_sql

from ...results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from ..query_helper import (
    _convert_bytes_to_binary,
    get_force_fallback,
)
from ..query_helper.utils import build_upsert_base_document
from ..type_correction import get_integer_id_for_doc_id
from ..type_utils import validate_session
from .base import QueryEngineProtocol


def _append_where_condition(where_clause: str, cond: str) -> str:
    """Append an extra ``AND`` predicate to an optional SQL ``WHERE`` clause.

    Args:
        where_clause: Existing ``WHERE`` clause string (may be empty or
            whitespace-only).
        cond: Boolean SQL predicate expression to wrap in parentheses and
            append.

    Returns:
        str: Combined SQL ``WHERE`` clause containing ``cond``.
    """
    if where_clause.strip():
        return f"{where_clause} AND ({cond})"
    return f"WHERE ({cond})"


class CRUDOperationsMixin(QueryEngineProtocol):
    """Mixin class providing CRUD operations for ``QueryEngine``.

    Implements ``insert_one``, ``insert_many``, ``update_one``,
    ``update_many``, ``delete_one``, ``delete_many``, and ``replace_one``
    with SQL-tier JSON/JSONB expression paths, GridFS metadata updates,
    session validation, tier-change notifications, and Tier 3 Python
    fallbacks.
    """

    def insert_one(
        self, document: dict[str, Any], session: ClientSession | None = None
    ) -> InsertOneResult:
        """Insert a single document into the collection.

        Args:
            document: Document dictionary to insert.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            InsertOneResult: Result containing the inserted document's ``_id``.
        """
        validate_session(session, self.collection._database)
        inserted_id = self.helpers._internal_insert(document)
        return InsertOneResult(inserted_id)

    def insert_many(
        self,
        documents: list[dict[str, Any]],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> InsertManyResult:
        """Insert multiple documents into the collection.

        Args:
            documents: List of document dictionaries to insert.
            ordered: Accepted for PyMongo API compatibility; documents are
                inserted sequentially in list order.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            InsertManyResult: Result containing the list of inserted document
                ``_id`` values in insertion order.
        """
        validate_session(session, self.collection._database)
        inserted_ids = [self.helpers._internal_insert(doc) for doc in documents]
        return InsertManyResult(inserted_ids)

    def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """Update a single document matching the filter criteria.

        Routes GridFS ``_files`` collections to specialized metadata updates,
        attempts single-query SQL JSON/JSONB expression updates when the
        filter and update operators are SQL-translatable, and falls back to
        Tier 3 Python updates otherwise.

        Args:
            filter: Query criteria selecting the document to update.
            update: Update operator specification to apply.
            upsert: If ``True``, inserts a new document when no document
                matches ``filter``. Defaults to ``False``.
            array_filters: Optional list of filter documents for positional
                array operators.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result containing ``matched_count``,
                ``modified_count``, and ``upserted_id``.
        """
        validate_session(session, self.collection._database)
        # Special handling for GridFS files collections
        if (
            self.collection._is_gridfs_collection()
            and self.collection.name.endswith("_files")
        ):
            return self._update_gridfs_file(filter, update, upsert)

        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None and not get_force_fallback():
            sql_expr_res = self.helpers._build_set_data_expression(
                update, array_filters
            )
            if sql_expr_res is not None and self.helpers._validate_inc_mul_sql(
                where_clause, params, update, limit_one=True
            ):
                data_expr, expr_params, op_tier = sql_expr_res
                tbl = quote_table_name(self.collection.name)
                if data_expr == "data":
                    row = self.collection.db.execute(
                        f"SELECT id FROM {tbl} {where_clause} LIMIT 1", params
                    ).fetchone()
                    if row is not None:
                        self._notify_tier_change(op_tier, [filter, update])
                        return UpdateResult(
                            matched_count=1, modified_count=0, upserted_id=None
                        )
                else:
                    row = self.collection.db.execute(
                        f"SELECT id, ({data_expr}) "
                        f"FROM {tbl} {where_clause} LIMIT 1",
                        expr_params + params,
                    ).fetchone()
                    if row is not None:
                        int_id, new_data = row
                        # Evaluate data_expr once in SELECT, then write only when
                        # serialized JSON differs so triggers/modified_count match.
                        cursor = self.collection.db.execute(
                            f"UPDATE {tbl} SET data = ? "
                            f"WHERE id = ? AND (json(data) IS NOT json(?))",
                            (new_data, int_id, new_data),
                        )
                        is_modified = max(cursor.rowcount, 0) > 0
                        self._notify_tier_change(op_tier, [filter, update])
                        return UpdateResult(
                            matched_count=1,
                            modified_count=1 if is_modified else 0,
                            upserted_id=None,
                        )
                self._notify_tier_change(op_tier, [filter, update])
                if upsert:
                    return self._execute_upsert(
                        filter, update, array_filters, session
                    )
                return UpdateResult(
                    matched_count=0, modified_count=0, upserted_id=None
                )

            # Fallback for non-SQL update expressions or $inc/$mul type errors
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                _, was_modified = self.helpers._internal_update(
                    int_id, update, doc, array_filters, filter
                )
                self._notify_tier_change("tier3", [filter, update])
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if was_modified else 0,
                    upserted_id=None,
                )
            self._notify_tier_change("tier3", [filter, update])
        else:
            # Python fallback when kill switch enabled or filter un-translatable
            self._notify_tier_change("tier3", [filter, update])
            if doc := self.find_one(filter, session=session):
                # Get integer id by looking up the stored ObjectId
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                _, was_modified = self.helpers._internal_update(
                    int_doc_id, update, doc, array_filters, filter
                )
                self._notify_tier_change("tier3", [filter, update])
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if was_modified else 0,
                    upserted_id=None,
                )

        if upsert:
            return self._execute_upsert(filter, update, array_filters, session)

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

    def _execute_upsert(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        array_filters: list[dict[str, Any]] | None,
        session: ClientSession | None,
    ) -> UpdateResult:
        """Build and insert an upserted document from filter and update specs.

        Constructs the base document from ``filter``, applies ``update``
        (including ``$setOnInsert``) via ``_internal_update``, and inserts the
        resulting document into the collection.

        Args:
            filter: Normalized query filter used to seed the base document.
            update: Update operator specification applied to the new document.
            array_filters: Optional filter predicates for positional array
                updates.
            session: Optional ``ClientSession`` forwarded to ``insert_one``.

        Returns:
            UpdateResult: Result with ``matched_count=0``,
                ``modified_count=0``, and ``upserted_id`` set to the inserted
                document's ``_id``.
        """
        new_doc = build_upsert_base_document(filter)
        updated_doc, _ = self.helpers._internal_update(
            0, update, new_doc, array_filters, filter
        )
        inserted_id = self.insert_one(updated_doc, session=session).inserted_id
        return UpdateResult(
            matched_count=0, modified_count=0, upserted_id=inserted_id
        )

    def _update_gridfs_file(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> UpdateResult:
        """Handle metadata updates for GridFS ``_files`` collections.

        Supports ``$set`` operations targeting ``metadata`` or nested
        ``metadata.<field>`` paths via SQLite JSON/JSONB ``set`` functions.
        Returns an unmatched ``UpdateResult`` if no file matches (including
        when ``upsert`` is ``True``, as file creation via upsert is not
        implemented here).

        Args:
            filter: Query filter identifying the GridFS file row.
            update: Update specification (inspected for ``"$set"`` metadata
                keys).
            upsert: Whether upsert was requested by the caller.

        Returns:
            UpdateResult: Result with ``matched_count=1`` and
                ``modified_count=1`` if a file matched, or zero counts
                otherwise.
        """
        from ...gridfs.grid_file import build_gridfs_files_where

        # Get the integer ID for the file
        filter = self.helpers._normalize_id_query(filter)
        where_clause, params = build_gridfs_files_where(filter)

        cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
        cursor = self.collection.db.execute(cmd, params)
        if not (row := cursor.fetchone()):
            if upsert:
                # For upsert, we would need to create a new file, but that's complex
                # For now, just return no match
                return UpdateResult(
                    matched_count=0, modified_count=0, upserted_id=None
                )
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=None
            )

        int_id = row[0]

        # Handle $set operations on metadata using SQL JSON functions
        if "$set" in update:
            set_clauses = []
            set_params = []

            for key, value in update["$set"].items():
                if key == "metadata":
                    # Update entire metadata column directly
                    # Use jsonb_set for consistency and gradual migration to JSONB storage
                    func_name = self.jsonb.function("set")

                    # Need to serialize dict/list to JSON for SQLite storage
                    if isinstance(value, (dict, list)):
                        set_clauses.append(
                            "metadata = "
                            + self.jsonb.json_placeholder(wrap_text=True)
                        )
                        set_params.append(neosqlite_json_dumps_for_sql(value))
                    else:
                        set_clauses.append("metadata = ?")
                        set_params.append(value)
                elif key.startswith("metadata."):
                    # Update nested field in metadata using json_set/jsonb_set
                    # The field path is like "metadata.priority", we need to update
                    # the metadata column with json_set(data, '$.priority', value)
                    field_path = key[9:]  # Remove "metadata."
                    json_path = f"'{parse_json_path(field_path)}'"

                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(value)

                    # Determine if we should use jsonb_* or json_* functions
                    func_name = self.jsonb.function("set")
                    meta_expr = "COALESCE(metadata, json('{}'))"

                    if isinstance(converted_val, (dict, list, Binary)):
                        # For complex/binary objects, serialize to JSON
                        set_clauses.append(
                            f"metadata = {func_name}({meta_expr}, {json_path}, json(?))"
                        )
                        set_params.append(
                            neosqlite_json_dumps_for_sql(converted_val)
                        )
                    else:
                        set_clauses.append(
                            f"metadata = {func_name}({meta_expr}, {json_path}, ?)"
                        )
                        set_params.append(converted_val)

            # Execute the update if we have any clauses
            if set_clauses:
                cmd = (
                    f"UPDATE {quote_table_name(self.collection.name)} "
                    f"SET {', '.join(set_clauses)} "
                    f"WHERE id = ?"
                )
                sql_params = set_params + [int_id]
                self.collection.db.execute(cmd, sql_params)

        return UpdateResult(matched_count=1, modified_count=1, upserted_id=None)

    def _get_integer_id_for_oid(self, oid) -> int:
        """Resolve a document ``_id`` to its SQLite integer row ``id``.

        Delegates to ``get_integer_id_for_doc_id`` to ensure consistent ID
        resolution across NeoSQLite components.

        Args:
            oid: Document ``_id`` value (such as ``ObjectId``, ``int``, or
                ``str``) to look up.

        Returns:
            int: Corresponding integer primary key from the collection table.

        Raises:
            ValueError: If no row with the given ``_id`` exists.
        """
        return get_integer_id_for_doc_id(
            self.collection.db, self.collection.name, oid
        )

    def update_many(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """Update all documents matching the filter criteria.

        Uses a bulk SQL ``UPDATE`` path when the filter and update operators
        are SQL-translatable, and falls back to Tier 3 per-document updates
        otherwise. Performs an upsert when no documents match and ``upsert``
        is ``True``.

        Args:
            filter: Query criteria selecting documents to update.
            update: Update operator specification to apply.
            upsert: If ``True``, inserts a new document when no document
                matches ``filter``. Defaults to ``False``.
            array_filters: Optional list of filter documents for positional
                array operators.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result containing ``matched_count``,
                ``modified_count``, and ``upserted_id``.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Try to use SQLTranslator for the WHERE clause
        where_clause, where_params = self.sql_translator.translate_match(filter)

        # Fallback / SQL path for update_many
        if where_clause is not None and not get_force_fallback():
            sql_expr_res = self.helpers._build_set_data_expression(
                update, array_filters
            )
            if sql_expr_res is not None and self.helpers._validate_inc_mul_sql(
                where_clause, where_params, update, limit_one=False
            ):
                data_expr, expr_params, op_tier = sql_expr_res
                tbl = quote_table_name(self.collection.name)
                matched_count = self.collection.db.execute(
                    f"SELECT COUNT(*) FROM {tbl} {where_clause}", where_params
                ).fetchone()[0]
                if matched_count == 0:
                    self._notify_tier_change(op_tier, [filter, update])
                    if upsert:
                        return self._execute_upsert(
                            filter, update, array_filters, session
                        )
                    return UpdateResult(
                        matched_count=0, modified_count=0, upserted_id=None
                    )
                if data_expr == "data":
                    modified_count = 0
                else:
                    update_where = _append_where_condition(
                        where_clause, f"json(data) IS NOT json({data_expr})"
                    )
                    cursor = self.collection.db.execute(
                        f"UPDATE {tbl} SET data = {data_expr} {update_where}",
                        expr_params + where_params + expr_params,
                    )
                    modified_count = max(cursor.rowcount, 0)
                self._notify_tier_change(op_tier, [filter, update])
                return UpdateResult(
                    matched_count=matched_count,
                    modified_count=modified_count,
                    upserted_id=None,
                )

            cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause}"
            cursor = self.collection.db.execute(cmd, where_params)
            ids = [row[0] for row in cursor.fetchall()]
            fallback_tier = "tier3"
        else:
            # If we can't translate the filter, we'll need to get all docs and filter in memory
            docs = list(self.find(filter, session=session))
            ids = []
            for doc in docs:
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                ids.append(int_doc_id)
            fallback_tier = "tier3"

        self._notify_tier_change(fallback_tier, [filter, update])
        if not ids and upsert:
            return self._execute_upsert(filter, update, array_filters, session)

        modified_count = 0
        for int_doc_id in ids:
            # Get the document using the integer ID for the update
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
            )
            cursor = self.collection.db.execute(cmd, (int_doc_id,))
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                _, was_modified = self.helpers._internal_update(
                    int_doc_id, update, doc, array_filters, filter
                )
                if was_modified:
                    modified_count += 1
        return UpdateResult(
            matched_count=len(ids),
            modified_count=modified_count,
            upserted_id=None,
        )

    def delete_one(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """Delete a single document matching the filter criteria.

        Returns ``deleted_count=0`` when the collection table does not exist.
        Uses SQL ``LIMIT 1`` lookup when ``filter`` can be translated and
        falls back to ``find_one`` otherwise.

        Args:
            filter: Query criteria identifying the document to delete.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            DeleteResult: Result with ``deleted_count`` set to ``1`` if a
                document was deleted or ``0`` otherwise.
        """
        validate_session(session, self.collection._database)

        # Check if the table exists - if not, return 0 deleted
        table_name = quote_table_name(self.collection.name)
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (self.collection.name,),
        )
        if cursor.fetchone() is None:
            return DeleteResult(deleted_count=0)

        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Use direct query to get integer ID for the delete operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None and not get_force_fallback():
            cmd = f"SELECT id FROM {table_name} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id = row[0]
                self.helpers._internal_delete(int_id)
                return DeleteResult(deleted_count=1)
        else:
            # Fallback approach
            if doc := self.find_one(filter, session=session):
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                self.helpers._internal_delete(int_doc_id)
                return DeleteResult(deleted_count=1)
        return DeleteResult(deleted_count=0)

    def delete_many(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """Delete all documents matching the filter criteria.

        Returns ``deleted_count=0`` when the collection table does not exist.
        Uses a direct SQL ``DELETE`` statement when ``filter`` translates to
        SQL, or falls back to querying matching documents and deleting by
        integer ``id``.

        Args:
            filter: Query criteria identifying the documents to delete.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            DeleteResult: Result containing the count of deleted documents.
        """
        validate_session(session, self.collection._database)

        # Check if the table exists - if not, return 0 deleted
        table_name = quote_table_name(self.collection.name)
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (self.collection.name,),
        )
        if cursor.fetchone() is None:
            return DeleteResult(deleted_count=0)

        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Try to use SQLTranslator for the WHERE clause
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None and not get_force_fallback():
            cmd = f"DELETE FROM {table_name} {where_clause}"
            cursor = self.collection.db.execute(cmd, params)
            return DeleteResult(deleted_count=cursor.rowcount)

        # Fallback for complex queries
        # Fallback to finding documents and getting their integer IDs
        docs = list(self.find(filter, session=session))
        if not docs:
            return DeleteResult(deleted_count=0)
        ids = []
        for d in docs:
            int_doc_id = self._get_integer_id_for_oid(d["_id"])
            ids.append(int_doc_id)

        if not ids:
            return DeleteResult(deleted_count=0)

        placeholders = ",".join("?" for _ in ids)
        self.collection.db.execute(
            f"DELETE FROM {quote_table_name(self.collection.name)} WHERE id IN ({placeholders})",
            ids,
        )
        return DeleteResult(deleted_count=len(ids))

    def replace_one(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        upsert: bool = False,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """Replace a single document matching the filter criteria.

        Uses SQL lookup when ``filter`` can be translated, or falls back to
        ``find_one``, then replaces the matched row via ``_internal_replace``.
        When no document matches and ``upsert`` is ``True``, inserts
        ``replacement`` instead.

        Args:
            filter: Query criteria identifying the document to replace.
            replacement: Replacement document dictionary.
            upsert: If ``True``, inserts ``replacement`` when no document
                matches ``filter``. Defaults to ``False``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            UpdateResult: Result containing ``matched_count``,
                ``modified_count``, and ``upserted_id``.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Find the document using the filter, but get the integer ID for internal operations
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None and not get_force_fallback():
            data_col = self.jsonb.data_column()
            cmd = f"SELECT id, _id, {data_col} as data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                self.helpers._internal_replace(int_id, replacement)
                return UpdateResult(
                    matched_count=1, modified_count=1, upserted_id=None
                )
        else:
            # Fallback approach
            if doc := self.find_one(filter, session=session):
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                self.helpers._internal_replace(int_doc_id, replacement)
                return UpdateResult(
                    matched_count=1, modified_count=1, upserted_id=None
                )

        if upsert:
            inserted_id = self.insert_one(
                replacement, session=session
            ).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)
