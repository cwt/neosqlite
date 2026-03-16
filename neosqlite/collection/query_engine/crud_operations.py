"""CRUD operations for the QueryEngine."""

from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client_session import ClientSession

from .base import QueryEngineProtocol
from ...results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from ...sql_utils import quote_table_name
from ..query_helper import _convert_bytes_to_binary, _get_json_function
from ..json_path_utils import parse_json_path
from ..type_correction import get_integer_id_for_oid
from neosqlite.binary import Binary
from neosqlite.collection.json_helpers import neosqlite_json_dumps_for_sql


from ..type_utils import validate_session


class CRUDOperationsMixin(QueryEngineProtocol):
    """Mixin class providing CRUD operations for QueryEngine."""

    def insert_one(
        self, document: Dict[str, Any], session: ClientSession | None = None
    ) -> InsertOneResult:
        """
        Insert a single document into the collection.

        Args:
            document (Dict[str, Any]): The document to insert.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            InsertOneResult: The result of the insert operation.
        """
        validate_session(session, self.collection._database)
        inserted_id = self.helpers._internal_insert(document)
        return InsertOneResult(inserted_id)

    def insert_many(
        self,
        documents: List[Dict[str, Any]],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> InsertManyResult:
        """
        Insert multiple documents into the collection.

        Args:
            documents (List[Dict[str, Any]]): List of documents to insert.
            ordered (bool, optional): If True, insert documents in the order they
                                      appear in the list. If an error occurs,
                                      the operation will stop. If False, the
                                      operation will continue even if an error
                                      occurs.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            InsertManyResult: Result of the insert operation, containing a list of inserted document IDs.
        """
        validate_session(session, self.collection._database)
        inserted_ids = [self.helpers._internal_insert(doc) for doc in documents]
        return InsertManyResult(inserted_ids)

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        array_filters: List[Dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        Updates a single document in the collection based on the provided filter
        and update operations.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query criteria for finding the document to update.
            update (Dict[str, Any]): A dictionary specifying the update operations to apply to the document.
            upsert (bool, optional): If True, inserts a new document if no document matches the filter. Defaults to False.
            array_filters (List[Dict[str, Any]], optional): A list of filter documents for array positional operators.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            UpdateResult: An object containing information about the update operation,
                          including the count of matched and modified documents,
                          and the upserted ID if applicable.
        """
        validate_session(session, self.collection._database)
        # Special handling for GridFS files collections
        if self.collection.name.endswith("_files"):
            return self._update_gridfs_file(filter, update, upsert)

        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Try fast path: use simple SQL UPDATE without fetching document first
        # This only works for simple operations that don't need to read the document
        if not array_filters and not upsert:
            fast_result = self._try_fast_update_one(filter, update)
            if fast_result is not None:
                return fast_result

        # Fall back to the original implementation that fetches the document first
        # Find the document using the filter, but we need to work with integer IDs internally
        # For internal operations, we need to retrieve the document differently to get the integer id
        # We'll use a direct SQL query to get both the integer id and the stored _id
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            # Get the integer id as well for internal operations
            # Use the same approach as the original code considering JSONB support
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                # Load the document the normal way for the update processing
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                # Use the integer id for internal operations
                _, was_modified = self.helpers._internal_update(
                    int_id, update, doc, array_filters, filter
                )
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if was_modified else 0,
                    upserted_id=None,
                )
        else:
            # Fallback to find_one if translation doesn't work
            if doc := self.find_one(filter):
                # Get integer id by looking up the stored ObjectId
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                _, was_modified = self.helpers._internal_update(
                    int_doc_id, update, doc, array_filters, filter
                )
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if was_modified else 0,
                    upserted_id=None,
                )

        if upsert:
            # For upsert, we need to create a document that includes:
            # 1. The filter fields (as base document)
            # 2. Apply the update operations to that document
            new_doc: Dict[str, Any] = dict(filter)  # Start with filter fields
            updated_doc, _ = self.helpers._internal_update(
                0, update, new_doc, array_filters, filter
            )  # Apply updates
            inserted_id = self.insert_one(updated_doc).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

    def _update_gridfs_file(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> UpdateResult:
        """Handle updates for GridFS files collections."""
        # Get the integer ID for the file
        filter = self.helpers._normalize_id_query(filter)
        where_clause, params = self.sql_translator.translate_match(filter)
        if not where_clause:
            where_clause = ""

        cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
        cursor = self.collection.db.execute(cmd, params)
        row = cursor.fetchone()
        if not row:
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
                    func_name = _get_json_function("set", self._jsonb_supported)

                    # Need to serialize dict/list to JSON for SQLite storage
                    if isinstance(value, (dict, list)):
                        if self._jsonb_supported:
                            # Use jsonb_set to store as JSONB for better performance
                            set_clauses.append(
                                f"metadata = {func_name}(metadata, '$', json(?))"
                            )
                            set_params.append(
                                neosqlite_json_dumps_for_sql(value)
                            )
                        else:
                            # Fallback to json() for non-JSONB databases
                            set_clauses.append("metadata = json(?)")
                            set_params.append(
                                neosqlite_json_dumps_for_sql(value)
                            )
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
                    func_name = _get_json_function("set", self._jsonb_supported)

                    if isinstance(converted_val, (dict, list)):
                        # For complex objects, serialize to JSON
                        set_clauses.append(
                            f"metadata = {func_name}(metadata, {json_path}, json(?))"
                        )
                        set_params.append(
                            neosqlite_json_dumps_for_sql(converted_val)
                        )
                    elif isinstance(converted_val, Binary):
                        set_clauses.append(
                            f"metadata = {func_name}(metadata, {json_path}, json(?))"
                        )
                        set_params.append(
                            neosqlite_json_dumps_for_sql(converted_val)
                        )
                    else:
                        set_clauses.append(
                            f"metadata = {func_name}(metadata, {json_path}, ?)"
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
        """
        Get the integer ID for a given ObjectId.

        This method delegates to the centralized get_integer_id_for_oid function
        to ensure consistent ID handling across all NeoSQLite components.

        Args:
            oid: The ObjectId to look up.

        Returns:
            int: The corresponding integer ID from the database.

        Raises:
            ValueError: If the integer ID for the ObjectId cannot be found.
        """
        return get_integer_id_for_oid(
            self.collection.db, self.collection.name, oid
        )

    def _try_fast_update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
    ) -> UpdateResult | None:
        """
        Try to use a fast SQL UPDATE without fetching the document first.

        This method attempts to execute a simple UPDATE in a single SQL statement
        without needing to first SELECT the document. This is much faster for
        simple field updates.

        Args:
            filter: The query filter
            update: The update operations

        Returns:
            UpdateResult if fast path was successful, None otherwise
        """
        from ..query_helper.utils import get_force_fallback

        if get_force_fallback():
            return None

        simple_ops = {
            "$set",
            "$min",
            "$max",
            "$unset",
            "$currentDate",
            "$inc",
            "$mul",
            "$setOnInsert",
        }
        complex_ops = {
            "$push",
            "$pull",
            "$pullAll",
            "$pop",
            "$addToSet",
            "$rename",
        }

        update_keys = set(update.keys())
        if update_keys & complex_ops:
            return None

        if not update_keys.issubset(simple_ops):
            return None

        for op_key in update_keys:
            op_value = update[op_key]
            if isinstance(op_value, dict):
                for field_path in op_value.keys():
                    if "$" in field_path or field_path.startswith("[]"):
                        return None

        update_result = self.helpers._build_update_clause(update)
        if update_result is None:
            return None

        set_clause, set_params = update_result

        where_clause, where_params = self.sql_translator.translate_match(filter)
        if where_clause is None:
            return None

        if "$inc" in update_keys or "$mul" in update_keys:
            from ..query_helper.update_operations import UpdateOperationsMixin

            if not UpdateOperationsMixin._validate_inc_mul_types_sql(
                self.collection.db,
                self.collection.name,
                where_clause,
                where_params,
                update,
                self._jsonb_supported,
            ):
                return None

        try:
            # For update_one, we MUST only update a single document.
            # Since standard SQLite doesn't support LIMIT in UPDATE (without a compile flag),
            # we use a subquery with LIMIT 1 to identify the specific row.
            cmd = (
                f"UPDATE {quote_table_name(self.collection.name)} "
                f"SET {set_clause} "
                f"WHERE id IN (SELECT id FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1)"
            )
            cursor = self.collection.db.execute(cmd, set_params + where_params)

            if cursor.rowcount > 0:
                return UpdateResult(
                    matched_count=1,
                    modified_count=1,
                    upserted_id=None,
                )
            elif cursor.rowcount == 0:
                return UpdateResult(
                    matched_count=0,
                    modified_count=0,
                    upserted_id=None,
                )
        except Exception:
            return None

        return None

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        array_filters: List[Dict[str, Any]] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        Update multiple documents based on a filter.

        This method updates documents in the collection that match the given filter
        using the specified update.

        Args:
            filter (Dict[str, Any]): A dictionary representing the filter to select documents to update.
            update (Dict[str, Any]): A dictionary representing the updates to apply.
            upsert (bool, optional): If True, inserts a new document if no document matches the filter. Defaults to False.
            array_filters (List[Dict[str, Any]], optional): A list of filter documents for array positional operators.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            UpdateResult: A result object containing information about the update operation.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Try to use SQLTranslator for the WHERE clause
        where_clause, where_params = self.sql_translator.translate_match(filter)

        # Get the update clause using existing helper
        update_result = self.helpers._build_update_clause(update)

        if where_clause is not None and update_result is not None:
            set_clause, set_params = update_result
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET {set_clause} {where_clause}"
            cursor = self.collection.db.execute(cmd, set_params + where_params)
            return UpdateResult(
                matched_count=cursor.rowcount,
                modified_count=cursor.rowcount,
                upserted_id=None,
            )

        # Fallback for complex queries
        # Get the integer IDs for the documents to update
        where_clause, where_params = self.sql_translator.translate_match(filter)
        if where_clause is not None:
            cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause}"
            cursor = self.collection.db.execute(cmd, where_params)
            ids = [row[0] for row in cursor.fetchall()]
        else:
            # If we can't translate the filter, we'll need to get all docs and filter in memory
            docs = list(self.find(filter))
            ids = []
            for doc in docs:
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                ids.append(int_doc_id)

        modified_count = 0
        for int_doc_id in ids:
            # Get the document using the integer ID for the update
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} WHERE id = ?"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} WHERE id = ?"
            cursor = self.collection.db.execute(cmd, (int_doc_id,))
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_update(int_doc_id, update, doc)
                modified_count += 1
        return UpdateResult(
            matched_count=len(ids),
            modified_count=modified_count,
            upserted_id=None,
        )

    def delete_one(
        self, filter: Dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """
        Delete a single document matching the filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter conditions
                                     for the document to delete.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            DeleteResult: A result object indicating whether the deletion was
                          successful or not.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Use direct query to get integer ID for the delete operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id = row[0]
                self.helpers._internal_delete(int_id)
                return DeleteResult(deleted_count=1)
        else:
            # Fallback approach
            if doc := self.find_one(filter):
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                self.helpers._internal_delete(int_doc_id)
                return DeleteResult(deleted_count=1)
        return DeleteResult(deleted_count=0)

    def delete_many(
        self, filter: Dict[str, Any], session: ClientSession | None = None
    ) -> DeleteResult:
        """
        Deletes multiple documents in the collection that match the provided filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query criteria
                                     for finding the documents to delete.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            DeleteResult: A result object indicating whether the deletion was successful or not.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Try to use SQLTranslator for the WHERE clause
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None:
            cmd = f"DELETE FROM {quote_table_name(self.collection.name)} {where_clause}"
            cursor = self.collection.db.execute(cmd, params)
            return DeleteResult(deleted_count=cursor.rowcount)

        # Fallback for complex queries
        # Get integer IDs for the documents to delete
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause is not None:
            # Use direct SQL if possible
            cmd = f"SELECT id FROM {quote_table_name(self.collection.name)} {where_clause}"
            cursor = self.collection.db.execute(cmd, params)
            ids = [row[0] for row in cursor.fetchall()]
        else:
            # Fallback to finding documents and getting their integer IDs
            docs = list(self.find(filter))
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
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = False,
        session: ClientSession | None = None,
    ) -> UpdateResult:
        """
        Replace one document in the collection that matches the filter with the
        replacement document.

        Args:
            filter (Dict[str, Any]): A query that matches the document to replace.
            replacement (Dict[str, Any]): The new document that replaces the matched document.
            upsert (bool, optional): If true, inserts the replacement document if no document matches the filter.
                                     Default is False.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            UpdateResult: A result object containing the number of matched and
                          modified documents and the upserted ID.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Find the document using the filter, but get the integer ID for internal operations
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                self.helpers._internal_replace(int_id, replacement)
                return UpdateResult(
                    matched_count=1, modified_count=1, upserted_id=None
                )
        else:
            # Fallback approach
            if doc := self.find_one(filter):
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
