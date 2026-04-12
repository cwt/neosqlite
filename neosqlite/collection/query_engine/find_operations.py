"""Find operations for the QueryEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client_session import ClientSession

from ...sql_utils import quote_table_name
from ..cursor import Cursor
from ..jsonb_support import json_data_column

# Import feature detection
from ..query_helper import _supports_returning_clause, get_force_fallback
from ..raw_batch_cursor import RawBatchCursor
from ..type_utils import validate_session
from .base import QueryEngineProtocol


class FindOperationsMixin(QueryEngineProtocol):
    """Mixin class providing find operations for QueryEngine."""

    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        """
        Query the database and retrieve documents matching the provided filter.

        Args:
            filter (dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (dict[str, Any] | None): A dictionary specifying which fields to return.
            hint (str | None): A string specifying the index to use.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            Cursor: A cursor object to iterate over the results.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        return Cursor(
            self.collection, filter, projection, hint, session=session
        )

    def find_raw_batches(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """
        Query the database and retrieve batches of raw JSON.

        Similar to the :meth:`find` method but returns a
        :class:`~neosqlite.raw_batch_cursor.RawBatchCursor`.

        This method returns raw JSON batches which can be more efficient for
        certain use cases where you want to process data in batches rather than
        individual documents.

        Args:
            filter (dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (dict[str, Any] | None): A dictionary specifying which fields to return.
            hint (str | None): A string specifying the index to use.
            batch_size (int): The number of documents to include in each batch.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            RawBatchCursor instance.
        """
        validate_session(session, self.collection._database)
        return RawBatchCursor(
            self.collection,
            filter,
            projection,
            hint,
            batch_size,
            session=session,
        )

    def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, Any] | None:
        """
        Find a single document matching the filter.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter conditions.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            hint (str): A string specifying the index to use (not used in SQLite).
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            dict[str, Any]: A dictionary representing the found document,
                            or None if no document matches.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        try:
            return next(
                iter(
                    self.find(filter, projection, hint, session=session).limit(
                        1
                    )
                )
            )
        except StopIteration:
            return None

    def find_one_and_delete(
        self,
        filter: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Find a single document and delete it.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The document before it was deleted,
                                 or None if not found.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Check if RETURNING clause is supported (Tier-1 optimization)
        use_returning = (
            _supports_returning_clause() and not get_force_fallback()
        )

        # Build sorting clause
        order_by = ""
        if sort:
            order_by = self.helpers._build_sort_clause(dict(sort))

        # Use direct query to get integer ID for the delete operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if not where_clause:
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, projection, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    self.helpers._internal_delete(int_doc_id)
                    return doc
            except StopIteration:
                pass
            return None

        if use_returning:
            # Tier-1: Use RETURNING clause for atomic find-and-delete
            # Note: SQLite doesn't support LIMIT with DELETE RETURNING directly
            # We need to use a subquery approach
            jsonb = self._jsonb_supported
            data_col = json_data_column(jsonb)
            cmd = (
                f"DELETE FROM {quote_table_name(self.collection.name)} "
                f"WHERE id = (SELECT id FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1) "
                f"RETURNING id, _id, {data_col} as data"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                return self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
            return None
        else:
            # Tier-1 (Fallback): Two-step process (SELECT then DELETE)
            # Used when RETURNING clause is not supported (SQLite < 3.35.0)
            jsonb = self._jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_delete(int_id)
                return doc
            return None

    def find_one_and_replace(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Find a single document and replace it.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            replacement (dict[str, Any]): The replacement document.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The document before or after replacement,
                                   or None if not found.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Check if RETURNING clause is supported (Tier-1 optimization)
        use_returning = (
            _supports_returning_clause() and not get_force_fallback()
        )

        # Build sorting clause
        order_by = ""
        if sort:
            order_by = self.helpers._build_sort_clause(dict(sort))

        # Find document and get its integer ID for the replace operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if not where_clause:
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, projection, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    self.helpers._internal_replace(int_doc_id, replacement)
                    if return_document:
                        return self.find_one(
                            {"_id": doc["_id"]}, projection, session=session
                        )
                    return doc
            except StopIteration:
                pass

            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None

        if use_returning:
            # Tier-1: Use RETURNING clause for atomic find-and-replace
            jsonb = self._jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                # Perform the replace
                self.helpers._internal_replace(int_id, replacement)
                if return_document:
                    # Use RETURNING to get the updated document
                    jsonb = self._jsonb_supported
                    data_col = json_data_column(jsonb)
                    update_cmd = (
                        f"UPDATE {quote_table_name(self.collection.name)} "
                        f"SET data = ? WHERE id = ? "
                        f"RETURNING id, _id, {data_col} as data"
                    )
                    from ..json_helpers import neosqlite_json_dumps

                    update_cursor = self.collection.db.execute(
                        update_cmd, (neosqlite_json_dumps(replacement), int_id)
                    )
                    update_row = update_cursor.fetchone()
                    if update_row:
                        return self.collection._load_with_stored_id(
                            update_row[0], update_row[2], update_row[1]
                        )
                return original_doc
            # No document found, handle upsert
            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None
        else:
            # Tier-1 (Fallback): Two-step process
            # Used when RETURNING clause is not supported (SQLite < 3.35.0)
            jsonb = self._jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_replace(int_id, replacement)
                if return_document:
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                return original_doc

            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None

            return None

    def find_one_and_update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Find and update a single document.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            update (dict[str, Any]): A dictionary specifying the update operations.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            array_filters (list[dict[str, Any]]): Filters for array updates.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The original document (before update),
                                   or None if no document was found and updated.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Check if RETURNING clause is supported (Tier-1 optimization)
        use_returning = (
            _supports_returning_clause() and not get_force_fallback()
        )

        # Build sorting clause
        order_by = ""
        if sort:
            order_by = self.helpers._build_sort_clause(dict(sort))

        # Find document and get its integer ID for the update operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if not where_clause:
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, projection, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    # Load the document for the update processing
                    doc_to_update = self.find_one(
                        {"_id": doc["_id"]}, session=session
                    )
                    if doc_to_update is not None:
                        self.helpers._internal_update(
                            int_doc_id,
                            update,
                            doc_to_update,
                            array_filters,
                            filter,
                        )
                    if return_document:
                        return self.find_one(
                            {"_id": doc["_id"]}, projection, session=session
                        )
                    return doc
            except StopIteration:
                pass

            if upsert:
                # Basic upsert logic
                new_doc = dict(filter)
                res = self.insert_one(new_doc, session=session)
                self.update_one(
                    {"_id": res.inserted_id},
                    update,
                    array_filters=array_filters,
                    session=session,
                )
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None

        if use_returning:
            # Tier-1: Use RETURNING clause for atomic find-and-update
            jsonb = self._jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                # Perform the update
                self.helpers._internal_update(
                    int_id, update, original_doc, array_filters, filter
                )
                if return_document:
                    # Fetch the updated document
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                return original_doc
            # No document found, handle upsert
            if upsert:
                new_doc = dict(filter)
                res = self.insert_one(new_doc, session=session)
                self.update_one(
                    {"_id": res.inserted_id},
                    update,
                    array_filters=array_filters,
                    session=session,
                )
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None
        else:
            # Tier-1 (Fallback): Two-step process
            # Used when RETURNING clause is not supported (SQLite < 3.35.0)
            jsonb = self._jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_update(
                    int_id, update, original_doc, array_filters, filter
                )
                if return_document:
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                return original_doc

            if upsert:
                # Basic upsert logic
                new_doc = dict(filter)
                res = self.insert_one(new_doc, session=session)
                self.update_one(
                    {"_id": res.inserted_id},
                    update,
                    array_filters=array_filters,
                    session=session,
                )
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None

            return None
