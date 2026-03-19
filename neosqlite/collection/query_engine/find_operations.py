"""Find operations for the QueryEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Tuple

if TYPE_CHECKING:
    from ..client_session import ClientSession

from ...sql_utils import quote_table_name
from ..cursor import Cursor
from ..json_path_utils import parse_json_path
from ..raw_batch_cursor import RawBatchCursor
from ..type_utils import validate_session
from .base import QueryEngineProtocol


class FindOperationsMixin(QueryEngineProtocol):
    """Mixin class providing find operations for QueryEngine."""

    def find(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        """
        Query the database and retrieve documents matching the provided filter.

        Args:
            filter (Dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (Dict[str, Any] | None): A dictionary specifying which fields to return.
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
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
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
            filter (Dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (Dict[str, Any] | None): A dictionary specifying which fields to return.
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
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Dict[str, Any] | None:
        """
        Find a single document matching the filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter conditions.
            projection (Dict[str, Any]): A dictionary specifying which fields to return.
            hint (str): A string specifying the index to use (not used in SQLite).
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            Dict[str, Any]: A dictionary representing the found document,
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
        filter: Dict[str, Any],
        projection: Dict[str, Any] | None = None,
        sort: List[Tuple[str, int]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> Dict[str, Any] | None:
        """
        Find a single document and delete it.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria.
            projection (Dict[str, Any]): A dictionary specifying which fields to return.
            sort (List[Tuple[str, int]]): A list of (key, direction) pairs for sorting.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any] | None: The document before it was deleted,
                                 or None if not found.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Build sorting clause
        order_by = ""
        if sort:
            sort_clauses = []
            for key, direction in sort:
                sort_clauses.append(
                    f"json_extract(data, '{parse_json_path(key)}') {'DESC' if direction == -1 else 'ASC'}"
                )
            order_by = "ORDER BY " + ", ".join(sort_clauses)

        # Use direct query to get integer ID for the delete operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_delete(int_id)
                return doc
        else:
            # Fallback approach
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

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        projection: Dict[str, Any] | None = None,
        sort: List[Tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> Dict[str, Any] | None:
        """
        Find a single document and replace it.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria.
            replacement (Dict[str, Any]): The replacement document.
            projection (Dict[str, Any]): A dictionary specifying which fields to return.
            sort (List[Tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any] | None: The document before or after replacement,
                                   or None if not found.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Build sorting clause
        order_by = ""
        if sort:
            sort_clauses = []
            for key, direction in sort:
                sort_clauses.append(
                    f"json_extract(data, '{parse_json_path(key)}') {'DESC' if direction == -1 else 'ASC'}"
                )
            order_by = "ORDER BY " + ", ".join(sort_clauses)

        # Find document and get its integer ID for the replace operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
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
        else:
            # Fallback approach
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

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        projection: Dict[str, Any] | None = None,
        sort: List[Tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        array_filters: List[Dict[str, Any]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> Dict[str, Any] | None:
        """
        Find and update a single document.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria.
            update (Dict[str, Any]): A dictionary specifying the update operations.
            projection (Dict[str, Any]): A dictionary specifying which fields to return.
            sort (List[Tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            array_filters (List[Dict[str, Any]]): Filters for array updates.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any] | None: The original document (before update),
                                   or None if no document was found and updated.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Build sorting clause
        order_by = ""
        if sort:
            sort_clauses = []
            for key, direction in sort:
                sort_clauses.append(
                    f"json_extract(data, '{parse_json_path(key)}') {'DESC' if direction == -1 else 'ASC'}"
                )
            order_by = "ORDER BY " + ", ".join(sort_clauses)

        # Find document and get its integer ID for the update operation
        where_clause, params = self.sql_translator.translate_match(filter)
        if where_clause:
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} {order_by} LIMIT 1"
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
        else:
            # Fallback approach
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
