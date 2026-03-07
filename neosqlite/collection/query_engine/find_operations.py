"""Find operations for the QueryEngine."""

from __future__ import annotations

from typing import Any, Dict

from .base import QueryEngineProtocol
from ...sql_utils import quote_table_name
from ..cursor import Cursor
from ..raw_batch_cursor import RawBatchCursor


class FindOperationsMixin(QueryEngineProtocol):
    """Mixin class providing find operations for QueryEngine."""

    def find(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> Cursor:
        """
        Query the database and retrieve documents matching the provided filter.

        Args:
            filter (Dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (Dict[str, Any] | None): A dictionary specifying which fields to return.
            hint (str | None): A string specifying the index to use.

        Returns:
            Cursor: A cursor object to iterate over the results.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        return Cursor(self.collection, filter, projection, hint)

    def find_raw_batches(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
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

        Returns:
            RawBatchCursor instance.

        Example usage:

        >>> import json
        >>> cursor = collection.find_raw_batches()
        >>> for batch in cursor:
        ...     # Each batch is raw bytes containing JSON documents separated by newlines.
        ...     documents = [json.loads(doc) for doc in batch.decode('utf-8').split('\\n') if doc]
        ...     print(documents)
        """
        return RawBatchCursor(
            self.collection, filter, projection, hint, batch_size
        )

    def find_one(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> Dict[str, Any] | None:
        """
        Find a single document matching the filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter conditions.
            projection (Dict[str, Any]): A dictionary specifying which fields to return.
            hint (str): A string specifying the index to use (not used in SQLite).

        Returns:
            Dict[str, Any]: A dictionary representing the found document,
                            or None if no document matches.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        try:
            return next(iter(self.find(filter, projection, hint).limit(1)))
        except StopIteration:
            return None

    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        """
        Deletes a document that matches the filter and returns it.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria.

        Returns:
            Dict[str, Any] | None: The document that was deleted,
                                   or None if no document matches.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Find document and get its integer ID for the delete operation
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
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_delete(int_id)
                return doc
        else:
            # Fallback approach
            if doc := self.find_one(filter):
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                self.helpers._internal_delete(int_doc_id)
                return doc
        return None

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        """
        Replaces a single document in the collection based on a filter with a new document.

        This method first finds a document matching the filter, then replaces it
        with the new document. If the document is found and replaced, the original
        document is returned; otherwise, None is returned.

        Args:
            filter (Dict[str, Any]): A dictionary representing the filter to search for the document to replace.
            replacement (Dict[str, Any]): A dictionary representing the new document to replace the existing one.

        Returns:
            Dict[str, Any] | None: The original document that was replaced,
                                   or None if no document was found and replaced.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        # Find document and get its integer ID for the replace operation
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
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_replace(int_id, replacement)
                return original_doc
        else:
            # Fallback approach
            if doc := self.find_one(filter):
                int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                self.helpers._internal_replace(int_doc_id, replacement)
                return doc
        return None

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        """
        Find and update a single document.

        Finds a document matching the given filter, updates it using the specified
        update expression, and returns the original document (before update).

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria for the document to find.
            update (Dict[str, Any]): A dictionary specifying the update operations to perform on the document.

        Returns:
            Dict[str, Any] | None: The original document (before update),
                                   or None if no document was found and updated.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        if doc := self.find_one(filter):
            # Get the integer id for the internal operation
            int_doc_id = self._get_integer_id_for_oid(doc["_id"])
            # Update by integer id to avoid conflicts
            where_clause = "WHERE id = ?"
            params = [int_doc_id]
            if self._jsonb_supported:
                cmd = f"SELECT id, _id, json(data) as data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            else:
                cmd = f"SELECT id, _id, data FROM {quote_table_name(self.collection.name)} {where_clause} LIMIT 1"
            cursor = self.collection.db.execute(cmd, params)
            row = cursor.fetchone()
            if row:
                int_id, stored_id, data = row
                doc_to_update = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_update(int_id, update, doc_to_update)
        return doc
