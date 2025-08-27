from ..bulk_operations import BulkOperationExecutor
from ..cursor import Cursor, DESCENDING
from ..exceptions import MalformedQueryException
from ..raw_batch_cursor import RawBatchCursor
from ..requests import DeleteOne, InsertOne, UpdateOne
from ..results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from .query_helper import QueryHelper
from neosqlite.collection.json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)
from typing import Any, Dict, List
import json


class QueryEngine:
    """
    A class that provides methods for querying and manipulating documents in a collection.

    The QueryEngine handles all database operations including inserting, updating, deleting,
    and finding documents. It also supports aggregation pipelines, bulk operations, and
    various utility methods for counting and retrieving distinct values.
    """

    def __init__(self, collection):
        """
        Initialize the QueryEngine with a collection.

        Args:
            collection: The collection instance this QueryEngine will operate on.
        """
        self.collection = collection
        self.helpers = QueryHelper(collection)

    def insert_one(self, document: Dict[str, Any]) -> InsertOneResult:
        """
        Insert a single document into the collection.

        Args:
            document (Dict[str, Any]): The document to insert.

        Returns:
            InsertOneResult: The result of the insert operation, containing the inserted document ID.
        """
        inserted_id = self.helpers._internal_insert(document)
        return InsertOneResult(inserted_id)

    def insert_many(self, documents: List[Dict[str, Any]]) -> InsertManyResult:
        """
        Insert multiple documents into the collection.

        Args:
            documents (List[Dict[str, Any]]): List of documents to insert.

        Returns:
            InsertManyResult: Result of the insert operation, containing a list of inserted document IDs.
        """
        inserted_ids = [self.helpers._internal_insert(doc) for doc in documents]
        return InsertManyResult(inserted_ids)

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Updates a single document in the collection based on the provided filter
        and update operations.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query criteria for finding the document to update.
            update (Dict[str, Any]): A dictionary specifying the update operations to apply to the document.
            upsert (bool, optional): If True, inserts a new document if no document matches the filter. Defaults to False.

        Returns:
            UpdateResult: An object containing information about the update operation,
                          including the count of matched and modified documents,
                          and the upserted ID if applicable.
        """
        doc = self.find_one(filter)
        if doc:
            self.helpers._internal_update(doc["_id"], update, doc)
            return UpdateResult(
                matched_count=1, modified_count=1, upserted_id=None
            )

        if upsert:
            # For upsert, we need to create a document that includes:
            # 1. The filter fields (as base document)
            # 2. Apply the update operations to that document
            new_doc: Dict[str, Any] = dict(filter)  # Start with filter fields
            new_doc = self.helpers._internal_update(
                0, update, new_doc
            )  # Apply updates
            inserted_id = self.insert_one(new_doc).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
    ) -> UpdateResult:
        """
        Update multiple documents based on a filter.

        This method updates documents in the collection that match the given filter
        using the specified update.

        Args:
            filter (Dict[str, Any]): A dictionary representing the filter to select documents to update.
            update (Dict[str, Any]): A dictionary representing the updates to apply.

        Returns:
            UpdateResult: A result object containing information about the update operation.
        """
        where_result = self.helpers._build_simple_where_clause(filter)
        update_result = self.helpers._build_update_clause(update)

        if where_result is not None and update_result is not None:
            where_clause, where_params = where_result
            set_clause, set_params = update_result
            cmd = (
                f"UPDATE {self.collection.name} SET {set_clause} {where_clause}"
            )
            cursor = self.collection.db.execute(cmd, set_params + where_params)
            return UpdateResult(
                matched_count=cursor.rowcount,
                modified_count=cursor.rowcount,
                upserted_id=None,
            )

        # Fallback for complex queries
        docs = list(self.find(filter))
        modified_count = 0
        for doc in docs:
            self.helpers._internal_update(doc["_id"], update, doc)
            modified_count += 1
        return UpdateResult(
            matched_count=len(docs),
            modified_count=modified_count,
            upserted_id=None,
        )

    def delete_one(self, filter: Dict[str, Any]) -> DeleteResult:
        """
        Delete a single document matching the filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter conditions
                                     for the document to delete.

        Returns:
            DeleteResult: A result object indicating whether the deletion was
                          successful or not.
        """
        doc = self.find_one(filter)
        if doc:
            self.helpers._internal_delete(doc["_id"])
            return DeleteResult(deleted_count=1)
        return DeleteResult(deleted_count=0)

    def delete_many(self, filter: Dict[str, Any]) -> DeleteResult:
        """
        Deletes multiple documents in the collection that match the provided filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query criteria
                                     for finding the documents to delete.

        Returns:
            DeleteResult: A result object indicating whether the deletion was successful or not.
        """
        where_result = self.helpers._build_simple_where_clause(filter)
        if where_result is not None:
            where_clause, params = where_result
            cmd = f"DELETE FROM {self.collection.name} {where_clause}"
            cursor = self.collection.db.execute(cmd, params)
            return DeleteResult(deleted_count=cursor.rowcount)

        # Fallback for complex queries
        docs = list(self.find(filter))
        if not docs:
            return DeleteResult(deleted_count=0)

        ids = tuple(d["_id"] for d in docs)
        placeholders = ",".join("?" for _ in ids)
        self.collection.db.execute(
            f"DELETE FROM {self.collection.name} WHERE id IN ({placeholders})",
            ids,
        )
        return DeleteResult(deleted_count=len(docs))

    def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Replace one document in the collection that matches the filter with the
        replacement document.

        Args:
            filter (Dict[str, Any]): A query that matches the document to replace.
            replacement (Dict[str, Any]): The new document that replaces the matched document.
            upsert (bool, optional): If true, inserts the replacement document if no document matches the filter.
                                     Default is False.

        Returns:
            UpdateResult: A result object containing the number of matched and
                          modified documents and the upserted ID.
        """
        doc = self.find_one(filter)
        if doc:
            self.helpers._internal_replace(doc["_id"], replacement)
            return UpdateResult(
                matched_count=1, modified_count=1, upserted_id=None
            )

        if upsert:
            inserted_id = self.insert_one(replacement).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

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
        doc = self.find_one(filter)
        if doc:
            self.delete_one({"_id": doc["_id"]})
        return doc

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
        doc = self.find_one(filter)
        if doc:
            self.replace_one({"_id": doc["_id"]}, replacement)
        return doc

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        """
        Find and update a single document.

        Finds a document matching the given filter, updates it using the specified
        update expression, and returns the updated document.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the filter criteria for the document to find.
            update (Dict[str, Any]): A dictionary specifying the update operations to perform on the document.

        Returns:
            Dict[str, Any] | None: The document that was updated,
                                   or None if no document was found and updated.
        """
        doc = self.find_one(filter)
        if doc:
            self.update_one({"_id": doc["_id"]}, update)
        return doc

    def count_documents(self, filter: Dict[str, Any]) -> int:
        """
        Return the count of documents that match the given filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query filter.

        Returns:
            int: The number of documents matching the filter.
        """
        where_result = self.helpers._build_simple_where_clause(filter)
        if where_result is not None:
            where_clause, params = where_result
            cmd = f"SELECT COUNT(id) FROM {self.collection.name} {where_clause}"
            row = self.collection.db.execute(cmd, params).fetchone()
            return row[0] if row else 0
        return len(list(self.find(filter)))

    def estimated_document_count(self) -> int:
        """
        Return the estimated number of documents in the collection.

        Returns:
            int: The estimated number of documents.
        """
        row = self.collection.db.execute(
            f"SELECT COUNT(1) FROM {self.collection.name}"
        ).fetchone()
        return row[0] if row else 0

    def distinct(self, key: str, filter: Dict[str, Any] | None = None) -> set:
        """
        Return a set of distinct values from the specified key in the documents
        of this collection, optionally filtered by a query.

        Args:
            key (str): The field name to extract distinct values from.
            filter (Optional[Dict[str, Any]]): An optional query filter to apply to the documents.

        Returns:
            Set[Any]: A set containing the distinct values from the specified key.
        """
        params: List[Any] = []
        where_clause = ""

        if filter:
            where_result = self.helpers._build_simple_where_clause(filter)
            if where_result:
                where_clause, params = where_result

        cmd = (
            f"SELECT DISTINCT json_extract(data, '$.{key}') "
            f"FROM {self.collection.name} {where_clause}"
        )
        cursor = self.collection.db.execute(cmd, params)
        results: set[Any] = set()
        for row in cursor.fetchall():
            if row[0] is None:
                continue
            try:
                val = neosqlite_json_loads(row[0])
                if isinstance(val, list):
                    results.add(tuple(val))
                elif isinstance(val, dict):
                    results.add(neosqlite_json_dumps(val, sort_keys=True))
                else:
                    results.add(val)
            except (json.JSONDecodeError, TypeError):
                results.add(row[0])
        return results

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Applies a list of aggregation pipeline stages to the collection.

        This method handles both simple and complex queries. For simpler queries,
        it leverages the database's native indexing capabilities to optimize
        performance. For more complex queries, it falls back to a Python-based
        processing mechanism.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages to apply.

        Returns:
            List[Dict[str, Any]]: The list of documents after applying the aggregation pipeline.
        """
        query_result = self.helpers._build_aggregation_query(pipeline)
        if query_result is not None:
            cmd, params, output_fields = query_result
            db_cursor = self.collection.db.execute(cmd, params)
            if output_fields:
                # Handle results from a GROUP BY query
                return [
                    dict(zip(output_fields, row))
                    for row in db_cursor.fetchall()
                ]
            else:
                # Handle results from a regular find query
                return [
                    self.collection._load(row[0], row[1])
                    for row in db_cursor.fetchall()
                ]

        # Fallback to old method for complex queries
        docs: List[Dict[str, Any]] = list(self.find())
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            if stage_name == "$match":
                query = stage["$match"]
                docs = [
                    doc for doc in docs if self.helpers._apply_query(query, doc)
                ]
            elif stage_name == "$sort":
                sort_spec = stage["$sort"]
                for key, direction in reversed(list(sort_spec.items())):
                    docs.sort(
                        key=lambda doc: self.collection._get_val(doc, key),
                        reverse=direction == DESCENDING,
                    )
            elif stage_name == "$skip":
                count = stage["$skip"]
                docs = docs[count:]
            elif stage_name == "$limit":
                count = stage["$limit"]
                docs = docs[:count]
            elif stage_name == "$project":
                projection = stage["$project"]
                docs = [
                    self.helpers._apply_projection(projection, doc)
                    for doc in docs
                ]
            elif stage_name == "$group":
                group_spec = stage["$group"]
                docs = self.helpers._process_group_stage(group_spec, docs)
            elif stage_name == "$unwind":
                field = stage["$unwind"]
                unwound_docs = []
                field_name = field.lstrip("$")
                for doc in docs:
                    array_to_unwind = self.collection._get_val(doc, field_name)
                    if isinstance(array_to_unwind, list):
                        for item in array_to_unwind:
                            new_doc = doc.copy()
                            new_doc[field_name] = item
                            unwound_docs.append(new_doc)
                    else:
                        unwound_docs.append(doc)
                docs = unwound_docs
            else:
                raise MalformedQueryException(
                    f"Aggregation stage '{stage_name}' not supported"
                )
        return docs

    # --- Bulk Write methods ---
    def bulk_write(
        self,
        requests: List[Any],
        ordered: bool = True,
    ) -> BulkWriteResult:
        """
        Execute bulk write operations on the collection.

        Args:
            requests: List of write operations to execute.
            ordered: If true, operations will be performed in order and will
                     raise an exception if a single operation fails.

        Returns:
            BulkWriteResult: A result object containing the number of matched,
                             modified, and inserted documents.
        """
        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_count = 0

        self.collection.db.execute("SAVEPOINT bulk_write")
        try:
            for req in requests:
                match req:
                    case InsertOne(document=doc):
                        self.insert_one(doc)
                        inserted_count += 1
                    case UpdateOne(filter=f, update=u, upsert=up):
                        update_res = self.update_one(f, u, up)
                        matched_count += update_res.matched_count
                        modified_count += update_res.modified_count
                        if update_res.upserted_id:
                            upserted_count += 1
                    case DeleteOne(filter=f):
                        delete_res = self.delete_one(f)
                        deleted_count += delete_res.deleted_count
            self.collection.db.execute("RELEASE SAVEPOINT bulk_write")
        except Exception as e:
            self.collection.db.execute("ROLLBACK TO SAVEPOINT bulk_write")
            raise e

        return BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=upserted_count,
        )

    def initialize_ordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an ordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for ordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=True)

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an unordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for unordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=False)
