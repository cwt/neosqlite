from copy import deepcopy
import json
from typing import Any, Dict, List, overload
from typing_extensions import Literal

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore

from .bulk_operations import BulkOperationExecutor
from .raw_batch_cursor import RawBatchCursor
from .results import (
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    DeleteResult,
    BulkWriteResult,
)
from .requests import InsertOne, UpdateOne, DeleteOne
from .exceptions import MalformedQueryException, MalformedDocument
from .cursor import Cursor, DESCENDING
from .changestream import ChangeStream
from . import query_operators


class Collection:
    """
    Provides a class representing a collection in a SQLite database.

    This class encapsulates operations on a collection such as inserting,
    updating, deleting, and querying documents.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        name: str,
        create: bool = True,
        database=None,
    ):
        """
        Initialize a new collection object.

        Args:
            db: Database object to which the collection belongs.
            name: Name of the collection.
            **kwargs: Additional arguments for initialization.
        """
        self.db = db
        self.name = name
        self._database = database
        if create:
            self.create()

    def create(self):
        """
        Initialize the collection table if it does not exist.

        This method creates a table with an 'id' column and a 'data' column for
        storing JSON data. If the JSONB data type is supported, it will be used,
        otherwise, TEXT data type will be used.
        """
        try:
            self.db.execute("""SELECT jsonb('{"key": "value"}')""")
        except sqlite3.OperationalError:
            self.db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL
                )
                """
            )
        else:
            self.db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data JSONB NOT NULL
                )"""
            )

    def _load(self, id: int, data: str | bytes) -> Dict[str, Any]:
        """
        Deserialize and load a document from its ID and JSON data.

        Deserialize the JSON string or bytes back into a Python dictionary,
        add the document ID to it, and return the document.

        Args:
            id (int): The document ID.
            data (str | bytes): The JSON string or bytes representing the document.

        Returns:
            Dict[str, Any]: The deserialized document with the _id field added.
        """
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        document: Dict[str, Any] = json.loads(data)
        document["_id"] = id
        return document

    def _get_val(self, item: Dict[str, Any], key: str) -> Any:
        """
        Retrieves a value from a dictionary using a key, handling nested keys and
        optional prefixes.

        Args:
            item (Dict[str, Any]): The dictionary to search.
            key (str): The key to retrieve, may include nested keys separated by
                       dots or may be prefixed with '$'.

        Returns:
            Any: The value associated with the key, or None if the key is not found.
        """
        if key.startswith("$"):
            key = key[1:]
        val: Any = item
        for k in key.split("."):
            if val is None:
                return None
            val = val.get(k)
        return val

    def _internal_insert(self, document: Dict[str, Any]) -> int:
        """
        Inserts a document into the collection and returns the inserted document's _id.

        Args:
            document (dict): The document to insert. Must be a dictionary.

        Returns:
            int: The _id of the inserted document.
        """
        if not isinstance(document, dict):
            raise MalformedDocument(
                f"document must be a dictionary, not a {type(document)}"
            )

        doc_to_insert = deepcopy(document)
        doc_to_insert.pop("_id", None)

        cursor = self.db.execute(
            f"INSERT INTO {self.name}(data) VALUES (?)",
            (json.dumps(doc_to_insert),),
        )
        inserted_id = cursor.lastrowid

        if inserted_id is None:
            raise sqlite3.Error("Failed to get last row id.")

        document["_id"] = inserted_id
        return inserted_id

    def insert_one(self, document: Dict[str, Any]) -> InsertOneResult:
        """
        Insert a single document into the collection.

        Args:
            document (Dict[str, Any]): The document to insert.

        Returns:
            InsertOneResult: The result of the insert operation, containing the inserted document ID.
        """
        inserted_id = self._internal_insert(document)
        return InsertOneResult(inserted_id)

    def insert_many(self, documents: List[Dict[str, Any]]) -> InsertManyResult:
        """
        Insert multiple documents into the collection.

        Args:
            documents (List[Dict[str, Any]]): List of documents to insert.

        Returns:
            InsertManyResult: Result of the insert operation, containing a list of inserted document IDs.
        """
        inserted_ids = [self._internal_insert(doc) for doc in documents]
        return InsertManyResult(inserted_ids)

    def _internal_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
    ):
        """
        Helper method for updating documents.

        Attempts to use SQL-based updates for simple operations, falling back to
        Python-based updates for complex operations.

        Args:
            doc_id (int): The ID of the document to update.
            update_spec (Dict[str, Any]): The update specification.
            original_doc (Dict[str, Any]): The original document before the update.

        Returns:
            Dict[str, Any]: The updated document.
        """
        # Try to use SQL-based updates for simple operations
        if self._can_use_sql_updates(update_spec, doc_id):
            return self._perform_sql_update(doc_id, update_spec)
        else:
            # Fall back to Python-based updates for complex operations
            return self._perform_python_update(
                doc_id, update_spec, original_doc
            )

    def _can_use_sql_updates(
        self,
        update_spec: Dict[str, Any],
        doc_id: int,
    ) -> bool:
        """
        Check if all operations in the update spec can be handled with SQL.

        This method determines whether the update operations can be efficiently
        executed using SQL directly, which allows for better performance compared
        to iterating over each document and applying updates in Python.

        Args:
            update_spec (Dict[str, Any]): The update operations to be checked.
            doc_id (int): The document ID, which is used to determine if the update is an upsert.

        Returns:
            bool: True if all operations can be handled with SQL, False otherwise.
        """
        # Only handle operations that can be done purely with SQL
        supported_ops = {"$set", "$unset", "$inc", "$mul", "$min", "$max"}
        # Also check that doc_id is not 0 (which indicates an upsert)
        return doc_id != 0 and all(
            op in supported_ops for op in update_spec.keys()
        )

    def _perform_sql_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform update operations using SQL JSON functions.

        This method builds SQL clauses for updating document fields based on the
        provided update specification. It supports both `$set` and `$unset` operations
        using SQLite's `json_set` and `json_remove` functions, respectively. The
        method then executes the SQL commands to apply the updates and fetches
        the updated document from the database.

        Args:
            doc_id (int): The ID of the document to be updated.
            update_spec (Dict[str, Any]): A dictionary specifying the update operations to be performed.

        Returns:
            Dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the update process.
        """
        set_clauses = []
        set_params = []
        unset_clauses = []
        unset_params = []

        # Build SQL update clauses for each operation
        for op, value in update_spec.items():
            clauses, params = self._build_sql_update_clause(op, value)
            if clauses:
                if op == "$unset":
                    unset_clauses.extend(clauses)
                    unset_params.extend(params)
                else:
                    set_clauses.extend(clauses)
                    set_params.extend(params)

        # Execute the SQL updates
        sql_params = []
        if unset_clauses:
            # Handle $unset operations with json_remove
            cmd = (
                f"UPDATE {self.name} "
                f"SET data = json_remove(data, {', '.join(unset_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = unset_params + [doc_id]
            self.db.execute(cmd, sql_params)

        if set_clauses:
            # Handle other operations with json_set
            cmd = (
                f"UPDATE {self.name} "
                f"SET data = json_set(data, {', '.join(set_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = set_params + [doc_id]
            cursor = self.db.execute(cmd, sql_params)

            # Check if any rows were updated
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        elif not unset_clauses:
            # No operations to perform
            raise RuntimeError("No valid operations to perform")

        # Fetch and return the updated document
        row = self.db.execute(
            f"SELECT data FROM {self.name} WHERE id = ?", (doc_id,)
        ).fetchone()
        if row:
            return self._load(doc_id, row[0])

        # This shouldn't happen, but just in case
        raise RuntimeError("Failed to fetch updated document")

    def _build_sql_update_clause(
        self,
        op: str,
        value: Any,
    ) -> tuple[List[str], List[Any]]:
        """
        Build SQL update clause for a single operation.

        Args:
            op (str): The update operation, such as "$set", "$inc", "$mul", etc.
            value (Any): The value associated with the update operation.

        Returns:
            tuple[List[str], List[Any]]: A tuple containing the SQL update clauses and parameters.
        """
        clauses = []
        params = []

        match op:
            case "$set":
                for field, field_val in value.items():
                    clauses.append(f"'$.{field}', ?")
                    params.append(field_val)
            case "$inc":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(f"{path}, json_extract(data, {path}) + ?")
                    params.append(field_val)
            case "$mul":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(f"{path}, json_extract(data, {path}) * ?")
                    params.append(field_val)
            case "$min":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(
                        f"{path}, min(json_extract(data, {path}), ?)"
                    )
                    params.append(field_val)
            case "$max":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(
                        f"{path}, max(json_extract(data, {path}), ?)"
                    )
                    params.append(field_val)
            case "$unset":
                # For $unset, we use json_remove
                for field in value:
                    path = f"'$.{field}'"
                    clauses.append(path)

        return clauses, params

    def _perform_python_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform update operations using Python-based logic.

        Args:
            doc_id (int): The document ID of the document to update.
            update_spec (Dict[str, Any]): A dictionary specifying the update operations to perform.
            original_doc (Dict[str, Any]): The original document before applying the updates.

        Returns:
            Dict[str, Any]: The updated document.
        """
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            match op:
                case "$set":
                    doc_to_update.update(value)
                case "$unset":
                    for k in value:
                        doc_to_update.pop(k, None)
                case "$inc":
                    for k, v in value.items():
                        doc_to_update[k] = doc_to_update.get(k, 0) + v
                case "$push":
                    for k, v in value.items():
                        doc_to_update.setdefault(k, []).append(v)
                case "$pull":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[k] = [
                                item for item in doc_to_update[k] if item != v
                            ]
                case "$pop":
                    for k, v in value.items():
                        if v == 1:
                            doc_to_update.get(k, []).pop()
                        elif v == -1:
                            doc_to_update.get(k, []).pop(0)
                case "$rename":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[v] = doc_to_update.pop(k)
                case "$mul":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[k] *= v
                case "$min":
                    for k, v in value.items():
                        if k in doc_to_update and doc_to_update[k] > v:
                            doc_to_update[k] = v
                case "$max":
                    for k, v in value.items():
                        if k in doc_to_update and doc_to_update[k] < v:
                            doc_to_update[k] = v
                case _:
                    raise MalformedQueryException(
                        f"Update operator '{op}' not supported"
                    )

        # If this is an upsert (doc_id == 0), we don't update the database
        # We just return the updated document for insertion by the caller
        if doc_id != 0:
            self.db.execute(
                f"UPDATE {self.name} SET data = ? WHERE id = ?",
                (json.dumps(doc_to_update), doc_id),
            )

        return doc_to_update

    def _internal_replace(self, doc_id: int, replacement: Dict[str, Any]):
        """
        Replace the document with the specified ID with a new document.

        Args:
            doc_id (int): The ID of the document to replace.
            replacement (Dict[str, Any]): The new document to replace the existing one.
        """
        self.db.execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (json.dumps(replacement), doc_id),
        )

    def _internal_delete(self, doc_id: int):
        """
        Deletes a document from the collection based on the document ID.

        Args:
            doc_id (int): The ID of the document to delete.
        """
        self.db.execute(f"DELETE FROM {self.name} WHERE id = ?", (doc_id,))

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
            self._internal_update(doc["_id"], update, doc)
            return UpdateResult(
                matched_count=1, modified_count=1, upserted_id=None
            )

        if upsert:
            # For upsert, we need to create a document that includes:
            # 1. The filter fields (as base document)
            # 2. Apply the update operations to that document
            new_doc: Dict[str, Any] = dict(filter)  # Start with filter fields
            new_doc = self._internal_update(0, update, new_doc)  # Apply updates
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
        where_result = self._build_simple_where_clause(filter)
        update_result = self._build_update_clause(update)

        if where_result is not None and update_result is not None:
            where_clause, where_params = where_result
            set_clause, set_params = update_result
            cmd = f"UPDATE {self.name} SET {set_clause} {where_clause}"
            cursor = self.db.execute(cmd, set_params + where_params)
            return UpdateResult(
                matched_count=cursor.rowcount,
                modified_count=cursor.rowcount,
                upserted_id=None,
            )

        # Fallback for complex queries
        docs = list(self.find(filter))
        modified_count = 0
        for doc in docs:
            self._internal_update(doc["_id"], update, doc)
            modified_count += 1
        return UpdateResult(
            matched_count=len(docs),
            modified_count=modified_count,
            upserted_id=None,
        )

    def _build_update_clause(
        self,
        update: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Build the SQL update clause based on the provided update operations.

        Args:
            update (Dict[str, Any]): A dictionary containing update operations.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL update clause and parameters,
                                          or None if no update clauses are generated.
        """
        set_clauses = []
        params = []

        for op, value in update.items():
            match op:
                case "$set":
                    for field, field_val in value.items():
                        set_clauses.append(f"'$.{field}', ?")
                        params.append(field_val)
                case "$inc":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, json_extract(data, {path}) + ?"
                        )
                        params.append(field_val)
                case "$mul":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, json_extract(data, {path}) * ?"
                        )
                        params.append(field_val)
                case "$min":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, min(json_extract(data, {path}), ?)"
                        )
                        params.append(field_val)
                case "$max":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, max(json_extract(data, {path}), ?)"
                        )
                        params.append(field_val)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        path = f"'$.{field}'"
                        set_clauses.append(path)
                    # json_remove has a different syntax
                    if set_clauses:
                        return (
                            f"data = json_remove(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        # No fields to unset
                        return None
                case "$rename":
                    # $rename is complex to do in SQL, so we'll fall back to the Python implementation
                    return None
                case _:
                    return None  # Fallback for unsupported operators

        if not set_clauses:
            return None

        # For $unset, we already returned above
        if "$unset" not in update:
            return f"data = json_set(data, {', '.join(set_clauses)})", params
        else:
            # This case should have been handled above
            return None

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
            self._internal_replace(doc["_id"], replacement)
            return UpdateResult(
                matched_count=1, modified_count=1, upserted_id=None
            )

        if upsert:
            inserted_id = self.insert_one(replacement).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

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

        self.db.execute("SAVEPOINT bulk_write")
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
            self.db.execute("RELEASE SAVEPOINT bulk_write")
        except Exception as e:
            self.db.execute("ROLLBACK TO SAVEPOINT bulk_write")
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
        return BulkOperationExecutor(self, ordered=True)

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an unordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for unordered bulk operations.
        """
        return BulkOperationExecutor(self, ordered=False)

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
            self._internal_delete(doc["_id"])
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
        where_result = self._build_simple_where_clause(filter)
        if where_result is not None:
            where_clause, params = where_result
            cmd = f"DELETE FROM {self.name} {where_clause}"
            cursor = self.db.execute(cmd, params)
            return DeleteResult(deleted_count=cursor.rowcount)

        # Fallback for complex queries
        docs = list(self.find(filter))
        if not docs:
            return DeleteResult(deleted_count=0)

        ids = tuple(d["_id"] for d in docs)
        placeholders = ",".join("?" for _ in ids)
        self.db.execute(
            f"DELETE FROM {self.name} WHERE id IN ({placeholders})", ids
        )
        return DeleteResult(deleted_count=len(docs))

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
        return Cursor(self, filter, projection, hint)

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
              # Each batch is raw bytes containing JSON documents separated by newlines.
              documents = [json.loads(doc) for doc in batch.decode('utf-8').split('\n') if doc]
              print(documents)
        """
        return RawBatchCursor(self, filter, projection, hint, batch_size)

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

    def count_documents(self, filter: Dict[str, Any]) -> int:
        """
        Return the count of documents that match the given filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query filter.

        Returns:
            int: The number of documents matching the filter.
        """
        where_result = self._build_simple_where_clause(filter)
        if where_result is not None:
            where_clause, params = where_result
            cmd = f"SELECT COUNT(id) FROM {self.name} {where_clause}"
            row = self.db.execute(cmd, params).fetchone()
            return row[0] if row else 0
        return len(list(self.find(filter)))

    def estimated_document_count(self) -> int:
        """
        Return the estimated number of documents in the collection.

        Returns:
            int: The estimated number of documents.
        """
        row = self.db.execute(f"SELECT COUNT(1) FROM {self.name}").fetchone()
        return row[0] if row else 0

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
        query_result = self._build_aggregation_query(pipeline)
        if query_result is not None:
            cmd, params = query_result
            db_cursor = self.db.execute(cmd, params)
            return [self._load(row[0], row[1]) for row in db_cursor.fetchall()]

        # Fallback to old method for complex queries
        docs: List[Dict[str, Any]] = list(self.find())
        for stage in pipeline:
            match stage:
                case {"$match": query}:
                    docs = [
                        doc for doc in docs if self._apply_query(query, doc)
                    ]
                case {"$sort": sort_spec}:
                    for key, direction in reversed(list(sort_spec.items())):
                        docs.sort(
                            key=lambda doc: self._get_val(doc, key),
                            reverse=direction == DESCENDING,
                        )
                case {"$skip": count}:
                    docs = docs[count:]
                case {"$limit": count}:
                    docs = docs[:count]
                case {"$project": projection}:
                    docs = [
                        self._apply_projection(projection, doc) for doc in docs
                    ]
                case {"$group": group_spec}:
                    docs = self._process_group_stage(group_spec, docs)
                case {"$unwind": field}:
                    unwound_docs = []
                    field_name = field.lstrip("$")
                    for doc in docs:
                        array_to_unwind = self._get_val(doc, field_name)
                        if isinstance(array_to_unwind, list):
                            for item in array_to_unwind:
                                new_doc = doc.copy()
                                new_doc[field_name] = item
                                unwound_docs.append(new_doc)
                        else:
                            unwound_docs.append(doc)
                    docs = unwound_docs
                case _:
                    stage_name = next(iter(stage.keys()))
                    raise MalformedQueryException(
                        f"Aggregation stage '{stage_name}' not supported"
                    )
        return docs

    def _build_aggregation_query(
        self,
        pipeline: List[Dict[str, Any]],
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL query for the given MongoDB-like aggregation pipeline.

        This method constructs a SQL query based on the stages provided in the
        aggregation pipeline. It currently handles $match, $sort, $skip,
        and $limit stages, while $group stages are handled in Python. The method
        returns a tuple containing the SQL command and a list of parameters.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL command and a list of parameters,
                                          or None if the pipeline contains unsupported stages or complex queries.
        """
        where_clause = ""
        params: List[Any] = []
        order_by = ""
        limit = ""
        offset = ""

        for stage in pipeline:
            match stage:
                case {"$match": query}:
                    where_result = self._build_simple_where_clause(query)
                    if where_result is None:
                        return None  # Fallback for complex queries
                    where_clause, params = where_result
                case {"$sort": sort_spec}:
                    sort_clauses = []
                    for key, direction in sort_spec.items():
                        sort_clauses.append(
                            f"json_extract(data, '$.{key}') {'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    order_by = "ORDER BY " + ", ".join(sort_clauses)
                case {"$skip": count}:
                    offset = f"OFFSET {count}"
                case {"$limit": count}:
                    limit = f"LIMIT {count}"
                case {"$group": group_spec}:
                    # Handle $group stage in Python for now
                    # This is complex to do in SQL and would require significant changes
                    # to the result processing pipeline
                    return None
                case _:
                    return None  # Fallback for unsupported stages

        cmd = f"SELECT id, data FROM {self.name} {where_clause} {order_by} {limit} {offset}"
        return cmd, params

    def _process_group_stage(
        self,
        group_query: Dict[str, Any],
        docs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Process the $group stage of an aggregation pipeline.

        This method groups documents by a specified field and performs specified
        accumulator operations on other fields.

        Args:
            group_query (Dict[str, Any]): A dictionary representing the $group stage of the aggregation pipeline.
            docs (List[Dict[str, Any]]): A list of documents to be grouped.

        Returns:
            List[Dict[str, Any]]: A list of grouped documents with applied accumulator operations.
        """
        grouped_docs: Dict[Any, Dict[str, Any]] = {}
        group_id_key = group_query.pop("_id")

        for doc in docs:
            group_id = self._get_val(doc, group_id_key)
            group = grouped_docs.setdefault(group_id, {"_id": group_id})

            for field, accumulator in group_query.items():
                op, key = next(iter(accumulator.items()))
                value = self._get_val(doc, key)

                if op == "$sum":
                    group[field] = (group.get(field, 0) or 0) + (value or 0)
                elif op == "$avg":
                    avg_info = group.get(field, {"sum": 0, "count": 0})
                    avg_info["sum"] += value or 0
                    avg_info["count"] += 1
                    group[field] = avg_info
                elif op == "$min":
                    group[field] = min(group.get(field, value), value)
                elif op == "$max":
                    group[field] = max(group.get(field, value), value)
                elif op == "$push":
                    group.setdefault(field, []).append(value)

        # Finalize results (e.g., calculate average)
        for group in grouped_docs.values():
            for field, value in group.items():
                if (
                    isinstance(value, dict)
                    and "sum" in value
                    and "count" in value
                ):
                    group[field] = value["sum"] / value["count"]

        return list(grouped_docs.values())

    def _apply_projection(
        self,
        projection: Dict[str, Any],
        document: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Applies the projection to the document, selecting or excluding fields
        based on the projection criteria.

        Args:
            projection (Dict[str, Any]): A dictionary specifying which fields to include or exclude.
            document (Dict[str, Any]): The document to apply the projection to.

        Returns:
            Dict[str, Any]: The document with fields applied based on the projection.
        """
        if not projection:
            return document

        doc = deepcopy(document)
        projected_doc: Dict[str, Any] = {}
        include_id = projection.get("_id", 1) == 1

        # Inclusion mode
        if any(v == 1 for v in projection.values()):
            for key, value in projection.items():
                if value == 1 and key in doc:
                    projected_doc[key] = doc[key]
            if include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]
            return projected_doc

        # Exclusion mode
        for key, value in projection.items():
            if value == 0 and key in doc:
                doc.pop(key, None)
        if not include_id and "_id" in doc:
            doc.pop("_id", None)
        return doc

    def _is_text_search_query(self, query: Dict[str, Any]) -> bool:
        """
        Check if the query is a text search query (contains $text operator).

        Args:
            query: The query to check.

        Returns:
            True if the query is a text search query, False otherwise.
        """
        return "$text" in query

    def _build_text_search_query(
        self, query: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL query for text search using FTS5.

        Args:
            query: A dictionary representing the text search query with $text operator.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause and a list of parameters,
                                          or None if the query is invalid or FTS index doesn't exist.
        """
        if "$text" not in query:
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        # Find FTS tables for this collection
        cursor = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
            (f"{self.name}_%_fts",),
        )
        fts_tables = cursor.fetchall()

        if not fts_tables:
            return None

        # For simplicity, we'll use the first FTS table found
        fts_table_name = fts_tables[0][0]
        # Extract field name from FTS table name (collection_field_fts -> field)
        index_name = fts_table_name[
            len(f"{self.name}_") : -4
        ]  # Remove collection_ prefix and _fts suffix

        # Build the FTS query
        where_clause = f"""
        WHERE id IN (
            SELECT rowid FROM {fts_table_name} WHERE {index_name} MATCH ?
        )
        """
        return where_clause, [search_term]

    def _build_simple_where_clause(
        self,
        query: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL WHERE clause for simple queries that can be handled with json_extract.

        This method constructs a SQL WHERE clause based on the query provided.
        It handles simple equality checks and query operators like $eq, $gt, $lt,
        etc. for fields stored in JSON data. For more complex queries, it returns
        None, indicating that a Python-based method should be used instead.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause and a list of parameters,
                                          or None if the query contains unsupported operators.
        """
        # Handle text search queries separately
        if self._is_text_search_query(query):
            return self._build_text_search_query(query)

        clauses = []
        params = []

        for field, value in query.items():
            # Handle logical operators by falling back to Python processing
            # This is more robust than trying to build complex SQL queries
            if field in ("$and", "$or", "$nor", "$not"):
                return (
                    None  # Fall back to Python processing for logical operators
                )

            elif field == "_id":
                # Handle _id field specially since it's stored as a column,
                # not in the JSON data
                clauses.append("id = ?")
                params.append(value)
                continue

            else:
                # For all fields (including nested ones), use json_extract to get
                # values from the JSON data.

                # Convert dot notation to JSON path notation.
                # (e.g., "profile.age" -> "$.profile.age")
                json_path = f"'$.{field}'"

                if isinstance(value, dict):
                    # Handle query operators like $eq, $gt, $lt, etc.
                    clause, clause_params = self._build_operator_clause(
                        json_path, value
                    )
                    if clause is None:
                        return None  # Unsupported operator, fallback to Python
                    clauses.append(clause)
                    params.extend(clause_params)
                else:
                    # Simple equality check
                    clauses.append(f"json_extract(data, {json_path}) = ?")
                    params.append(value)

        if not clauses:
            return "", []
        return "WHERE " + " AND ".join(clauses), params

    def _build_operator_clause(
        self,
        json_path: str,
        operators: Dict[str, Any],
    ) -> tuple[str | None, List[Any]]:
        """
        Builds a SQL clause for query operators.

        This method constructs a SQL clause based on the provided operators for
        a specific JSON path. It handles various operators like $eq, $gt, $lt, etc.,
        and returns a tuple containing the SQL clause and a list of parameters.
        If an unsupported operator is encountered, it returns None, indicating
        that a fallback to Python processing is needed.

        Args:
            json_path (str): The JSON path to extract the value from.
            operators (Dict[str, Any]): A dictionary of operators and their values.

        Returns:
            tuple[str | None, List[Any]]: A tuple containing the SQL clause and parameters.
                                          If the operator is unsupported, returns (None, []).
        """
        for op, op_val in operators.items():
            match op:
                case "$eq":
                    return f"json_extract(data, {json_path}) = ?", [op_val]
                case "$gt":
                    return f"json_extract(data, {json_path}) > ?", [op_val]
                case "$lt":
                    return f"json_extract(data, {json_path}) < ?", [op_val]
                case "$gte":
                    return f"json_extract(data, {json_path}) >= ?", [op_val]
                case "$lte":
                    return f"json_extract(data, {json_path}) <= ?", [op_val]
                case "$ne":
                    return f"json_extract(data, {json_path}) != ?", [op_val]
                case "$in":
                    placeholders = ", ".join("?" for _ in op_val)
                    return (
                        f"json_extract(data, {json_path}) IN ({placeholders})",
                        op_val,
                    )
                case "$nin":
                    placeholders = ", ".join("?" for _ in op_val)
                    return (
                        f"json_extract(data, {json_path}) NOT IN ({placeholders})",
                        op_val,
                    )
                case "$exists":
                    # Handle boolean value for $exists
                    if op_val is True:
                        return (
                            f"json_extract(data, {json_path}) IS NOT NULL",
                            [],
                        )
                    elif op_val is False:
                        return f"json_extract(data, {json_path}) IS NULL", []
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(op_val, (list, tuple)) and len(op_val) == 2:
                        divisor, remainder = op_val
                        return f"json_extract(data, {json_path}) % ? = ?", [
                            divisor,
                            remainder,
                        ]
                    else:
                        # Invalid format for $mod, fallback to Python
                        return None, []
                case "$size":
                    # Handle array size comparison
                    if isinstance(op_val, int):
                        return (
                            f"json_array_length(json_extract(data, {json_path})) = ?",
                            [op_val],
                        )
                    else:
                        # Invalid value for $size, fallback to Python
                        return None, []
                case "$contains":
                    # Handle case-insensitive substring search
                    if isinstance(op_val, str):
                        return (
                            f"lower(json_extract(data, {json_path})) LIKE ?",
                            [f"%{op_val.lower()}%"],
                        )
                    else:
                        # Invalid value for $contains, fallback to Python
                        return None, []
                case _:
                    # Unsupported operator, return None to indicate we should fallback to Python
                    return None, []

        # This shouldn't happen, but just in case
        return None, []

    def _apply_query(
        self,
        query: Dict[str, Any],
        document: Dict[str, Any],
    ) -> bool:
        """
        Applies a query to a document to determine if it matches the query criteria.

        Handles logical operators ($and, $or, $nor, $not) and nested field paths.
        Processes both simple equality checks and complex query operators.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.
            document (Dict[str, Any]): The document to apply the query to.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        if document is None:
            return False
        matches: List[bool] = []

        def reapply(q: Dict[str, Any]) -> bool:
            """
            Recursively apply the query to the document to determine if it matches
            the query criteria.

            Args:
                q (Dict[str, Any]): The query to apply.
                document (Dict[str, Any]): The document to apply the query to.

            Returns:
                bool: True if the document matches the query, False otherwise.
            """
            return self._apply_query(q, document)

        for field, value in query.items():
            if field == "$text":
                # Handle $text operator in Python fallback
                # This is a simplified implementation that just does basic string matching
                if isinstance(value, dict) and "$search" in value:
                    search_term = value["$search"]
                    if isinstance(search_term, str):
                        # Find FTS tables for this collection to determine which fields are indexed
                        cursor = self.db.execute(
                            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
                            (f"{self.name}_%_fts",),
                        )
                        fts_tables = cursor.fetchall()

                        # Check each FTS-indexed field for matches
                        for fts_table in fts_tables:
                            fts_table_name = fts_table[0]
                            # Extract field name from FTS table name (collection_field_fts -> field)
                            index_name = fts_table_name[
                                len(f"{self.name}_") : -4
                            ]  # Remove collection_ prefix and _fts suffix
                            # Convert underscores back to dots for nested keys
                            field_name = index_name.replace("_", ".")
                            # Check if this field has content that matches the search term
                            field_value = self._get_val(document, field_name)
                            if field_value and isinstance(field_value, str):
                                # Simple case-insensitive substring search
                                if search_term.lower() in field_value.lower():
                                    matches.append(True)
                                    break
                        else:
                            # If no FTS indexes exist, check all string fields
                            def check_all_fields(doc, search_term):
                                """Recursively check all fields in the document for the search term"""
                                for key, val in doc.items():
                                    if isinstance(val, str):
                                        if search_term.lower() in val.lower():
                                            return True
                                    elif isinstance(val, dict):
                                        if check_all_fields(val, search_term):
                                            return True
                                return False

                            if check_all_fields(document, search_term):
                                matches.append(True)
                            else:
                                matches.append(False)
                    else:
                        matches.append(False)
                else:
                    matches.append(False)
            elif field == "$and":
                matches.append(all(map(reapply, value)))
            elif field == "$or":
                matches.append(any(map(reapply, value)))
            elif field == "$nor":
                matches.append(not any(map(reapply, value)))
            elif field == "$not":
                matches.append(not self._apply_query(value, document))
            elif isinstance(value, dict):
                for operator, arg in value.items():
                    if not self._get_operator_fn(operator)(
                        field, arg, document
                    ):
                        matches.append(False)
                        break
                else:
                    matches.append(True)
            else:
                doc_value: Dict[str, Any] | None = document
                if doc_value and field in doc_value:
                    doc_value = doc_value.get(field, None)
                else:
                    for path in field.split("."):
                        if not isinstance(doc_value, dict):
                            break
                        doc_value = doc_value.get(path, None)
                if value != doc_value:
                    matches.append(False)
        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """
        Retrieve the function associated with the given operator from the
        query_operators module.

        Args:
            op (str): The operator string, which should start with a '$' prefix.

        Returns:
            Any: The function corresponding to the operator.

        Raises:
            MalformedQueryException: If the operator does not start with '$'.
            MalformedQueryException: If the operator is not currently implemented.
        """
        if not op.startswith("$"):
            raise MalformedQueryException(
                f"Operator '{op}' is not a valid query operation"
            )
        try:
            return getattr(query_operators, op.replace("$", "_"))
        except AttributeError:
            raise MalformedQueryException(
                f"Operator '{op}' is not currently implemented"
            )

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
            where_result = self._build_simple_where_clause(filter)
            if where_result:
                where_clause, params = where_result

        cmd = (
            f"SELECT DISTINCT json_extract(data, '$.{key}') "
            f"FROM {self.name} {where_clause}"
        )
        cursor = self.db.execute(cmd, params)
        results: set[Any] = set()
        for row in cursor.fetchall():
            if row[0] is None:
                continue
            try:
                val = json.loads(row[0])
                if isinstance(val, list):
                    results.add(tuple(val))
                elif isinstance(val, dict):
                    results.add(json.dumps(val, sort_keys=True))
                else:
                    results.add(val)
            except (json.JSONDecodeError, TypeError):
                results.add(row[0])
        return results

    def create_index(
        self,
        key: str | List[str],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
        fts: bool = False,
    ):
        """
        Create an index on the specified key(s) for this collection.

        Handles both single-key and compound indexes by using SQLite's json_extract
        function to create indexes on the JSON-stored data. For compound indexes,
        multiple json_extract calls are used for each key in the list.

        Args:
            key: A string or list of strings representing the field(s) to index.
            reindex: Boolean indicating whether to reindex (not used in this implementation).
            sparse: Boolean indicating whether the index should be sparse (only include documents with the field).
            unique: Boolean indicating whether the index should be unique.
            fts: Boolean indicating whether to create an FTS index for text search.
        """
        # For single key indexes, we can use SQLite's native JSON indexing
        if isinstance(key, str):
            if fts:
                # Create FTS index
                self._create_fts_index(key)
            else:
                # Create index name (replace dots with underscores for valid identifiers)
                index_name = key.replace(".", "_")

                # Create the index using json_extract
                self.db.execute(
                    (
                        f"CREATE {'UNIQUE ' if unique else ''}INDEX "
                        f"IF NOT EXISTS [idx_{self.name}_{index_name}] "
                        f"ON {self.name}(json_extract(data, '$.{key}'))"
                    )
                )
        else:
            # For compound indexes, we still need to handle them differently
            # This is a simplified implementation - we could expand on this later
            index_name = "_".join(key).replace(".", "_")

            # Create the compound index using multiple json_extract calls
            index_columns = ", ".join(
                f"json_extract(data, '$.{k}')" for k in key
            )
            self.db.execute(
                (
                    f"CREATE {'UNIQUE ' if unique else ''}INDEX "
                    f"IF NOT EXISTS [idx_{self.name}_{index_name}] "
                    f"ON {self.name}({index_columns})"
                )
            )

    def _create_fts_index(self, field: str):
        """
        Create an FTS5 index on the specified field for text search.

        Args:
            field: The field to create the FTS index on.
        """
        from .exceptions import MalformedQueryException

        # Create FTS5 virtual table
        index_name = field.replace(".", "_")
        fts_table_name = f"{self.name}_{index_name}_fts"

        # Check if FTS5 is available by trying to create a simple FTS table
        try:
            # Try to create a temporary FTS table to check if FTS5 is available
            self.db.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS temp.fts_test USING fts5(test)"
            )
            self.db.execute("DROP TABLE IF EXISTS temp.fts_test")
        except sqlite3.OperationalError:
            raise MalformedQueryException(
                "FTS5 is not available in this SQLite installation"
            )

        # Create the FTS table
        self.db.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name} 
            USING fts5(content='{self.name}', content_rowid='id', {index_name})
            """
        )

        # Create triggers to keep the FTS index in sync with the main table
        # Delete existing triggers if they exist
        self.db.execute(
            f"DROP TRIGGER IF EXISTS {self.name}_{index_name}_fts_insert"
        )
        self.db.execute(
            f"DROP TRIGGER IF EXISTS {self.name}_{index_name}_fts_update"
        )
        self.db.execute(
            f"DROP TRIGGER IF EXISTS {self.name}_{index_name}_fts_delete"
        )

        # Insert trigger
        self.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {self.name}_{index_name}_fts_insert
            AFTER INSERT ON {self.name}
            BEGIN
                INSERT INTO {fts_table_name}(rowid, {index_name}) 
                VALUES (new.id, json_extract(new.data, '$.{field}'));
            END
            """
        )

        # Update trigger
        self.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {self.name}_{index_name}_fts_update
            AFTER UPDATE ON {self.name}
            BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {index_name}) 
                VALUES ('delete', old.id, json_extract(old.data, '$.{field}'));
                INSERT INTO {fts_table_name}(rowid, {index_name}) 
                VALUES (new.id, json_extract(new.data, '$.{field}'));
            END
            """
        )

        # Delete trigger
        self.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {self.name}_{index_name}_fts_delete
            AFTER DELETE ON {self.name}
            BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {index_name}) 
                VALUES ('delete', old.id, json_extract(old.data, '$.{field}'));
            END
            """
        )

        # Populate the FTS index with existing data
        self.db.execute(
            f"""
            INSERT INTO {fts_table_name}(rowid, {index_name})
            SELECT id, json_extract(data, '$.{field}') 
            FROM {self.name} 
            WHERE json_extract(data, '$.{field}') IS NOT NULL
            """
        )

    def create_indexes(
        self,
        indexes: List[Dict[str, Any]],
        reindex: bool = True,
    ) -> List[str]:
        """
        Create multiple indexes at once.

        This method processes a list of index specifications and creates corresponding
        indexes in the database. It supports various formats for index specifications,
        including PyMongo IndexModel objects, strings, lists, and dictionaries.

        Args:
            indexes (List[Dict[str, Any]]): A list of index specifications. Each
                specification can be a PyMongo-like IndexModel object, a dictionary
                with 'key' and optional 'unique' and 'sparse' parameters, a string
                key, or a list of keys for compound indexes.
            reindex (bool, optional): Whether to reindex. Not used in this implementation.

        Returns:
            List[str]: A list of index names that were created.

        Notes:
            - Index names are generated based on the key, using underscores to
              separate nested keys.
            - The method handles both single-key and compound indexes.
            - The 'reindex' parameter is not used in this implementation.
        """
        created_indexes = []

        for index_spec in indexes:
            # Handle PyMongo IndexModel objects
            if hasattr(index_spec, "document"):
                # This is a PyMongo IndexModel object
                doc = index_spec.get("document", {})
                key = doc.get("key", {})
                unique = doc.get("unique", False)
                sparse = doc.get("sparse", False)
                fts = doc.get("fts", False)

                # Convert key dict to our format
                if isinstance(key, dict):
                    # Convert {'name': 1, 'age': -1} to ['name', 'age'] for simplicity
                    # In a more advanced implementation, we could handle sort order
                    key_list = list(key.keys())
                    if len(key_list) == 1:
                        key = key_list[0]  # Single key
                    else:
                        key = key_list  # Compound key
                elif isinstance(key, list):
                    # Handle list format like [('name', 1), ('age', -1)]
                    key_list = [k for k, _ in key] if key else []
                    if len(key_list) == 1:
                        key = key_list[0]
                    else:
                        key = key_list

                self.create_index(key, unique=unique, sparse=sparse, fts=fts)
                if isinstance(key, str):
                    index_name = key.replace(".", "_")
                else:
                    index_name = "_".join(key).replace(".", "_")
                created_indexes.append(f"idx_{self.name}_{index_name}")

            # Handle our existing formats
            elif isinstance(index_spec, str):
                # Simple string key
                self.create_index(index_spec)
                index_name = index_spec.replace(".", "_")
                created_indexes.append(f"idx_{self.name}_{index_name}")
            elif isinstance(index_spec, list):
                # List of keys for compound index
                # Handle both ['name', 'age'] and [('name', 1), ('age', -1)] formats
                if index_spec and isinstance(index_spec[0], tuple):  # type: ignore
                    # Format [('name', 1), ('age', -1)] - extract just the field names
                    key_list = [k for k, _ in index_spec]
                    self.create_index(key_list)
                    index_name = "_".join(key_list).replace(".", "_")
                else:
                    # Format ['name', 'age']
                    self.create_index(index_spec)
                    index_name = "_".join(index_spec).replace(".", "_")
                created_indexes.append(f"idx_{self.name}_{index_name}")
            elif isinstance(index_spec, dict):
                # Dictionary with key and options
                key = index_spec.get("key")
                unique = index_spec.get("unique", False)
                sparse = index_spec.get("sparse", False)
                fts = index_spec.get("fts", False)

                if key is not None:
                    self.create_index(
                        key, unique=unique, sparse=sparse, fts=fts
                    )
                    if isinstance(key, str):
                        index_name = key.replace(".", "_")
                    else:
                        index_name = "_".join(str(k) for k in key).replace(
                            ".", "_"
                        )
                    created_indexes.append(f"idx_{self.name}_{index_name}")

        return created_indexes

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: List[Dict[str, Any]] | None = None,
    ):
        """
        Reindex the collection.

        With native JSON indexing, reindexing is handled automatically by SQLite.
        This method is kept for API compatibility but does nothing.

        Args:
            table (str): The table name (not used in this implementation).
            sparse (bool): Whether the index should be sparse (not used in this implementation).
            documents (List[Dict[str, Any]]): List of documents to reindex (not used in this implementation).
        """
        # With native JSON indexing, reindexing is handled automatically by SQLite
        # This method is kept for API compatibility but does nothing
        pass

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> List[List[str]]: ...
    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> List[str]: ...
    def list_indexes(
        self,
        as_keys: bool = False,
    ) -> List[str] | List[List[str]]:
        """
        Retrieve indexes for the collection. Indexes are identified by names following a specific pattern.

        Args:
            as_keys (bool): If True, return the key names (converted from
                            underscores to dots) instead of the full index names.

        Returns:
            List[str] or List[List[str]]: List of index names or keys, depending on the as_keys parameter.
                If as_keys is True, each entry is a list containing a single string (the key name).
        """
        # Get indexes that match our naming convention
        cmd = (
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE ?"
        )
        like_pattern = f"idx_{self.name}_%"
        if as_keys:
            # Extract key names from index names
            indexes = self.db.execute(cmd, (like_pattern,)).fetchall()
            result = []
            for idx in indexes:
                # Extract key name from index name (idx_collection_key -> key)
                key_name = idx[0][len(f"idx_{self.name}_") :]
                # Convert underscores back to dots for nested keys
                key_name = key_name.replace("_", ".")
                result.append([key_name])
            return result
        # Return index names
        return [
            idx[0] for idx in self.db.execute(cmd, (like_pattern,)).fetchall()
        ]

    def drop_index(self, index: str):
        """
        Drop an index from the collection.

        Handles both single-key and compound indexes. For compound indexes, the
        input should be a list of field names. The index name is generated by
        joining the field names with underscores and replacing dots with underscores.

        Args:
            index (str or list): The name of the index to drop. If a list is provided,
                                 it represents a compound index.
        """
        # With native JSON indexing, we just need to drop the index
        if isinstance(index, str):
            # For single indexes
            index_name = index.replace(".", "_")
            self.db.execute(
                f"DROP INDEX IF EXISTS idx_{self.name}_{index_name}"
            )
        else:
            # For compound indexes
            index_name = "_".join(index).replace(".", "_")
            self.db.execute(
                f"DROP INDEX IF EXISTS idx_{self.name}_{index_name}"
            )

    def drop_indexes(self):
        """
        Drop all indexes associated with this collection.

        This method retrieves the list of indexes using the list_indexes method
        and drops each one.
        """
        indexes = self.list_indexes()
        for index in indexes:
            # Extract the actual index name from the full name
            self.db.execute(f"DROP INDEX IF EXISTS {index}")

    def rename(self, new_name: str) -> None:
        """
        Renames the collection to the specified new name.
        If the new name is the same as the current name, does nothing.

        Checks if a table with the new name exists and raises an error if it does.
        Renames the underlying table and updates the collection's name.

        Args:
            new_name (str): The new name for the collection.

        Raises:
            sqlite3.Error: If a collection with the new name already exists.
        """
        # If the new name is the same as the current name, do nothing
        if new_name == self.name:
            return

        # Check if a collection with the new name already exists
        if self._object_exists("table", new_name):
            raise sqlite3.Error(f"Collection '{new_name}' already exists")

        # Rename the table
        self.db.execute(f"ALTER TABLE {self.name} RENAME TO {new_name}")

        # Update the collection name
        self.name = new_name

    def options(self) -> Dict[str, Any]:
        """
        Retrieves options set on this collection.

        Returns:
            dict: A dictionary containing various options for the collection,
                including the table's name, columns, indexes, and count of documents.
        """
        # For SQLite, we can provide information about the table structure
        options: Dict[str, Any] = {
            "name": self.name,
        }

        # Get table information
        try:
            # Get table info
            table_info = self.db.execute(
                f"PRAGMA table_info({self.name})"
            ).fetchall()
            options["columns"] = [
                {
                    "name": str(col[1]),
                    "type": str(col[2]),
                    "notnull": bool(col[3]),
                    "default": col[4],
                    "pk": bool(col[5]),
                }
                for col in table_info
            ]

            # Get index information
            indexes = self.db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (self.name,),
            ).fetchall()
            options["indexes"] = [
                {
                    "name": str(idx[0]),
                    "definition": str(idx[1]) if idx[1] is not None else "",
                }
                for idx in indexes
            ]

            # Get row count
            count_row = self.db.execute(
                f"SELECT COUNT(*) FROM {self.name}"
            ).fetchone()
            options["count"] = (
                int(count_row[0])
                if count_row and count_row[0] is not None
                else 0
            )

        except sqlite3.Error:
            # If we can't get detailed information, return basic info
            options["columns"] = []
            options["indexes"] = []
            options["count"] = 0

        return options

    def index_information(self) -> Dict[str, Any]:
        """
        Retrieves information on this collection's indexes.

        The function fetches all indexes associated with the collection and extracts
        relevant details such as whether the index is unique and the keys used in
        the index. It constructs a dictionary where the keys are the index names
        and the values are dictionaries containing the index information.

        Returns:
            dict: A dictionary containing index information.
        """
        info: Dict[str, Any] = {}

        try:
            # Get all indexes for this collection
            indexes = self.db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (self.name,),
            ).fetchall()

            for idx_name, idx_sql in indexes:
                # Parse the index information
                index_info: Dict[str, Any] = {
                    "v": 2,  # Index version
                }

                # Check if it's a unique index
                if idx_sql and "UNIQUE" in idx_sql.upper():
                    index_info["unique"] = True
                else:
                    index_info["unique"] = False

                # Try to extract key information from the SQL
                if idx_sql:
                    # Extract key information from json_extract expressions
                    import re

                    json_extract_matches = re.findall(
                        r"json_extract\(data, '(\$..*?)'\)", idx_sql
                    )
                    if json_extract_matches:
                        # Convert SQLite JSON paths back to dot notation
                        keys = []
                        for path in json_extract_matches:
                            # Remove $ and leading dot
                            if path.startswith("$."):
                                path = path[2:]
                            keys.append(path)

                        if len(keys) == 1:
                            index_info["key"] = {keys[0]: 1}
                        else:
                            index_info["key"] = {key: 1 for key in keys}

                info[idx_name] = index_info

        except sqlite3.Error:
            # If we can't get index information, return empty dict
            pass

        return info

    @property
    def database(self):
        """
        Get the database that this collection is a part of.

        Returns:
            Database: The database this collection is associated with.
        """
        return self._database

    def _object_exists(self, type: str, name: str) -> bool:
        """
        Check if an object (table or index) of a specific type and name exists within the database.

        Args:
            type (str): The type of object to check, either "table" or "index".
            name (str): The name of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        if type == "table":
            row = self.db.execute(
                "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
                (type, name.strip("[]")),
            ).fetchone()
            return bool(row and int(row[0]) > 0)
        elif type == "index":
            # For indexes, check if it exists with our naming convention
            row = self.db.execute(
                "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
                (type, name),
            ).fetchone()
            return bool(row and int(row[0]) > 0)
        return False

    def watch(
        self,
        pipeline: List[Dict[str, Any]] | None = None,
        full_document: str | None = None,
        resume_after: Dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
        batch_size: int | None = None,
        collation: Dict[str, Any] | None = None,
        start_at_operation_time: Any | None = None,
        session: Any | None = None,
        start_after: Dict[str, Any] | None = None,
    ) -> "ChangeStream":
        """
        Monitor changes on this collection using SQLite's change tracking features.

        This method creates a change stream that allows iterating over change events
        generated by modifications to the collection. While SQLite doesn't natively
        support change streams like MongoDB, this implementation uses triggers and
        SQLite's built-in change tracking mechanisms to provide similar functionality.

        Args:
            pipeline (List[Dict[str, Any]]): Aggregation pipeline stages to apply to change events.
            full_document (str): Determines how the 'fullDocument' field is populated in change events.
            resume_after (Dict[str, Any]): Logical starting point for the change stream.
            max_await_time_ms (int): Maximum time to wait for new documents in milliseconds.
            batch_size (int): Number of documents to return per batch.
            collation (Dict[str, Any]): Collation settings for the operation.
            start_at_operation_time (Any): Operation time to start monitoring from.
            session (Any): Client session for the operation.
            start_after (Dict[str, Any]): Logical starting point for the change stream.

        Returns:
            ChangeStream: A change stream object that can be iterated over to receive change events.
        """
        return ChangeStream(
            collection=self,
            pipeline=pipeline,
            full_document=full_document,
            resume_after=resume_after,
            max_await_time_ms=max_await_time_ms,
            batch_size=batch_size,
            collation=collation,
            start_at_operation_time=start_at_operation_time,
            session=session,
            start_after=start_after,
        )
