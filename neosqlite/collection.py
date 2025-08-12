from copy import deepcopy
import json
from typing import Any, Dict, List, overload
from typing_extensions import Literal

try:
    import pysqlite3.dbapi2 as sqlite3
except ImportError:
    import sqlite3

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
    def __init__(
        self,
        db: sqlite3.Connection,
        name: str,
        create: bool = True,
        database=None,
    ):
        self.db = db
        self.name = name
        self._database = database
        if create:
            self.create()

    def create(self):
        try:
            self.db.execute("""SELECT jsonb('{"key": "value"}')""")
        except sqlite3.OperationalError:
            self.db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL
                )"""
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
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        document: Dict[str, Any] = json.loads(data)
        document["_id"] = id
        return document

    def _get_val(self, item: Dict[str, Any], key: str) -> Any:
        if key.startswith("$"):
            key = key[1:]
        val: Any = item
        for k in key.split("."):
            if val is None:
                return None
            val = val.get(k)
        return val

    def _internal_insert(self, document: Dict[str, Any]) -> int:
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

        # With native JSON indexing, SQLite handles index updates automatically
        # No need to manually reindex

        return inserted_id

    def insert_one(self, document: Dict[str, Any]) -> InsertOneResult:
        inserted_id = self._internal_insert(document)
        return InsertOneResult(inserted_id)

    def insert_many(self, documents: List[Dict[str, Any]]) -> InsertManyResult:
        inserted_ids = [self._internal_insert(doc) for doc in documents]
        return InsertManyResult(inserted_ids)

    def _internal_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
    ):
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
        """Check if all operations in the update spec can be handled with SQL."""
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
        """Perform update operations using SQL JSON functions."""
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
            cmd = f"UPDATE {self.name} SET data = json_remove(data, {', '.join(unset_clauses)}) WHERE id = ?"
            sql_params = unset_params + [doc_id]
            self.db.execute(cmd, sql_params)

        if set_clauses:
            # Handle other operations with json_set
            cmd = f"UPDATE {self.name} SET data = json_set(data, {', '.join(set_clauses)}) WHERE id = ?"
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
        """Build SQL update clause for a single operation."""
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
        """Perform update operations using Python-based logic."""
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
            # With native JSON indexing, SQLite handles index updates automatically
            # No need to manually reindex

        return doc_to_update

    def _internal_replace(self, doc_id: int, replacement: Dict[str, Any]):
        self.db.execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (json.dumps(replacement), doc_id),
        )
        # With native JSON indexing, SQLite handles index updates automatically
        # No need to manually reindex

    def _internal_delete(self, doc_id: int):
        self.db.execute(f"DELETE FROM {self.name} WHERE id = ?", (doc_id,))

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> UpdateResult:
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
        doc = self.find_one(filter)
        if doc:
            self._internal_delete(doc["_id"])
            return DeleteResult(deleted_count=1)
        return DeleteResult(deleted_count=0)

    def delete_many(self, filter: Dict[str, Any]) -> DeleteResult:
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

        Example usage:

          >>> import json
          >>> cursor = collection.find_raw_batches()
          >>> for batch in cursor:
          ...     # Each batch is raw bytes containing JSON documents
          ...     # separated by newlines
          ...     documents = [json.loads(doc) for doc in batch.decode('utf-8').split('\n') if doc]
          ...     print(documents)

        :param filter: A dictionary specifying the query criteria.
        :param projection: A dictionary specifying which fields to return.
        :param hint: A string specifying the index to use.
        :param batch_size: The number of documents to include in each batch.
        :return: A RawBatchCursor instance.
        """
        return RawBatchCursor(self, filter, projection, hint, batch_size)

    def find_one(
        self,
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> Dict[str, Any] | None:
        try:
            return next(iter(self.find(filter, projection, hint).limit(1)))
        except StopIteration:
            return None

    def count_documents(self, filter: Dict[str, Any]) -> int:
        where_result = self._build_simple_where_clause(filter)
        if where_result is not None:
            where_clause, params = where_result
            cmd = f"SELECT COUNT(id) FROM {self.name} {where_clause}"
            row = self.db.execute(cmd, params).fetchone()
            return row[0] if row else 0
        return len(list(self.find(filter)))

    def estimated_document_count(self) -> int:
        row = self.db.execute(f"SELECT COUNT(1) FROM {self.name}").fetchone()
        return row[0] if row else 0

    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        doc = self.find_one(filter)
        if doc:
            self.delete_one({"_id": doc["_id"]})
        return doc

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        doc = self.find_one(filter)
        if doc:
            self.replace_one({"_id": doc["_id"]}, replacement)
        return doc

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        doc = self.find_one(filter)
        if doc:
            self.update_one({"_id": doc["_id"]}, update)
        return doc

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    def _build_simple_where_clause(
        self,
        query: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """Build a SQL WHERE clause for simple queries that can be handled with json_extract."""
        clauses = []
        params = []

        for field, value in query.items():
            # Handle _id field specially since it's stored as a column, not in the JSON data
            if field == "_id":
                clauses.append("id = ?")
                params.append(value)
                continue

            # For all fields (including nested ones), use json_extract to get values from the JSON data
            # Convert dot notation to JSON path notation (e.g., "profile.age" -> "$.profile.age")
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
        """Build a SQL clause for query operators."""
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
        if document is None:
            return False
        matches: List[bool] = []

        def reapply(q: Dict[str, Any]) -> bool:
            return self._apply_query(q, document)

        for field, value in query.items():
            if field == "$and":
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
        params: List[Any] = []
        where_clause = ""

        if filter:
            where_result = self._build_simple_where_clause(filter)
            if where_result:
                where_clause, params = where_result

        cmd = f"SELECT DISTINCT json_extract(data, '$.{key}') FROM {self.name} {where_clause}"
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
    ):
        # For single key indexes, we can use SQLite's native JSON indexing
        if isinstance(key, str):
            # Create index name (replace dots with underscores for valid identifiers)
            index_name = key.replace(".", "_")

            # Create the index using json_extract
            self.db.execute(
                f"""
                CREATE {'UNIQUE ' if unique else ''}INDEX
                IF NOT EXISTS [idx_{self.name}_{index_name}]
                ON {self.name}(json_extract(data, '$.{key}'))
                """
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
                f"""
                CREATE {'UNIQUE ' if unique else ''}INDEX
                IF NOT EXISTS [idx_{self.name}_{index_name}]
                ON {self.name}({index_columns})
                """
            )

    def create_indexes(
        self,
        indexes: List[Dict[str, Any]],
        reindex: bool = True,
    ) -> List[str]:
        """
        Create multiple indexes at once.

        Args:
            indexes: A list of index specifications. Each specification can be:
                     - A string for a single key index
                     - A list of strings for a compound index
                     - A dict with 'key' (string or list) and optional 'unique' (bool)
                     - A PyMongo IndexModel object
            reindex: Whether to reindex (kept for API compatibility)

        Returns:
            A list of index names that were created
        """
        created_indexes = []

        for index_spec in indexes:
            # Handle PyMongo IndexModel objects
            if hasattr(index_spec, "document"):
                # This is a PyMongo IndexModel object
                doc = index_spec.document
                key = doc.get("key", {})
                unique = doc.get("unique", False)
                sparse = doc.get("sparse", False)

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

                self.create_index(key, unique=unique, sparse=sparse)
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
                if index_spec and isinstance(index_spec[0], tuple):
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

                if key is not None:
                    self.create_index(key, unique=unique, sparse=sparse)
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
        indexes = self.list_indexes()
        for index in indexes:
            # Extract the actual index name from the full name
            self.db.execute(f"DROP INDEX IF EXISTS {index}")

    def rename(self, new_name: str) -> None:
        """
        Rename this collection.

        :param new_name: The new name for this collection.
        :raises sqlite3.Error: If the rename operation fails
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
        Get the options set on this collection.

        :return: A dictionary of collection options.
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
        Get information on this collection's indexes.

        :return: A dictionary of index information.
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

        :return: The database object.
        """
        return self._database

    def _object_exists(self, type: str, name: str) -> bool:
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
        Watch changes on this collection.

        This implementation uses SQLite's built-in features to monitor changes.
        It creates a change stream that can be iterated over to receive change events.

        :param pipeline: A list of aggregation pipeline stages to apply to the change events.
        :param full_document: Determines how the 'fullDocument' response field is populated.
        :param resume_after: Specifies the logical starting point for the new change stream.
        :param max_await_time_ms: The maximum amount of time for the server to wait on new documents.
        :param batch_size: The number of documents to return per batch.
        :param collation: Specifies the collation to use for the operation.
        :param start_at_operation_time: The operation time to use as the starting point.
        :param session: The client session to use.
        :param start_after: Specifies the logical starting point for the new change stream.
        :return: A ChangeStream object that can be iterated over.
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
