from copy import deepcopy
from functools import partial
from itertools import starmap
from typing import Any, Dict, List, Iterator, Iterable, overload
from typing_extensions import Literal
from contextlib import contextmanager
import json
import re
import sys

# Try to use pysqlite3 for enhanced SQLite features (including JSONB)
# Fall back to standard sqlite3 if not available
try:
    import pysqlite3.dbapi2 as sqlite3
except ImportError:
    import sqlite3

ASCENDING = 1
DESCENDING = -1


class MalformedQueryException(Exception):
    pass


class MalformedDocument(Exception):
    pass


class InsertOneResult:
    def __init__(self, inserted_id: int):
        self._inserted_id = inserted_id

    @property
    def inserted_id(self) -> int:
        return self._inserted_id


class InsertManyResult:
    def __init__(self, inserted_ids: List[int]):
        self._inserted_ids = inserted_ids

    @property
    def inserted_ids(self) -> List[int]:
        return self._inserted_ids


class UpdateResult:
    def __init__(
        self,
        matched_count: int,
        modified_count: int,
        upserted_id: int | None,
    ):
        self._matched_count = matched_count
        self._modified_count = modified_count
        self._upserted_id = upserted_id

    @property
    def matched_count(self) -> int:
        return self._matched_count

    @property
    def modified_count(self) -> int:
        return self._modified_count

    @property
    def upserted_id(self) -> int | None:
        return self._upserted_id


class DeleteResult:
    def __init__(self, deleted_count: int):
        self._deleted_count = deleted_count

    @property
    def deleted_count(self) -> int:
        return self._deleted_count


class BulkWriteResult:
    def __init__(
        self,
        inserted_count: int,
        matched_count: int,
        modified_count: int,
        deleted_count: int,
        upserted_count: int,
    ):
        self.inserted_count = inserted_count
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count
        self.upserted_count = upserted_count


class InsertOne:
    def __init__(self, document: Dict[str, Any]):
        self.document = document


class UpdateOne:
    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ):
        self.filter = filter
        self.update = update
        self.upsert = upsert


class DeleteOne:
    def __init__(self, filter: Dict[str, Any]):
        self.filter = filter


class Cursor:
    def __init__(
        self,
        collection: "Collection",
        filter: Dict[str, Any] | None = None,
        projection: Dict[str, Any] | None = None,
        hint: str | None = None,
    ):
        self._collection = collection
        self._filter = filter or {}
        self._projection = projection or {}
        self._hint = hint
        self._skip = 0
        self._limit: int | None = None
        self._sort: Dict[str, int] | None = None

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return self._execute_query()

    def limit(self, limit: int) -> "Cursor":
        self._limit = limit
        return self

    def skip(self, skip: int) -> "Cursor":
        self._skip = skip
        return self

    def sort(
        self,
        key_or_list: str | List[tuple],
        direction: int | None = None,
    ) -> "Cursor":
        if isinstance(key_or_list, str):
            self._sort = {key_or_list: direction or ASCENDING}
        else:
            self._sort = dict(key_or_list)
        return self

    def _execute_query(self) -> Iterator[Dict[str, Any]]:
        # Get the documents based on filter
        docs = self._get_filtered_documents()

        # Apply sorting if specified
        docs = self._apply_sorting(docs)

        # Apply skip and limit
        docs = self._apply_pagination(docs)

        # Apply projection
        docs = self._apply_projection(docs)

        # Yield results
        yield from docs

    def _get_filtered_documents(self) -> Iterable[Dict[str, Any]]:
        """Get documents based on the filter criteria."""
        where_result = self._collection._build_simple_where_clause(self._filter)

        if where_result is not None:
            # Use SQL-based filtering
            where_clause, params = where_result
            cmd = f"SELECT id, data FROM {self._collection.name} {where_clause}"
            db_cursor = self._collection.db.execute(cmd, params)
            return starmap(self._collection._load, db_cursor.fetchall())
        else:
            # Fallback to Python-based filtering for complex queries
            cmd = f"SELECT id, data FROM {self._collection.name}"
            db_cursor = self._collection.db.execute(cmd)
            apply = partial(self._collection._apply_query, self._filter)
            all_docs = starmap(self._collection._load, db_cursor.fetchall())
            return filter(apply, all_docs)

    def _apply_sorting(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply sorting to the documents."""
        if not self._sort:
            return list(docs)

        sort_keys = list(self._sort.keys())
        sort_keys.reverse()
        sorted_docs = list(docs)
        for key in sort_keys:
            get_val = partial(self._collection._get_val, key=key)
            reverse = self._sort[key] == DESCENDING
            sorted_docs.sort(key=get_val, reverse=reverse)
        return sorted_docs

    def _apply_pagination(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply skip and limit to the documents."""
        doc_list = list(docs)
        skipped_docs = doc_list[self._skip :]

        if self._limit is not None:
            return skipped_docs[: self._limit]
        return skipped_docs

    def _apply_projection(
        self, docs: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply projection to the documents."""
        project = partial(self._collection._apply_projection, self._projection)
        return list(map(project, docs))


class Connection:
    def __init__(self, *args: Any, **kwargs: Any):
        self._collections: Dict[str, "Collection"] = {}
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any):
        self.db = sqlite3.connect(*args, **kwargs)
        self.db.isolation_level = None
        self.db.execute("PRAGMA journal_mode=WAL")

    def close(self):
        if self.db is not None:
            if self.db.in_transaction:
                self.db.commit()
            self.db.close()

    def __getitem__(self, name: str) -> "Collection":
        if name not in self._collections:
            self._collections[name] = Collection(self.db, name, database=self)
        return self._collections[name]

    def __getattr__(self, name: str) -> Any:
        if name in self.__dict__:
            return self.__dict__[name]
        return self[name]

    def __enter__(self) -> "Connection":
        return self

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_traceback: Any
    ) -> Literal[False]:
        self.close()
        return False

    def drop_collection(self, name: str):
        self.db.execute(f"DROP TABLE IF EXISTS {name}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """A context manager for database transactions."""
        try:
            self.db.execute("BEGIN")
            yield
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise


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
        self, update_spec: Dict[str, Any], doc_id: int
    ) -> bool:
        """Check if all operations in the update spec can be handled with SQL."""
        # Only handle operations that can be done purely with SQL
        supported_ops = {"$set", "$unset", "$inc", "$mul", "$min", "$max"}
        # Also check that doc_id is not 0 (which indicates an upsert)
        return doc_id != 0 and all(
            op in supported_ops for op in update_spec.keys()
        )

    def _perform_sql_update(
        self, doc_id: int, update_spec: Dict[str, Any]
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
        self, op: str, value: Any
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
            new_doc: Dict[str, Any] = {}
            self._internal_update(0, update, new_doc)
            inserted_id = self.insert_one(new_doc).inserted_id
            return UpdateResult(
                matched_count=0, modified_count=0, upserted_id=inserted_id
            )

        return UpdateResult(matched_count=0, modified_count=0, upserted_id=None)

    def update_many(
        self, filter: Dict[str, Any], update: Dict[str, Any]
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
        self, update: Dict[str, Any]
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

    def bulk_write(self, requests: List[Any]) -> BulkWriteResult:
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
        self, filter: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        doc = self.find_one(filter)
        if doc:
            self.delete_one({"_id": doc["_id"]})
        return doc

    def find_one_and_replace(
        self, filter: Dict[str, Any], replacement: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        doc = self.find_one(filter)
        if doc:
            self.replace_one({"_id": doc["_id"]}, replacement)
        return doc

    def find_one_and_update(
        self, filter: Dict[str, Any], update: Dict[str, Any]
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
        self, pipeline: List[Dict[str, Any]]
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
        self, group_query: Dict[str, Any], docs: List[Dict[str, Any]]
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
        self, projection: Dict[str, Any], document: Dict[str, Any]
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
        self, query: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """Build a SQL WHERE clause for simple queries that can be handled with json_extract."""
        clauses = []
        params = []

        for field, value in query.items():
            # If field contains dots, it's a complex nested query that we can't handle with SQL
            if "." in field:
                return None  # Fallback to Python-based filtering

            # Handle _id field specially since it's stored as a column, not in the JSON data
            if field == "_id":
                clauses.append("id = ?")
                params.append(value)
                continue

            # For other fields, use json_extract to get values from the JSON data
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
        self, json_path: str, operators: Dict[str, Any]
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
                case _:
                    # Unsupported operator, return None to indicate we should fallback to Python
                    return None, []

        # This shouldn't happen, but just in case
        return None, []

    def _apply_query(
        self, query: Dict[str, Any], document: Dict[str, Any]
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
            return getattr(sys.modules[__name__], op.replace("$", "_"))
        except AttributeError:
            raise MalformedQueryException(
                f"Operator '{op}' is not currently implemented"
            )

    def distinct(self, key: str) -> set:
        cmd = f"SELECT DISTINCT json_extract(data, '$.{key}') FROM {self.name}"
        cursor = self.db.execute(cmd)
        results = set()
        for row in cursor.fetchall():
            if row[0] is None:
                continue
            try:
                results.add(json.loads(row[0]))
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
            reindex: Whether to reindex (kept for API compatibility)

        Returns:
            A list of index names that were created
        """
        created_indexes = []

        for index_spec in indexes:
            # Handle different index specification formats
            if isinstance(index_spec, str):
                # Simple string key
                self.create_index(index_spec)
                index_name = index_spec.replace(".", "_")
                created_indexes.append(f"idx_{self.name}_{index_name}")
            elif isinstance(index_spec, list):
                # List of keys for compound index
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
                        index_name = "_".join(key).replace(".", "_")
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
        self, as_keys: bool = False
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
        :raises sqlite3.Error: If the rename operation fails.
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
                        r"json_extract\(data, '(\$.+?)'\)", idx_sql
                    )
                    if json_extract_matches:
                        # Convert SQLite JSON paths back to dot notation
                        keys = []
                        for path in json_extract_matches:
                            # Remove $ and leading dot, then convert _ back to .
                            if path.startswith("$"):
                                path = path[1:]  # Remove $
                            if path.startswith("."):
                                path = path[1:]  # Remove leading dot
                            path = path.replace("_", ".")
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


# Query operators
def _eq(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) == value
    except (TypeError, AttributeError):
        return False


def _gt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) > value
    except TypeError:
        return False


def _lt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) < value
    except TypeError:
        return False


def _gte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) >= value
    except TypeError:
        return False


def _lte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    try:
        return document.get(field, None) <= value
    except TypeError:
        return False


def _all(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    try:
        a = set(value)
    except TypeError:
        raise MalformedQueryException("'$all' must accept an iterable")
    try:
        b = set(document.get(field, []))
    except TypeError:
        return False
    else:
        return a.issubset(b)


def _in(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$in' must accept an iterable")
    return document.get(field, None) in values


def _ne(field: str, value: Any, document: Dict[str, Any]) -> bool:
    return document.get(field, None) != value


def _nin(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$nin' must accept an iterable")
    return document.get(field, None) not in values


def _mod(field: str, value: List[int], document: Dict[str, Any]) -> bool:
    try:
        divisor, remainder = list(map(int, value))
    except (TypeError, ValueError):
        raise MalformedQueryException(
            "'$mod' must accept an iterable: [divisor, remainder]"
        )
    try:
        val = document.get(field, None)
        if val is None:
            return False
        return int(val) % divisor == remainder
    except (TypeError, ValueError):
        return False


def _exists(field: str, value: bool, document: Dict[str, Any]) -> bool:
    if value not in (True, False):
        raise MalformedQueryException("'$exists' must be supplied a boolean")
    if value:
        return field in document
    else:
        return field not in document


def _regex(field: str, value: str, document: Dict[str, Any]) -> bool:
    try:
        return re.search(value, document.get(field, "")) is not None
    except (TypeError, re.error):
        return False


def _elemMatch(
    field: str, value: Dict[str, Any], document: Dict[str, Any]
) -> bool:
    field_val = document.get(field)
    if not isinstance(field_val, list):
        return False
    for elem in field_val:
        if isinstance(elem, dict) and all(
            _eq(k, v, elem) for k, v in value.items()
        ):
            return True
    return False


def _size(field: str, value: int, document: Dict[str, Any]) -> bool:
    field_val = document.get(field)
    if not isinstance(field_val, list):
        return False
    return len(field_val) == value
