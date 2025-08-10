from copy import deepcopy
from functools import partial
from itertools import starmap
from typing import Any, Dict, List, Iterator, Iterable, overload
from typing_extensions import Literal
import json
import re
import sqlite3
import sys

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


class Cursor:
    def __init__(
        self,
        collection: "Collection",
        filter: Dict[str, Any] | None = None,
        hint: str | None = None,
    ):
        self._collection = collection
        self._filter = filter or {}
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
        query = self._filter

        index_name = ""
        where = ""
        if self._hint:
            keys = self._collection._table_name_as_keys(self._hint)
            index_name = self._hint
        else:
            keys = [
                key.replace(".", "_")
                for key in query
                if not key.startswith("$")
            ]
            if keys:
                index_name = f'[{self._collection.name}{{{",".join(keys)}}}]'

        if index_name in self._collection.list_indexes():
            index_query = " AND ".join(
                [
                    f"{key}='{json.dumps(query[key.replace('_', '.')])}'"
                    for key in keys
                ]
            )
            where = (
                f"WHERE id IN (SELECT id FROM {index_name} WHERE {index_query})"
            )

        cmd = f"SELECT id, data FROM {self._collection.name} {where}"
        db_cursor = self._collection.db.execute(cmd)
        apply = partial(self._collection._apply_query, query)

        all_docs = starmap(self._collection._load, db_cursor.fetchall())
        filtered_docs: Iterable[Dict[str, Any]] = filter(apply, all_docs)

        if self._sort:
            sort_keys = list(self._sort.keys())
            sort_keys.reverse()
            for key in sort_keys:
                get_val = partial(self._collection._get_val, key=key)
                reverse = self._sort[key] == DESCENDING
                filtered_docs = sorted(
                    filtered_docs, key=get_val, reverse=reverse
                )

        skipped_docs = list(filtered_docs)[self._skip :]

        if self._limit is not None:
            yield from skipped_docs[: self._limit]
        else:
            yield from skipped_docs


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
            self._collections[name] = Collection(self.db, name)
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


class Collection:
    def __init__(self, db: sqlite3.Connection, name: str, create: bool = True):
        self.db = db
        self.name = name
        if create:
            self.create()

    def create(self):
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            )"""
        )

    def _load(self, id: int, data: str | bytes) -> Dict[str, Any]:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        document: Dict[str, Any] = json.loads(data)
        document["_id"] = id
        return document

    def _get_val(self, item: Dict[str, Any], key: str) -> Any:
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

        try:
            for index in self.list_indexes():
                self.reindex(table=index, documents=[document])
        except sqlite3.IntegrityError as ie:
            self.delete_one({"_id": inserted_id})
            raise ie
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
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            if op == "$set":
                doc_to_update.update(value)
            elif op == "$unset":
                for k in value:
                    doc_to_update.pop(k, None)
            elif op == "$inc":
                for k, v in value.items():
                    doc_to_update[k] = doc_to_update.get(k, 0) + v
            else:
                raise MalformedQueryException(
                    f"Update operator '{op}' not supported"
                )

        self.db.execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (json.dumps(doc_to_update), doc_id),
        )

        try:
            doc_to_update["_id"] = doc_id
            for index in self.list_indexes():
                self.reindex(table=index, documents=[doc_to_update])
        except sqlite3.IntegrityError as ie:
            self.db.execute(
                f"UPDATE {self.name} SET data = ? WHERE id = ?",
                (json.dumps(original_doc), doc_id),
            )
            raise ie
        return doc_to_update

    def _internal_replace(self, doc_id: int, replacement: Dict[str, Any]):
        self.db.execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (json.dumps(replacement), doc_id),
        )
        try:
            replacement["_id"] = doc_id
            for index in self.list_indexes():
                self.reindex(table=index, documents=[replacement])
        except sqlite3.IntegrityError as ie:
            raise ie

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

    def delete_one(self, filter: Dict[str, Any]) -> DeleteResult:
        doc = self.find_one(filter)
        if doc:
            self.db.execute(
                f"DELETE FROM {self.name} WHERE id = ?", (doc["_id"],)
            )
            return DeleteResult(deleted_count=1)
        return DeleteResult(deleted_count=0)

    def delete_many(self, filter: Dict[str, Any]) -> DeleteResult:
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
        hint: str | None = None,
    ) -> Cursor:
        return Cursor(self, filter, hint)

    def find_one(
        self,
        filter: Dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> Dict[str, Any] | None:
        try:
            return next(iter(self.find(filter, hint).limit(1)))
        except StopIteration:
            return None

    def count_documents(self, filter: Dict[str, Any]) -> int:
        return len(list(self.find(filter)))

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
        return {d[key] for d in self.find() if key in d}

    def create_index(
        self,
        key: str | List[str],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
    ):
        if isinstance(key, (list, tuple)):
            index_name = ",".join(key)
            index_columns = ", ".join(f"{f} text" for f in key)
        else:
            index_name = key
            index_columns = f"{key} text"

        index_name = index_name.replace(".", "_")
        index_columns = index_columns.replace(".", "_")
        table_name = f"[{self.name}{{{index_name}}}]"
        reindex = reindex or not self._object_exists("table", table_name)

        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                {index_columns},
                FOREIGN KEY(id) REFERENCES {self.name}(id)
                ON DELETE CASCADE ON UPDATE CASCADE
            )"""
        )
        self.db.execute(
            f"""
            CREATE {'UNIQUE ' if unique else ''}INDEX
            IF NOT EXISTS [idx.{self.name}{{{index_name}}}]
            ON {table_name}({index_name})
            """
        )
        if reindex:
            try:
                self.reindex(table_name)
            except sqlite3.IntegrityError as ie:
                self.drop_index(table_name)
                raise ie

    def _table_name_as_keys(self, table: str) -> List[str]:
        return re.findall(r"^\[.*\{(.*)\}\]$", table)[0].split(",")

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: List[Dict[str, Any]] | None = None,
    ):
        index_keys = self._table_name_as_keys(table)
        update_sql = "UPDATE {table} SET {key} = ? WHERE id = ?"
        insert_sql = "INSERT INTO {table}({index},id) VALUES({q},{_id})"
        delete_sql = "DELETE FROM {table} WHERE id = {_id}"
        count_sql = "SELECT COUNT(1) FROM {table} WHERE id = ?"
        qs = ("?," * len(index_keys)).rstrip(",")

        docs_to_index = documents or self.find()

        for document in docs_to_index:
            _id = document["_id"]
            row = self.db.execute(
                count_sql.format(table=table), (_id,)
            ).fetchone()
            if row and int(row[0]) == 0:
                self.db.execute(
                    insert_sql.format(
                        table=table, index=",".join(index_keys), q=qs, _id=_id
                    ),
                    [None for _ in index_keys],
                )
            for key in index_keys:
                doc = deepcopy(document)
                val: Any = doc
                for k in key.split("_"):
                    if isinstance(val, dict):
                        if k not in val and sparse:
                            val = None
                            break
                        val = val.get(k, None)
                    else:
                        val = None
                        break
                try:
                    self.db.execute(
                        update_sql.format(table=table, key=key),
                        (json.dumps(val), _id),
                    )
                except sqlite3.IntegrityError as ie:
                    self.db.execute(delete_sql.format(table=table, _id=_id))
                    raise ie

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> List[List[str]]: ...
    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> List[str]: ...
    def list_indexes(
        self, as_keys: bool = False
    ) -> List[str] | List[List[str]]:
        cmd = (
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?"
        )
        like_pattern = f"{self.name}{{{'%'}}}"
        if as_keys:
            return [
                self._table_name_as_keys(f"[{t[0]}]")
                for t in self.db.execute(cmd, (like_pattern,)).fetchall()
            ]
        return [
            f"[{t[0]}]"
            for t in self.db.execute(cmd, (like_pattern,)).fetchall()
        ]

    def drop_index(self, index: str):
        self.db.execute(f"DROP TABLE {index}")

    def drop_indexes(self):
        indexes = self.list_indexes()
        for index in indexes:
            if isinstance(index, str):
                self.drop_index(index)

    def _object_exists(self, type: str, name: str) -> bool:
        row = self.db.execute(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
            (type, name.strip("[]")),
        ).fetchone()
        return bool(row and int(row[0]) > 0)


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
