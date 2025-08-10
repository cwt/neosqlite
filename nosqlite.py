from copy import deepcopy
from functools import partial
from itertools import starmap
import json
import re
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Union

ASCENDING = False
DESCENDING = True


class MalformedQueryException(Exception):
    pass


class MalformedDocument(Exception):
    pass


class Connection:
    """
    The high-level connection to a sqlite database. Creating a connection
    accepts the same args and keyword args as the ``sqlite3.connect`` method
    """

    def __init__(self, *args, **kwargs):
        self._collections = {}
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        """
        Connect to a sqlite database only if no connection exists. Isolation
        level for the connection is automatically set to autocommit
        """
        self.db = sqlite3.connect(*args, **kwargs)
        self.db.isolation_level = None
        self.db.execute("PRAGMA journal_mode=WAL")  # Set WAL journal mode

    def close(self):
        """
        Terminate the connection to the sqlite database
        """
        if self.db is not None:
            if self.db.in_transaction:
                self.db.commit()
            self.db.close()

    def __getitem__(self, name: str) -> "Collection":
        """
        A pymongo-like behavior for dynamically obtaining a collection of
        documents
        """
        if name not in self._collections:
            self._collections[name] = Collection(self.db, name)
        return self._collections[name]

    def __getattr__(self, name: str) -> Any:
        if name in self.__dict__:
            return self.__dict__[name]
        return self[name]

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback) -> bool:
        self.close()
        return False

    def drop_collection(self, name: str):
        """
        Drops a collection permanently if it exists
        """
        self.db.execute(f"DROP TABLE IF EXISTS {name}")


class Collection:
    """
    A virtual database table that holds JSON-type documents
    """

    def __init__(self, db: sqlite3.Connection, name: str, create: bool = True):
        self.db = db
        self.name = name

        if create:
            self.create()

    def begin(self):
        if not self.db.in_transaction:
            self.db.execute("BEGIN")

    def commit(self):
        if self.db.in_transaction:
            self.db.commit()

    def rollback(self):
        if self.db.in_transaction:
            self.db.rollback()

    def clear(self):
        """
        Clears all stored documents in this database. THERE IS NO GOING BACK
        """
        self.db.execute(f"DELETE FROM {self.name}")

    def exists(self) -> bool:
        """
        Checks if this collection exists
        """
        return self._object_exists("table", self.name)

    def _object_exists(self, type: str, name: str) -> bool:
        row = self.db.execute(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = ? AND name = ?",
            (type, name.strip("[]")),
        ).fetchone()

        return int(row[0]) > 0

    def create(self):
        """
        Creates the collections database only if it does not already exist
        """
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            )"""
        )

    def insert(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inserts a document into this collection. If a document already has an
        '_id' value it will be updated

        :returns: inserted document with id
        """
        if "_id" in document:
            return self.save(document)

        # Check if document is a dict
        if not isinstance(document, dict):
            raise MalformedDocument(
                f"document must be a dictionary, not a {type(document)}"
            )

        # Create it and return a modified one with the id
        cursor = self.db.execute(
            f"INSERT INTO {self.name}(data) VALUES (?)",
            (json.dumps(document),),
        )

        document["_id"] = cursor.lastrowid
        try:
            [
                self.reindex(table=index, documents=[document])
                for index in self.list_indexes()
            ]
        except sqlite3.IntegrityError as ie:
            self.delete_one({"_id": document["_id"]})
            raise ie
        return document

    def update(
        self,
        spec: Dict[str, Any],
        document: Dict[str, Any],
        upsert: bool = False,
        hint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        DEPRECATED in pymongo
        Updates a document stored in this collection.
        """
        to_update = self.find(query=spec, skip=0, limit=1, hint=hint)
        if to_update:
            to_update = to_update[0]
        else:
            if upsert:
                return self.insert(document)
            return None

        _id = to_update["_id"]

        self.db.execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (json.dumps(document), _id),
        )

        document["_id"] = _id
        try:
            [
                self.reindex(table=index, documents=[document])
                for index in self.list_indexes()
            ]
        except sqlite3.IntegrityError as ie:
            self.save(to_update)
            raise ie
        return document

    def _remove(self, document: Dict[str, Any]):
        """
        Removes a document from this collection. This will raise AssertionError
        if the document does not have an _id attribute
        """
        assert "_id" in document, "Document must have an id"
        self.db.execute(
            f"DELETE FROM {self.name} WHERE id = ?",
            (document["_id"],)
        )

    def save(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alias for ``update`` with upsert=True
        """
        return self.update(
            {"_id": document.pop("_id", None)}, document, upsert=True
        )

    def delete(self, document: Dict[str, Any]):
        """
        DEPRECATED
        Alias for ``remove``
        """
        return self._remove(document)

    def delete_one(
        self, filter: Dict[str, Any], hint: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Delete only the first document according the filter
        Params:
            - filter: dict with the condition ({'foo':'bar'})
        """
        try:
            document = self.find(query=filter, limit=1, hint=hint)[0]
        except:
            return None
        return self._remove(document)

    def _load(self, id: int, data: Union[str, bytes]) -> Dict[str, Any]:
        """
        Loads a JSON document taking care to apply the document id
        """
        if isinstance(data, bytes):  # pragma: no cover Python >= 3.0
            data = data.decode("utf-8")

        document = json.loads(data)
        document["_id"] = id
        return document

    def __get_val(self, item: Dict[str, Any], key: str) -> Any:
        for k in key.split("."):
            item = item.get(k)
        return item

    def find(
        self,
        query: Optional[Dict[str, Any]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        hint: Optional[str] = None,
        sort: Optional[Dict[str, bool]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns a list of documents in this collection that match a given query
        """
        results = []
        query = query or {}
        if skip is None:
            skip = 0

        index_name = ""
        where = ""
        if hint:
            keys = self.__table_name_as_keys(hint)
            index_name = hint
        else:
            keys = [
                key.replace(".", "_")
                for key in query
                if not key.startswith("$")
            ]
            if keys:
                index_name = f'[{self.name}{{{",".join(keys)}}}]'
        if index_name in self.list_indexes():
            index_query = " AND ".join(
                [
                    f"{key}='{json.dumps(query[key.replace('_', '.')])}'"
                    for key in keys
                ]
            )
            where = (
                f"WHERE id IN (SELECT id FROM {index_name} WHERE {index_query})"
            )
        cmd = f"SELECT id, data FROM {self.name} {where}"
        cursor = self.db.execute(cmd)
        apply = partial(self._apply_query, query)

        for match in filter(apply, starmap(self._load, cursor.fetchall())):
            if skip > 0:  # Discard match before skip
                skip -= 1
            else:
                results.append(match)

            # Just return if we already reached the limit
            if limit and len(results) == limit and sort is None:
                break

        if sort:  # sort={key1:direction1, key2:direction2, ...}
            sort_keys = list(sort.keys())
            sort_keys.reverse()  # sort from right to left
            for key in sort_keys:
                get_val = partial(self.__get_val, key=key)
                results = sorted(results, key=get_val, reverse=sort[key])

        return results[:limit] if isinstance(limit, int) else results

    def _apply_query(
        self, query: Dict[str, Any], document: Dict[str, Any]
    ) -> bool:
        """
        Applies a query to a document. Returns True if the document meets the
        criteria of the supplied query. The ``query`` argument generally
        follows mongodb style syntax and consists of the following logical
        checks and operators.

        Logical: $and, $or, $nor, $not
        Operators: $eq, $ne, $gt, $gte, $lt, $lte, $mod, $in, $nin, $all

        If no logical operator is supplied, it assumed that all field checks
        must pass. For example, these are equivalent:

            {'foo': 'bar', 'baz': 'qux'}
            {'$and': [{'foo': 'bar'}, {'baz': 'qux'}]}

        Both logical and operational queries can be nested in a complex fashion:

            {
                'bar': 'baz',
                '$or': [
                    {
                        'foo': {
                            '$gte': 0,
                            '$lte': 10,
                            '$mod': [2, 0]
                        }
                    },
                    {
                        'foo': {
                            '$gt': 10,
                            '$mod': [2, 1]
                        }
                    },
                ]
            }

        In the previous example, this will return any document where the 'bar'
        key is equal to 'baz' and either the 'foo' key is an even number
        between 0 and 10 or is an odd number greater than 10.
        """
        if document is None:
            return False
        matches = []  # A list of booleans
        reapply = lambda q: self._apply_query(q, document)

        for field, value in query.items():
            # A more complex query type $and, $or, etc
            if field == "$and":
                matches.append(all(map(reapply, value)))
            elif field == "$or":
                matches.append(any(map(reapply, value)))
            elif field == "$nor":
                matches.append(not any(map(reapply, value)))
            elif field == "$not":
                matches.append(not self._apply_query(value, document))

            # Invoke a query operator
            elif isinstance(value, dict):
                for operator, arg in value.items():
                    if not self._get_operator_fn(operator)(
                        field, arg, document
                    ):
                        matches.append(False)
                        break
                else:
                    matches.append(True)

            # Standard
            else:
                doc_value = document
                if field in doc_value:
                    doc_value = doc_value.get(field, None)
                else:
                    for path in field.split("."):
                        if doc_value is None:
                            break
                        doc_value = doc_value.get(path, None)
                if value != doc_value:
                    matches.append(False)

        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """
        Returns the function in this module that corresponds to an operator
        string. This simply checks if there is a method that handles the
        operator defined in this module, replacing '$' with '_' (i.e. if this
        module has a _gt method for $gt) and returns it. If no match is found,
        or the operator does not start with '$', a MalformedQueryException is
        raised.
        """
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

    def find_one(
        self,
        query: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Equivalent to ``find(query, limit=1)[0]``
        """
        try:
            return self.find(query=query, limit=1, hint=hint)[0]
        except (sqlite3.OperationalError, IndexError):
            return None

    def find_and_modify(
        self,
        query: Optional[Dict[str, Any]] = None,
        update: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
    ):
        """
        Finds documents in this collection that match a given query and updates
        them
        """
        update = update or {}

        for document in self.find(query=query, hint=hint):
            document.update(update)
            self.save(document)

    def count(
        self,
        query: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
    ) -> int:
        """
        Equivalent to ``len(find(query))``
        """
        return len(self.find(query=query, hint=hint))

    def rename(self, new_name: str):
        """
        Rename this collection
        """
        new_collection = Collection(self.db, new_name, create=False)
        assert not new_collection.exists()

        self.db.execute(f"ALTER TABLE {self.name} RENAME TO {new_name}")
        self.name = new_name

    def distinct(self, key: str) -> set:
        """
        Get a set of distinct values for the given key excluding an implicit
        None for documents that do not contain the key
        """
        return {d[key] for d in self.find() if key in d}

    def create_index(
        self,
        key: Union[str, List[str]],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
    ):
        """
        Creates an index if it does not exist then performs a full reindex for
        this collection
        """
        if isinstance(key, (list, tuple)):
            index_name = ",".join(key)
            index_columns = ", ".join(f"{f} text" for f in key)
        else:
            index_name = key
            index_columns = f"{key} text"

        # Allow dot notation, but save it as underscore
        index_name = index_name.replace(".", "_")
        index_columns = index_columns.replace(".", "_")

        table_name = f"[{self.name}{{{index_name}}}]"
        reindex = reindex or not self._object_exists("table", table_name)

        # Create a table store for the index data
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                {index_columns},
                FOREIGN KEY(id) REFERENCES {self.name}(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
            )
            """
        )

        # Create the index
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

    def ensure_index(self, key: Union[str, List[str]], sparse: bool = False):
        """
        Equivalent to ``create_index(key, reindex=False)``
        """
        self.create_index(key, reindex=False, sparse=False)

    def __table_name_as_keys(self, table: str) -> List[str]:
        return re.findall(r"^\[.*\{(.*)\}\]$", table)[0].split(",")

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: Optional[List[Dict[str, Any]]] = None,
    ):
        index = self.__table_name_as_keys(table)
        update = "UPDATE {table} SET {key} = ? WHERE id = ?"
        insert = "INSERT INTO {table}({index},id) VALUES({q},{_id})"
        delete = "DELETE FROM {table} WHERE id = {_id}"
        count = "SELECT COUNT(1) FROM {table} WHERE id = ?"
        qs = ("?," * len(index)).rstrip(",")

        for document in documents or self.find():
            _id = document["_id"]
            # Ensure there's a row before we update
            row = self.db.execute(count.format(table=table), (_id,)).fetchone()
            if int(row[0]) == 0:
                self.db.execute(
                    insert.format(
                        table=table, index=",".join(index), q=qs, _id=_id
                    ),
                    [None for _ in index],
                )
            for key in index:
                doc = deepcopy(document)
                for k in key.split("_"):
                    if isinstance(doc, dict):
                        # Ignore this document if it doesn't have the key
                        if k not in doc and sparse:
                            continue
                        doc = doc.get(k, None)
                try:
                    self.db.execute(
                        update.format(table=table, key=key),
                        (json.dumps(doc), _id),
                    )
                except sqlite3.IntegrityError as ie:
                    self.db.execute(delete.format(table=table, _id=_id))
                    raise ie

    def list_indexes(self, as_keys: bool = False) -> List[str]:
        cmd = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name LIKE '{name}{{%}}'"
        )
        if as_keys:
            return [
                self.__table_name_as_keys("[{index}]".format(index=t[0]))
                for t in self.db.execute(cmd.format(name=self.name)).fetchall()
            ]
        return [
            "[{index}]".format(index=t[0])
            for t in self.db.execute(cmd.format(name=self.name)).fetchall()
        ]

    def drop_index(self, index: str):
        cmd = "DROP TABLE {index}"
        self.db.execute(cmd.format(index=index))

    def drop_indexes(self):
        """
        Drop all indexes for this collection
        """
        [self.drop_index(index) for index in self.list_indexes()]


# BELOW ARE OPERATIONS FOR LOOKUPS
# TypeErrors are caught specifically for python 3 compatibility
def _eq(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of a document field is equal to a given value
    """
    try:
        return document.get(field, None) == value
    except (TypeError, AttributeError):
        return False


def _gt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of a document field is greater than a given value
    """
    try:
        return document.get(field, None) > value
    except TypeError:
        return False


def _lt(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of a document field is less than a given value
    """
    try:
        return document.get(field, None) < value
    except TypeError:
        return False


def _gte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of a document field is greater than or equal to
    a given value
    """
    try:
        return document.get(field, None) >= value
    except TypeError:
        return False


def _lte(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of a document field is less than or equal to
    a given value
    """
    try:
        return document.get(field, None) <= value
    except TypeError:
        return False


def _all(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of document field contains all the values
    specified by ``value``. If supplied value is not an iterable, a
    MalformedQueryException is raised. If the value of the document field
    is not an iterable, False is returned
    """
    try:
        a = set(value)
    except TypeError:
        raise MalformedQueryException("'$all' must accept an iterable")

    try:
        b = set(document.get(field, []))
    except TypeError:
        return False
    else:
        return a.intersection(b) == a


def _in(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Returns True if document[field] is in the iterable value. If the
    supplied value is not an iterable, then a MalformedQueryException is raised
    """
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$in' must accept an iterable")

    return document.get(field, None) in values


def _ne(field: str, value: Any, document: Dict[str, Any]) -> bool:
    """
    Returns True if the value of document[field] is not equal to a given value
    """
    return document.get(field, None) != value


def _nin(field: str, value: List[Any], document: Dict[str, Any]) -> bool:
    """
    Returns True if document[field] is NOT in the iterable value. If the
    supplied value is not an iterable, then a MalformedQueryException is raised
    """
    try:
        values = iter(value)
    except TypeError:
        raise MalformedQueryException("'$nin' must accept an iterable")

    return document.get(field, None) not in values


def _mod(field: str, value: List[int], document: Dict[str, Any]) -> bool:
    """
    Performs a mod on a document field. Value must be a list or tuple with
    two values divisor and remainder (i.e. [2, 0]). This will essentially
    perform the following:

        document[field] % divisor == remainder

    If the value does not contain integers or is not a two-item list/tuple,
    a MalformedQueryException will be raised. If the value of document[field]
    cannot be converted to an integer, this will return False.
    """
    try:
        divisor, remainder = list(map(int, value))
    except (TypeError, ValueError):
        raise MalformedQueryException(
            "'$mod' must accept an iterable: [divisor, remainder]"
        )

    try:
        return int(document.get(field, None)) % divisor == remainder
    except (TypeError, ValueError):
        return False


def _exists(field: str, value: bool, document: Dict[str, Any]) -> bool:
    """
    Ensures a document has a given field or not. ``value`` must be either True
    or False, otherwise a MalformedQueryException is raised
    """
    if value not in (True, False):
        raise MalformedQueryException("'$exists' must be supplied a boolean")

    if value:
        return field in document
    else:
        return field not in document

