from contextlib import contextmanager
from typing import Any, Dict, Iterator
from typing_extensions import Literal

try:
    import pysqlite3.dbapi2 as sqlite3
except ImportError:
    import sqlite3

from .collection import Collection


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
