from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict

from .exceptions import InvalidOperation

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)


class ClientSession:
    """
    Represents a client session for transactions in NeoSQLite.

    This class provides PyMongo-compatible session and transaction management
    by wrapping SQLite's native ACID transactions.
    """

    def __init__(
        self, client: Connection, options: Dict[str, Any] | None = None
    ):
        """
        Initialize a new ClientSession.

        Args:
            client (Connection): The connection instance that created this session.
            options (dict, optional): Session options.
        """
        self.client = client
        self.options = options or {}
        self._in_transaction = False

    @property
    def in_transaction(self) -> bool:
        """
        Check if the session is currently in a transaction.

        Returns:
            bool: True if in a transaction, False otherwise.
        """
        return self._in_transaction

    def start_transaction(self, write_concern: Dict[str, Any] | None = None):
        """
        Start a new transaction.

        Args:
            write_concern (dict, optional): Write concern for the transaction.
        """
        if self._in_transaction:
            raise InvalidOperation("Transaction already in progress")

        # SQLite transaction logic
        if self.client.db.in_transaction:
            # Use SAVEPOINT for nested transactions
            self._savepoint_name = f"session_tx_{id(self)}"
            self.client.db.execute(f"SAVEPOINT {self._savepoint_name}")
            self._is_savepoint = True
        else:
            # Start a normal transaction
            self.client.db.execute("BEGIN IMMEDIATE")
            self._is_savepoint = False

        self._in_transaction = True

    def commit_transaction(self):
        """
        Commit the current transaction.
        """
        if not self._in_transaction:
            raise InvalidOperation("No transaction in progress")

        if self._is_savepoint:
            self.client.db.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        else:
            self.client.db.commit()

        self._in_transaction = False

    def abort_transaction(self):
        """
        Abort (rollback) the current transaction.
        """
        if not self._in_transaction:
            raise InvalidOperation("No transaction in progress")

        if self._is_savepoint:
            self.client.db.execute(
                f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
            )
            self.client.db.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
        else:
            self.client.db.rollback()

        self._in_transaction = False

    def end_session(self):
        """
        End the session. If in a transaction, it will be aborted.
        """
        if self._in_transaction:
            try:
                self.abort_transaction()
            except Exception as e:
                logger.warning(
                    f"Failed to abort transaction during session close: {e}"
                )
                pass

    def with_transaction(
        self,
        callback: Callable[[ClientSession], Any],
        read_concern: Any | None = None,
        write_concern: Any | None = None,
        read_preference: Any | None = None,
        max_commit_time_ms: int | None = None,
    ) -> Any:
        """
        Execute a callback in a transaction.

        This method automatically starts a transaction, executes the callback,
        and commits the transaction if the callback succeeds. If the callback
        raises an exception, the transaction is aborted.

        Args:
            callback: A function that takes a ClientSession as its only argument.
            read_concern (optional): Unused in NeoSQLite.
            write_concern (optional): Unused in NeoSQLite.
            read_preference (optional): Unused in NeoSQLite.
            max_commit_time_ms (optional): Unused in NeoSQLite.

        Returns:
            The return value of the callback.
        """
        self.start_transaction()
        try:
            result = callback(self)
            self.commit_transaction()
            return result
        except Exception as e:
            logger.debug(f"Transaction context manager failed: {e}")
            if self._in_transaction:
                self.abort_transaction()
            raise

    def __enter__(self) -> ClientSession:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and self._in_transaction:
            self.abort_transaction()
        self.end_session()
