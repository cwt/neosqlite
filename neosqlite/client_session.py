from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from .exceptions import InvalidOperation

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)


class _TransactionContext:
    """Context manager returned by ``ClientSession.start_transaction()``.

    Exposes the underlying ``ClientSession`` inside a ``with`` block and
    automatically commits on normal exit or aborts on exception when a
    transaction is still active.
    """

    def __init__(self, session: ClientSession) -> None:
        """Initialize the transaction context manager.

        Args:
            session: Active ``ClientSession`` bound to this transaction.
        """
        self._session = session

    def __enter__(self) -> ClientSession:
        """Enter the transaction context and return the bound session.

        Returns:
            ClientSession: The session associated with this transaction.
        """
        return self._session

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Commit or abort the active transaction when exiting the context.

        Aborts the transaction if an exception was raised and the session is
        still in a transaction; otherwise commits if still in a transaction.

        Args:
            exc_type: Exception class if an exception occurred, else ``None``.
            exc_val: Exception instance if an exception occurred, else
                ``None``.
            exc_tb: Traceback object if an exception occurred, else ``None``.
        """
        if exc_type is not None:
            if self._session.in_transaction:
                try:
                    self._session.abort_transaction()
                except Exception as e:
                    logger.warning(
                        f"Failed to abort transaction on context exit: {e}"
                    )
        else:
            if self._session.in_transaction:
                self._session.commit_transaction()


class ClientSession:
    """Represents a client session for transactions in NeoSQLite.

    Provides PyMongo-compatible session and transaction management by wrapping
    SQLite's native ACID transactions and savepoints.
    """

    def __init__(
        self, client: Connection, options: dict[str, Any] | None = None
    ):
        """Initialize a new ClientSession.

        Args:
            client: Connection instance that owns this session.
            options: Optional session configuration dictionary. Defaults to
                an empty dictionary when ``None``.
        """
        self.client = client
        self.options = options or {}
        self._in_transaction = False
        self._ended = False
        self._is_savepoint = False
        self._savepoint_name: str | None = None

    @property
    def in_transaction(self) -> bool:
        """Check whether the session currently has an active transaction.

        Returns:
            bool: ``True`` if a transaction is in progress, ``False``
                otherwise.
        """
        return self._in_transaction

    @property
    def has_ended(self) -> bool:
        """Check whether this session has been ended.

        Returns:
            bool: ``True`` if ``end_session()`` has been called, ``False``
                otherwise.
        """
        return self._ended

    def _check_not_ended(self) -> None:
        """Raise ``InvalidOperation`` if this session has already ended."""
        if self._ended:
            raise InvalidOperation("Cannot use ended ClientSession")

    def _clear_transaction_state(self) -> None:
        """Reset local and connection-level transaction tracking state."""
        self._in_transaction = False
        self._is_savepoint = False
        self._savepoint_name = None
        if getattr(self.client, "_active_tx_session", None) is self:
            self.client._active_tx_session = None

    def start_transaction(
        self, write_concern: dict[str, Any] | None = None
    ) -> _TransactionContext:
        """Start a new transaction on the underlying SQLite connection.

        Issues ``BEGIN IMMEDIATE`` for a top-level transaction, or uses a
        unique SQLite ``SAVEPOINT`` when nested inside an existing
        ``Connection.transaction()`` block. Overlapping transactions across
        distinct ``ClientSession`` instances on the same single SQLite
        connection are rejected with ``InvalidOperation``.

        Args:
            write_concern: Optional write concern dictionary accepted for
                PyMongo API compatibility (unused).

        Returns:
            _TransactionContext: Context manager that commits on normal exit
                and aborts on exception.

        Raises:
            InvalidOperation: If this session has ended, already has an active
                transaction, or another ``ClientSession`` already holds an open
                transaction on the same connection.
        """
        self._check_not_ended()
        if self._in_transaction:
            raise InvalidOperation("Transaction already in progress")

        active_session = getattr(self.client, "_active_tx_session", None)
        if active_session is not None and active_session is not self:
            raise InvalidOperation(
                "Concurrent transactions across multiple sessions on a single "
                "SQLite connection are not supported"
            )

        # SQLite transaction logic
        if self.client.db.in_transaction:
            # Use SAVEPOINT when nested inside Connection.transaction()
            self._savepoint_name = f"session_tx_{uuid.uuid4().hex[:12]}"
            self.client.db.execute(f"SAVEPOINT {self._savepoint_name}")
            self._is_savepoint = True
        else:
            # Start a normal transaction
            self.client.db.execute("BEGIN IMMEDIATE")
            self._savepoint_name = None
            self._is_savepoint = False

        self._in_transaction = True
        self.client._active_tx_session = self
        return _TransactionContext(self)

    def commit_transaction(self) -> None:
        """Commit the current transaction or release its savepoint.

        Raises:
            InvalidOperation: If the session has ended or no transaction is
                currently in progress.
        """
        self._check_not_ended()
        if not self._in_transaction:
            raise InvalidOperation("No transaction in progress")

        try:
            if self._is_savepoint:
                self.client.db.execute(
                    f"RELEASE SAVEPOINT {self._savepoint_name}"
                )
            else:
                self.client.db.commit()
        finally:
            self._clear_transaction_state()

    def abort_transaction(self) -> None:
        """Abort and roll back the current transaction or savepoint.

        Raises:
            InvalidOperation: If the session has ended or no transaction is
                currently in progress.
        """
        self._check_not_ended()
        if not self._in_transaction:
            raise InvalidOperation("No transaction in progress")

        try:
            if self._is_savepoint:
                self.client.db.execute(
                    f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
                )
                self.client.db.execute(
                    f"RELEASE SAVEPOINT {self._savepoint_name}"
                )
            else:
                self.client.db.rollback()
        finally:
            self._clear_transaction_state()

    def end_session(self) -> None:
        """End the session, aborting any active transaction if present."""
        if self._ended:
            return
        try:
            if self._in_transaction:
                try:
                    self.abort_transaction()
                except Exception as e:
                    logger.warning(
                        f"Failed to abort transaction during session close: {e}"
                    )
        finally:
            self._clear_transaction_state()
            self._ended = True

    def with_transaction(
        self,
        callback: Callable[[ClientSession], Any],
        read_concern: Any | None = None,
        write_concern: Any | None = None,
        read_preference: Any | None = None,
        max_commit_time_ms: int | None = None,
    ) -> Any:
        """Execute a callback inside a transaction.

        Starts a transaction, runs ``callback(self)``, and commits if it
        returns normally. If ``callback`` raises an exception, aborts any
        active transaction and re-raises.

        Args:
            callback: Callable receiving this ``ClientSession`` as its single
                argument.
            read_concern: Accepted for PyMongo API compatibility (unused).
            write_concern: Accepted for PyMongo API compatibility (unused).
            read_preference: Accepted for PyMongo API compatibility (unused).
            max_commit_time_ms: Accepted for PyMongo API compatibility
                (unused).

        Returns:
            Any: The value returned by ``callback``.

        Raises:
            InvalidOperation: If this session has ended or cannot start a
                transaction.
        """
        self._check_not_ended()
        self.start_transaction()
        try:
            result = callback(self)
            if self._in_transaction:
                self.commit_transaction()
            return result
        except Exception as e:
            logger.debug(f"Transaction context manager failed: {e}")
            if self._in_transaction:
                self.abort_transaction()
            raise

    def __enter__(self) -> Self:
        """Enter the session context manager.

        Returns:
            Self: This ``ClientSession`` instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the session context manager and end the session.

        Delegates to ``end_session()`` so any active transaction is safely
        aborted without masking an exception raised inside the ``with`` block.

        Args:
            exc_type: Exception class if an exception occurred, else ``None``.
            exc_val: Exception instance if an exception occurred, else
                ``None``.
            exc_tb: Traceback object if an exception occurred, else ``None``.
        """
        self.end_session()
