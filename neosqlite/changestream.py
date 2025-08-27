from typing import Any, Dict, List, TYPE_CHECKING
import json
import time

if TYPE_CHECKING:
    from .collection import Collection


class ChangeStream:
    """
    A change stream that watches for changes on a collection.

    This implementation uses SQLite's built-in features to monitor changes.
    It provides an iterator interface to receive change events.
    """

    def __init__(
        self,
        collection: "Collection",
        pipeline: List[Dict[str, Any]] | None = None,
        full_document: str | None = None,
        resume_after: Dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
        batch_size: int | None = None,
        collation: Dict[str, Any] | None = None,
        start_at_operation_time: Any | None = None,
        session: Any | None = None,
        start_after: Dict[str, Any] | None = None,
    ):
        """
        Initialize a change stream for a specific collection.

        Args:
            collection (Collection): The collection to monitor for changes.
            pipeline (List[Dict[str, Any]], optional): A pipeline of operations to apply to the change stream.
            full_document (str, optional): Specifies whether to include the full document in change events.
            resume_after (Dict[str, Any], optional): A resume token to start the change stream from a specific point.
            max_await_time_ms (int, optional): The maximum time in milliseconds to wait for change events.
            batch_size (int, optional): The batch size for the change stream.
            collation (Dict[str, Any], optional): Collation options to apply to change events.
            start_at_operation_time (Any, optional): Operation time to start the change stream from.
            session (Any, optional): The session to use for the change stream.
            start_after (Dict[str, Any], optional): A document ID to start the change stream from.
        """
        self._collection = collection
        self._pipeline = pipeline or []
        self._full_document = full_document
        self._resume_after = resume_after
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size or 1
        self._collation = collation
        self._start_at_operation_time = start_at_operation_time
        self._session = session
        self._start_after = start_after

        # For SQLite-based implementation, we'll use a simple polling approach
        # In a more advanced implementation, we could use SQLite's update hooks
        self._closed = False
        self._last_id = 0

        # Set up triggers to capture changes
        self._setup_triggers()

    def _setup_triggers(self):
        """
        Set up SQLite triggers to capture changes to the collection.

        This method ensures that triggers are created in the SQLite database to
        log INSERT, UPDATE, and DELETE operations on the specified collection.
        These triggers insert records into a change tracking table, enabling the
        change stream to monitor these events.

        Triggers are created dynamically using SQL commands. They are designed
        to capture the essential details of each change operation, including the
        operation type, document ID, and data.
        """
        # Create a table to store change events if it doesn't exist
        self._collection.db.execute(
            """
            CREATE TABLE IF NOT EXISTS _neosqlite_changestream (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                document_id INTEGER,
                document_data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create triggers for INSERT, UPDATE, DELETE operations
        # Insert trigger
        self._collection.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._collection.name}_insert_trigger
            AFTER INSERT ON {self._collection.name}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data)
                VALUES ('{self._collection.name}', 'insert', NEW.id, NEW.data);
            END
            """
        )

        # Update trigger
        self._collection.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._collection.name}_update_trigger
            AFTER UPDATE ON {self._collection.name}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data)
                VALUES ('{self._collection.name}', 'update', NEW.id, NEW.data);
            END
            """
        )

        # Delete trigger
        self._collection.db.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._collection.name}_delete_trigger
            AFTER DELETE ON {self._collection.name}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data)
                VALUES ('{self._collection.name}', 'delete', OLD.id, OLD.data);
            END
            """
        )

        # Commit the changes
        self._collection.db.commit()

    def _cleanup_triggers(self):
        """
        Clean up the triggers when the change stream is closed.

        This method ensures that triggers created for capturing changes to the
        collection are properly dropped from the SQLite database when the change
        stream is no longer needed. This cleanup helps in freeing up resources
        and avoiding unnecessary logging.

        The method handles exceptions gracefully, ensuring that any errors during
        the cleanup process are ignored, thus allowing the change stream to close
        without interruption.
        """

        if self._closed:
            return

        try:
            # Drop the triggers
            self._collection.db.execute(
                f"DROP TRIGGER IF EXISTS _neosqlite_{self._collection.name}_insert_trigger"
            )
            self._collection.db.execute(
                f"DROP TRIGGER IF EXISTS _neosqlite_{self._collection.name}_update_trigger"
            )
            self._collection.db.execute(
                f"DROP TRIGGER IF EXISTS _neosqlite_{self._collection.name}_delete_trigger"
            )

            # Note: We don't drop the _neosqlite_changestream table as it might be used by other change streams
            self._collection.db.commit()
        except Exception:
            # Ignore errors during cleanup
            pass

    def __iter__(self) -> "ChangeStream":
        """
        Return the iterator object for the change stream.

        This method is required for the change stream to be used in a for loop
        or other iteration contexts. It returns the iterator object itself,
        allowing the change stream to provide a sequence of change events for
        iteration.
        """
        return self

    def __next__(self) -> Dict[str, Any]:
        """
        Poll for and return the next change event from the change stream.

        This method continuously polls for new change events from the change tracking
        table created by the change stream. It waits for changes, respecting the
        specified timeout, and returns the first change event found. If no changes
        are detected within the timeout, it continues polling until a change is
        available or the timeout is exceeded, raising a StopIteration exception
        if no changes are detected within the timeout.

        Returns:
            Dict[str, Any]: The next change event document, containing details
                            such as the operation type, document ID, and data.
                            If full_document is set to "updateLookup", the full
                            document before and/or after the change operation is
                            also included.

        Raises:
            StopIteration: If the timeout is exceeded and no changes are detected.
        """
        if self._closed:
            raise StopIteration("Change stream is closed")

        # Record the start time for timeout checking
        start_time = time.time()
        timeout = (
            self._max_await_time_ms or 10000
        ) / 1000.0  # Convert to seconds

        # Poll for changes
        while True:
            # Check if we've exceeded the timeout
            if time.time() - start_time > timeout:
                raise StopIteration("Change stream timeout exceeded")

            # Query for new changes
            cursor = self._collection.db.execute(
                """
                SELECT id, operation, document_id, document_data, timestamp
                FROM _neosqlite_changestream
                WHERE collection_name = ? AND id > ?
                ORDER BY id
                LIMIT ?
                """,
                (self._collection.name, self._last_id, self._batch_size),
            )

            rows = cursor.fetchall()

            if rows:
                # Process the first change
                row = rows[0]
                change_id, operation, document_id, document_data, timestamp = (
                    row
                )

                # Update the last processed ID
                self._last_id = change_id

                # Create the change document
                change_doc = {
                    "_id": {"id": change_id},
                    "operationType": operation,
                    "clusterTime": timestamp,
                    "ns": {
                        "db": (
                            "default"
                        ),  # Default database name since Connection doesn't have a name property
                        "coll": self._collection.name,
                    },
                    "documentKey": {"_id": document_id},
                }

                # Add full document if requested
                if self._full_document == "updateLookup" and document_data:
                    try:
                        doc = json.loads(document_data)
                        doc["_id"] = document_id
                        change_doc["fullDocument"] = doc
                    except (json.JSONDecodeError, TypeError):
                        pass

                return change_doc

            # If no changes, sleep briefly before polling again
            time.sleep(0.1)

    def __enter__(self) -> "ChangeStream":
        """
        Return the change stream itself.

        This method is required to support the context manager protocol, allowing
        the change stream to be used in a `with` statement. By returning the
        change stream itself, the `with` statement can manage the lifecycle of
        the change stream, ensuring that it is properly closed when the block is
        exited.

        This method is essential for enabling the change stream to be used in a
        clean and efficient manner within a `with` block, facilitating the
        monitoring of collection changes within a controlled and predictable scope.
        """
        return self

    def close(self) -> None:
        """
        Close the change stream and clean up resources.

        This method ensures that the change stream is properly closed and resources
        are released. It sets the `_closed` flag to True and calls the `_cleanup_triggers`
        method to clean up any triggers that were set up for capturing changes to
        the collection. This helps in freeing up resources and avoiding unnecessary
        logging or data handling.

        By calling this method, the change stream is effectively terminated, and
        no further change events will be received.
        """
        if not self._closed:
            self._closed = True
            self._cleanup_triggers()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_traceback: Any) -> None:
        """
        Handle the context management exit of the change stream.

        This method ensures that the change stream is properly closed and resources
        are released. It calls the `close` method to terminate the change stream.
        """
        self.close()
