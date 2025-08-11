import json
import time
from typing import Any, Dict, List, TYPE_CHECKING

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
        """Set up SQLite triggers to capture changes to the collection."""
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
        """Clean up the triggers when the change stream is closed."""
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
        return self

    def __next__(self) -> Dict[str, Any]:
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
                        "db": "default",  # Default database name since Connection doesn't have a name property
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

    def close(self) -> None:
        """Close the change stream and clean up resources."""
        if not self._closed:
            self._closed = True
            self._cleanup_triggers()

    def __enter__(self) -> "ChangeStream":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_traceback: Any) -> None:
        self.close()
