from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

from .sql_utils import quote_table_name

if TYPE_CHECKING:
    from .client_session import ClientSession
    from .collection import Collection

logger = logging.getLogger(__name__)


_stream_registry: dict[int, dict[str, Any]] = {}

# Rows of history retained per collection when the last stream closes, so a
# later ``watch(resume_after=token)`` can still replay recent events instead
# of finding an empty table. Bounds table growth for crash-restart resume.
_RESUME_RETENTION_ROWS = 1000


def _parse_resume_id(token: Any) -> int:
    """Extract the changelog row id from a NeoSQLite resume token.

    Our resume token is the change event's ``_id`` field, i.e.
    ``{"id": <changelog row id>}``. Raw ints and ``{"_id": {...}}``
    wrappers are also accepted.

    Raises:
        ValueError: If the token has none of the known shapes.
    """
    candidate = token
    if isinstance(candidate, dict) and set(candidate) == {"_id"}:
        candidate = candidate["_id"]
    if isinstance(candidate, dict) and set(candidate) == {"id"}:
        candidate = candidate["id"]
    if isinstance(candidate, bool) or not isinstance(candidate, int):
        raise ValueError(
            "Invalid resume token: expected {'id': <int>} as returned in "
            f"change events' _id field, got {token!r}"
        )
    return candidate


def _get_dotted(document: Any, path: str) -> Any:
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _match_operators(value: Any, operators: dict[str, Any]) -> bool:
    for op, expected in operators.items():
        if op == "$eq":
            if value != expected:
                return False
        elif op == "$ne":
            if value == expected:
                return False
        elif op == "$gt":
            if not (value is not None and value > expected):
                return False
        elif op == "$gte":
            if not (value is not None and value >= expected):
                return False
        elif op == "$lt":
            if not (value is not None and value < expected):
                return False
        elif op == "$lte":
            if not (value is not None and value <= expected):
                return False
        elif op == "$in":
            if value not in expected:
                return False
        elif op == "$nin":
            if value in expected:
                return False
        elif op == "$exists":
            exists = value is not None
            if bool(expected) != exists:
                return False
        else:
            logger.debug("Unsupported $match operator %r; no match", op)
            return False
    return True


def _match_filter(document: dict[str, Any], spec: dict[str, Any]) -> bool:
    for key, condition in spec.items():
        if key == "$and":
            if not all(_match_filter(document, sub) for sub in condition):
                return False
        elif key == "$or":
            if not any(_match_filter(document, sub) for sub in condition):
                return False
        elif key == "$nor":
            if any(_match_filter(document, sub) for sub in condition):
                return False
        elif isinstance(condition, dict) and any(
            k.startswith("$") for k in condition
        ):
            if not _match_operators(_get_dotted(document, key), condition):
                return False
        else:
            if _get_dotted(document, key) != condition:
                return False
    return True


def _pipeline_matches(
    change: dict[str, Any], pipeline: list[dict[str, Any]]
) -> bool:
    """Library-side pipeline filtering (minimal useful subset).

    Only ``$match`` stages are evaluated; any other stage is accepted and
    ignored (debug-logged), matching the documented subset.
    """
    for stage in pipeline:
        if not isinstance(stage, dict) or set(stage) != {"$match"}:
            logger.debug("Ignoring unsupported change-stream stage: %r", stage)
            continue
        spec = stage["$match"]
        if not isinstance(spec, dict) or not _match_filter(change, spec):
            return False
    return True


def _registry_entry(db: Any) -> dict[str, Any]:
    """Per-connection registry of active ChangeStreams (#110).

    Keyed by id(db) with a strong reference to db kept in the entry, which
    pins the id for as long as any stream is registered.
    """
    entry = _stream_registry.get(id(db))
    if entry is None or entry["db"] is not db:
        entry = {"db": db, "counts": {}, "streams": {}}
        _stream_registry[id(db)] = entry
    return entry


class ChangeStream:
    """
    A change stream that watches for changes on a collection.

    This implementation uses SQLite's built-in features to monitor changes.
    It provides an iterator interface to receive change events.
    """

    def __init__(
        self,
        collection: Collection,
        pipeline: list[dict[str, Any]] | None = None,
        full_document: str | None = None,
        resume_after: dict[str, Any] | None = None,
        max_await_time_ms: int | None = None,
        batch_size: int | None = None,
        collation: dict[str, Any] | None = None,
        start_at_operation_time: Any | None = None,
        session: ClientSession | None = None,
        start_after: dict[str, Any] | None = None,
    ):
        """
        Initialize a change stream for a specific collection.

        Args:
            collection (Collection): The collection to monitor for changes.
            pipeline (list[dict[str, Any]], optional): A pipeline of operations to apply to the change stream.
            full_document (str, optional): Specifies whether to include the full document in change events.
            resume_after (dict[str, Any], optional): A resume token to start the change stream from a specific point.
            max_await_time_ms (int, optional): The maximum time in milliseconds to wait for change events.
            batch_size (int, optional): The batch size for the change stream.
            collation (dict[str, Any], optional): Collation options to apply to change events.
            start_at_operation_time (Any, optional): Operation time to start the change stream from.
            session (Any, optional): The session to use for the change stream.
            start_after (dict[str, Any], optional): A document ID to start the change stream from.
        """
        if resume_after is not None and start_after is not None:
            raise ValueError(
                "resume_after and start_after are mutually exclusive"
            )
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
        self._last_token_id: int | None = None

        self._sanitized_name = self._sanitize_collection_name(collection.name)

        # Ensure _id column exists before creating triggers that reference it
        self._collection._ensure_id_column_exists()

        # Set up triggers to capture changes
        self._setup_triggers()

    @staticmethod
    def _sanitize_collection_name(name: str) -> str:
        """
        Validate and sanitize a collection name to prevent SQL injection.

        Args:
            name: The collection name to validate.

        Returns:
            The validated collection name.

        Raises:
            ValueError: If the collection name contains invalid characters.
        """
        if not isinstance(name, str):
            raise ValueError(
                f"Collection name must be a string, got {type(name).__name__}"
            )
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            raise ValueError(
                f"Invalid collection name '{name}': must contain only "
                f"alphanumeric characters and underscores, and must not start with a digit"
            )
        return name

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
        self._collection.db.execute("""
            CREATE TABLE IF NOT EXISTS _neosqlite_changestream (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                document_id INTEGER,
                document_data TEXT,
                document_id_value TEXT,  -- Store the actual _id value separately
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)

        # Register this stream BEFORE creating shared triggers (#110): each
        # collection's triggers are created once and shared by all streams;
        # events are consumed via per-stream watermarks instead of deletion.
        entry = _registry_entry(self._collection.db)
        counts = entry["counts"]
        streams: dict[int, ChangeStream] = entry["streams"].setdefault(
            self._collection.name, {}
        )
        streams[id(self)] = self
        first = counts.get(self._collection.name, 0) == 0

        # Create triggers for INSERT, UPDATE, DELETE operations
        # Insert trigger
        if first:
            self._collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_insert_trigger
            AFTER INSERT ON {quote_table_name(self._sanitized_name)}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data, document_id_value)
                VALUES ('{self._sanitized_name}', 'insert', NEW.id, NEW.data, NEW._id);
            END
            """)

            # Update trigger
            self._collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_update_trigger
            AFTER UPDATE ON {quote_table_name(self._sanitized_name)}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data, document_id_value)
                VALUES ('{self._sanitized_name}', 'update', NEW.id, NEW.data, NEW._id);
            END
            """)

            # Delete trigger
            self._collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_delete_trigger
            AFTER DELETE ON {quote_table_name(self._sanitized_name)}
            BEGIN
                INSERT INTO _neosqlite_changestream
                (collection_name, operation, document_id, document_data, document_id_value)
                VALUES ('{self._sanitized_name}', 'delete', OLD.id, OLD.data, OLD._id);
            END
            """)
        counts[self._collection.name] = counts.get(self._collection.name, 0) + 1

        # Commit the changes only if not inside an active transaction
        if not self._collection.db.in_transaction:
            self._collection.db.commit()

        # Start from "now": only deliver events that occur after open,
        # unless an explicit resume point was requested (#110 parity).
        token = (
            self._resume_after
            if self._resume_after is not None
            else self._start_after
        )
        if token is not None:
            # Replay rows after the token instead of starting from now.
            self._last_id = _parse_resume_id(token)
        else:
            if self._start_at_operation_time is not None:
                logger.debug(
                    "start_at_operation_time is accepted but has no "
                    "operation-time concept in NeoSQLite; starting from now"
                )
            row = self._collection.db.execute(
                "SELECT COALESCE(MAX(id), 0) FROM _neosqlite_changestream WHERE collection_name = ?",
                (self._collection.name,),
            ).fetchone()
            self._last_id = row[0] if row else 0

    def _cleanup_triggers(self):
        """Drop shared triggers when the LAST stream for this collection
        closes, and purge its now-unconsumable events (#110, #111).

        Called from close(); the _closed flag is set by the caller.
        """
        entry = _registry_entry(self._collection.db)
        counts: dict[str, int] = entry["counts"]
        streams_by_coll: dict[str, dict[int, ChangeStream]] = entry["streams"]
        streams = streams_by_coll.get(self._collection.name, {})
        streams.pop(id(self), None)

        remaining = counts.get(self._collection.name, 0) - 1
        counts[self._collection.name] = max(remaining, 0)

        try:
            if remaining <= 0:
                # Last consumer gone: drop shared triggers and retain only
                # recent rows so a later resume_after token can still replay
                # (bounded growth for crash-restart resume).
                self._collection.db.execute(
                    f"DROP TRIGGER IF EXISTS _neosqlite_{self._sanitized_name}_insert_trigger"
                )
                self._collection.db.execute(
                    f"DROP TRIGGER IF EXISTS _neosqlite_{self._sanitized_name}_update_trigger"
                )
                self._collection.db.execute(
                    f"DROP TRIGGER IF EXISTS _neosqlite_{self._sanitized_name}_delete_trigger"
                )
                self._collection.db.execute(
                    "DELETE FROM _neosqlite_changestream "
                    "WHERE collection_name = ? AND id NOT IN ("
                    "SELECT id FROM _neosqlite_changestream "
                    "WHERE collection_name = ? "
                    "ORDER BY id DESC LIMIT ?)",
                    (
                        self._collection.name,
                        self._collection.name,
                        _RESUME_RETENTION_ROWS,
                    ),
                )
            else:
                # Other streams still active: prune only rows every active
                # stream has already consumed.
                self._prune_consumed()
            if counts.get(self._collection.name, 0) == 0:
                counts.pop(self._collection.name, None)
                streams_by_coll.pop(self._collection.name, None)
            if not self._collection.db.in_transaction:
                self._collection.db.commit()
        except Exception as e:
            logger.warning(f"Error during ChangeStream cleanup: {e}")

    def _advance_watermark(self, change_id: int) -> None:
        self._last_id = change_id

    def _prune_consumed(self) -> None:
        """Delete events that EVERY active stream on this collection has
        already consumed (#111). With no co-streams this is just this
        stream's own watermark."""
        try:
            entry = _registry_entry(self._collection.db)
            streams = entry["streams"].get(self._collection.name, {})
            marks = [
                st._last_id
                for sid, st in streams.items()
                if not st._closed and sid != id(self)
            ]
            watermark = min(marks + [self._last_id])
            if watermark > 0:
                self._collection.db.execute(
                    "DELETE FROM _neosqlite_changestream WHERE collection_name = ? AND id < ?",
                    (self._collection.name, watermark),
                )
        except Exception as e:
            logger.debug(f"ChangeStream prune skipped: {e}")

    def __iter__(self) -> ChangeStream:
        """
        Return the iterator object for the change stream.

        This method is required for the change stream to be used in a for loop
        or other iteration contexts. It returns the iterator object itself,
        allowing the change stream to provide a sequence of change events for
        iteration.
        """
        return self

    @property
    def resume_token(self) -> dict[str, int] | None:
        """
        Resume token for the most recently returned change event.

        Pass it as ``watch(resume_after=token)`` to replay events after a
        worker restart without missing committed changes or processing any
        event twice.

        Returns:
            dict | None: ``{"id": <changelog row id>}``, or None when no
                event has been returned yet.
        """
        if self._last_token_id is None:
            return None
        return {"id": self._last_token_id}

    def __next__(self) -> dict[str, Any]:
        """
        Poll for and return the next change event from the change stream.

        This method continuously polls for new change events from the change tracking
        table created by the change stream. It waits for changes, respecting the
        specified timeout, and returns the first change event found. If no changes
        are detected within the timeout, it continues polling until a change is
        available or the timeout is exceeded, raising a StopIteration exception
        if no changes are detected within the timeout.

        Returns:
            dict[str, Any]: The next change event document, containing details
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

            cursor = self._collection.db.execute(
                """
                SELECT id, operation, document_id, document_data, document_id_value, timestamp
                FROM _neosqlite_changestream
                WHERE collection_name = ? AND id > ?
                ORDER BY id
                LIMIT ?
                """,
                (self._collection.name, self._last_id, self._batch_size),
            )
            rows = cursor.fetchall()
            if rows:

                if rows:
                    # Process the first change
                    row = rows[0]
                    (
                        change_id,
                        operation,
                        document_id,
                        document_data,
                        document_id_value,
                        timestamp,
                    ) = row

                    # Get the actual _id of the document
                    # Try to get _id from the stored document_id_value first (this works even for deleted documents)
                    actual_id = document_id  # Default to integer ID if nothing else works
                    if document_id_value is not None:
                        # Use the stored _id value
                        # If it looks like a hex string (ObjectId), convert it back to ObjectId
                        from neosqlite.objectid import ObjectId

                        try:
                            actual_id = ObjectId(document_id_value)
                        except (ValueError, TypeError) as e:
                            # If not a valid ObjectId hex, use as-is
                            logger.debug(
                                f"Document ID '{document_id_value}' is not a valid ObjectId: {e}"
                            )
                            actual_id = document_id_value
                    elif document_data:
                        try:
                            import json

                            # Handle bytes data - decode to string first
                            if isinstance(document_data, bytes):
                                try:
                                    document_str = document_data.decode("utf-8")
                                except UnicodeDecodeError as e:
                                    # If UTF-8 decoding fails, use default ID
                                    logger.debug(
                                        f"Failed to decode document_data as UTF-8: {e}"
                                    )
                                    document_str = None
                            else:
                                document_str = document_data

                            doc_dict = (
                                json.loads(document_str)
                                if document_str is not None
                                else None
                            )
                            if doc_dict and "_id" in doc_dict:
                                actual_id = doc_dict["_id"]
                            else:
                                # If not in JSON, try to get from the _id column in the database
                                stored_id = self._collection._get_stored_id(
                                    document_id
                                )
                                actual_id = (
                                    stored_id
                                    if stored_id is not None
                                    else document_id
                                )
                        except (json.JSONDecodeError, TypeError) as e:
                            # If JSON parsing fails, try database lookup
                            logger.debug(f"Failed to parse document JSON: {e}")
                            stored_id = self._collection._get_stored_id(
                                document_id
                            )
                            actual_id = (
                                stored_id
                                if stored_id is not None
                                else document_id
                            )
                    else:
                        # No JSON data, try database lookup
                        stored_id = self._collection._get_stored_id(document_id)
                        actual_id = (
                            stored_id if stored_id is not None else document_id
                        )

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
                        "documentKey": {"_id": actual_id},
                    }

                    # Add full document if requested
                    skip_change = False
                    if self._full_document == "updateLookup" and document_data:
                        full_doc_str: str | None = None
                        try:
                            import json

                            # Handle bytes data - decode to string first
                            if isinstance(document_data, bytes):
                                try:
                                    full_doc_str = document_data.decode("utf-8")
                                except UnicodeDecodeError as e:
                                    # If UTF-8 decoding fails, skip this change
                                    logger.debug(
                                        f"Failed to decode full document data: {e}"
                                    )
                                    skip_change = True
                            else:
                                full_doc_str = document_data

                            if not skip_change and full_doc_str is not None:
                                doc = json.loads(full_doc_str)
                                # Ensure the _id in the full document is correct
                                # Use the stored document_id_value if available (e.g., for deleted docs)
                                if document_id_value is not None:
                                    from neosqlite.objectid import ObjectId

                                    try:
                                        actual_doc_id = ObjectId(
                                            document_id_value
                                        )
                                    except (ValueError, TypeError) as e:
                                        logger.debug(
                                            f"Document ID '{document_id_value}' is not a valid ObjectId: {e}"
                                        )
                                        actual_doc_id = document_id_value
                                    doc["_id"] = actual_doc_id
                                elif "_id" not in doc:
                                    # Fallback: get from database if not in JSON
                                    stored_id = self._collection._get_stored_id(
                                        document_id
                                    )
                                    doc["_id"] = (
                                        stored_id
                                        if stored_id is not None
                                        else document_id
                                    )
                                change_doc["fullDocument"] = doc
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.debug(f"Failed to parse document JSON: {e}")
                            pass

                    if skip_change:
                        self._advance_watermark(change_id)
                        continue

                    if self._pipeline and not _pipeline_matches(
                        change_doc, self._pipeline
                    ):
                        self._advance_watermark(change_id)
                        self._prune_consumed()
                        continue

                    # Events are NOT deleted on read (#110): other active
                    # streams on this collection keep their own watermarks.
                    self._last_id = change_id
                    self._last_token_id = change_id
                    self._prune_consumed()

                    return change_doc
                del rows
            # No changes were found; sleep before retrying
            time.sleep(0.1)

    def __enter__(self) -> ChangeStream:
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
