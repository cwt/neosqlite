from __future__ import annotations

import json
import logging
import operator
import re
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from .sql_utils import quote_table_name

if TYPE_CHECKING:
    from .client_session import ClientSession
    from .collection import Collection

logger = logging.getLogger(__name__)


_registry_lock = threading.Lock()
_stream_registry: dict[int, dict[str, Any]] = {}


def clear_changestream_registry(db: Any) -> None:
    """Remove any ChangeStream registry entry associated with ``db``."""
    with _registry_lock:
        entry = _stream_registry.get(id(db))
        if entry is not None and entry["db"] is db:
            _stream_registry.pop(id(db), None)


# Rows of history retained per collection when the last stream closes, so a
# later ``watch(resume_after=token)`` can still replay recent events instead
# of finding an empty table. Bounds table growth for crash-restart resume.
_RESUME_RETENTION_ROWS = 1000


def _parse_resume_id(token: Any) -> int:
    """Extract the changelog row id from a NeoSQLite resume token.

    Accepts the change event's ``_id`` field (``{"id": <int>}``), raw integer
    ids, or full ``{"_id": {"id": <int>}}`` wrappers (excluding booleans).

    Args:
        token: Resume token dictionary or integer changelog row id.

    Returns:
        int: Integer primary-key id in ``_neosqlite_changestream``.

    Raises:
        ValueError: If ``token`` does not match any supported resume shape.
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
    """Resolve a dotted field path against a nested dictionary.

    Args:
        document: Root document or value to traverse.
        path: Dot-separated field path (e.g., ``"fullDocument.status"``).

    Returns:
        Any: Resolved value, or ``None`` if any path segment is missing or
            a non-dict value is encountered along the path.
    """
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _field_present(document: Any, path: str) -> bool:
    """Return whether a dotted field path exists in a nested dictionary.

    Distinguishes missing keys from keys explicitly mapped to ``None``.

    Args:
        document: Root document or value to inspect.
        path: Dot-separated field path.

    Returns:
        bool: ``True`` if every segment exists as a dict key, else ``False``.
    """
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


_RANGE_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "$gt": operator.gt,
    "$gte": operator.ge,
    "$lt": operator.lt,
    "$lte": operator.le,
}


def _compare_range_op(
    value: Any, expected: Any, comparator: Callable[[Any, Any], bool]
) -> bool:
    """Compare two values with a range operator, guarding incompatible types."""
    if value is None or expected is None:
        return False
    # In Python bool subclasses int, so exclude bool from int/float comparisons.
    if isinstance(value, bool) or isinstance(expected, bool):
        if not (isinstance(value, bool) and isinstance(expected, bool)):
            return False
    elif isinstance(value, (int, float)) and isinstance(expected, (int, float)):
        pass
    elif isinstance(value, (bytes, bytearray)) and isinstance(
        expected, (bytes, bytearray)
    ):
        pass
    elif type(value) is not type(expected):
        return False

    try:
        return comparator(value, expected)
    except TypeError:
        return False


def _match_operators(value: Any, operators: dict[str, Any]) -> bool:
    """Evaluate field-level ``$match`` comparison operators on a value.

    Supports ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$in``,
    ``$nin``, and ``$exists`` (treating non-``None`` as existing). Unsupported
    operators are debug-logged and cause the match to return ``False``.

    Args:
        value: Resolved field value to test.
        operators: Mapping from MongoDB operator names to expected operands.

    Returns:
        bool: ``True`` if all operators match ``value``, else ``False``.
    """
    for op, expected in operators.items():
        if op == "$eq":
            if value != expected:
                return False
        elif op == "$ne":
            if value == expected:
                return False
        elif op in _RANGE_OPS:
            if not _compare_range_op(value, expected, _RANGE_OPS[op]):
                return False
        elif op == "$in":
            try:
                matched = value in expected
            except TypeError:
                return False
            if not matched:
                return False
        elif op == "$nin":
            try:
                matched = value in expected
            except TypeError:
                return False
            if matched:
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
    """Evaluate a change-stream ``$match`` filter against a change event.

    Supports logical combinators (``$and``, ``$or``, ``$nor``), field-level
    operator dicts (handling ``$exists`` via ``_field_present``), and direct
    equality comparisons on dotted field paths.

    Args:
        document: Change event dictionary to evaluate.
        spec: MongoDB ``$match`` filter specification dictionary.

    Returns:
        bool: ``True`` if ``document`` satisfies every criterion in ``spec``.
    """
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
            # Handle $exists with field presence (distinguishing missing
            # from present-but-null), delegating the rest to _match_operators.
            ops = dict(condition)
            exists_expected = ops.pop("$exists", None)
            if exists_expected is not None:
                present = _field_present(document, key)
                if bool(exists_expected) != present:
                    return False
            if ops and not _match_operators(_get_dotted(document, key), ops):
                return False
        else:
            if _get_dotted(document, key) != condition:
                return False
    return True


def _pipeline_matches(
    change: dict[str, Any], pipeline: list[dict[str, Any]]
) -> bool:
    """Filter a change event through a library-side change-stream pipeline.

    Only ``$match`` stages with dict specs are evaluated; non-``$match`` stages
    are debug-logged and ignored, matching the documented subset.

    Args:
        change: Change event dictionary constructed by ``ChangeStream``.
        pipeline: List of change-stream aggregation stage dictionaries.

    Returns:
        bool: ``True`` if all ``$match`` stages accept ``change``, else
            ``False``.
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
    """Return or create the per-connection active ``ChangeStream`` registry.

    Keyed by ``id(db)`` while retaining a strong reference to ``db`` and
    verifying identity with ``is`` so recycled connection ids never collide
    (#110).

    Args:
        db: SQLite database connection associated with a collection.

    Returns:
        dict[str, Any]: Registry entry containing ``"db"``, ``"counts"``, and
            ``"streams"`` dictionaries.
    """
    entry = _stream_registry.get(id(db))
    if entry is None or entry["db"] is not db:
        entry = {"db": db, "counts": {}, "streams": {}}
        _stream_registry[id(db)] = entry
    return entry


class ChangeStream:
    """Watch SQLite-backed collection changes through an iterator interface.

    Captures ``INSERT``, ``UPDATE``, and ``DELETE`` operations via shared
    SQLite triggers writing to ``_neosqlite_changestream``, tracks per-stream
    watermarks for concurrent consumers, and supports context management,
    ``resume_after``/``start_after`` tokens, ``updateLookup`` full documents,
    and ``$match`` pipeline filtering.
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
        """Initialize a change stream and install collection change triggers.

        Args:
            collection: Target ``Collection`` to watch for writes.
            pipeline: Optional aggregation pipeline stages (only ``$match``
                stages are applied). Defaults to ``None``.
            full_document: Set to ``"updateLookup"`` to embed the logged
                document under ``fullDocument``. Defaults to ``None``.
            resume_after: Resume token to replay changes after a prior event.
                Mutually exclusive with ``start_after``.
            max_await_time_ms: Polling timeout in milliseconds before
                ``__next__`` raises ``StopIteration``. Defaults to ``10000``
                ms when ``None``; ``0`` performs a single non-blocking poll.
            batch_size: Query limit per poll cycle (defaults to ``1`` when
                falsy or ``None``).
            collation: Accepted collation configuration stored on the stream.
            start_at_operation_time: Accepted for API compatibility; ignored
                when no resume token is supplied.
            session: Optional ``ClientSession`` stored on the stream.
            start_after: Resume token to start after a prior event. Mutually
                exclusive with ``resume_after``.

        Raises:
            ValueError: If both ``resume_after`` and ``start_after`` are
                provided, or if the collection name or resume token is invalid.
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
        """Validate a collection name before interpolating into SQL triggers.

        Args:
            name: Collection name string to validate.

        Returns:
            str: The validated collection name.

        Raises:
            ValueError: If ``name`` is not a string or fails identifier checks.
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
        """Create changelog storage, triggers, and initial stream watermark.

        Ensures ``_neosqlite_changestream`` exists, registers this stream in
        the per-connection registry, creates shared ``INSERT``/``UPDATE``/
        ``DELETE`` triggers if this is the first active stream on the
        collection, commits when not inside a transaction, and sets
        ``self._last_id`` from any resume token or the current max row id.
        """
        # Create a table to store change events if it doesn't exist
        self._collection.db.execute("""
            CREATE TABLE IF NOT EXISTS _neosqlite_changestream (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                document_id INTEGER,
                document_data TEXT,
                document_id_value BLOB,  -- Store the actual _id value without TEXT coercion
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)

        # Register this stream BEFORE creating shared triggers (#110): each
        # collection's triggers are created once and shared by all streams;
        # events are consumed via per-stream watermarks instead of deletion.
        with _registry_lock:
            entry = _registry_entry(self._collection.db)
            counts = entry["counts"]
            streams: dict[int, ChangeStream] = entry["streams"].setdefault(
                self._collection.name, {}
            )
            streams[id(self)] = self
            first = counts.get(self._collection.name, 0) == 0
            counts[self._collection.name] = (
                counts.get(self._collection.name, 0) + 1
            )

        # Create triggers for INSERT, UPDATE, DELETE operations
        # Store normalized JSON text in changelog so JSONB writes remain readable
        # by change stream JSON decoders (including fullDocument=updateLookup).
        try:
            if first:
                from .collection.jsonb_support import JSONBContext

                jsonb = JSONBContext.from_db(self._collection.db)
                new_data_expr = jsonb.data_column("NEW.data")
                old_data_expr = jsonb.data_column("OLD.data")

                # Insert trigger
                self._collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_insert_trigger
                AFTER INSERT ON {quote_table_name(self._sanitized_name)}
                BEGIN
                    INSERT INTO _neosqlite_changestream
                    (collection_name, operation, document_id, document_data, document_id_value)
                    VALUES ('{self._sanitized_name}', 'insert', NEW.id, {new_data_expr}, NEW._id);
                END
                """)

                # Update trigger
                self._collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_update_trigger
                AFTER UPDATE ON {quote_table_name(self._sanitized_name)}
                BEGIN
                    INSERT INTO _neosqlite_changestream
                    (collection_name, operation, document_id, document_data, document_id_value)
                    VALUES ('{self._sanitized_name}', 'update', NEW.id, {new_data_expr}, NEW._id);
                END
                """)

                # Delete trigger
                self._collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS _neosqlite_{self._sanitized_name}_delete_trigger
                AFTER DELETE ON {quote_table_name(self._sanitized_name)}
                BEGIN
                    INSERT INTO _neosqlite_changestream
                    (collection_name, operation, document_id, document_data, document_id_value)
                    VALUES ('{self._sanitized_name}', 'delete', OLD.id, {old_data_expr}, OLD._id);
                END
                """)
        except Exception:
            with _registry_lock:
                streams.pop(id(self), None)
                remaining = counts.get(self._collection.name, 1) - 1
                if remaining <= 0:
                    counts.pop(self._collection.name, None)
                    entry["streams"].pop(self._collection.name, None)
                else:
                    counts[self._collection.name] = remaining
                if not counts and not entry["streams"]:
                    _stream_registry.pop(id(self._collection.db), None)
            raise

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

    def _changelog_table_exists(self) -> bool:
        """Return whether the shared changelog table still exists.

        ``dropDatabase`` removes internal ``_neosqlite_*`` tables, including
        the changelog, while watchers may still be registered. Cleanup and
        pruning must therefore tolerate the table being gone.

        Returns:
            bool: ``True`` when ``_neosqlite_changestream`` is present in
                ``sqlite_master``, ``False`` otherwise.
        """
        try:
            row = self._collection.db.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='_neosqlite_changestream'"
            ).fetchone()
            return row is not None
        except Exception:
            return False

    def _retain_recent_changelog_rows(
        self, before_id: int | None = None
    ) -> None:
        """Delete changelog rows beyond the newest ``_RESUME_RETENTION_ROWS``.

        Args:
            before_id: When provided, restrict deletion to rows strictly below
                this changelog row id so unconsumed co-stream rows stay intact.
        """
        before_clause = "AND id < ? " if before_id is not None else ""
        params: tuple[Any, ...]
        if before_id is not None:
            params = (
                self._collection.name,
                before_id,
                self._collection.name,
                _RESUME_RETENTION_ROWS,
            )
        else:
            params = (
                self._collection.name,
                self._collection.name,
                _RESUME_RETENTION_ROWS,
            )
        self._collection.db.execute(
            f"DELETE FROM _neosqlite_changestream "
            f"WHERE collection_name = ? {before_clause}AND id NOT IN ("
            f"SELECT id FROM _neosqlite_changestream "
            f"WHERE collection_name = ? "
            f"ORDER BY id DESC LIMIT ?)",
            params,
        )

    def _cleanup_triggers(self):
        """Drop shared triggers on last close and prune unconsumable rows.

        When no active streams remain for this collection (#110, #111), drops
        the ``INSERT``/``UPDATE``/``DELETE`` triggers and retains only the
        most recent ``_RESUME_RETENTION_ROWS`` rows for future resume tokens;
        otherwise prunes rows already consumed by every active co-stream.
        Called from ``close()`` after ``_closed`` is set.
        """
        with _registry_lock:
            entry = _stream_registry.get(id(self._collection.db))
            remaining = 0
            if entry is not None and entry["db"] is self._collection.db:
                counts: dict[str, int] = entry["counts"]
                streams_by_coll: dict[str, dict[int, ChangeStream]] = entry[
                    "streams"
                ]
                streams = streams_by_coll.get(self._collection.name, {})
                streams.pop(id(self), None)

                remaining = max(counts.get(self._collection.name, 0) - 1, 0)
                if remaining <= 0:
                    counts.pop(self._collection.name, None)
                    streams_by_coll.pop(self._collection.name, None)
                else:
                    counts[self._collection.name] = remaining

                if not counts and not streams_by_coll:
                    _stream_registry.pop(id(self._collection.db), None)

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
                if self._changelog_table_exists():
                    self._retain_recent_changelog_rows()
            else:
                # Other streams still active: prune only rows every active
                # stream has already consumed beyond the retention window.
                self._prune_consumed()
            if not self._collection.db.in_transaction:
                self._collection.db.commit()
        except Exception as e:
            logger.warning(f"Error during ChangeStream cleanup: {e}")

    def _advance_watermark(self, change_id: int) -> None:
        """Advance this stream's last processed changelog row id.

        Args:
            change_id: Changelog row id most recently inspected or delivered.
        """
        self._last_id = change_id

    def _prune_consumed(self) -> None:
        """Delete changelog rows consumed by every active co-stream (#111).

        Computes the minimum ``_last_id`` watermark across all open streams
        watching the same collection and deletes rows with smaller ids while
        retaining the newest ``_RESUME_RETENTION_ROWS`` rows (#186).
        """
        try:
            with _registry_lock:
                entry = _stream_registry.get(id(self._collection.db))
                if entry is not None and entry["db"] is self._collection.db:
                    streams = entry["streams"].get(self._collection.name, {})
                    marks = [
                        st._last_id
                        for sid, st in streams.items()
                        if not st._closed and sid != id(self)
                    ]
                else:
                    marks = []
            watermark = min(marks + [self._last_id])
            if watermark > 0 and self._changelog_table_exists():
                self._retain_recent_changelog_rows(before_id=watermark)
        except Exception as e:
            logger.debug(f"ChangeStream prune skipped: {e}")

    def __iter__(self) -> Self:
        """Return the iterator object for this change stream.

        Returns:
            Self: This ``ChangeStream`` instance.
        """
        return self

    @property
    def resume_token(self) -> dict[str, int] | None:
        """Resume token for the most recently returned change event.

        Pass as ``watch(resume_after=token)`` to replay subsequent events after
        reopening a stream without missing committed changes or duplicating
        delivered events.

        Returns:
            dict[str, int] | None: ``{"id": <changelog row id>}``, or ``None``
                if no event has been yielded yet.
        """
        if self._last_token_id is None:
            return None
        return {"id": self._last_token_id}

    @staticmethod
    def _decode_document_data(
        document_data: Any,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Decode stored trigger JSON payloads using ``neosqlite_json_loads``.

        Returns:
            tuple[dict[str, Any] | None, bool]: ``(parsed_dict, utf8_failed)``.
        """
        if document_data is None:
            return None, False
        if isinstance(document_data, bytes):
            try:
                document_str = document_data.decode("utf-8")
            except UnicodeDecodeError as e:
                logger.debug(f"Failed to decode document_data as UTF-8: {e}")
                return None, True
        else:
            document_str = document_data

        try:
            from .collection.json_helpers import neosqlite_json_loads

            parsed = neosqlite_json_loads(document_str)
        except (json.JSONDecodeError, TypeError) as e:
            logger.debug(f"Failed to parse document JSON: {e}")
            return None, False

        return (parsed, False) if isinstance(parsed, dict) else (None, False)

    def __next__(self) -> dict[str, Any]:
        """Poll for and return the next matching change event.

        Polls ``_neosqlite_changestream`` in up to 0.05-second intervals until
        a new event matching ``self._pipeline`` arrives or ``max_await_time_ms``
        elapses (default 10 seconds when ``None``; ``0`` performs a single poll).
        Decodes document ids and BSON types via ``neosqlite_json_loads``,
        attaches ``fullDocument`` when ``full_document == "updateLookup"``,
        advances this stream's watermark, and prunes rows beyond
        ``_RESUME_RETENTION_ROWS`` that have been consumed by all active
        co-streams.

        Returns:
            dict[str, Any]: Change event containing ``_id``, ``operationType``,
                ``clusterTime``, ``ns``, ``documentKey``, and optionally
                ``fullDocument``.

        Raises:
            StopIteration: If the stream is closed or no matching change event
                arrives before the await timeout expires.
        """
        if self._closed:
            raise StopIteration("Change stream is closed")

        timeout_s = (
            10.0
            if self._max_await_time_ms is None
            else max(0.0, self._max_await_time_ms / 1000.0)
        )
        deadline = time.monotonic() + timeout_s

        # Poll for changes
        while True:
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
            if not rows:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StopIteration("Change stream timeout exceeded")
                time.sleep(min(0.05, remaining))
                continue

            row = rows[0]
            (
                change_id,
                operation,
                document_id,
                document_data,
                document_id_value,
                timestamp,
            ) = row

            parsed_doc, utf8_failed = self._decode_document_data(document_data)
            if self._full_document == "updateLookup" and utf8_failed:
                self._advance_watermark(change_id)
                self._prune_consumed()
                if timeout_s > 0 and time.monotonic() >= deadline:
                    raise StopIteration("Change stream timeout exceeded")
                continue

            if parsed_doc is not None and "_id" in parsed_doc:
                actual_id = parsed_doc["_id"]
            elif document_id_value is not None:
                actual_id = document_id_value
            else:
                stored_id = self._collection._get_stored_id(document_id)
                actual_id = stored_id if stored_id is not None else document_id

            change_doc: dict[str, Any] = {
                "_id": {"id": change_id},
                "operationType": operation,
                "clusterTime": timestamp,
                "ns": {
                    "db": "default",  # Default database name since Connection doesn't have a name property
                    "coll": self._collection.name,
                },
                "documentKey": {"_id": actual_id},
            }

            if self._full_document == "updateLookup" and parsed_doc is not None:
                doc = dict(parsed_doc)
                if "_id" not in doc:
                    doc["_id"] = actual_id
                change_doc["fullDocument"] = doc

            if self._pipeline and not _pipeline_matches(
                change_doc, self._pipeline
            ):
                self._advance_watermark(change_id)
                self._prune_consumed()
                if timeout_s > 0 and time.monotonic() >= deadline:
                    raise StopIteration("Change stream timeout exceeded")
                continue

            # Events are NOT deleted on read (#110): other active
            # streams on this collection keep their own watermarks.
            self._last_id = change_id
            self._last_token_id = change_id
            self._prune_consumed()

            return change_doc

    def __enter__(self) -> Self:
        """Enter the context manager and return this change stream.

        Returns:
            Self: This ``ChangeStream`` instance.
        """
        return self

    def close(self) -> None:
        """Close the change stream and clean up triggers and changelog state.

        Marks the stream closed and invokes ``_cleanup_triggers()`` on the
        first close call; subsequent calls are no-ops.
        """
        if not self._closed:
            self._closed = True
            self._cleanup_triggers()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_traceback: Any) -> None:
        """Exit the context manager and close the change stream.

        Args:
            exc_type: Exception type raised inside the ``with`` block, if any.
            exc_val: Exception instance raised inside the ``with`` block.
            exc_traceback: Traceback object for the exception, if any.
        """
        self.close()
