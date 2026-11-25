"""Change streams for NX-27017, backed by NeoSQLite trigger tracking.

Design (P0 unification): instead of notifying wire streams from each
mutation path (which missed bulk_write, TTL purges and direct-SQL writes),
one persistent :class:`neosqlite.changestream.ChangeStream` per
(db, collection) owns the SQLite triggers. Wire streams are pull-based:
each keeps its own watermark (changelog row id) and ``getMore`` reads new
rows from the shared ``_neosqlite_changestream`` table, translating them to
wire change documents.

No background threads: all tracking queries run under the handler's
per-database lock and are plain indexed SELECTs.
"""

from __future__ import annotations

import logging
import re
import secrets
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from neosqlite.changestream import (
    _RANGE_OPS,
    _compare_range_op,
    _field_present,
)
from neosqlite.collection.json_helpers import neosqlite_json_loads

logger = logging.getLogger("nx_27017")

# Rows of history retained per collection for resume replay.
_RESUME_RETENTION_ROWS = 1000


def _generate_stream_cursor_id() -> int:
    """Return a non-zero unpredictable 63-bit cursor identifier."""
    return secrets.randbits(63) or 1


def _wire_token(rowid: int) -> dict[str, str]:
    """Build an opaque resume token embedding a changelog row ID.

    Produces a deterministic token per row ID so repeated ``getMore`` calls
    agree, preserving the ``postBatchResumeToken == last event _id``
    invariant.

    Args:
        rowid: Changelog row ID from ``_neosqlite_changestream``.

    Returns:
        dict[str, str]: Resume token dictionary of the form
            ``{"_data": "nx:<rowid>"}``.
    """
    return {"_data": f"nx:{int(rowid)}"}


def _wire_rowid(token: Any) -> int | None:
    """Extract a changelog row ID from a resume token if recognizable.

    Unwraps ``{"_id": ...}``, ``{"id": ...}``, and ``{"_data": ...}``
    envelopes and parses ``"nx:<int>"`` strings or non-boolean integers.

    Args:
        token: Candidate resume token dictionary, string, or integer.

    Returns:
        int | None: Parsed changelog row ID, or ``None`` if unrecognized or
            malformed.
    """
    try:
        candidate = token
        if isinstance(candidate, dict) and set(candidate) == {"_id"}:
            candidate = candidate["_id"]
        if isinstance(candidate, dict) and set(candidate) == {"id"}:
            candidate = candidate["id"]
        if isinstance(candidate, dict) and set(candidate) == {"_data"}:
            candidate = candidate["_data"]
        if isinstance(candidate, str) and candidate.startswith("nx:"):
            return int(candidate.split(":")[1])
        if isinstance(candidate, bool):
            return None
        if isinstance(candidate, int):
            return candidate
    except (ValueError, TypeError, IndexError, AttributeError):
        return None
    return None


def _get_dotted(document: Any, path: str) -> Any:
    """Resolve a dot-separated field path from a nested mapping.

    Args:
        document: Root value (typically a ``dict``) to traverse.
        path: Dot-delimited key path such as ``"fullDocument.status"``.

    Returns:
        Any: Value at ``path``, or ``None`` if any intermediate value is not a
            ``dict`` or a key is missing.
    """
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _match_spec(value: Any, condition: Any) -> bool:
    """Evaluate a ``$match`` predicate against a field value.

    Supports direct equality, compiled ``re.Pattern``, as well as operator
    dictionaries using ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``,
    ``$lte``, ``$in``, ``$nin``, ``$exists``, ``$regex``/``$options``, and
    ``$not``. Unrecognized ``$``-prefixed operators evaluate to ``False``.

    Args:
        value: Actual field value extracted from a change event.
        condition: Expected scalar/dict/pattern value or operator dict.

    Returns:
        bool: ``True`` if ``value`` satisfies ``condition``, otherwise
            ``False``.
    """
    if isinstance(condition, dict) and any(
        k.startswith("$") for k in condition
    ):
        for op, expected in condition.items():
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
                if bool(expected) != (value is not None):
                    return False
            elif op == "$regex":
                if isinstance(expected, re.Pattern):
                    pattern = expected
                elif isinstance(expected, str):
                    flags = 0
                    raw_opts = condition.get("$options", "")
                    if isinstance(raw_opts, str):
                        if "i" in raw_opts:
                            flags |= re.IGNORECASE
                        if "m" in raw_opts:
                            flags |= re.MULTILINE
                        if "s" in raw_opts:
                            flags |= re.DOTALL
                        if "x" in raw_opts:
                            flags |= re.VERBOSE
                    try:
                        pattern = re.compile(expected, flags)
                    except re.error:
                        return False
                else:
                    return False
                if not isinstance(value, str) or pattern.search(value) is None:
                    return False
            elif op == "$options":
                if "$regex" not in condition:
                    return False
            elif op == "$not":
                if isinstance(expected, re.Pattern):
                    if (
                        isinstance(value, str)
                        and expected.search(value) is not None
                    ):
                        return False
                elif isinstance(expected, dict):
                    if _match_spec(value, expected):
                        return False
                else:
                    return False
            else:
                return False
        return True
    if isinstance(condition, re.Pattern):
        return isinstance(value, str) and condition.search(value) is not None
    return bool(value == condition)


def _match_filter(change: dict[str, Any], spec: dict[str, Any]) -> bool:
    """Evaluate a change-stream ``$match`` filter against a change event."""
    for key, condition in spec.items():
        if key == "$and":
            if not isinstance(condition, list) or not all(
                isinstance(sub, dict) and _match_filter(change, sub)
                for sub in condition
            ):
                return False
        elif key == "$or":
            if not isinstance(condition, list) or not any(
                isinstance(sub, dict) and _match_filter(change, sub)
                for sub in condition
            ):
                return False
        elif key == "$nor":
            if not isinstance(condition, list) or any(
                isinstance(sub, dict) and _match_filter(change, sub)
                for sub in condition
            ):
                return False
        elif isinstance(condition, dict) and any(
            k.startswith("$") for k in condition
        ):
            ops = dict(condition)
            if "$exists" in ops:
                exists_expected = ops.pop("$exists")
                if bool(exists_expected) != _field_present(change, key):
                    return False
            if ops and not _match_spec(_get_dotted(change, key), ops):
                return False
        elif not _match_spec(_get_dotted(change, key), condition):
            return False
    return True


def _pipeline_matches(
    change: dict[str, Any], pipeline: list[dict[str, Any]]
) -> bool:
    """Filter a change event through ``$match`` stages in a pipeline.

    ``$changeStream`` stages and non-``dict`` entries are skipped; other
    stages are logged at debug level and ignored.

    Args:
        change: Translated wire change document to test.
        pipeline: Aggregation pipeline configured on the change stream.

    Returns:
        bool: ``True`` if all ``$match`` stages accept ``change``.
    """
    for stage in pipeline:
        if not isinstance(stage, dict):
            continue
        if "$changeStream" in stage:
            continue
        if set(stage) == {"$match"}:
            spec = stage["$match"]
            if not isinstance(spec, dict):
                continue
            if not _match_filter(change, spec):
                return False
        else:
            logger.debug("Ignoring unsupported change-stream stage: %r", stage)
    return True


def _restore_id(document_id_value: Any, document_id: Any) -> Any:
    """Restore a document ``_id`` from trigger tracking columns.

    When ``document_id_value`` is present, attempts to reconstruct an
    ``ObjectId`` from it and falls back to the raw value if invalid;
    otherwise returns ``document_id``.

    Args:
        document_id_value: Serialized ObjectId hex value from the trigger
            table, or ``None``.
        document_id: Scalar ``_id`` value stored in the changelog row.

    Returns:
        Any: Restored ``ObjectId`` or scalar document identifier.
    """
    if document_id_value is not None:
        try:
            from neosqlite.objectid import ObjectId

            return ObjectId(document_id_value)
        except (ValueError, TypeError):
            return document_id_value
    return document_id


class ChangeStreamCursor:
    """Wire-level cursor over a single ``(db, collection)`` change log.

    Tracks the stream's filtering pipeline, resume options, unique cursor ID,
    and changelog watermark across ``getMore`` pulls.

    Attributes:
        collection_name: Target collection name being watched.
        pipeline: Change stream aggregation pipeline stages.
        resume_after: Optional ``resumeAfter`` token from ``$changeStream``.
        start_after: Optional ``startAfter`` token from ``$changeStream``.
        start_at_operation_time: Optional ``startAtOperationTime`` timestamp.
        full_document: Full-document lookup mode (defaults to ``"default"``).
        db_name: Database name owning the watched collection.
    """

    def __init__(
        self,
        collection_name: str,
        pipeline: list[dict],
        resume_after: dict | None = None,
        start_at_operation_time: datetime | None = None,
        full_document: str | None = None,
        db_name: str = "test",
        owner: Any = None,
        start_after: dict | None = None,
        watermark: int = 0,
    ):
        """Initialize a wire-level change stream cursor.

        Args:
            collection_name: Name of the watched collection.
            pipeline: Aggregation pipeline stages applied to events.
            resume_after: Optional resume token specifying where to resume.
            start_at_operation_time: Optional operation timestamp option.
            full_document: Full-document mode (e.g., ``"default"``,
                ``"updateLookup"``, or ``"off"``).
            db_name: Database name containing the watched collection.
            owner: Opaque owner handle (such as a client connection) used for
                bulk cleanup.
            start_after: Optional ``startAfter`` resume token.
            watermark: Initial changelog row ID already seen by this cursor.
        """
        self.collection_name = collection_name
        self.pipeline = pipeline or []
        self.resume_after = resume_after
        self.start_after = start_after
        self.start_at_operation_time = start_at_operation_time
        self.full_document = full_document or "default"
        self.db_name = db_name
        self._owner = owner
        self._id = _generate_stream_cursor_id()
        self._closed = False
        self._start_time = time.time()
        # Changelog row id already delivered; getMore reads (watermark, +inf).
        self._watermark = watermark
        self._last_token_id: int | None = None

    def get_resume_token(self) -> dict:
        """Return the resume token for the cursor's current position.

        Returns:
            dict: Wire resume token for the most recently matched event, or
                the current watermark when no event has matched yet.
        """
        if self._last_token_id is None:
            return _wire_token(self._watermark)
        return _wire_token(self._last_token_id)


class ChangeStreamManager:
    """Registry of wire change streams and per-collection trigger trackers.

    Manages shared :class:`neosqlite.changestream.ChangeStream` trigger
    watchers keyed by ``(db_name, collection_name)`` alongside active
    :class:`ChangeStreamCursor` instances, supporting thread-safe lifecycle
    tracking, pull-based event reading, and changelog retention pruning.
    """

    def __init__(self) -> None:
        """Initialize empty stream and tracker registries with a lock."""
        self._streams: dict[int, ChangeStreamCursor] = {}
        self._trackers: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    # -- trackers ----------------------------------------------------

    def ensure_tracked(
        self, db_name: str, collection_name: str, get_collection: Callable
    ) -> dict[str, Any] | None:
        """Ensure trigger tracking exists for a collection.

        Dotted collection names (for example, ``fs.files``) cannot have
        SQLite triggers in NeoSQLite and return ``None`` so streams fall back
        to empty results. Re-creates the tracker if its triggers were dropped.

        Args:
            db_name: Database name containing the collection.
            collection_name: Collection name to track.
            get_collection: Zero-argument factory returning the NeoSQLite
                collection object.

        Returns:
            dict[str, Any] | None: Tracker dictionary containing
                ``"watcher"`` and ``"collection"``, or ``None`` when trigger
                tracking is unsupported for ``collection_name``.
        """
        from neosqlite.changestream import ChangeStream as NeoChangeStream

        try:
            NeoChangeStream._sanitize_collection_name(collection_name)
        except ValueError:
            return None
        key = (db_name, collection_name)
        with self._lock:
            tracker = self._trackers.get(key)
            if tracker is not None and self._triggers_live(tracker):
                return tracker
            if tracker is not None:
                self._drop_tracker_locked(tracker)
            collection = get_collection()
            watcher = NeoChangeStream(
                collection,
                pipeline=[],
                full_document="updateLookup",
                max_await_time_ms=1000,
            )
            tracker = {"watcher": watcher, "collection": collection}
            self._trackers[key] = tracker
            return tracker

    @staticmethod
    def _triggers_live(tracker: dict[str, Any]) -> bool:
        """Check whether all three SQLite triggers still exist for a tracker.

        Args:
            tracker: Tracker dictionary containing the tracked
                ``"collection"``.

        Returns:
            bool: ``True`` if insert, update, and delete triggers exist in
                ``sqlite_master``, otherwise ``False``.
        """
        from neosqlite.changestream import ChangeStream as NeoChangeStream

        collection = tracker["collection"]
        try:
            name = NeoChangeStream._sanitize_collection_name(collection.name)
            rows = collection.db.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' "
                "AND name IN (?, ?, ?)",
                (
                    f"_neosqlite_{name}_insert_trigger",
                    f"_neosqlite_{name}_update_trigger",
                    f"_neosqlite_{name}_delete_trigger",
                ),
            ).fetchone()
            return bool(rows and rows[0] == 3)
        except Exception:
            return False

    def _drop_tracker_locked(self, tracker: dict[str, Any]) -> None:
        """Close the ``ChangeStream`` watcher stored in a tracker dict.

        Args:
            tracker: Tracker dictionary whose ``"watcher"`` should be closed.
        """
        watcher = tracker.get("watcher")
        if watcher is not None:
            try:
                watcher.close()
            except Exception:
                pass

    def _watermark_now(self, tracker: dict[str, Any]) -> int:
        """Return the latest changelog row ID for a tracked collection.

        Args:
            tracker: Tracker dictionary containing the tracked
                ``"collection"``.

        Returns:
            int: Highest ``id`` in ``_neosqlite_changestream`` for the
                collection, or ``0`` if none exist or querying fails.
        """
        collection = tracker["collection"]
        try:
            row = collection.db.execute(
                "SELECT COALESCE(MAX(id), 0) FROM _neosqlite_changestream "
                "WHERE collection_name = ?",
                (collection.name,),
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    # -- wire streams --------------------------------------------------

    def create_stream(
        self,
        collection_name: str,
        pipeline: list[dict],
        resume_after: dict | None = None,
        start_at_operation_time: datetime | None = None,
        full_document: str | None = None,
        db_name: str = "test",
        owner: Any = None,
        start_after: dict | None = None,
        get_collection: Callable | None = None,
    ) -> ChangeStreamCursor:
        """Create and register a wire stream positioned at the right offset.

        Installs trigger tracking if ``get_collection`` is provided and sets
        the initial watermark from ``resume_after`` or ``start_after`` when
        recognizable, or from the current changelog head otherwise.

        Args:
            collection_name: Name of the watched collection.
            pipeline: Aggregation pipeline stages applied to events.
            resume_after: Optional resume token for resuming after an event.
            start_at_operation_time: Optional operation timestamp (ignored in
                NeoSQLite; starts from current head).
            full_document: Full-document inclusion mode.
            db_name: Database name owning ``collection_name``.
            owner: Opaque owner handle associated with the stream.
            start_after: Optional ``startAfter`` resume token.
            get_collection: Optional factory returning the NeoSQLite
                collection to ensure trigger tracking.

        Returns:
            ChangeStreamCursor: Newly registered cursor instance.
        """
        watermark = 0
        if get_collection is not None:
            tracker = self.ensure_tracked(
                db_name, collection_name, get_collection
            )
            if tracker is not None:
                watermark = self._watermark_now(tracker)
        token = resume_after if resume_after is not None else start_after
        if token is not None:
            rowid = _wire_rowid(token)
            if rowid is not None:
                watermark = rowid
            else:
                logger.debug(
                    "Unrecognized resume token %r; starting now", token
                )
        if start_at_operation_time is not None:
            logger.debug(
                "start_at_operation_time has no operation-time concept in "
                "NeoSQLite; starting from now"
            )
        stream = ChangeStreamCursor(
            collection_name=collection_name,
            pipeline=pipeline,
            resume_after=resume_after,
            start_at_operation_time=start_at_operation_time,
            full_document=full_document,
            db_name=db_name,
            owner=owner,
            start_after=start_after,
            watermark=watermark,
        )
        with self._lock:
            while stream._id in self._streams:
                stream._id = _generate_stream_cursor_id()
            self._streams[stream._id] = stream
        return stream

    def has_activity(self, db_name: str) -> bool:
        """Return whether any watch stream or tracker exists for a database.

        Used by the connection-cache LRU because a live stream reads the
        database on ``getMore``, so its connection must not be evicted or
        closed underneath it.

        Args:
            db_name: Database name to check for active streams or trackers.

        Returns:
            bool: ``True`` if at least one stream or tracker targets
                ``db_name``.
        """
        with self._lock:
            return any(
                stream.db_name == db_name for stream in self._streams.values()
            ) or any(key[0] == db_name for key in self._trackers)

    def get_stream(self, stream_id: int) -> ChangeStreamCursor | None:
        """Look up a registered change stream cursor by ID.

        Args:
            stream_id: Wire cursor identifier assigned to the stream.

        Returns:
            ChangeStreamCursor | None: Matching stream cursor, or ``None`` if
                not found.
        """
        with self._lock:
            return self._streams.get(stream_id)

    def close_stream(self, stream_id: int) -> None:
        """Close and unregister a change stream cursor by ID.

        Marks the cursor closed, prunes consumed changelog rows for its
        collection, and drops the backing tracker when no open streams remain.

        Args:
            stream_id: Wire cursor identifier of the stream to close.
        """
        with self._lock:
            stream = self._streams.pop(stream_id, None)
        if stream is None:
            return
        stream._closed = True
        self._prune(
            stream.db_name,
            stream.collection_name,
            fallback_watermark=stream._watermark,
        )

    def close_streams_for_owner(
        self,
        owner: Any,
        lock_for: Callable[[str], Any] | None = None,
    ) -> None:
        """Close every stream created by the given owner.

        Args:
            owner: Owner handle (for example, a client connection) whose
                streams should be closed and pruned.
            lock_for: Optional callable returning a context-manager lock for a
                database name before executing SQLite changelog pruning.
        """
        with self._lock:
            owned = [
                stream
                for stream in self._streams.values()
                if stream._owner == owner
            ]
            for stream in owned:
                del self._streams[stream._id]
        closed_targets: dict[tuple[str, str], int] = {}
        for stream in owned:
            stream._closed = True
            key = (stream.db_name, stream.collection_name)
            closed_targets[key] = max(
                closed_targets.get(key, 0), stream._watermark
            )
        for (db_name, coll_name), max_mark in closed_targets.items():
            if lock_for is not None:
                try:
                    with lock_for(db_name):
                        self._prune(
                            db_name,
                            coll_name,
                            fallback_watermark=max_mark,
                        )
                except ValueError:
                    # DB already invalidated/unmapped; still drop tracker ref.
                    with self._lock:
                        still_open = any(
                            not s._closed
                            and s.db_name == db_name
                            and s.collection_name == coll_name
                            for s in self._streams.values()
                        )
                        if not still_open:
                            tracker = self._trackers.pop(
                                (db_name, coll_name), None
                            )
                            if tracker is not None:
                                self._drop_tracker_locked(tracker)
            else:
                self._prune(db_name, coll_name, fallback_watermark=max_mark)

    def invalidate(self, db_name: str | None = None) -> None:
        """Drop trigger trackers for a database or for all databases.

        Args:
            db_name: Database name whose trackers should be dropped, or
                ``None`` to drop all trackers across every database.
        """
        with self._lock:
            keys = [
                key
                for key in self._trackers
                if db_name is None or key[0] == db_name
            ]
            trackers = [self._trackers.pop(key) for key in keys]
        for tracker in trackers:
            self._drop_tracker_locked(tracker)

    # -- pull ----------------------------------------------------------

    def pull_stream(
        self, stream: ChangeStreamCursor, batch_size: int = 101
    ) -> tuple[list[dict[str, Any]], dict]:
        """Read new changelog events past the stream's watermark.

        Queries ``_neosqlite_changestream`` for rows with ``id > watermark``,
        translates each row, filters through ``stream.pipeline``, advances
        the watermark past all scanned rows, and prunes obsolete history.

        Args:
            stream: Change stream cursor to advance.
            batch_size: Maximum number of changelog rows to read in one pull.

        Returns:
            tuple[list[dict[str, Any]], dict]: Pair of the matched change
                event batch and the updated wire resume token.
        """
        with self._lock:
            tracker = self._trackers.get(
                (stream.db_name, stream.collection_name)
            )
        batch: list[dict[str, Any]] = []
        if tracker is None:
            return batch, stream.get_resume_token()
        collection = tracker["collection"]
        try:
            # Updated documents may be stored as JSONB binary blobs
            # (pysqlite3 builds); convert to text like normal reads do.
            from neosqlite.collection.jsonb_support import JSONBContext

            data_expr = JSONBContext.from_db(collection.db).data_column(
                "document_data"
            )
            rows = collection.db.execute(
                "SELECT id, operation, document_id, "
                f"{data_expr} AS document_data, document_id_value "
                "FROM _neosqlite_changestream "
                "WHERE collection_name = ? AND id > ? "
                "ORDER BY id LIMIT ?",
                (stream.collection_name, stream._watermark, batch_size),
            ).fetchall()
        except Exception as exc:
            logger.debug("ChangeStream pull skipped: %s", exc)
            return batch, stream.get_resume_token()
        max_seen = stream._watermark
        for row in rows:
            rowid = int(row[0])
            max_seen = max(max_seen, rowid)
            doc = self._translate_row(stream, rowid, row)
            if doc is not None and _pipeline_matches(doc, stream.pipeline):
                batch.append(doc)
                stream._last_token_id = rowid
        stream._watermark = max_seen
        self._prune(stream.db_name, stream.collection_name)
        return batch, stream.get_resume_token()

    def _translate_row(
        self, stream: ChangeStreamCursor, rowid: int, row: Any
    ) -> dict[str, Any] | None:
        """Translate a changelog table row into a wire change event dict.

        Args:
            stream: Cursor specifying ``db_name``, ``collection_name``, and
                ``full_document`` options.
            rowid: Integer row ID of the changelog entry.
            row: Five-element row tuple ``(id, operation, document_id,
                document_data, document_id_value)``.

        Returns:
            dict[str, Any] | None: Formatted MongoDB wire change event
                dictionary.
        """
        _rid, operation, document_id, document_data, document_id_value = row[:5]
        actual_id = _restore_id(document_id_value, document_id)
        full_doc = None
        if stream.full_document != "off" and document_data:
            try:
                data = document_data
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                full_doc = neosqlite_json_loads(data)
                if isinstance(full_doc, dict) and "_id" not in full_doc:
                    full_doc["_id"] = actual_id
            except (ValueError, TypeError, UnicodeDecodeError) as exc:
                logger.debug("ChangeStream fullDocument skipped: %s", exc)
                full_doc = None
        now = datetime.now(UTC)
        return {
            "_id": _wire_token(rowid),
            "operationType": operation,
            "clusterTime": now,
            "wallTime": now,
            "fullDocument": full_doc,
            "ns": {"db": stream.db_name, "coll": stream.collection_name},
            "documentKey": {"_id": actual_id},
            "updateDescription": None,
        }

    def _prune(
        self,
        db_name: str,
        collection_name: str,
        fallback_watermark: int = 0,
    ) -> None:
        """Delete changelog rows passed by all open streams on a collection.

        Retains the newest ``_RESUME_RETENTION_ROWS`` entries per collection
        so resume tokens remain replayable. When no open streams remain for
        ``(db_name, collection_name)``, prunes up to ``fallback_watermark``
        (still capped by ``_RESUME_RETENTION_ROWS``), drops the SQLite
        triggers, and removes the tracker entry so database LRU eviction is
        not blocked.

        Args:
            db_name: Database name owning the collection.
            collection_name: Collection name whose changelog may be pruned.
            fallback_watermark: Watermark used when the last stream closes.
        """
        tracker: dict[str, Any] | None = None
        dropping_last = False
        floor = fallback_watermark
        with self._lock:
            marks = [
                stream._watermark
                for stream in self._streams.values()
                if not stream._closed
                and stream.db_name == db_name
                and stream.collection_name == collection_name
            ]
            if marks:
                tracker = self._trackers.get((db_name, collection_name))
                floor = min(marks)
            else:
                tracker = self._trackers.pop((db_name, collection_name), None)
                dropping_last = tracker is not None
                if tracker is not None:
                    self._drop_tracker_locked(tracker)

        if tracker is None:
            return
        if floor > 0:
            try:
                conn = tracker["collection"].db
                conn.execute(
                    "DELETE FROM _neosqlite_changestream "
                    "WHERE collection_name = ? AND id < ? AND id NOT IN ("
                    "SELECT id FROM _neosqlite_changestream "
                    "WHERE collection_name = ? ORDER BY id DESC LIMIT ?)",
                    (
                        collection_name,
                        floor,
                        collection_name,
                        _RESUME_RETENTION_ROWS,
                    ),
                )
                if dropping_last and not conn.in_transaction:
                    conn.commit()
            except Exception as exc:
                logger.debug("ChangeStream prune skipped: %s", exc)

    # -- legacy compat ---------------------------------------------------

    def notify_change(
        self,
        collection_name: str,
        operation_type: str,
        document: dict,
        document_key: dict,
        update_description: dict | None = None,
        db_name: str = "test",
    ) -> None:
        """Ignore deprecated change notifications (kept for tests).

        Change tracking is trigger-backed via SQLite triggers and pulled by
        ``pull_stream``.

        Args:
            collection_name: Name of the mutated collection.
            operation_type: Mutation operation name.
            document: Document payload associated with the mutation.
            document_key: Key document containing ``_id``.
            update_description: Optional update description payload.
            db_name: Database name containing the collection.
        """
        logger.debug(
            "notify_change(%r) ignored: streams are trigger-backed",
            collection_name,
        )


def is_change_stream_pipeline(pipeline: list[dict]) -> bool:
    """Check if an aggregation pipeline contains a ``$changeStream`` stage.

    Args:
        pipeline: Aggregation pipeline list of stage dictionaries.

    Returns:
        bool: ``True`` if any stage dictionary has a ``"$changeStream"`` key.
    """
    for stage in pipeline:
        if isinstance(stage, dict) and "$changeStream" in stage:
            return True
    return False


def extract_change_stream_options(pipeline: list[dict]) -> dict[str, Any]:
    """Extract change-stream options from the first ``$changeStream`` stage.

    Args:
        pipeline: Aggregation pipeline list of stage dictionaries.

    Returns:
        dict[str, Any]: Dictionary mapping normalized option names
            (``resume_after``, ``start_after``, ``start_at_operation_time``,
            and ``full_document``) when present, or empty if no dict-valued
            ``$changeStream`` stage is found.
    """
    options: dict[str, Any] = {}
    for stage in pipeline:
        if isinstance(stage, dict) and "$changeStream" in stage:
            opts = stage["$changeStream"]
            if isinstance(opts, dict):
                options["resume_after"] = opts.get("resumeAfter")
                options["start_after"] = opts.get("startAfter")
                options["start_at_operation_time"] = opts.get(
                    "startAtOperationTime"
                )
                options["full_document"] = opts.get("fullDocument")
            break
    return options
