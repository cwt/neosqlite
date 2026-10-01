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

import json
import logging
import threading
import time
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable

logger = logging.getLogger("nx_27017")

# Counter for wire cursor IDs (data cursors live at 10**12+, see handler).
_cursor_id_counter = count(1000)

# Rows of history retained per collection for resume replay.
_RESUME_RETENTION_ROWS = 1000


def _wire_token(rowid: int) -> dict[str, str]:
    """Opaque resume token embedding the changelog row id.

    Deterministic per row id so repeated getMore calls agree, matching the
    ``postBatchResumeToken == last event _id`` invariant.
    """
    return {"_data": f"nx:{int(rowid)}"}


def _wire_rowid(token: Any) -> int | None:
    """Extract a changelog row id from a resume token, if recognizable."""
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
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _match_spec(value: Any, condition: Any) -> bool:
    """Minimal $match predicate: equality plus $eq/$ne/$in/$nin/$exists."""
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
            elif op == "$in":
                if value not in expected:
                    return False
            elif op == "$nin":
                if value in expected:
                    return False
            elif op == "$exists":
                if bool(expected) != (value is not None):
                    return False
            else:
                return False
        return True
    return bool(value == condition)


def _pipeline_matches(
    change: dict[str, Any], pipeline: list[dict[str, Any]]
) -> bool:
    """Apply $match stages; other stages are accepted and ignored."""
    for stage in pipeline:
        if not isinstance(stage, dict):
            continue
        if "$changeStream" in stage:
            continue
        if set(stage) == {"$match"}:
            spec = stage["$match"]
            if not isinstance(spec, dict):
                continue
            for key, condition in spec.items():
                if not _match_spec(_get_dotted(change, key), condition):
                    return False
        else:
            logger.debug("Ignoring unsupported change-stream stage: %r", stage)
    return True


def _restore_id(document_id_value: Any, document_id: Any) -> Any:
    """Best-effort restore of a document _id from trigger columns."""
    if document_id_value is not None:
        try:
            from neosqlite.objectid import ObjectId

            return ObjectId(document_id_value)
        except (ValueError, TypeError):
            return document_id_value
    return document_id


class ChangeStreamCursor:
    """Wire-level cursor over one (db, collection) change log."""

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
        self.collection_name = collection_name
        self.pipeline = pipeline or []
        self.resume_after = resume_after
        self.start_after = start_after
        self.start_at_operation_time = start_at_operation_time
        self.full_document = full_document or "default"
        self.db_name = db_name
        self._owner = owner
        self._id = next(_cursor_id_counter)
        self._closed = False
        self._start_time = time.time()
        # Changelog row id already delivered; getMore reads (watermark, +inf).
        self._watermark = watermark
        self._last_token_id: int | None = None

    def get_resume_token(self) -> dict:
        """Token for the most recently delivered event (or current point)."""
        if self._last_token_id is None:
            return _wire_token(self._watermark)
        return _wire_token(self._last_token_id)


class ChangeStreamManager:
    """Wire stream registry plus per-(db, collection) trigger trackers."""

    def __init__(self) -> None:
        self._streams: dict[int, ChangeStreamCursor] = {}
        self._trackers: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    # -- trackers ----------------------------------------------------

    def ensure_tracked(
        self, db_name: str, collection_name: str, get_collection: Callable
    ) -> dict[str, Any] | None:
        """Ensure trigger tracking exists; None when unsupported.

        Dotted names (e.g. ``fs.files``) cannot have triggers in NeoSQLite
        and fall back to empty streams.
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
        """Check the shared triggers still exist (survives table drops)."""
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
        watcher = tracker.get("watcher")
        if watcher is not None:
            try:
                watcher.close()
            except Exception:
                pass

    def _watermark_now(self, tracker: dict[str, Any]) -> int:
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
        """Create a wire stream, seeking past any resume token."""
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
            self._streams[stream._id] = stream
        return stream

    def has_activity(self, db_name: str) -> bool:
        """True while any watch stream or tracker exists for a database.

        Used by the connection-cache LRU: a live stream reads the
        database on getMore, so its connection must not be evicted or
        closed under it.
        """
        with self._lock:
            return any(
                stream.db_name == db_name for stream in self._streams.values()
            ) or any(key[0] == db_name for key in self._trackers)

    def get_stream(self, stream_id: int) -> ChangeStreamCursor | None:
        """Get a change stream by ID."""
        with self._lock:
            return self._streams.get(stream_id)

    def close_stream(self, stream_id: int) -> None:
        """Close a change stream."""
        with self._lock:
            stream = self._streams.pop(stream_id, None)
        if stream is None:
            return
        stream._closed = True
        self._prune(stream.db_name, stream.collection_name)

    def close_streams_for_owner(self, owner: Any) -> None:
        """Close every stream created by the given owner (e.g. a connection)."""
        with self._lock:
            owned = [
                stream
                for stream in self._streams.values()
                if stream._owner == owner
            ]
            for stream in owned:
                del self._streams[stream._id]
        for stream in owned:
            stream._closed = True
            self._prune(stream.db_name, stream.collection_name)

    def invalidate(self, db_name: str | None = None) -> None:
        """Drop trigger trackers (database dropped or handler closing)."""
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
        """Read events after the stream watermark, advancing past skips."""
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
            from neosqlite.collection.jsonb_support import (
                json_data_column,
                supports_jsonb,
            )

            data_expr = json_data_column(
                supports_jsonb(collection.db), "document_data"
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
        _rid, operation, document_id, document_data, document_id_value = row[:5]
        actual_id = _restore_id(document_id_value, document_id)
        full_doc = None
        if stream.full_document != "off" and document_data:
            try:
                data = document_data
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                full_doc = json.loads(data)
                if isinstance(full_doc, dict) and "_id" not in full_doc:
                    full_doc["_id"] = actual_id
            except (ValueError, TypeError, UnicodeDecodeError) as exc:
                logger.debug("ChangeStream fullDocument skipped: %s", exc)
                full_doc = None
        now = datetime.now(timezone.utc)
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

    def _prune(self, db_name: str, collection_name: str) -> None:
        """Delete rows every open stream has passed (keeping retention)."""
        with self._lock:
            tracker = self._trackers.get((db_name, collection_name))
            marks = [
                stream._watermark
                for stream in self._streams.values()
                if not stream._closed
                and stream.db_name == db_name
                and stream.collection_name == collection_name
            ]
        if tracker is None or not marks:
            return
        floor = min(marks)
        if floor <= 0:
            return
        try:
            tracker["collection"].db.execute(
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
        """Deprecated: tracking is trigger-backed; kept for tests."""
        logger.debug(
            "notify_change(%r) ignored: streams are trigger-backed",
            collection_name,
        )


def is_change_stream_pipeline(pipeline: list[dict]) -> bool:
    """Check if a pipeline contains a $changeStream stage."""
    for stage in pipeline:
        if isinstance(stage, dict) and "$changeStream" in stage:
            return True
    return False


def extract_change_stream_options(pipeline: list[dict]) -> dict[str, Any]:
    """Extract options from $changeStream stage."""
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
