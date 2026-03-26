"""Change streams implementation for NX-27017."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable

from bson import ObjectId as BsonObjectId

logger = logging.getLogger("nx_27017")

# Counter for generating unique cursor IDs
_cursor_id_counter = count(1000)


class ChangeStreamCursor:
    """A cursor-like object for change streams."""

    def __init__(
        self,
        collection_name: str,
        pipeline: list[dict],
        resume_after: dict | None = None,
        start_at_operation_time: datetime | None = None,
        full_document: str | None = None,
    ):
        self.collection_name = collection_name
        self.pipeline = pipeline
        self.resume_after = resume_after
        self.start_at_operation_time = start_at_operation_time
        self.full_document = full_document or "default"
        self._id = next(_cursor_id_counter)  # Integer cursor ID
        self._stream_id = str(
            uuid.uuid4()
        )  # String stream ID for internal tracking
        self._closed = False
        self._start_time = time.time()
        self._changes: list[dict] = []
        self._position = 0

        # Extract filter from $match stage if present
        self._filter = self._extract_filter()

    def _extract_filter(self) -> dict:
        """Extract filter from pipeline's $match stage."""
        for stage in self.pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                return stage["$match"]
        return {}

    def close(self) -> None:
        """Close the change stream."""
        self._closed = True

    def is_closed(self) -> bool:
        """Check if the change stream is closed."""
        return self._closed

    def to_list(self) -> list[dict]:
        """Return all changes (for initial response)."""
        # Change streams initially return empty batch
        return []

    def get_resume_token(self) -> dict:
        """Get the resume token for this change stream."""
        return {"_id": BsonObjectId()}

    def _on_change(
        self,
        operation_type: str,
        document: dict,
        document_key: dict,
        update_description: dict | None = None,
    ) -> None:
        """Handle a change event for this stream."""
        if not self._closed:
            change_doc = self._create_change_document(
                operation_type=operation_type,
                document_key=document_key,
                full_doc=document if self.full_document != "off" else None,
                update_description=update_description,
            )
            self._changes.append(change_doc)

    def _create_change_document(
        self,
        operation_type: str,
        document_key: dict,
        full_doc: dict | None = None,
        update_description: dict | None = None,
    ) -> dict:
        """Create a change document for the change stream."""
        now = datetime.now(timezone.utc)
        return {
            "_id": {"_data": str(uuid.uuid4())},
            "operationType": operation_type,
            "clusterTime": now,
            "wallTime": now,
            "fullDocument": full_doc,
            "ns": {"db": "test", "coll": self.collection_name},
            "documentKey": document_key,
            "updateDescription": update_description,
        }


class ChangeStreamManager:
    """Manages active change streams."""

    def __init__(self) -> None:
        self._streams: dict[int, ChangeStreamCursor] = {}
        self._listeners: dict[str, list[Callable]] = {}

    def create_stream(
        self,
        collection_name: str,
        pipeline: list[dict],
        resume_after: dict | None = None,
        start_at_operation_time: datetime | None = None,
        full_document: str | None = None,
    ) -> ChangeStreamCursor:
        """Create a new change stream."""
        stream = ChangeStreamCursor(
            collection_name=collection_name,
            pipeline=pipeline,
            resume_after=resume_after,
            start_at_operation_time=start_at_operation_time,
            full_document=full_document,
        )
        self._streams[stream._id] = stream

        # Register listener for collection changes
        if collection_name not in self._listeners:
            self._listeners[collection_name] = []
        self._listeners[collection_name].append(stream._on_change)

        logger.debug(
            f"Created change stream {stream._id} for collection {collection_name}"
        )
        return stream

    def close_stream(self, stream_id: int) -> None:
        """Close a change stream."""
        if stream_id in self._streams:
            stream = self._streams[stream_id]
            stream.close()
            del self._streams[stream_id]
            logger.debug(f"Closed change stream {stream_id}")

    def get_stream(self, stream_id: int) -> ChangeStreamCursor | None:
        """Get a change stream by ID."""
        return self._streams.get(stream_id)

    def notify_change(
        self,
        collection_name: str,
        operation_type: str,
        document: dict,
        document_key: dict,
        update_description: dict | None = None,
    ) -> None:
        """Notify all change streams of a change."""
        if collection_name not in self._listeners:
            return

        for listener in self._listeners[collection_name]:
            try:
                listener(
                    operation_type, document, document_key, update_description
                )
            except Exception as e:
                logger.error(f"Error notifying change stream listener: {e}")

    def _on_change(
        self,
        stream_id: int,
        operation_type: str,
        document: dict,
        document_key: dict,
        update_description: dict | None = None,
    ) -> None:
        """Handle a change for a specific stream."""
        stream = self._streams.get(stream_id)
        if stream and not stream.is_closed():
            change_doc = stream._create_change_document(
                operation_type=operation_type,
                document_key=document_key,
                full_doc=document if stream.full_document != "off" else None,
                update_description=update_description,
            )
            stream._changes.append(change_doc)


def is_change_stream_pipeline(pipeline: list[dict]) -> bool:
    """Check if a pipeline contains a $changeStream stage."""
    for stage in pipeline:
        if isinstance(stage, dict) and "$changeStream" in stage:
            return True
    return False


def extract_change_stream_options(pipeline: list[dict]) -> dict[str, Any]:
    """Extract options from $changeStream stage."""
    options = {}
    for stage in pipeline:
        if isinstance(stage, dict) and "$changeStream" in stage:
            opts = stage["$changeStream"]
            if isinstance(opts, dict):
                options["resume_after"] = opts.get("resumeAfter")
                options["start_at_operation_time"] = opts.get(
                    "startAtOperationTime"
                )
                options["full_document"] = opts.get("fullDocument")
            break
    return options
