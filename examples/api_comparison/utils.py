"""
Utility functions for API comparison tests
"""

from datetime import timezone
from typing import Any, Callable

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


def _sort_key(value: Any) -> str:
    """Sort key for comparing documents regardless of field order."""
    if isinstance(value, dict):
        return str(sorted(value.items()))
    return str(value)


def test_pymongo_connection() -> MongoClient | None:
    """Test connection to MongoDB"""
    try:
        client: MongoClient = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        print("MongoDB connection successful")
        return client
    except ConnectionFailure:
        print("Failed to connect to MongoDB")
        return None


def _normalize_id(value: Any) -> Any:
    """
    Normalize ObjectId and _id fields for comparison.

    Converts ObjectId to string and removes _id fields for comparison purposes.
    This handles auto-generated IDs that differ between NeoSQLite and MongoDB.
    Also normalizes datetime objects (tz-aware vs naive).
    """
    if value is None:
        return None

    # Handle ObjectId
    if hasattr(value, "__class__") and value.__class__.__name__ == "ObjectId":
        return str(value)

    # Handle NeoSQLite's internal dict representation of ObjectId
    if isinstance(value, dict) and "__neosqlite_objectid__" in value:
        return str(value.get("id"))

    # Handle datetime - normalize to naive UTC for comparison
    if hasattr(value, "__class__") and value.__class__.__name__ == "datetime":
        # If timezone-aware, convert to UTC and make naive
        if getattr(value, "tzinfo", None) is not None:
            # Convert to UTC
            utc_dt = value.astimezone(timezone.utc)
            # Return as naive datetime
            return utc_dt.replace(tzinfo=None)
        return value

    # Handle dict - remove _id and normalize nested values
    if isinstance(value, dict):
        return {
            k: _normalize_id(v)
            for k, v in value.items()
            if k != "_id"  # Remove _id field
        }

    # Handle list - normalize each item
    if isinstance(value, list):
        return [_normalize_id(item) for item in value]

    # Handle other types as-is
    return value


def compare_results(
    neo_results: list,
    mongo_results: list,
    tolerance: float = 1e-9,
    ignore_order: bool = True,
) -> tuple[bool, str | None]:
    """
    Compare NeoSQLite and MongoDB aggregation results.

    Args:
        neo_results: Results from NeoSQLite
        mongo_results: Results from MongoDB
        tolerance: Tolerance for floating point comparisons
        ignore_order: If True, sort results before comparison (for set-like comparison)

    Returns:
        Tuple of (passed: bool, error_message: str | None)
    """
    # Normalize results (remove _id, convert ObjectId to string)
    neo_normalized = [_normalize_id(doc) for doc in neo_results]
    mongo_normalized = [_normalize_id(doc) for doc in mongo_results]

    # Sort if order doesn't matter
    if ignore_order:
        try:
            neo_normalized = sorted(neo_normalized, key=_sort_key)
            mongo_normalized = sorted(mongo_normalized, key=_sort_key)
        except (TypeError, AttributeError):
            # If sorting fails, compare as-is
            pass

    # Check length
    if len(neo_normalized) != len(mongo_normalized):
        return (
            False,
            f"Length mismatch: NeoSQLite={len(neo_normalized)}, MongoDB={len(mongo_normalized)}",
        )

    # Compare each document
    for i, (neo_doc, mongo_doc) in enumerate(
        zip(neo_normalized, mongo_normalized)
    ):
        match, error = _compare_documents(neo_doc, mongo_doc, tolerance)
        if not match:
            return False, f"Document {i} mismatch: {error}"

    return True, None


def _compare_documents(
    neo_doc: Any,
    mongo_doc: Any,
    tolerance: float = 1e-9,
) -> tuple[bool, str | None]:
    """
    Compare two documents for equality.

    Args:
        neo_doc: Document from NeoSQLite
        mongo_doc: Document from MongoDB
        tolerance: Tolerance for floating point comparisons

    Returns:
        Tuple of (passed: bool, error_message: str | None)
    """
    # Handle None
    if neo_doc is None and mongo_doc is None:
        return True, None
    if neo_doc is None or mongo_doc is None:
        return False, "None mismatch"

    # Handle different types
    if type(neo_doc).__name__ != type(mongo_doc).__name__:
        # Special case: int vs float comparison
        if isinstance(neo_doc, (int, float)) and isinstance(
            mongo_doc, (int, float)
        ):
            if abs(float(neo_doc) - float(mongo_doc)) > tolerance:
                return False, f"Numeric mismatch: {neo_doc} vs {mongo_doc}"
            return True, None
        return (
            False,
            f"Type mismatch: {type(neo_doc).__name__} vs {type(mongo_doc).__name__}",
        )

    # Handle dicts
    if isinstance(neo_doc, dict):
        neo_keys = set(neo_doc.keys())
        mongo_keys = set(mongo_doc.keys())

        if neo_keys != mongo_keys:
            return False, f"Key mismatch: {neo_keys} vs {mongo_keys}"

        for key in neo_keys:
            match, error = _compare_documents(
                neo_doc[key], mongo_doc[key], tolerance
            )
            if not match:
                return False, f"Key '{key}': {error}"
        return True, None

    # Handle lists
    if isinstance(neo_doc, list):
        if len(neo_doc) != len(mongo_doc):
            return (
                False,
                f"List length mismatch: {len(neo_doc)} vs {len(mongo_doc)}",
            )

        for i, (neo_item, mongo_item) in enumerate(zip(neo_doc, mongo_doc)):
            match, error = _compare_documents(neo_item, mongo_item, tolerance)
            if not match:
                return False, f"Item {i}: {error}"
        return True, None

    # Handle numeric types with tolerance
    if isinstance(neo_doc, (int, float)) and isinstance(
        mongo_doc, (int, float)
    ):
        if abs(float(neo_doc) - float(mongo_doc)) > tolerance:
            return False, f"Numeric mismatch: {neo_doc} vs {mongo_doc}"
        return True, None

    # Handle other types with direct comparison
    if neo_doc != mongo_doc:
        return False, f"Value mismatch: {neo_doc} vs {mongo_doc}"

    return True, None


# ============================================================================
# Tier Change Tracking for detecting unexpected tier fallsbacks
# ============================================================================


class TierChangeTracker:
    """Tracks tier changes during API comparison tests."""

    def __init__(self) -> None:
        self.enabled = False
        self.changes: list[tuple[str | None, str, list]] = []
        self._callback: Callable | None = None
        self._connections: list = []

    def _on_tier_change(
        self, prev_tier: str | None, new_tier: str, pipeline: list
    ) -> None:
        """Callback for tier changes."""
        if self.enabled:
            self.changes.append((prev_tier, new_tier, pipeline))

    def register_connection(self, connection) -> None:
        """Register a connection for tier change tracking."""
        if not self.enabled:
            return
        try:
            qe = connection.test_collection.query_engine
            qe.add_tier_change_callback(self._on_tier_change)
            self._connections.append((connection, self._on_tier_change))
        except Exception:
            pass

    def enable(self) -> None:
        """Enable tier change tracking."""
        self.enabled = True

    def disable(self) -> None:
        """Disable tier change tracking."""
        for conn, callback in self._connections:
            try:
                qe = conn.test_collection.query_engine
                qe.remove_tier_change_callback(callback)
            except Exception:
                pass
        self.enabled = False
        self._connections = []

    def get_changes(self) -> list[tuple[str | None, str, list]]:
        """Get all recorded tier changes."""
        return self.changes

    def has_fallback(self) -> bool:
        """Check if any tier fallback occurred (tier3 is involved)."""
        return any(
            new == "tier3" or prev == "tier3" for prev, new, _ in self.changes
        )

    def clear(self):
        """Clear recorded changes."""
        self.changes = []


# Global tier change tracker instance
_tier_tracker = TierChangeTracker()


def enable_tier_tracking(_connection=None):
    """Enable tier change tracking for comparison tests.

    Call this before running tests to track unexpected tier changes.
    """
    _tier_tracker.enable()


def disable_tier_tracking():
    """Disable tier change tracking."""
    _tier_tracker.disable()


def register_connection_for_tier_tracking(connection):
    """Register a connection for tier tracking.

    Call this after creating a new connection to enable tier tracking on it.
    """
    _tier_tracker.register_connection(connection)


def get_tier_changes() -> list[tuple[str | None, str, list]]:
    """Get recorded tier changes."""
    return _tier_tracker.get_changes()


def has_tier_fallback() -> bool:
    """Check if any tier fallback occurred."""
    return _tier_tracker.has_fallback()


def clear_tier_changes():
    """Clear recorded tier changes."""
    _tier_tracker.clear()


def get_tier_change_report() -> str:
    """Generate a report of tier changes."""
    changes = _tier_tracker.get_changes()
    if not changes:
        return "No tier changes recorded."

    lines = ["Tier Changes Detected:"]
    for prev, new, pipeline in changes:
        prev_str = prev if prev else "None"
        lines.append(f"  {prev_str} -> {new}")
        # Truncate pipeline for display
        pipeline_str = (
            str(pipeline)[:80] + "..."
            if len(str(pipeline)) > 80
            else str(pipeline)
        )
        lines.append(f"    Pipeline: {pipeline_str}")

    return "\n".join(lines)
