"""
Translation Cache for SQL Tier Aggregation.

This module provides caching for translated pipeline-to-SQL queries,
with O(1) LRU (Least Recently Used) eviction using OrderedDict.

The cache stores SQL templates together with their exact bound parameters.
Cache keys canonicalize the full pipeline content (including literal
values), so a cache hit always corresponds to an identical pipeline and the
stored parameters are guaranteed to match the template's placeholders.
"""

from __future__ import annotations

from collections import OrderedDict
from operator import itemgetter
from typing import Any


class CacheEntry:
    """Single cache entry with hit statistics."""

    __slots__ = ("sql_template", "payload", "hit_count")

    def __init__(self, sql_template: str, payload: tuple[Any, ...]):
        self.sql_template = sql_template
        self.payload = payload
        self.hit_count = 0


class TranslationCache:
    """
    LRU cache for SQL translation templates with O(1) get/put operations.

    Uses OrderedDict for efficient LRU eviction: most recently used entries
    are moved to the end, least recently used are evicted from the front.
    """

    DEFAULT_MAX_SIZE = 100

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._miss_count = 0
        self._hit_count = 0

    def get(self, key: str) -> tuple[str, tuple[Any, ...]] | None:
        """Get cached SQL template by key. Returns (sql, payload) or None."""
        if self._max_size == 0:
            self._miss_count += 1
            return None
        entry = self._cache.get(key)
        if entry is None:
            self._miss_count += 1
            return None

        # Move to end (most recently used) for LRU
        self._cache.move_to_end(key)
        entry.hit_count += 1
        self._hit_count += 1
        return entry.sql_template, entry.payload

    def put(
        self, key: str, sql_template: str, payload: tuple[Any, ...]
    ) -> None:
        """Store SQL template in cache with its associated payload.

        The payload is consumer-defined (e.g. the exact bound parameters for
        the template). It must correspond 1:1 with this exact key, which is
        guaranteed because ``make_key`` canonicalizes literal values into the
        key — a cache hit therefore implies an identical pipeline.
        """
        if self._max_size == 0:
            return  # Cache disabled
        if key in self._cache:
            # Already exists, update and move to end (most recently used)
            entry = self._cache[key]
            entry.sql_template = sql_template
            entry.payload = payload
            self._cache.move_to_end(key)
            return

        # Evict if full (O(1) LRU: remove least recently used from front)
        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)

        # Add new entry at end (most recent)
        self._cache[key] = CacheEntry(sql_template, payload)

    def make_key(self, pipeline: list[dict[str, Any]]) -> str:
        """
        Create a cache key from the full pipeline content.

        Literal values are canonicalized (type-tagged) into the key so that a
        cache hit implies an identical pipeline. This is required for
        correctness: sort directions, limits and other literals may be baked
        into the cached SQL template rather than bound as parameters.

        Field references ($field) are preserved verbatim; dicts are sorted
        for order-insensitivity where ordering is not semantic.
        """
        key_parts = []
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]
            key_parts.append(f"{stage_name}:{self._canonicalize(spec)}")
        return "|".join(key_parts)

    def _canonicalize(self, value: Any) -> tuple:
        """
        Recursively convert a value into a hashable, type-tagged structure.

        Type tags distinguish values that Python would otherwise equate
        (e.g. True vs 1, 1 vs 1.0) so that semantically different pipelines
        never share a cache entry. Unhashable/exotic objects (ObjectId,
        datetime, bytes, ...) are represented via their type name and repr,
        which is stable for equal instances.
        """
        match value:
            case bool():
                return ("b", value)
            case int():
                return ("i", value)
            case float():
                return ("f", repr(value))
            case str():
                return ("s", value)
            case None:
                return ("z",)
            case dict():
                return (
                    "d",
                    tuple(
                        (k, self._canonicalize(v))
                        for k, v in sorted(value.items())
                    ),
                )
            case list() | tuple():
                return ("l", tuple(self._canonicalize(v) for v in value))
            case bytes():
                return ("y", value.hex())
            case _:
                return ("o", type(value).__name__, str(value))

    def _extract_structure(self, obj: Any) -> tuple:
        """
        Recursively convert a pipeline fragment into a hashable nested tuple.

        Preserves both field references ($field) and literal values so that
        structurally identical but semantically different fragments (e.g.
        differing sort directions or comparison bounds) map to distinct keys.
        """
        return self._canonicalize(obj)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hit_count + self._miss_count
        hit_rate = self._hit_count / total if total > 0 else 0.0

        entries: list[dict[str, Any]] = []
        for key, entry in self._cache.items():
            entries.append(
                {
                    "key": key[:50] + "..." if len(key) > 50 else key,
                    "hit_count": entry.hit_count,
                    "sql_preview": (
                        entry.sql_template[:60].replace("\n", " ") + "..."
                        if len(entry.sql_template) > 60
                        else entry.sql_template
                    ),
                }
            )

        entries.sort(key=itemgetter("hit_count"), reverse=True)

        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hit_count,
            "misses": self._miss_count,
            "hit_rate": hit_rate,
            "total_accesses": total,
            "entries": entries,
        }

    def clear(self) -> None:
        """Clear the cache and reset statistics."""
        self._cache.clear()
        self._miss_count = 0
        self._hit_count = 0

    def resize(self, new_size: int) -> None:
        """Resize cache, evicting entries if needed."""
        self._max_size = new_size
        while len(self._cache) > new_size:
            self._cache.popitem(last=False)

    def evict(self, key: str) -> bool:
        """Evict a specific entry by key. Returns True if evicted."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def contains(self, key: str) -> bool:
        """Check if a key is in the cache."""
        return key in self._cache

    def get_entry(self, key: str) -> dict | None:
        """Get detailed info about a specific cache entry."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        return {
            "key": key,
            "sql_template": entry.sql_template,
            "payload": entry.payload,
            "hit_count": entry.hit_count,
        }

    def _get_entry_hit_count(self, item: tuple[str, CacheEntry]) -> int:
        """Helper to extract hit_count from cache entry for sorting."""
        return item[1].hit_count

    def dump(self) -> list[dict]:
        """Dump all cache entries for debugging."""
        sorted_items = sorted(
            self._cache.items(), key=self._get_entry_hit_count, reverse=True
        )
        return [
            {
                "key": key,
                "sql_preview": entry.sql_template[:100].replace("\n", " "),
                "payload": entry.payload,
                "hit_count": entry.hit_count,
            }
            for key, entry in sorted_items
        ]

    def is_enabled(self) -> bool:
        """Check if cache is enabled."""
        return self._max_size > 0

    def __len__(self) -> int:
        """Return number of entries in cache."""
        return len(self._cache)
