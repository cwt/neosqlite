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

from ..type_utils import canonical_value_key

_ORDER_SENSITIVE_KEYS = frozenset(
    {
        "$sort",
        "sortBy",
        "$addFields",
        "$set",
        "$project",
        "$setWindowFields",
        "$fill",
    }
)


class CacheEntry:
    """Single cache entry with hit statistics.

    Stores a cached SQL template string alongside its associated bound
    parameter payload and tracks how many cache hits the entry has served
    via ``__slots__`` for low memory overhead.
    """

    __slots__ = ("sql_template", "payload", "hit_count")

    def __init__(self, sql_template: str, payload: tuple[Any, ...]):
        """Initialize a cache entry with a SQL template and payload.

        Args:
            sql_template: Cached SQL query template string.
            payload: Bound parameter tuple corresponding 1:1 with the
                template placeholders.
        """
        self.sql_template = sql_template
        self.payload = payload
        self.hit_count = 0


class TranslationCache:
    """LRU cache for SQL translation templates with O(1) get/put operations.

    Uses ``OrderedDict`` for efficient LRU eviction: most recently used
    entries are moved to the end, while least recently used entries are
    evicted from the front once ``_max_size`` is reached.
    """

    DEFAULT_MAX_SIZE = 100

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE):
        """Initialize the translation cache.

        Args:
            max_size: Maximum number of entries to retain in the cache
                before LRU eviction. A value of ``0`` disables caching.
                Defaults to ``DEFAULT_MAX_SIZE``.
        """
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._miss_count = 0
        self._hit_count = 0

    def get(self, key: str) -> tuple[str, tuple[Any, ...]] | None:
        """Get a cached SQL template and payload by key.

        Updates hit/miss counters and promotes matched entries to the most
        recently used position in the ``OrderedDict``.

        Args:
            key: Canonicalized cache key produced by ``make_key()``.

        Returns:
            tuple[str, tuple[Any, ...]] | None: ``(sql_template, payload)``
            if caching is enabled and the key is present, or None on a miss.
        """
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
        """Store a SQL template in the cache with its associated payload.

        The payload is consumer-defined (e.g., the exact bound parameters for
        the template) and must correspond 1:1 with this key, which is
        guaranteed because ``make_key()`` canonicalizes literal values into
        the key. When the cache is at capacity, the least recently used entry
        is evicted. Does nothing if ``_max_size == 0``.

        Args:
            key: Canonicalized cache key for the pipeline.
            sql_template: Translated SQL query template string.
            payload: Bound parameter tuple associated with the template.
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
        """Create a cache key from the full pipeline content.

        Literal values are canonicalized (type-tagged) into the key so that a
        cache hit implies an identical pipeline. This is required for
        correctness because sort directions, limits, and other literals may be
        baked into the cached SQL template rather than bound as parameters.
        Field references (``$field``) are preserved verbatim and dicts are
        sorted by key.

        Args:
            pipeline: List of aggregation stage dictionaries.

        Returns:
            str: Pipe-delimited canonical cache key representing all stages
            in order.
        """
        key_parts = []
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]
            preserve_order = stage_name in _ORDER_SENSITIVE_KEYS
            key_parts.append(
                f"{stage_name}:{self._canonicalize(spec, preserve_order=preserve_order)}"
            )
        return "|".join(key_parts)

    def _canonicalize(
        self, value: Any, *, preserve_order: bool = False
    ) -> tuple:
        """Recursively convert a value into a hashable, type-tagged tuple.

        Delegates to :func:`canonical_value_key`, preserving key insertion order
        inside order-sensitive clauses defined by ``_ORDER_SENSITIVE_KEYS``.
        """
        return canonical_value_key(
            value,
            preserve_order=preserve_order,
            order_sensitive_keys=_ORDER_SENSITIVE_KEYS,
        )

    def _extract_structure(self, obj: Any) -> tuple:
        """Recursively convert a pipeline fragment into a hashable tuple.

        Delegates to ``_canonicalize()`` so both field references and literal
        values are preserved in a type-tagged tuple.

        Args:
            obj: Pipeline fragment or expression value to canonicalize.

        Returns:
            tuple: Hashable, type-tagged nested tuple for ``obj``.
        """
        return self._canonicalize(obj)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics and per-entry hit summaries.

        Returns:
            dict[str, Any]: Dictionary containing ``size``, ``max_size``,
            ``hits``, ``misses``, ``hit_rate``, ``total_accesses``, and
            ``entries`` (sorted by descending ``hit_count``).
        """
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
        """Resize the cache, evicting oldest entries if needed.

        Args:
            new_size: New maximum entry capacity for the cache.
        """
        self._max_size = new_size
        while len(self._cache) > new_size:
            self._cache.popitem(last=False)

    def evict(self, key: str) -> bool:
        """Evict a specific entry from the cache by key.

        Args:
            key: Cache key to remove.

        Returns:
            bool: True if the entry existed and was removed, False otherwise.
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def contains(self, key: str) -> bool:
        """Check if a key is currently present in the cache.

        Args:
            key: Cache key to look up.

        Returns:
            bool: True if the key is in the cache, False otherwise.
        """
        return key in self._cache

    def get_entry(self, key: str) -> dict | None:
        """Get detailed metadata for a specific cache entry.

        Args:
            key: Cache key to inspect.

        Returns:
            dict | None: Dictionary with ``key``, ``sql_template``,
            ``payload``, and ``hit_count``, or None if the key is absent.
        """
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
        """Extract ``hit_count`` from a cache item tuple for sorting.

        Args:
            item: ``(key, entry)`` pair from ``self._cache.items()``.

        Returns:
            int: Hit count recorded on the entry.
        """
        return item[1].hit_count

    def dump(self) -> list[dict]:
        """Dump all cache entries sorted by hit count for debugging.

        Returns:
            list[dict]: Entry dictionaries with ``key``, ``sql_preview``
            (truncated to 100 chars with newlines collapsed), ``payload``,
            and ``hit_count``, ordered by descending hits.
        """
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
        """Check whether caching is enabled (``_max_size > 0``).

        Returns:
            bool: True if ``_max_size > 0``, False otherwise.
        """
        return self._max_size > 0

    def __len__(self) -> int:
        """Return the number of entries currently stored in the cache.

        Returns:
            int: Current cache entry count.
        """
        return len(self._cache)
