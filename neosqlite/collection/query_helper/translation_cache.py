"""
Translation Cache for SQL Tier Aggregation.

This module provides caching for translated pipeline-to-SQL queries,
with O(1) LRU (Least Recently Used) eviction using OrderedDict.

The cache stores SQL templates (not results), allowing the same translated
SQL to be reused across multiple query executions with different parameters.
"""

from __future__ import annotations
from collections import OrderedDict
from typing import Any


class CacheEntry:
    """Single cache entry with hit statistics."""

    __slots__ = ("sql_template", "param_names", "hit_count")

    def __init__(self, sql_template: str, param_names: tuple[str, ...]):
        self.sql_template = sql_template
        self.param_names = param_names
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

    def get(self, key: str) -> tuple[str, tuple[str, ...]] | None:
        """Get cached SQL template by key. Returns (sql, param_names) or None."""
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
        return entry.sql_template, entry.param_names

    def put(
        self, key: str, sql_template: str, param_names: tuple[str, ...]
    ) -> None:
        """Store SQL template in cache with extracted parameter names."""
        if self._max_size == 0:
            return  # Cache disabled
        if key in self._cache:
            # Already exists, update and move to end (most recently used)
            entry = self._cache[key]
            entry.sql_template = sql_template
            entry.param_names = param_names
            self._cache.move_to_end(key)
            return

        # Evict if full (O(1) LRU: remove least recently used from front)
        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)

        # Add new entry at end (most recent)
        self._cache[key] = CacheEntry(sql_template, param_names)

    def make_key(self, pipeline: list[dict[str, Any]]) -> str:
        """
        Create a cache key from pipeline structure.

        Key includes: operator names + field names + nested operator names.

        Values like $sample.size, $limit, $skip are NOT included in the key because
        we now parameterize them in SQL (using ?) - the same cached SQL template can
        be reused with different parameter values.
        """
        key_parts = []
        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            spec = stage[stage_name]

            if isinstance(spec, str):
                # $unset: "field" or $project: "field"
                key_parts.append(f"{stage_name}:{spec}")
            elif isinstance(spec, list):
                # $unset: ["field1", "field2"] or $project: ["field1", "field2"]
                key_parts.append(f"{stage_name}:{tuple(sorted(spec))}")
            elif isinstance(spec, dict):
                # Use deep structural hashing: preserves field names but replaces
                # values with "?" placeholder to avoid cache collisions on different
                # field queries that happen to use the same operators (e.g., age.$gt vs score.$gt)
                nested_struct = self._extract_structure(spec)
                key_parts.append(f"{stage_name}:{nested_struct}")
            else:
                key_parts.append(stage_name)
        return "|".join(key_parts)

    def _extract_structure(self, obj: Any) -> Any:
        """
        Recursively convert pipeline dict into a hashable nested tuple.
        Replaces all terminal values with '?' placeholder to parameterize the key.
        Preserves field names, operators, and structure.
        """
        if isinstance(obj, dict):
            return tuple(
                (k, self._extract_structure(v)) for k, v in sorted(obj.items())
            )
        elif isinstance(obj, list):
            return tuple(self._extract_structure(item) for item in obj)
        else:
            # Replace actual values (ints, strings, booleans) with placeholder
            return "?"

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

        # Sort by hit count descending
        def get_hit_count(x: dict[str, Any]) -> int:
            return int(x["hit_count"])

        entries.sort(key=get_hit_count, reverse=True)

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
            "param_names": entry.param_names,
            "hit_count": entry.hit_count,
        }

    def dump(self) -> list[dict]:
        """Dump all cache entries for debugging."""
        return [
            {
                "key": key,
                "sql_preview": entry.sql_template[:100].replace("\n", " "),
                "param_names": entry.param_names,
                "hit_count": entry.hit_count,
            }
            for key, entry in sorted(
                self._cache.items(), key=lambda x: x[1].hit_count, reverse=True
            )
        ]

    def is_enabled(self) -> bool:
        """Check if cache is enabled."""
        return self._max_size > 0

    def __len__(self) -> int:
        """Return number of entries in cache."""
        return len(self._cache)
