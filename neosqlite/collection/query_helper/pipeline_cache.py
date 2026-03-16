"""
Pipeline Translation Cache for SQL Tier Aggregation.

This module provides caching for translated pipeline-to-SQL queries,
with hit-rate-based eviction for optimal memory usage.
"""

from __future__ import annotations
from typing import Any


class CacheEntry:
    """Single cache entry with hit statistics."""

    __slots__ = ("sql_template", "param_names", "hit_count", "last_hit")

    def __init__(self, sql_template: str, param_names: tuple[str, ...]):
        self.sql_template = sql_template
        self.param_names = param_names
        self.hit_count = 0
        self.last_hit = 0


class PipelineCache:
    """
    LFU-inspired cache with hit rate tracking and automatic eviction.

    When cache is full, entries with lowest hit rate are evicted.
    Hit rate = hit_count / (last_hit_time - creation_time), normalized.
    """

    DEFAULT_MAX_SIZE = 100
    MIN_HITS_TO_EVICT = 5  # Minimum hits before considering for eviction

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE):
        self._cache: dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._miss_count = 0
        self._hit_count = 0
        self._access_counter = 0  # For tie-breaking in eviction

    def get(self, key: str) -> tuple[str, tuple[str, ...]] | None:
        """Get cached SQL template by key. Returns (sql, param_names) or None."""
        if self._max_size == 0:
            self._miss_count += 1
            return None
        entry = self._cache.get(key)
        if entry is None:
            self._miss_count += 1
            return None

        # Update hit statistics
        self._access_counter += 1
        entry.hit_count += 1
        entry.last_hit = self._access_counter
        self._hit_count += 1
        return entry.sql_template, entry.param_names

    def put(
        self, key: str, sql_template: str, param_names: tuple[str, ...]
    ) -> None:
        """Store SQL template in cache with extracted parameter names."""
        if self._max_size == 0:
            return  # Cache disabled
        if key in self._cache:
            # Already exists, update
            entry = self._cache[key]
            entry.sql_template = sql_template
            entry.param_names = param_names
            return

        # Evict if full
        if len(self._cache) >= self._max_size:
            self._evict_lowest_hit_rate()

        # Add new entry
        self._cache[key] = CacheEntry(sql_template, param_names)

    def _evict_lowest_hit_rate(self) -> None:
        """Evict entry with lowest hit rate."""
        if not self._cache:
            return

        # Find entry with lowest hit rate (hit_count / age)
        # Use raw hit_count for entries with fewer than MIN_HITS_TO_EVICT
        worst_key = None
        worst_score = float("inf")

        current_time = self._access_counter

        for key, entry in self._cache.items():
            if entry.hit_count < self.MIN_HITS_TO_EVICT:
                # Prefer to keep entries with few hits (might become popular)
                score = (
                    entry.hit_count / (current_time - entry.last_hit + 1) * 0.5
                )
            else:
                # Higher hit count = higher score (keep)
                # More recent = higher score (temporal locality)
                age = current_time - entry.last_hit
                score = entry.hit_count / (age + 1)

            if score < worst_score:
                worst_score = score
                worst_key = key

        if worst_key is not None:
            del self._cache[worst_key]

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
                # For dict specs, we need to extract nested operator names too
                # especially for $setWindowFields where output contains operators
                nested_ops = self._extract_nested_operators(spec)
                if nested_ops:
                    key_parts.append(f"{stage_name}:{nested_ops}")
                else:
                    key_parts.append(
                        f"{stage_name}:{tuple(sorted(spec.keys()))}"
                    )
            else:
                key_parts.append(stage_name)
        return "|".join(key_parts)

    def _extract_nested_operators(self, d: dict) -> tuple | None:
        """Recursively extract operator names from nested dict structure."""
        operators = []

        def recurse(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key.startswith("$"):
                        operators.append(key)
                    recurse(value)
            elif isinstance(obj, list):
                for item in obj:
                    recurse(item)

        recurse(d)

        if operators:
            return tuple(sorted(operators))
        return None

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
        self._access_counter = 0

    def resize(self, new_size: int) -> None:
        """Resize cache, evicting entries if needed."""
        self._max_size = new_size
        while len(self._cache) > new_size:
            self._evict_lowest_hit_rate()

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
            "last_hit": entry.last_hit,
        }

    def dump(self) -> list[dict]:
        """Dump all cache entries for debugging."""
        return [
            {
                "key": key,
                "sql_preview": entry.sql_template[:100].replace("\n", " "),
                "param_names": entry.param_names,
                "hit_count": entry.hit_count,
                "last_hit": entry.last_hit,
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
