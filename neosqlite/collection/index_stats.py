"""MongoDB-compatible ``$indexStats`` documents for NeoSQLite collections.

NeoSQLite does not track per-index access counters or an index creation
timestamp, so ``accesses.ops`` is always ``0``, ``accesses.since`` is the time
the stats were requested, and ``host`` is a stable local placeholder. The
remaining fields (index name, key pattern, and spec) mirror MongoDB's output
so the stage is useful for schema introspection and differential testing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..sql_utils import validate_identifier
from .index_manager import _load_custom_index_name

if TYPE_CHECKING:
    from .collection import Collection

_LOCAL_HOST = "neosqlite:27017"


def _mongo_index_name(key: dict[str, Any]) -> str:
    """Build MongoDB's default index name for a key pattern."""
    return "_".join(f"{field}_{direction}" for field, direction in key.items())


def _stats_entry(
    *,
    name: str,
    key: dict[str, Any],
    since: datetime,
    unique: bool = False,
    include_unique_in_spec: bool = True,
) -> dict[str, Any]:
    """Assemble one ``$indexStats`` result document in MongoDB field order."""
    spec: dict[str, Any] = {"v": 2}
    if unique and include_unique_in_spec:
        spec["unique"] = True
    spec["key"] = dict(key)
    spec["name"] = name
    return {
        "name": name,
        "key": dict(key),
        "host": _LOCAL_HOST,
        "accesses": {"ops": 0, "since": since},
        "spec": spec,
    }


def build_index_stats_documents(collection: Collection) -> list[dict[str, Any]]:
    """Return MongoDB-shaped ``$indexStats`` documents for ``collection``.

    The automatic ``_id_`` index is included, matching MongoDB (NeoSQLite's
    ``index_information()`` hides it, unlike ``$indexStats``).

    Args:
        collection: Collection whose indexes should be reported.

    Returns:
        list[dict[str, Any]]: One document per index.
    """
    since = datetime.now(UTC)
    results: list[dict[str, Any]] = [
        _stats_entry(
            name="_id_",
            key={"_id": 1},
            since=since,
            include_unique_in_spec=False,
        )
    ]

    coll_name = validate_identifier(collection.name)
    auto_prefix = f"idx_{coll_name}_"
    for idx_name, info in collection.index_information().items():
        key = info.get("key")
        if not isinstance(key, dict) or not key:
            continue
        custom_name = _load_custom_index_name(collection.db, idx_name)
        if custom_name:
            stat_name = custom_name
        elif not idx_name.startswith(auto_prefix):
            stat_name = idx_name
        else:
            stat_name = _mongo_index_name(key)
        results.append(
            _stats_entry(
                name=stat_name,
                key=key,
                since=since,
                unique=bool(info.get("unique")),
            )
        )

    return results
