"""Query methods for the QueryEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client_session import ClientSession

import json

from neosqlite.collection.json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)

from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from ..query_helper import get_force_fallback
from ..type_utils import _hashable_group_key, validate_session
from .base import QueryEngineProtocol


def _distinct_dedup_key(val: Any) -> Any:
    """Return a hashable BSON-type-safe deduplication key for ``distinct``."""
    if isinstance(val, bool):
        return ("b", val)
    if isinstance(val, (int, float)):
        return ("n", val)
    return _hashable_group_key(val)


def _add_distinct_value(val: Any, seen: set[Any], results: list[Any]) -> None:
    """Append ``val`` (or canonical JSON for dict/list) to ``results`` if unseen."""
    item = (
        neosqlite_json_dumps(val, sort_keys=True)
        if isinstance(val, (dict, list))
        else val
    )
    key = _distinct_dedup_key(item)
    if key not in seen:
        seen.add(key)
        results.append(item)


def _collect_distinct_values(
    val: Any,
    results: set[Any] | list[Any],
    seen: set[Any] | None = None,
) -> None:
    """Collect distinct scalar or canonical JSON values into ``results``.

    Ignores ``None``. Flattens top-level lists element-wise per MongoDB
    ``distinct`` semantics and canonicalizes nested dicts or lists with
    ``neosqlite_json_dumps(..., sort_keys=True)`` so they are hashable.

    Args:
        val: Extracted field value to add to ``results``.
        results: Mutable container accumulating distinct values.
        seen: Optional set tracking ``_distinct_dedup_key`` values when
            ``results`` is a ``list`` (preserving ``bool`` vs ``int``).
    """
    if val is None:
        return
    items = val if isinstance(val, list) else [val]
    if isinstance(results, list) and seen is not None:
        for item in items:
            _add_distinct_value(item, seen, results)
    else:
        assert isinstance(results, set)
        for item in items:
            if isinstance(item, (dict, list)):
                results.add(neosqlite_json_dumps(item, sort_keys=True))
            else:
                results.add(item)


def _extract_distinct_field_values(doc: Any, key: str) -> list[Any]:
    """Extract values for a dotted field path, traversing dicts and arrays.

    Resolves each dot-separated segment across dictionaries, integer list
    indices, and arrays of subdocuments.

    Args:
        doc: Source document or nested value to traverse.
        key: Field name or dot-separated path (e.g., ``"a.b.0.c"``).

    Returns:
        list[Any]: All matched values along the path, or an empty list if
        the path is not found.
    """
    current_values: list[Any] = [doc]
    for part in key.split("."):
        next_values: list[Any] = []
        for current in current_values:
            if isinstance(current, dict):
                if part in current:
                    next_values.append(current[part])
            elif isinstance(current, list):
                if part.isdigit():
                    idx = int(part)
                    if 0 <= idx < len(current):
                        next_values.append(current[idx])
                else:
                    next_values.extend(
                        elem[part]
                        for elem in current
                        if isinstance(elem, dict) and part in elem
                    )
        if not next_values:
            return []
        current_values = next_values
    return current_values


class QueryMethodsMixin(QueryEngineProtocol):
    """Mixin class providing query methods for QueryEngine.

    Implements document counting and distinct value queries using Tier-1
    SQL translation when possible, falling back to Python execution when
    forced or when a filter cannot be translated to SQL.
    """

    def count_documents(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> int:
        """Return the count of documents that match the given filter.

        Normalizes ``_id``/``id`` predicates and attempts Tier-1 SQL
        ``SELECT COUNT(id)`` execution unless fallback is forced or the
        filter cannot be translated, in which case it counts results from
        ``self.find()``.

        Args:
            filter: Dictionary specifying the query filter.
            session: Optional ``ClientSession`` for transaction validation.
                Defaults to None.

        Returns:
            int: Number of documents matching ``filter``.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        if not get_force_fallback():
            # Try to use SQLTranslator for the WHERE clause
            where_clause, params = self.sql_translator.translate_match(filter)
            if where_clause is not None:
                self._notify_tier_change("tier1", [filter])
                cmd = f"SELECT COUNT(id) FROM {quote_table_name(self.collection.name)} {where_clause}"
                row = self.collection.db.execute(cmd, params).fetchone()
                return row[0] if row else 0
        return len(list(self.find(filter, session=session)))

    def estimated_document_count(
        self, session: ClientSession | None = None
    ) -> int:
        """Return the estimated number of documents in the collection.

        Uses ``SELECT COUNT(1)`` in Tier-1 SQL mode, or iterates
        ``self.find()`` when fallback is forced.

        Args:
            session: Optional ``ClientSession`` for transaction validation.
                Defaults to None.

        Returns:
            int: Estimated number of documents in the collection.
        """
        validate_session(session, self.collection._database)
        if get_force_fallback():
            return sum(1 for _ in self.find(session=session))
        self._notify_tier_change("tier1", [{}])
        row = self.collection.db.execute(
            f"SELECT COUNT(1) FROM {quote_table_name(self.collection.name)}"
        ).fetchone()
        return row[0] if row else 0

    def distinct(
        self,
        key: str,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> list[Any]:
        """Return a list of distinct values for a field across matching docs.

        Uses SQLite ``SELECT DISTINCT json_extract(...)`` in Tier-1 SQL mode
        (always using ``json_*`` functions to avoid binary JSONB output
        issues), or delegates to ``_distinct_python()`` when fallback is
        forced or the filter cannot be translated to SQL.

        Args:
            key: Field name or dotted path to collect distinct values from.
            filter: Optional query filter to restrict examined documents.
                Defaults to None.
            session: Optional ``ClientSession`` for transaction validation.
                Defaults to None.

        Returns:
            list[Any]: List of distinct values found for ``key``.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)

        if get_force_fallback() or "." in key:
            return self._distinct_python(key, filter, session)

        params: list[Any] = []
        where_clause: str = ""

        if filter:
            # Try to use SQLTranslator for the WHERE clause
            where_result = self.sql_translator.translate_match(filter)
            if where_result[0] is None:
                return self._distinct_python(key, filter, session)
            where_clause = where_result[0]
            params = where_result[1]

        # For distinct operations, always use json_* functions to avoid binary data issues
        # Even if JSONB is supported, we use json_* for distinct to ensure proper text output
        func_prefix = "json"
        self._notify_tier_change("tier1", [filter or {}])

        table_name = quote_table_name(self.collection.name)
        if key == "_id":
            id_col = f"{table_name}.{self.collection._id_column}"
            select_expr = (
                f"CASE WHEN {func_prefix}_type(data, '$._id') IS NOT NULL "
                f"THEN {func_prefix}_extract(data, '$._id') ELSE {id_col} END"
            )
            type_expr = f"{func_prefix}_type(data, '$._id')"
        else:
            parsed_path = parse_json_path(key)
            select_expr = f"{func_prefix}_extract(data, '{parsed_path}')"
            type_expr = f"{func_prefix}_type(data, '{parsed_path}')"

        cmd = f"SELECT DISTINCT {select_expr}, {type_expr} FROM {table_name} {where_clause}"
        cursor = self.collection.db.execute(cmd, params)
        seen: set[Any] = set()
        results: list[Any] = []
        for row in cursor.fetchall():
            raw_val, row_type = row[0], row[1]
            if raw_val is None or row_type == "null":
                continue
            if row_type == "true":
                _add_distinct_value(True, seen, results)
                continue
            if row_type == "false":
                _add_distinct_value(False, seen, results)
                continue
            if row_type in ("object", "array") or (
                row_type is None
                and isinstance(raw_val, str)
                and (
                    (raw_val.startswith("{") and raw_val.endswith("}"))
                    or (raw_val.startswith("[") and raw_val.endswith("]"))
                )
            ):
                try:
                    val = neosqlite_json_loads(raw_val)
                    _collect_distinct_values(val, results, seen=seen)
                    continue
                except (json.JSONDecodeError, TypeError):
                    pass
            _add_distinct_value(raw_val, seen, results)
        return results

    def _distinct_python(
        self,
        key: str,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> list[Any]:
        """Execute distinct in Python Tier 3 as the authoritative reference.

        Iterates documents from ``self.find()``, extracts values via
        ``_extract_distinct_field_values()``, and collects unique values
        with ``_collect_distinct_values()``.

        Args:
            key: Field name or dotted path to collect distinct values from.
            filter: Optional query filter dictionary. Defaults to None.
            session: Optional ``ClientSession`` passed to ``self.find()``.
                Defaults to None.

        Returns:
            list[Any]: List of distinct values found for ``key``.
        """
        seen: set[Any] = set()
        results: list[Any] = []
        for doc in self.find(filter or {}, session=session):
            for val in _extract_distinct_field_values(doc, key):
                _collect_distinct_values(val, results, seen=seen)
        self._notify_tier_change("tier3", [filter or {}])
        return results
