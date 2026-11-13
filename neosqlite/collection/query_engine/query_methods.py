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
from ..type_utils import validate_session
from .base import QueryEngineProtocol


def _collect_distinct_values(val: Any, results: set[Any]) -> None:
    """Collect distinct scalar/canonical JSON values into results set."""
    if val is None:
        return
    match val:
        case list():
            # MongoDB distinct flattens arrays and returns each
            # element (#165); documents canonicalize via JSON
            for item in val:
                if isinstance(item, (dict, list)):
                    results.add(neosqlite_json_dumps(item, sort_keys=True))
                else:
                    results.add(item)
        case dict():
            results.add(neosqlite_json_dumps(val, sort_keys=True))
        case _:
            results.add(val)


def _extract_distinct_field_values(doc: Any, key: str) -> list[Any]:
    """Extract values for a dotted field path, traversing dicts and arrays."""
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
                    for elem in current:
                        if isinstance(elem, dict) and part in elem:
                            next_values.append(elem[part])
        if not next_values:
            return []
        current_values = next_values
    return current_values


class QueryMethodsMixin(QueryEngineProtocol):
    """Mixin class providing query methods for QueryEngine."""

    def count_documents(
        self, filter: dict[str, Any], session: ClientSession | None = None
    ) -> int:
        """
        Return the count of documents that match the given filter.

        Args:
            filter (dict[str, Any]): A dictionary specifying the query filter.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            int: The number of documents matching the filter.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
        if not get_force_fallback():
            # Try to use SQLTranslator for the WHERE clause
            where_clause, params = self.sql_translator.translate_match(filter)
            if where_clause is not None:
                cmd = f"SELECT COUNT(id) FROM {quote_table_name(self.collection.name)} {where_clause}"
                row = self.collection.db.execute(cmd, params).fetchone()
                return row[0] if row else 0
        return len(list(self.find(filter, session=session)))

    def estimated_document_count(
        self, session: ClientSession | None = None
    ) -> int:
        """
        Return the estimated number of documents in the collection.

        Args:
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            int: The estimated number of documents.
        """
        validate_session(session, self.collection._database)
        if get_force_fallback():
            return sum(1 for _ in self.find(session=session))
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
        """
        Return a list of distinct values from the specified key in the documents
        of this collection, optionally filtered by a query.

        Args:
            key (str): The field name to extract distinct values from.
            filter (dict[str, Any] | None): An optional query filter to apply to the documents.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            list[Any]: A list containing the distinct values from the specified key.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)

        if get_force_fallback():
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

        cmd = (
            f"SELECT DISTINCT {func_prefix}_extract(data, '{parse_json_path(key)}') "
            f"FROM {quote_table_name(self.collection.name)} {where_clause}"
        )
        cursor = self.collection.db.execute(cmd, params)
        results: set[Any] = set()
        for row in cursor.fetchall():
            if row[0] is None:
                continue
            try:
                val = neosqlite_json_loads(row[0])
                _collect_distinct_values(val, results)
            except (json.JSONDecodeError, TypeError):
                results.add(row[0])
        return list(results)

    def _distinct_python(
        self,
        key: str,
        filter: dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> list[Any]:
        """Execute distinct in Python Tier 3 as the authoritative reference."""
        results: set[Any] = set()
        for doc in self.find(filter or {}, session=session):
            for val in _extract_distinct_field_values(doc, key):
                _collect_distinct_values(val, results)
        return list(results)
