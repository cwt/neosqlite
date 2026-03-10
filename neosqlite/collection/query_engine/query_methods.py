"""Query methods for the QueryEngine."""

from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client_session import ClientSession

import json

from .base import QueryEngineProtocol
from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from neosqlite.collection.json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)


class QueryMethodsMixin(QueryEngineProtocol):
    """Mixin class providing query methods for QueryEngine."""

    def count_documents(
        self, filter: Dict[str, Any], session: ClientSession | None = None
    ) -> int:
        """
        Return the count of documents that match the given filter.

        Args:
            filter (Dict[str, Any]): A dictionary specifying the query filter.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            int: The number of documents matching the filter.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)
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
        row = self.collection.db.execute(
            f"SELECT COUNT(1) FROM {quote_table_name(self.collection.name)}"
        ).fetchone()
        return row[0] if row else 0

    def distinct(
        self,
        key: str,
        filter: Dict[str, Any] | None = None,
        session: ClientSession | None = None,
    ) -> List[Any]:
        """
        Return a list of distinct values from the specified key in the documents
        of this collection, optionally filtered by a query.

        Args:
            key (str): The field name to extract distinct values from.
            filter (Dict[str, Any] | None): An optional query filter to apply to the documents.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            List[Any]: A list containing the distinct values from the specified key.
        """
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        params: List[Any] = []
        where_clause: str = ""

        if filter:
            # Try to use SQLTranslator for the WHERE clause
            result = self.sql_translator.translate_match(filter)
            if result[0] is not None:
                where_clause = result[0]
            params = result[1]

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
                match val:
                    case list():
                        results.add(tuple(val))
                    case dict():
                        results.add(neosqlite_json_dumps(val, sort_keys=True))
                    case _:
                        results.add(val)
            except (json.JSONDecodeError, TypeError):
                results.add(row[0])
        return list(results)
