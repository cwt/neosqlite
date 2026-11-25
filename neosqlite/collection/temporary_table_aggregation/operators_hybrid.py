"""Per-stage Python fallback that stays inside Tier 2.

When a single SQL stage translation declines an option or expression, the
whole pipeline used to drop to Tier 3. This mixin runs just that stage in
Python over the rows already materialized in a temporary table, writes the
results back to a new temporary table, and lets the remaining stages continue
in SQL.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from ...sql_utils import quote_table_name
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)

StageNameResolver = str | Callable[[dict[str, Any]], str]


def python_hybrid_fallback(
    stage_name: StageNameResolver,
):
    """Decorate a Tier-2 stage processor with a per-stage Python fallback.

    A ``NotImplementedError`` from the SQL translation is treated as "this
    stage needs Python", not "the whole pipeline needs Python". The decorated
    method's first three arguments must be ``(create_temp, current_table,
    spec)``.

    Args:
        stage_name: Stage name to rebuild, or a callable deriving it from the
            stage specification (used when one method serves two stages).

    Returns:
        Callable: A decorator wrapping the stage processor.
    """

    def decorator(method):
        @functools.wraps(method)
        def wrapper(
            self,
            create_temp: Callable,
            current_table: str,
            spec: Any,
            *args: Any,
            **kwargs: Any,
        ) -> str:
            try:
                return method(
                    self, create_temp, current_table, spec, *args, **kwargs
                )
            except NotImplementedError as exc:
                name = stage_name(spec) if callable(stage_name) else stage_name
                logger.warning(
                    "Tier-2 stage %s using Python hybrid: %s", name, exc
                )
                return self._process_stage_python_hybrid(
                    create_temp, current_table, {name: spec}
                )

        return wrapper

    return decorator


def _id_column_value(value: Any) -> Any:
    """Serialize an ``_id`` value the way the base collection column does."""
    from ...objectid import ObjectId
    from ..json_helpers import neosqlite_json_dumps

    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return str(value)
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (dict, list)):
        return neosqlite_json_dumps(value)
    return value


class OperatorsHybridMixin(OperatorsBaseMixin):
    """Materialize a single pipeline stage through the Python engine."""

    def _process_stage_python_hybrid(
        self,
        create_temp: Callable,
        current_table: str,
        stage: dict[str, Any],
    ) -> str:
        """Run ``stage`` in Python over current rows and rematerialize them.

        Args:
            create_temp: Callback creating the next temporary table.
            current_table: Temporary table holding the input rows.
            stage: Single aggregation stage to evaluate in Python.

        Returns:
            str: Name of the new temporary table with the stage's output.

        Raises:
            NotImplementedError: If no query engine is available (the
                caller then degrades the whole pipeline to Tier 3).
        """
        from ..json_helpers import neosqlite_json_dumps, neosqlite_json_loads
        from ..query_engine.python_aggregation_engine import (
            execute_python_aggregation,
        )

        if self.query_engine is None:
            raise NotImplementedError(
                "Python hybrid stage requires a query engine"
            )

        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}

        select_parts: list[str] = []
        if "id" in column_names:
            select_parts.append("id")
        if "_id" in column_names:
            select_parts.append("_id")
        select_parts.append(f"{self.jsonb.data_column()} AS data")
        rows = self.db.execute(
            f"SELECT {', '.join(select_parts)} FROM {current_table}"
        ).fetchall()

        docs: list[dict[str, Any]] = []
        for row in rows:
            doc = neosqlite_json_loads(row[-1])
            offset = 0
            underscore_id = None
            if "id" in column_names:
                offset += 1
            if "_id" in column_names:
                underscore_id = row[offset]
                offset += 1
            if (
                isinstance(doc, dict)
                and "_id" not in doc
                and underscore_id is not None
            ):
                doc["_id"] = underscore_id
            docs.append(doc)

        results = execute_python_aggregation(
            self.query_engine, [stage], session=None, initial_docs=docs
        )

        new_table = create_temp(
            stage, "SELECT 1 AS id, 1 AS _id, '{}' AS data WHERE 0"
        )
        rows_to_insert = [
            (
                row_id,
                _id_column_value(
                    doc.get("_id") if isinstance(doc, dict) else None
                ),
                neosqlite_json_dumps(doc),
            )
            for row_id, doc in enumerate(results, start=1)
        ]
        if rows_to_insert:
            self.db.executemany(
                f"INSERT INTO {new_table} (id, _id, data) VALUES (?, ?, ?)",
                rows_to_insert,
            )
        return new_table
