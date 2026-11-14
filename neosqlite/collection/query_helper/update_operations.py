"""
Update operations for QueryHelper.

Dispatches between SQL-based and Python-based update paths.
The SQL implementation lives in _sql_updates.py (SqlUpdatesMixin).
"""

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..._sqlite import sqlite3
from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
)
from ..jsonb_support import json_data_column

logger = logging.getLogger(__name__)

from ...query_operators import (
    _field_exists,
    _get_nested_field,
)
from ._sql_updates import SqlUpdatesMixin
from .positional_update import (
    _apply_positional_update,
    _matches_filter,
    _set_nested_field,
)
from .utils import (
    _validate_inc_mul_field_value,
    get_force_fallback,
)


def _unset_nested_field(doc: dict[str, Any], field_path: str) -> None:
    """Remove a possibly dotted field path from a document in-place."""
    if "." not in field_path:
        doc.pop(field_path, None)
        return
    parts = field_path.split(".")
    cur: Any = doc
    for part in parts[:-1]:
        if not isinstance(cur, dict) or part not in cur:
            return
        cur = cur[part]
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def _get_field_for_update(doc: dict[str, Any], field_path: str) -> Any:
    """Read a top-level or dotted field for update validation/calculation."""
    if "." not in field_path:
        return doc.get(field_path)
    if _field_exists(field_path, doc):
        return _get_nested_field(field_path, doc)
    return None


def _set_field_for_update(
    doc: dict[str, Any], field_path: str, value: Any
) -> None:
    """Write a top-level or dotted field for Python update operations."""
    if "." not in field_path:
        doc[field_path] = value
    else:
        _set_nested_field(doc, field_path, value)


if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext


class UpdateOperationsMixin(SqlUpdatesMixin):
    """
    A mixin class providing update operations for QueryHelper.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self.jsonb.jsonb_supported
    - self.jsonb.json_function_prefix
    - self._build_simple_where_clause method
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _get_integer_id_for_oid: Any

    def _internal_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """
        Helper method for updating documents.

        Attempts to use SQL-based updates for simple operations, falling back to
        Python-based updates for complex operations.

        Args:
            doc_id (Any): The ID of the document to update (can be ObjectId, int, etc.).
            update_spec (dict[str, Any]): The update specification.
            original_doc (dict[str, Any]): The original document before the update.
            array_filters (list[dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (dict[str, Any], optional): The query filter for $ operator.

        Returns:
            tuple[dict[str, Any], bool]: The updated document and whether it was modified.
        """
        # Validate $inc and $mul operations before choosing implementation
        # This ensures consistent behavior between SQL and Python implementations
        for op, value in update_spec.items():
            if op in {"$inc", "$mul"}:
                for field_name in value.keys():
                    field_value = _get_field_for_update(
                        original_doc, field_name
                    )
                    _validate_inc_mul_field_value(field_name, field_value, op)

        # Respect the kill switch - force Python fallback if enabled
        if get_force_fallback():
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

        if doc_id != 0:
            sql_expr_res = self._build_set_data_expression(
                update_spec, array_filters
            )
            if sql_expr_res is not None:
                data_expr, expr_params, _ = sql_expr_res
                if data_expr == "data":
                    return deepcopy(original_doc), False
                int_doc_id = self._get_integer_id_for_oid(doc_id)
                tbl = quote_table_name(self.collection.name)
                try:
                    cursor = self.collection.db.execute(
                        f"UPDATE {tbl} SET data = {data_expr} WHERE id = ?",
                        expr_params + [int_doc_id],
                    )
                    if cursor.rowcount == 0:
                        raise RuntimeError(
                            f"No rows updated for doc_id {doc_id}"
                        )
                    data_col = json_data_column(self.jsonb.jsonb_supported)
                    row = self.collection.db.execute(
                        f"SELECT id, {data_col} as data FROM {tbl} WHERE id = ?",
                        (int_doc_id,),
                    ).fetchone()
                    if not row:
                        raise RuntimeError("Failed to fetch updated document")
                    updated_doc = self.collection._load(row[0], row[1])
                    return updated_doc, updated_doc != original_doc
                except (
                    sqlite3.IntegrityError,
                    MalformedQueryException,
                    ValueError,
                ):
                    raise
                except Exception as e:
                    logger.debug(
                        f"Unified SQL update failed: {e}. Falling back."
                    )

        # Legacy SQL path (only without array_filters) and Python fallback
        if not array_filters and self._can_use_sql_updates(
            update_spec, doc_id, original_doc
        ):
            # Use enhanced SQL update with json_insert/json_replace when possible
            try:
                updated_doc = self._perform_enhanced_sql_update(
                    doc_id, update_spec, original_doc
                )
                return updated_doc, updated_doc != original_doc
            except sqlite3.IntegrityError:
                raise
            except Exception as e:
                logger.debug(
                    f"Enhanced update failed: {e}. Falling back to standard SQL update."
                )
                try:
                    updated_doc = self._perform_sql_update(doc_id, update_spec)
                    return updated_doc, updated_doc != original_doc
                except sqlite3.IntegrityError:
                    raise
                except Exception as e2:
                    logger.debug(
                        f"Standard SQL update failed: {e2}. Falling back to Python update."
                    )
                    return self._perform_python_update(
                        doc_id,
                        update_spec,
                        original_doc,
                        array_filters,
                        query_filter,
                    )
        else:
            # Fall back to Python-based updates for complex operations
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

    def _perform_python_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """
        Perform update operations using Python-based logic.

        Args:
            doc_id (Any): The document ID of the document to update (can be ObjectId, int, etc.).
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to perform.
            original_doc (dict[str, Any]): The original document before applying
                                           the updates.
            array_filters (list[dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (dict[str, Any], optional): The query filter for $ operator.

        Returns:
            tuple[dict[str, Any], bool]: The updated document and whether it was modified.
        """
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            match op:
                case "$set":
                    # Handle positional operators in field paths
                    for k, v in value.items():
                        if "$" in k:
                            # Use positional update
                            _apply_positional_update(
                                doc_to_update, k, v, array_filters, query_filter
                            )
                        else:
                            _set_nested_field(doc_to_update, k, v)
                case "$unset":
                    for k in value:
                        _unset_nested_field(doc_to_update, k)
                case "$inc":
                    for k, v in value.items():
                        current_value = _get_field_for_update(doc_to_update, k)
                        _validate_inc_mul_field_value(k, current_value, "$inc")
                        new_val = (
                            current_value if current_value is not None else 0
                        ) + v
                        _set_field_for_update(doc_to_update, k, new_val)
                case "$push":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        if not exists:
                            _set_field_for_update(doc_to_update, k, [])
                        current_list = _get_field_for_update(doc_to_update, k)

                        # Check if v is a dict with modifiers ($each, $position, $slice)
                        if isinstance(v, dict) and "$each" in v:
                            values_to_add = v["$each"]
                            if not isinstance(values_to_add, list):
                                values_to_add = [values_to_add]

                            position = v.get("$position")
                            if position is not None:
                                for i, val in enumerate(values_to_add):
                                    current_list.insert(position + i, val)
                            else:
                                current_list.extend(values_to_add)

                            slice_val = v.get("$slice")
                            if slice_val is not None:
                                if slice_val == 0:
                                    _set_field_for_update(doc_to_update, k, [])
                                elif slice_val > 0:
                                    _set_field_for_update(
                                        doc_to_update,
                                        k,
                                        current_list[:slice_val],
                                    )
                                else:
                                    _set_field_for_update(
                                        doc_to_update,
                                        k,
                                        current_list[slice_val:],
                                    )
                        else:
                            current_list.append(v)
                case "$addToSet":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        if not exists:
                            _set_field_for_update(doc_to_update, k, [])
                        current_list = _get_field_for_update(doc_to_update, k)
                        values_to_add = []
                        if isinstance(v, dict) and "$each" in v:
                            each_values = v["$each"]
                            if not isinstance(each_values, list):
                                each_values = [each_values]
                            values_to_add = each_values
                        else:
                            values_to_add = [v]
                        for val in values_to_add:
                            if val not in current_list:
                                current_list.append(val)
                case "$pull":
                    for k, v in value.items():
                        if "." in k:
                            if _field_exists(k, doc_to_update):
                                arr = _get_nested_field(k, doc_to_update)
                                if isinstance(arr, list):
                                    new_arr = [
                                        item
                                        for item in arr
                                        if not _matches_filter(item, v)
                                    ]
                                    _set_nested_field(doc_to_update, k, new_arr)
                        else:
                            if k in doc_to_update and isinstance(
                                doc_to_update[k], list
                            ):
                                doc_to_update[k] = [
                                    item
                                    for item in doc_to_update[k]
                                    if not _matches_filter(item, v)
                                ]
                case "$pullAll":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        arr = (
                            _get_field_for_update(doc_to_update, k)
                            if exists
                            else None
                        )
                        if (
                            exists
                            and isinstance(v, (list, tuple))
                            and isinstance(arr, list)
                        ):
                            values_to_remove = list(v)
                            new_list = [
                                item
                                for item in arr
                                if item not in values_to_remove
                            ]
                            if new_list != arr:
                                _set_field_for_update(
                                    doc_to_update, k, new_list
                                )
                case "$pop":
                    for k, v in value.items():
                        arr = _get_field_for_update(doc_to_update, k)
                        if isinstance(arr, list) and arr:
                            if v == 1:
                                arr.pop()
                            elif v == -1:
                                arr.pop(0)
                case "$bit":
                    for k, bit_op in value.items():
                        if not isinstance(bit_op, dict):
                            raise MalformedQueryException(
                                "$bit operator requires a dict with 'and', 'or', or 'xor'"
                            )

                        current_val = _get_field_for_update(doc_to_update, k)
                        if current_val is None:
                            current_val = 0

                        if "and" in bit_op:
                            current_val &= bit_op["and"]
                        if "or" in bit_op:
                            current_val |= bit_op["or"]
                        if "xor" in bit_op:
                            current_val ^= bit_op["xor"]

                        _set_field_for_update(doc_to_update, k, current_val)
                case "$rename":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        if exists:
                            old_val = _get_field_for_update(doc_to_update, k)
                            _unset_nested_field(doc_to_update, k)
                            _set_field_for_update(doc_to_update, v, old_val)
                case "$mul":
                    for k, v in value.items():
                        current_value = _get_field_for_update(doc_to_update, k)
                        _validate_inc_mul_field_value(k, current_value, "$mul")
                        new_val = (
                            current_value if current_value is not None else 0
                        ) * v
                        _set_field_for_update(doc_to_update, k, new_val)
                case "$min":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        cur_val = (
                            _get_field_for_update(doc_to_update, k)
                            if exists
                            else None
                        )
                        if not exists or cur_val is None or cur_val > v:
                            _set_field_for_update(doc_to_update, k, v)
                case "$max":
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        cur_val = (
                            _get_field_for_update(doc_to_update, k)
                            if exists
                            else None
                        )
                        if not exists or cur_val is None or cur_val < v:
                            _set_field_for_update(doc_to_update, k, v)
                case "$currentDate":
                    for k, type_spec in value.items():
                        now_val = datetime.now(timezone.utc)
                        if "." in k:
                            _set_nested_field(doc_to_update, k, now_val)
                        else:
                            doc_to_update[k] = now_val
                case "$setOnInsert":
                    # Only apply on upsert (doc_id == 0)
                    if doc_id == 0:
                        for k, v in value.items():
                            _set_field_for_update(doc_to_update, k, v)
                case _:
                    raise MalformedQueryException(
                        f"Update operator '{op}' not supported"
                    )

        # If this is an upsert (doc_id == 0), we don't update the database
        # We just return the updated document for insertion by the caller
        if doc_id != 0:
            # Convert the doc_id to integer ID for internal operations
            int_doc_id = self._get_integer_id_for_oid(doc_id)
            self.collection.db.execute(
                f"UPDATE {quote_table_name(self.collection.name)} SET data = ? WHERE id = ?",
                (neosqlite_json_dumps(doc_to_update), int_doc_id),
            )

        # Check if document was actually modified
        was_modified = doc_to_update != original_doc

        return doc_to_update, was_modified
