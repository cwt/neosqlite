"""
Update operations for QueryHelper.

Dispatches between SQL-based and Python-based update paths.
The SQL implementation lives in _sql_updates.py (SqlUpdatesMixin).
"""

import logging
from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..._sqlite import sqlite3
from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
)

logger = logging.getLogger(__name__)

from ...query_operators import (
    _bson_values_equal,
    _field_exists,
    _get_nested_field,
)
from ..type_utils import bson_sort_key
from ._sql_updates import SqlUpdatesMixin
from .positional_update import (
    _apply_positional_update,
    _matches_filter,
    _set_nested_field,
)
from .utils import (
    _validate_bit_field_value,
    _validate_bit_operand,
    _validate_inc_mul_field_value,
    _validate_push_spec,
    get_force_fallback,
)


def _apply_push_sort(arr: list[Any], sort_spec: Any) -> None:
    """Sort an array in place according to a MongoDB ``$push`` ``$sort`` spec."""
    if (
        isinstance(sort_spec, int)
        and not isinstance(sort_spec, bool)
        and sort_spec in (1, -1)
    ):

        def elem_key(item: Any) -> Any:
            return bson_sort_key(item)

        arr.sort(key=elem_key, reverse=(sort_spec == -1))
        return
    if isinstance(sort_spec, dict) and sort_spec:
        for field_key in reversed(list(sort_spec.keys())):
            direction = sort_spec[field_key]
            if (
                not isinstance(direction, int)
                or isinstance(direction, bool)
                or direction not in (1, -1)
            ):
                raise MalformedQueryException(
                    f"Invalid $sort direction {direction!r} in $push"
                )

            def sub_key(item: Any, fk: str = field_key) -> Any:
                val = (
                    _get_nested_field(fk, item)
                    if isinstance(item, dict)
                    else None
                )
                return bson_sort_key(val)

            arr.sort(key=sub_key, reverse=(direction == -1))
        return
    raise MalformedQueryException(
        f"Invalid $sort specification {sort_spec!r} in $push"
    )


def _unset_nested_field(doc: dict[str, Any], field_path: str) -> None:
    """Remove a top-level or dotted field path from a document in place.

    Traverses intermediate dictionaries or numeric list indices along
    ``field_path`` when dotted and removes the leaf key (or sets list slots to
    ``None``), returning silently if any intermediate segment is missing.

    Args:
        doc: Document dictionary to mutate in place.
        field_path: Top-level field name or dot-separated nested path.
    """
    if "." not in field_path:
        doc.pop(field_path, None)
        return
    parts = field_path.split(".")
    cur: Any = doc
    for part in parts[:-1]:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isascii() and part.isdigit():
            idx = int(part)
            if 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return
        else:
            return
    last = parts[-1]
    if isinstance(cur, dict):
        cur.pop(last, None)
    elif isinstance(cur, list) and last.isascii() and last.isdigit():
        idx = int(last)
        if 0 <= idx < len(cur):
            cur[idx] = None


def _get_field_for_update(doc: dict[str, Any], field_path: str) -> Any:
    """Read a top-level or dotted field value for update calculations.

    Args:
        doc: Source document dictionary.
        field_path: Top-level field name or dot-separated nested path.

    Returns:
        Any: Field value if found, or ``None`` if absent.
    """
    if "." not in field_path:
        return doc.get(field_path)
    if _field_exists(field_path, doc):
        return _get_nested_field(field_path, doc)
    return None


def _set_field_for_update(
    doc: dict[str, Any], field_path: str, value: Any
) -> None:
    """Write a top-level or dotted field for Python update operations.

    Sets top-level keys directly and delegates dotted paths to
    :func:`_set_nested_field`.

    Args:
        doc: Document dictionary to update in place.
        field_path: Top-level field name or dot-separated nested path.
        value: Value to assign at ``field_path``.
    """
    if "." not in field_path:
        doc[field_path] = value
    else:
        _set_nested_field(doc, field_path, value)


if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext


class UpdateOperationsMixin(SqlUpdatesMixin):
    """Mixin class providing document update operations for QueryHelper.

    Dispatches updates across the unified SQL expression path, legacy SQL
    update paths, and the Python fallback implementation. Expects subclasses to
    provide ``collection``, ``jsonb``, and ``_get_integer_id_for_oid``, and
    inherits SQL update helpers from :class:`SqlUpdatesMixin`.
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
        """Apply an update specification using SQL or Python fallback paths.

        Pre-validates ``$inc`` and ``$mul`` operands, honors
        ``get_force_fallback()``, attempts unified SQL expression updates when
        ``doc_id != 0``, then falls back to legacy enhanced/standard SQL paths
        (when ``array_filters`` is empty) or Python-based evaluation.

        Args:
            doc_id: Document identifier (for example, ``ObjectId`` or ``int``),
                or ``0`` for an in-memory upsert document.
            update_spec: Mapping of MongoDB update operators to operands.
            original_doc: Original document snapshot before applying updates.
            array_filters: Optional filter documents for positional ``$[...]``
                array updates.
            query_filter: Optional query filter used to match positional
                ``$`` array elements.

        Returns:
            tuple[dict[str, Any], bool]: Pair ``(updated_doc, was_modified)``
                indicating whether the document changed.
        """
        # Validate $inc, $mul, $bit, and $push modifiers before choosing implementation
        # This ensures consistent behavior between SQL and Python implementations
        for op, value in update_spec.items():
            if op in {"$inc", "$mul"}:
                for field_name in value.keys():
                    if "$" in field_name:
                        continue
                    field_value = _get_field_for_update(
                        original_doc, field_name
                    )
                    _validate_inc_mul_field_value(field_name, field_value, op)
            elif op == "$bit" and isinstance(value, dict):
                for field_name, bit_op in value.items():
                    _validate_bit_operand(bit_op)
                    if "$" not in field_name:
                        field_value = _get_field_for_update(
                            original_doc, field_name
                        )
                        _validate_bit_field_value(field_name, field_value)
            elif op == "$push":
                _validate_push_spec(value)

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
                    data_col = self.jsonb.data_column()
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
        """Apply update operators in Python and persist changes when needed.

        Evaluates ``$set`` (including positional operators), ``$unset``,
        ``$inc``, ``$push`` (with ``$each``, ``$position``, ``$sort``,
        ``$slice``), ``$addToSet`` (with ``$each``), ``$pull``, ``$pullAll``,
        ``$pop``, ``$bit``, ``$rename``, ``$mul``, ``$min``, ``$max``,
        ``$currentDate``, and ``$setOnInsert`` (for ``doc_id == 0``) on a deep
        copy of ``original_doc``. Writes the updated JSON back to SQLite when
        ``doc_id != 0``.

        Args:
            doc_id: Document identifier (``ObjectId``, ``int``, etc.), or
                ``0`` when constructing a document for upsert insertion.
            update_spec: Mapping of MongoDB update operators to specifications.
            original_doc: Original document prior to applying updates.
            array_filters: Optional filter documents for positional array
                updates.
            query_filter: Optional query filter for positional ``$`` matching.

        Returns:
            tuple[dict[str, Any], bool]: Pair ``(doc_to_update, was_modified)``
                comparing the result to ``original_doc``.

        Raises:
            MalformedQueryException: If a ``$bit`` or ``$push`` operand is
                invalid or an update operator is unsupported.
        """
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            match op:
                case "$set":
                    for k, v in value.items():
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update, k, v, array_filters, query_filter
                            )
                        else:
                            _set_field_for_update(doc_to_update, k, v)
                case "$unset":
                    for k in value:
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                None,
                                array_filters,
                                query_filter,
                                op="$unset",
                            )
                        else:
                            _unset_nested_field(doc_to_update, k)
                case "$inc":
                    for k, v in value.items():
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                v,
                                array_filters,
                                query_filter,
                                op="$inc",
                            )
                        else:
                            current_value = _get_field_for_update(
                                doc_to_update, k
                            )
                            _validate_inc_mul_field_value(
                                k, current_value, "$inc"
                            )
                            new_val = (
                                current_value
                                if current_value is not None
                                else 0
                            ) + v
                            _set_field_for_update(doc_to_update, k, new_val)
                case "$push":
                    _validate_push_spec(value)
                    for k, v in value.items():
                        exists = (
                            _field_exists(k, doc_to_update)
                            if "." in k
                            else (k in doc_to_update)
                        )
                        if not exists:
                            _set_field_for_update(doc_to_update, k, [])
                        current_list = _get_field_for_update(doc_to_update, k)

                        # Check if v is a dict with modifiers ($each, $position, $sort, $slice)
                        if isinstance(v, dict) and "$each" in v:
                            values_to_add = v["$each"]
                            if not isinstance(values_to_add, list):
                                values_to_add = [values_to_add]

                            position = v.get("$position")
                            if position is not None:
                                if position < 0:
                                    ins_pos = max(
                                        len(current_list) + position, 0
                                    )
                                else:
                                    ins_pos = min(position, len(current_list))
                                current_list[ins_pos:ins_pos] = values_to_add
                            else:
                                current_list.extend(values_to_add)

                            sort_spec = v.get("$sort")
                            if sort_spec is not None:
                                _apply_push_sort(current_list, sort_spec)

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
                            if not any(
                                _bson_values_equal(val, existing)
                                for existing in current_list
                            ):
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
                                if not any(
                                    _bson_values_equal(item, rem)
                                    for rem in values_to_remove
                                )
                            ]
                            if len(new_list) != len(arr):
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
                        _validate_bit_operand(bit_op)
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                bit_op,
                                array_filters,
                                query_filter,
                                op="$bit",
                            )
                        else:
                            current_val = _get_field_for_update(
                                doc_to_update, k
                            )
                            _validate_bit_field_value(k, current_val)
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
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                v,
                                array_filters,
                                query_filter,
                                op="$mul",
                            )
                        else:
                            current_value = _get_field_for_update(
                                doc_to_update, k
                            )
                            _validate_inc_mul_field_value(
                                k, current_value, "$mul"
                            )
                            new_val = (
                                current_value
                                if current_value is not None
                                else 0
                            ) * v
                            _set_field_for_update(doc_to_update, k, new_val)
                case "$min":
                    for k, v in value.items():
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                v,
                                array_filters,
                                query_filter,
                                op="$min",
                            )
                        else:
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
                            if (
                                not exists
                                or cur_val is None
                                or bson_sort_key(v) < bson_sort_key(cur_val)
                            ):
                                _set_field_for_update(doc_to_update, k, v)
                case "$max":
                    for k, v in value.items():
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                v,
                                array_filters,
                                query_filter,
                                op="$max",
                            )
                        else:
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
                            if (
                                not exists
                                or cur_val is None
                                or bson_sort_key(v) > bson_sort_key(cur_val)
                            ):
                                _set_field_for_update(doc_to_update, k, v)
                case "$currentDate":
                    for k in value.keys():
                        now_val = datetime.now(UTC)
                        if "$" in k:
                            _apply_positional_update(
                                doc_to_update,
                                k,
                                now_val,
                                array_filters,
                                query_filter,
                                op="$currentDate",
                            )
                        else:
                            _set_field_for_update(doc_to_update, k, now_val)
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
