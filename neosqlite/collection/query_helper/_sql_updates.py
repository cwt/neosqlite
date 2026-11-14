"""SQL-based update operations mixin for QueryHelper.

Extracted from update_operations.py.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ...binary import Binary
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)
from ..json_path_utils import parse_json_path
from ..jsonb_support import json_data_column
from ..type_utils import (
    _is_numeric_value,
    build_bson_type_sql,
    regex_to_sql_pattern,
)
from .utils import (
    _convert_bytes_to_binary,
    _get_json_function,
    _supports_relative_json_indexing,
    get_force_fallback,
)

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


def _contains_binary_value(obj: Any) -> bool:
    """Return True if obj contains a Binary instance."""
    if isinstance(obj, bytes) and hasattr(obj, "encode_for_storage"):
        return True
    if isinstance(obj, dict):
        return any(_contains_binary_value(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_binary_value(v) for v in obj)
    return False


def _format_sql_value_expr(value: Any) -> tuple[str, list[Any]]:
    """Bind a Python value as a SQLite JSON/scalar parameter preserving bool/objects."""
    converted = _convert_bytes_to_binary(value)
    if isinstance(converted, bool):
        return "json(?)", ["true" if converted else "false"]
    if converted is None or isinstance(converted, (int, float, str)):
        return "?", [converted]
    return "json(?)", [neosqlite_json_dumps(converted)]


def _je_preserved_value_expr(alias: str) -> str:
    """Preserve JSON types (including booleans and sub-objects) from json_each."""
    return (
        f"CASE {alias}.type "
        f"WHEN 'object' THEN json({alias}.value) "
        f"WHEN 'array' THEN json({alias}.value) "
        f"WHEN 'true' THEN json('true') "
        f"WHEN 'false' THEN json('false') "
        f"ELSE {alias}.value END"
    )


def _extract_preserved_json_expr(
    extract_func: str, data_expr: str, json_path: str
) -> str:
    """Extract a JSON value preserving booleans, objects, and arrays for json_set."""
    extracted = f"{extract_func}({data_expr}, {json_path})"
    return (
        f"CASE json_type({data_expr}, {json_path}) "
        f"WHEN 'object' THEN json({extracted}) "
        f"WHEN 'array' THEN json({extracted}) "
        f"WHEN 'true' THEN json('true') "
        f"WHEN 'false' THEN json('false') "
        f"ELSE {extracted} END"
    )


def _wrap_single_eval(
    cur_expr: str,
    cur_params: list[Any],
    build_fn: Callable[[str], tuple[str, list[Any]]],
    counter: list[int],
) -> tuple[str, list[Any]]:
    """Evaluate cur_expr once via a subquery alias when transforming conditionally."""
    if cur_expr == "data":
        return build_fn("data")
    alias = f"_d{counter[0]}"
    counter[0] += 1
    body_sql, body_params = build_fn(alias)
    return f"(SELECT {body_sql} FROM (SELECT {cur_expr} AS {alias}))", (
        body_params + cur_params
    )


def _compile_json_scalar_eq(
    type_expr: str, val_expr: str, expected: Any
) -> tuple[str, list[Any]] | None:
    """Compile scalar equality predicate on (json_type, json_extract/value)."""
    if expected is None:
        return f"{type_expr} = 'null'", []
    if isinstance(expected, bool):
        expected_type = "true" if expected else "false"
        return f"{type_expr} = '{expected_type}'", []
    if isinstance(expected, (int, float)):
        return f"{type_expr} IN ('integer', 'real') AND {val_expr} = ?", [
            expected
        ]
    if isinstance(expected, str):
        return f"{type_expr} = 'text' AND {val_expr} = ?", [expected]
    return None


def _compile_json_value_operators(
    type_expr: str,
    val_expr: str,
    operators: dict[str, Any],
    *,
    exists_always_true: bool = False,
    safe_obj_expr: str | None = None,
) -> tuple[str, list[Any]] | None:
    """Compile MongoDB query operator dict against a SQLite JSON value."""
    if not operators:
        return "1", []
    clauses: list[str] = []
    params: list[Any] = []
    cmp_ops = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}

    for op, expected in operators.items():
        if op == "$options":
            if "$regex" not in operators:
                return None
            continue
        if op == "$eq":
            eq_res = _compile_json_scalar_eq(type_expr, val_expr, expected)
            if eq_res is None:
                return None
            clauses.append(f"({eq_res[0]})")
            params.extend(eq_res[1])
        elif op == "$ne":
            eq_res = _compile_json_scalar_eq(type_expr, val_expr, expected)
            if eq_res is None:
                return None
            clauses.append(f"NOT COALESCE(({eq_res[0]}), 0)")
            params.extend(eq_res[1])
        elif op in cmp_ops:
            sql_op = cmp_ops[op]
            if isinstance(expected, (int, float)) and not isinstance(
                expected, bool
            ):
                clauses.append(
                    f"({type_expr} IN ('integer', 'real') AND {val_expr} {sql_op} ?)"
                )
                params.append(expected)
            elif isinstance(expected, str):
                clauses.append(
                    f"({type_expr} = 'text' AND {val_expr} {sql_op} ?)"
                )
                params.append(expected)
            else:
                return None
        elif op in ("$in", "$nin"):
            if not isinstance(expected, (list, tuple)):
                return None
            if not expected:
                clauses.append("0" if op == "$in" else "1")
                continue
            item_sqls: list[str] = []
            item_params: list[Any] = []
            for item in expected:
                eq_res = _compile_json_scalar_eq(type_expr, val_expr, item)
                if eq_res is None:
                    return None
                item_sqls.append(f"({eq_res[0]})")
                item_params.extend(eq_res[1])
            joined = " OR ".join(item_sqls)
            if op == "$in":
                clauses.append(f"({joined})")
            else:
                clauses.append(f"NOT COALESCE(({joined}), 0)")
            params.extend(item_params)
        elif op == "$exists":
            if exists_always_true:
                clauses.append("1" if expected else "0")
            else:
                clauses.append(
                    f"{type_expr} IS NOT NULL"
                    if expected
                    else f"{type_expr} IS NULL"
                )
        elif op == "$size":
            if isinstance(expected, int) and not isinstance(expected, bool):
                clauses.append(
                    f"(CASE WHEN {type_expr} = 'array' "
                    f"THEN json_array_length({val_expr}) = ? ELSE 0 END)"
                )
                params.append(expected)
            else:
                clauses.append("0")
        elif op == "$regex":
            opts = operators.get("$options", "")
            if not isinstance(opts, str):
                return None
            pat = regex_to_sql_pattern(expected, opts)
            if pat is None:
                return None
            clauses.append(f"({type_expr} = 'text' AND ({val_expr} REGEXP ?))")
            params.append(pat)
        elif op == "$type":
            val_arg = safe_obj_expr if safe_obj_expr is not None else val_expr
            type_sql = build_bson_type_sql(
                "", "", expected, type_expr=type_expr, val_expr=val_arg
            )
            if type_sql is None:
                return None
            clauses.append(f"({type_sql})")
        else:
            return None

    return (" AND ".join(clauses) if clauses else "1"), params


def _compile_elem_predicate(
    je_alias: str, filter_spec: Any
) -> tuple[str, list[Any]] | None:
    """Compile an array-element filter (used by $pull and $[id] arrayFilters)."""
    type_col = f"{je_alias}.type"
    val_col = f"{je_alias}.value"
    safe_obj = (
        f"(CASE WHEN {type_col} = 'object' THEN json({val_col}) ELSE NULL END)"
    )

    if not isinstance(filter_spec, dict):
        return _compile_json_scalar_eq(type_col, val_col, filter_spec)
    if not filter_spec:
        return "1", []
    if all(k.startswith("$") for k in filter_spec):
        return _compile_json_value_operators(
            type_col,
            val_col,
            filter_spec,
            exists_always_true=True,
            safe_obj_expr=safe_obj,
        )
    if any(k.startswith("$") for k in filter_spec):
        return None

    clauses: list[str] = [f"{type_col} = 'object'"]
    params: list[Any] = []
    for sub_field, expected in filter_spec.items():
        if not sub_field or "$" in sub_field:
            return None
        sub_path = f"'{parse_json_path(sub_field)}'"
        sub_type = f"json_type({safe_obj}, {sub_path})"
        sub_val = f"json_extract({safe_obj}, {sub_path})"
        sub_safe_obj = f"(CASE WHEN {sub_type} = 'object' THEN json({sub_val}) ELSE NULL END)"
        if isinstance(expected, dict):
            if not all(k.startswith("$") for k in expected):
                return None
            if expected.get("$exists") is False:
                clauses.append(f"{sub_type} IS NULL")
            else:
                ops_res = _compile_json_value_operators(
                    sub_type,
                    sub_val,
                    expected,
                    exists_always_true=True,
                    safe_obj_expr=sub_safe_obj,
                )
                if ops_res is None:
                    return None
                clauses.append(f"({sub_type} IS NOT NULL AND ({ops_res[0]}))")
                params.extend(ops_res[1])
        else:
            eq_res = _compile_json_scalar_eq(sub_type, sub_val, expected)
            if eq_res is None:
                return None
            clauses.append(f"({sub_type} IS NOT NULL AND ({eq_res[0]}))")
            params.extend(eq_res[1])

    return " AND ".join(clauses), params


class SqlUpdatesMixin:
    """Mixin providing SQL-based update operations.

    Designed to be composed into UpdateOperationsMixin.
    """

    collection: Collection
    jsonb: JSONBContext
    _get_integer_id_for_oid: Any

    def _can_use_sql_updates(
        self,
        update_spec: dict[str, Any],
        doc_id: int,
        original_doc: dict[str, Any] | None = None,
    ) -> bool:
        """
        Check if all operations in the update spec can be handled with SQL.

        This method determines whether the update operations can be efficiently
        executed using SQL directly, which allows for better performance compared
        to iterating over each document and applying updates in Python.

        Args:
            update_spec (dict[str, Any]): The update operations to be checked.
            doc_id (int): The document ID, which is used to determine if the update
                          is an upsert.

        Returns:
            bool: True if all operations can be handled with SQL, False otherwise.
        """
        # Respect the kill switch - force fallback if enabled
        if get_force_fallback():
            return False

        # Tier 1: Simple operations that can use json_set/json_remove
        # Tier 2: More complex operations that can use SQL with some limitations
        supported_ops = {
            "$set",
            "$unset",
            "$inc",
            "$mul",
            "$min",
            "$max",
            "$pull",
            "$pullAll",
            "$currentDate",
            "$rename",
            "$setOnInsert",
        }

        # Also check that doc_id is not 0 (which indicates an upsert)
        # Only reject SQL updates when Binary appears in operators whose SQL
        # path compares JSON/BLOB payloads as scalars ($min/$max) or uses
        # unquoted JSON fragments ($pull/$pullAll). $set, $push, $addToSet, etc.
        # serialize Binary cleanly into JSON/JSONB.
        def _contains_binary(obj: Any) -> bool:
            if isinstance(obj, bytes) and hasattr(obj, "encode_for_storage"):
                return True
            if isinstance(obj, dict):
                return any(_contains_binary(v) for v in obj.values())
            if isinstance(obj, list):
                return any(_contains_binary(v) for v in obj)
            return False

        has_unsupported_binary = any(
            _contains_binary(val)
            for op_name, op_val in update_spec.items()
            if op_name in {"$min", "$max", "$pull", "$pullAll"}
            and isinstance(op_val, dict)
            for val in op_val.values()
        )

        # Check for positional operators in field paths (not supported in SQL tier)
        has_positional_operators = False
        for op, value in update_spec.items():
            if isinstance(value, dict):
                for field_path in value.keys():
                    if "$" in field_path:
                        has_positional_operators = True
                        break
            if has_positional_operators:
                break

        # Positional operators require Python fallback
        if has_positional_operators:
            return False

        # Check for complex $push modifiers
        # SQL optimization supports $each, $position, and $slice - all handled in SQL tier
        # No need to check for Python fallback anymore

        # Check for $pop operator
        has_pop = "$pop" in update_spec
        # SQL $pop requires relative indexing [#-1] support
        if has_pop and not _supports_relative_json_indexing():
            return False

        # Check for $addToSet operator - now supports $each in SQL
        # Note: We no longer return False for $each since we have SQL implementation

        # Check for $pull and $pullAll - require field to exist and be a list in original_doc
        if original_doc is not None:
            for op in update_spec:
                if op in {"$pull", "$pullAll"}:
                    op_spec = update_spec[op]
                    if isinstance(op_spec, dict):
                        for field, pull_val in op_spec.items():
                            if field not in original_doc or not isinstance(
                                original_doc.get(field), list
                            ):
                                return False
                            if op == "$pull" and isinstance(pull_val, dict):
                                return False

        return (
            doc_id != 0
            and not has_unsupported_binary
            and all(
                op in supported_ops
                or op in {"$push", "$bit", "$pop", "$addToSet"}
                for op in update_spec.keys()
            )
        )

    def _perform_sql_update(
        self,
        doc_id: int,
        update_spec: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Perform update operations using SQL JSON functions.

        This method builds SQL clauses for updating document fields based on the
        provided update specification. It supports both `$set` and `$unset` operations
        using SQLite's `json_set` and `json_remove` functions, respectively. The
        method then executes the SQL commands to apply the updates and fetches
        the updated document from the database.

        Args:
            doc_id (int): The ID of the document to be updated.
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.

        Returns:
            dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the
                          update process.
        """
        set_clauses = []
        set_params = []
        unset_clauses = []
        unset_params = []

        # Build SQL update clauses for each operation
        for op, value in update_spec.items():
            clauses, params = self._build_sql_update_clause(op, value)
            if clauses:
                if op == "$unset":
                    unset_clauses.extend(clauses)
                    unset_params.extend(params)
                else:
                    set_clauses.extend(clauses)
                    set_params.extend(params)

        # Get integer ID for the document
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # Execute the SQL updates using a single consolidated UPDATE statement if possible
        # This significantly reduces disk I/O and transaction overhead
        current_data = "data"
        all_params = []

        if unset_clauses:
            # Handle $unset operations with json_remove
            func_name = _get_json_function("remove", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(unset_clauses)})"
            )
            all_params.extend(unset_params)

        if set_clauses:
            # Handle other operations with json_set
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(set_clauses)})"
            )
            all_params.extend(set_params)

        if current_data != "data":
            cmd = (
                f"UPDATE {quote_table_name(self.collection.name)} "
                f"SET data = {current_data} "
                "WHERE id = ?"
            )
            cursor = self.collection.db.execute(cmd, all_params + [int_doc_id])

            # Check if any rows were updated
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        else:
            # No operations to perform
            raise RuntimeError("No valid operations to perform")

        # Fetch and return the updated document
        # Use the instance's JSONB support flag to determine how to select data
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT id, {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])

        # This shouldn't happen, but just in case
        raise RuntimeError("Failed to fetch updated document")

    @staticmethod
    def _build_push_position_slice_clause(
        field: str,
        values_json: str | None,
        position_value: int | None,
        slice_value: int | None,
        data_expr: str = "data",
    ) -> tuple[str, list[Any]] | None:
        """Build the SET-clause fragment for $push with $position/$slice.

        Assembles the new array from explicit ordered blocks (front /
        inserted values / tail) so no LIMIT appears before UNION ALL —
        the previous template was rejected by SQLite (#97). $slice follows
        MongoDB semantics including negative counts, which keep the LAST
        n elements instead of everything (#98).

        Args:
            field: target field name
            new_values_json: JSON text array of converted values to insert
                (None when only slicing is requested)
            position_value: $position value (negative counts from end)
            slice_value: $slice value (negative keeps the tail)

        Returns:
            (fragment, extra_params): the "<json_path>, (<subquery>)"
            fragment plus parameters to append to the statement's bind list
            (the inserted-values JSON array), or None when the caller should
            fall back to the Python tier.
        """
        json_path = f"'{parse_json_path(field)}'"
        cur = f"json_extract({data_expr}, {json_path})"

        extra_params: list[Any] = []

        boundary_sql: str | None = None
        if position_value is not None:
            p = int(position_value)
            if p >= 0:
                boundary_sql = str(p)
            else:
                # Negative $position counts back from the array's end,
                # clamped to 0; the length is resolved at run time.
                boundary_sql = (
                    f"MAX((SELECT COUNT(*) FROM json_each({cur})) + ({p}), 0)"
                )

        arms: list[str] = [
            f"SELECT value, type, 0 AS blk, key AS ord FROM json_each({cur})"
            + (f" WHERE key < {boundary_sql}" if boundary_sql else "")
        ]
        if values_json is not None:
            # Inserted values arrive as one bound JSON-array parameter
            arms.append(
                "SELECT value, type, 1 AS blk, key AS ord FROM json_each(?)"
            )
            extra_params.append(values_json)
        if boundary_sql:
            arms.append(
                f"SELECT value, type, 2 AS blk, key AS ord FROM json_each({cur}) "
                f"WHERE key >= {boundary_sql}"
            )
        union_sql = " UNION ALL ".join(arms)
        base_ordered = (
            f"SELECT value, type FROM ({union_sql}) ORDER BY blk, ord"
        )

        if slice_value is None:
            final_select = base_ordered
        elif slice_value == 0:
            return f"{json_path}, json('[]')", []
        elif slice_value > 0:
            final_select = f"{base_ordered} LIMIT {slice_value}"
        else:
            # Negative slice: keep the last n elements. Take them off a
            # reversed scan, then restore original order via blk, ord.
            reversed_scan = (
                f"SELECT value, type, blk, ord FROM ({union_sql}) "
                f"ORDER BY blk DESC, ord DESC LIMIT {-int(slice_value)}"
            )
            final_select = f"SELECT value, type FROM ({reversed_scan}) ORDER BY blk ASC, ord ASC"

        fragment = (
            f"{json_path}, json((SELECT json_group_array("
            f"CASE WHEN type IN ('object', 'array') THEN json(value) "
            f"WHEN type = 'true' THEN json('true') "
            f"WHEN type = 'false' THEN json('false') "
            f"ELSE value END) "
            f"FROM ({final_select})))"
        )
        return fragment, extra_params

    def _perform_enhanced_sql_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Perform update operations using SQL JSON functions with field-level granularity.

        This method optimizes update operations by using specialized JSON functions
        (json_insert, json_replace, etc.) based on whether fields already exist.
        It provides field-level updates rather than whole-document rewrites.

        Args:
            doc_id (Any): The ID of the document to be updated.
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.
            original_doc (dict[str, Any], optional): The original document before the update.
                                                     If provided, used to determine existing fields
                                                     instead of fetching again.

        Returns:
            dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the
                          update process.
        """
        # Get integer ID for the document immediately to avoid UnboundLocalError
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # First, we need to determine which fields exist in the document
        # and which are new to decide between json_insert and json_replace
        # Use original_doc if provided to avoid extra fetch
        if original_doc is not None:
            existing_fields = (
                set(original_doc.keys())
                if isinstance(original_doc, dict)
                else set()
            )
        else:
            existing_fields = self._get_document_fields(doc_id)

        insert_clauses = []
        insert_params = []
        replace_clauses = []
        replace_params = []
        set_clauses = []  # For backward compatibility with json_set
        set_params = []
        unset_clauses = []
        unset_params: list[Any] = []

        # Build SQL update clauses for each operation
        for op, value in update_spec.items():
            match op:
                case "$set":
                    # For $set, we need to determine whether to use json_insert or json_replace
                    for field, field_val in value.items():
                        # Convert bytes to Binary for proper JSON serialization
                        converted_val = _convert_bytes_to_binary(field_val)
                        # If it's a Binary object, serialize it to JSON and use json() function
                        if isinstance(converted_val, Binary):
                            param_value = neosqlite_json_dumps(converted_val)
                            use_json_func = True
                        # For complex objects (dict, list), serialize them to JSON
                        elif isinstance(converted_val, (dict, list)):
                            param_value = neosqlite_json_dumps(converted_val)
                            use_json_func = True
                        else:
                            param_value = converted_val
                            use_json_func = False

                        # For dotted field names, we should use json_set
                        if "." in field and field not in existing_fields:
                            json_path = f"'{parse_json_path(field)}'"
                            if use_json_func:
                                set_clauses.append(f"{json_path}, json(?)")
                            else:
                                set_clauses.append(f"{json_path}, ?")
                            set_params.append(param_value)
                        else:
                            # Check if field exists in the document
                            json_path = f"'{parse_json_path(field)}'"
                            if field in existing_fields:
                                # Use json_replace for existing fields
                                if use_json_func:
                                    replace_clauses.append(
                                        f"{json_path}, json(?)"
                                    )
                                else:
                                    replace_clauses.append(f"{json_path}, ?")
                                replace_params.append(param_value)
                            else:
                                # Use json_insert for new fields
                                if use_json_func:
                                    insert_clauses.append(
                                        f"{json_path}, json(?)"
                                    )
                                else:
                                    insert_clauses.append(f"{json_path}, ?")
                                insert_params.append(param_value)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        json_path = f"'{parse_json_path(field)}'"
                        unset_clauses.append(json_path)
                case "$push":
                    # Tier 2: $push (with $each, optionally with $position and/or $slice) can use SQL
                    for field, push_value in value.items():
                        # Extract values to push (handle $each)
                        values_to_push = []
                        slice_value = None
                        position_value = None
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            # Check for $slice and $position
                            if "$slice" in push_value:
                                slice_value = push_value["$slice"]
                            if "$position" in push_value:
                                position_value = push_value["$position"]
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Check for $slice without $each
                        if (
                            isinstance(push_value, dict)
                            and "$slice" in push_value
                            and "$each" not in push_value
                        ):
                            slice_value = push_value["$slice"]

                        # Check for $position without $each
                        if (
                            isinstance(push_value, dict)
                            and "$position" in push_value
                            and "$each" not in push_value
                        ):
                            position_value = push_value["$position"]

                        json_path = f"'{parse_json_path(field)}'"

                        # Convert values to add
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in values_to_push
                        ]
                        # Build JSON array of new values
                        new_values_json = neosqlite_json_dumps(converted_values)

                        # Handle $position / $slice via the shared,
                        # order-correct clause builder (#97, #98).
                        built = self._build_push_position_slice_clause(
                            field,
                            new_values_json,
                            position_value,
                            slice_value,
                        )
                        if built is not None:
                            clause, extra = built
                            set_clauses.append(clause)
                            set_params.extend(extra)
                        else:
                            # No $position or $slice - just append values using [#]
                            append_path = f"'{parse_json_path(field)}[#]'"
                            for val in values_to_push:
                                converted_val = _convert_bytes_to_binary(val)
                                if isinstance(
                                    converted_val, (dict, list, Binary)
                                ):
                                    param_value = neosqlite_json_dumps(
                                        converted_val
                                    )
                                    set_clauses.append(
                                        f"{append_path}, json(?)"
                                    )
                                else:
                                    param_value = converted_val
                                    set_clauses.append(f"{append_path}, ?")
                                set_params.append(param_value)
                case "$pop":
                    # Tier 2: $pop uses json_remove with [0] or [#-1]
                    for field, pop_direction in value.items():
                        index_path = (
                            "[0]" if int(pop_direction) < 0 else "[#-1]"
                        )
                        json_path = f"'{parse_json_path(field)}{index_path}'"
                        unset_clauses.append(json_path)
                case "$addToSet":
                    # Tier 2: $addToSet (with or without $each) can use conditional SQL
                    insert_func = _get_json_function(
                        "insert", self.jsonb.jsonb_supported
                    )
                    for field, val in value.items():
                        json_path = f"'{parse_json_path(field)}'"

                        # Handle $each modifier
                        values_to_add = []
                        if isinstance(val, dict) and "$each" in val:
                            each_values = val["$each"]
                            if not isinstance(each_values, list):
                                each_values = [each_values]
                            values_to_add = each_values
                        else:
                            values_to_add = [val]

                        # Process each value
                        for each_val in values_to_add:
                            converted_val = _convert_bytes_to_binary(each_val)
                            if isinstance(converted_val, (Binary, dict, list)):
                                param_value = neosqlite_json_dumps(
                                    converted_val
                                )
                                use_json = True
                            else:
                                param_value = converted_val
                                use_json = False

                            array_path = json_path
                            append_path = f"'{parse_json_path(field)}[#]'"
                            if not use_json:
                                cmd = (
                                    f"UPDATE {quote_table_name(self.collection.name)} "
                                    f"SET data = {insert_func}(data, {append_path}, ?) "
                                    f"WHERE id = ? AND NOT EXISTS ("
                                    f"  SELECT 1 FROM json_each(data, {array_path}) "
                                    f"  WHERE value = ?"
                                    f")"
                                )
                                self.collection.db.execute(
                                    cmd, (param_value, int_doc_id, param_value)
                                )
                            else:
                                # For complex values (dict, list, Binary), use a more complex SQL
                                # We need to check if the JSON value already exists
                                cmd = (
                                    f"UPDATE {quote_table_name(self.collection.name)} "
                                    f"SET data = {insert_func}(data, {append_path}, json(?)) "
                                    f"WHERE id = ? AND NOT EXISTS ("
                                    f"  SELECT 1 FROM json_each(data, {array_path}) "
                                    f"  WHERE json(value) = json(?)"
                                    f")"
                                )
                                self.collection.db.execute(
                                    cmd, (param_value, int_doc_id, param_value)
                                )
                case "$bit":
                    # Tier 2: $bit using bitwise operators
                    for field, bit_spec in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        extract_func = _get_json_function(
                            "extract", self.jsonb.jsonb_supported
                        )
                        current_expr = (
                            f"COALESCE({extract_func}(data, {json_path}), 0)"
                        )
                        bit_expr = current_expr
                        if "and" in bit_spec:
                            bit_expr = f"({bit_expr} & {int(bit_spec['and'])})"
                        if "or" in bit_spec:
                            bit_expr = f"({bit_expr} | {int(bit_spec['or'])})"
                        if "xor" in bit_spec:
                            xor_val = int(bit_spec["xor"])
                            bit_expr = f"(({bit_expr} | {xor_val}) & ~(({bit_expr}) & {xor_val}))"
                        set_clauses.append(f"{json_path}, {bit_expr}")
                case "$currentDate":
                    # SQL implementation for $currentDate
                    json_obj_func = _get_json_function(
                        "object", self.jsonb.jsonb_supported
                    )
                    for field in value.keys():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, {json_obj_func}('$date', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
                        )
                case "$rename":
                    # SQL implementation for $rename using single UPDATE
                    # Combine json_set and json_remove in a single operation
                    for old_field, new_field in value.items():
                        old_json_path = f"'{parse_json_path(old_field)}'"
                        new_json_path = f"'{parse_json_path(new_field)}'"
                        extract_func = _get_json_function(
                            "extract", self.jsonb.jsonb_supported
                        )
                        # First set the new field, then remove the old field
                        # We need to nest the operations: json_remove(json_set(data, new_path, value), old_path)
                        # This requires special handling - we'll use a combined approach
                        set_clauses.append(
                            f"{new_json_path}, {extract_func}(data, {old_json_path})"
                        )
                        unset_clauses.append(old_json_path)
                case _:
                    # For other operations, use the standard approach
                    clauses, params = self._build_sql_update_clause(op, value)
                    if clauses:
                        set_clauses.extend(clauses)
                        set_params.extend(params)

        # Execute updates using a single consolidated UPDATE statement if possible
        # This significantly reduces disk I/O and transaction overhead

        # Combine all operations into a single nested data update
        # We start with 'data' and wrap it with functions in order
        current_data = "data"
        all_params = []

        # Special handling for $rename: combine json_set and json_remove logic
        has_rename = any(op == "$rename" for op in update_spec.keys())
        if has_rename and set_clauses and unset_clauses:
            set_func = _get_json_function("set", self.jsonb.jsonb_supported)
            remove_func = _get_json_function(
                "remove", self.jsonb.jsonb_supported
            )
            current_data = f"{remove_func}({set_func}({current_data}, {', '.join(set_clauses)}), {', '.join(unset_clauses)})"
            all_params.extend(set_params)
            # Clear them so we don't process them again below
            set_clauses = []
            unset_clauses = []

        # Process remaining clauses in order to build a single nested expression
        if unset_clauses:
            func_name = _get_json_function("remove", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(unset_clauses)})"
            )
            all_params.extend(unset_params)

        if insert_clauses:
            func_name = _get_json_function("insert", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(insert_clauses)})"
            )
            all_params.extend(insert_params)

        if replace_clauses:
            func_name = _get_json_function(
                "replace", self.jsonb.jsonb_supported
            )
            current_data = (
                f"{func_name}({current_data}, {', '.join(replace_clauses)})"
            )
            all_params.extend(replace_params)

        if set_clauses:
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(set_clauses)})"
            )
            all_params.extend(set_params)

        if current_data != "data":
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {current_data} WHERE id = ?"
            cursor = self.collection.db.execute(cmd, all_params + [int_doc_id])
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        elif (
            not has_rename
        ):  # If has_rename was True, it was already handled or clauses were cleared
            # This check might be redundant if we ensured at least one op, but safe to keep
            pass

        # Fetch updated document
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT id, {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])
        raise RuntimeError("Failed to fetch updated document")

    def _get_document_fields(self, doc_id: Any) -> set[str]:
        """
        Get the set of field names in a document.

        This method extracts the field names from a document to determine which fields
        already exist and which are new. This is used to decide between json_insert
        and json_replace operations.

        Args:
            doc_id (Any): The ID of the document to analyze.

        Returns:
            set: A set of field names in the document.
        """
        # Get the integer ID for the document
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # Fetch the document data
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        row = self.collection.db.execute(cmd, (int_doc_id,)).fetchone()
        if not row:
            return set()

        # Parse the JSON to get field names
        try:
            doc_data = neosqlite_json_loads(
                row[0] if self.jsonb.jsonb_supported else row[0]
            )
            if isinstance(doc_data, dict):
                return set(doc_data.keys())
            else:
                return set()
        except Exception as e:
            # If we can't parse the document, return empty set
            logger.debug(
                f"Failed to parse document for indexed fields extraction: {e}"
            )
            return set()

    def _build_update_clause(
        self,
        update: dict[str, Any],
    ) -> tuple[str, list[Any]] | None:
        """
        Build the SQL update clause based on the provided update operations.

        Args:
            update (dict[str, Any]): A dictionary containing update operations.

        Returns:
            tuple[str, list[Any]] | None: A tuple containing the SQL update clause
                                          and parameters, or None if no update
                                          clauses are generated.
        """
        set_clauses = []
        params = []

        for op, value in update.items():
            match op:
                case "$set":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(f"{json_path}, ?")
                        params.append(field_val)
                case "$inc":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + ?"
                        )
                        params.append(field_val)
                case "$mul":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(field_val)
                case "$min":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        # Missing/NULL field must take the new value (scalar
                        # min(NULL, x) is NULL), matching MongoDB (#88).
                        extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                        set_clauses.append(
                            f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN ? ELSE min({extract_expr}, ?) END"
                        )
                        params.append(field_val)
                        params.append(field_val)
                case "$max":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                        set_clauses.append(
                            f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN ? ELSE max({extract_expr}, ?) END"
                        )
                        params.append(field_val)
                        params.append(field_val)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(json_path)
                    # json_remove has a different syntax
                    if set_clauses:
                        func_name = _get_json_function(
                            "remove", self.jsonb.jsonb_supported
                        )
                        return (
                            f"data = {func_name}(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        # No fields to unset
                        return None
                case "$currentDate":
                    json_obj_func = _get_json_function(
                        "object", self.jsonb.jsonb_supported
                    )
                    for field in value.keys():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, {json_obj_func}('$date', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
                        )
                case "$pop":
                    # For $pop, we use json_remove
                    if not _supports_relative_json_indexing():
                        return None
                    for field, pop_direction in value.items():
                        # 1: remove last, -1: remove first
                        index_path = (
                            "[0]" if int(pop_direction) < 0 else "[#-1]"
                        )
                        json_path = f"'{parse_json_path(field)}{index_path}'"
                        set_clauses.append(json_path)
                    # json_remove has a different syntax
                    if set_clauses:
                        func_name = _get_json_function(
                            "remove", self.jsonb.jsonb_supported
                        )
                        return (
                            f"data = {func_name}(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        return None
                case "$push":
                    for field, push_value in value.items():
                        # Handle $each, $position, and $slice
                        values_to_push = []
                        slice_value = None
                        position_value = None
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            # Check for $slice and $position
                            if "$slice" in push_value:
                                slice_value = push_value["$slice"]
                            if "$position" in push_value:
                                position_value = push_value["$position"]
                            # SQL optimization supports $each, $position, and $slice
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Check for $slice without $each
                        if (
                            isinstance(push_value, dict)
                            and "$slice" in push_value
                            and "$each" not in push_value
                        ):
                            slice_value = push_value["$slice"]

                        # Check for $position without $each
                        if (
                            isinstance(push_value, dict)
                            and "$position" in push_value
                            and "$each" not in push_value
                        ):
                            position_value = push_value["$position"]

                        json_path = f"'{parse_json_path(field)}'"

                        # Handle $position or $slice: need to reconstruct array
                        if (
                            position_value is not None
                            or slice_value is not None
                        ):
                            # Convert values to add
                            converted_values = [
                                _convert_bytes_to_binary(v)
                                for v in values_to_push
                            ]
                            # Check if any values are complex
                            has_complex = any(
                                isinstance(v, (dict, list, Binary))
                                for v in converted_values
                            )

                            # Build JSON array of new values
                            new_values_json = neosqlite_json_dumps(
                                converted_values
                            )

                            # Handle $position / $slice via the shared,
                            # order-correct clause builder (#97, #98).
                            built = self._build_push_position_slice_clause(
                                field,
                                new_values_json,
                                position_value,
                                slice_value,
                            )
                            if built is not None:
                                clause, extra = built
                                set_clauses.append(clause)
                                params.extend(extra)
                            else:
                                return None  # builder declined: Python tier
                        else:
                            # No $position or $slice - just append values
                            append_path = f"'{parse_json_path(field)}[#]'"
                            for val in values_to_push:
                                converted_val = _convert_bytes_to_binary(val)
                                if isinstance(
                                    converted_val, (dict, list, Binary)
                                ):
                                    set_clauses.append(
                                        f"{append_path}, json(?)"
                                    )
                                    params.append(
                                        neosqlite_json_dumps(converted_val)
                                    )
                                else:
                                    set_clauses.append(f"{append_path}, ?")
                                    params.append(converted_val)
                case "$setOnInsert":
                    # $setOnInsert only applies on upsert (doc_id == 0)
                    # For existing documents (doc_id != 0), this is a no-op
                    # We can safely skip it in the SQL path
                    pass
                case "$rename":
                    # $rename is handled in _perform_enhanced_sql_update for proper
                    # set + unset ordering. Return None here to use enhanced path.
                    return None
                case "$pull":
                    # SQL optimization for $pull: filter array elements using json_each
                    for field, pull_value in value.items():
                        if isinstance(pull_value, dict):
                            return None
                        json_path = f"'{parse_json_path(field)}'"
                        converted_val = _convert_bytes_to_binary(pull_value)
                        if isinstance(converted_val, (dict, list, Binary)):
                            pull_value_json = neosqlite_json_dumps(
                                converted_val
                            )
                            set_clauses.append(
                                f"{json_path}, json((SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE json(value) != {pull_value_json} OR json(value) IS NULL))"
                            )
                        else:
                            set_clauses.append(
                                f"{json_path}, json((SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value != ?))"
                            )
                            params.append(converted_val)
                case "$pullAll":
                    # SQL optimization for $pullAll: filter multiple values from array
                    for field, pull_values in value.items():
                        if not isinstance(pull_values, list):
                            pull_values = [pull_values]
                        json_path = f"'{parse_json_path(field)}'"

                        has_complex = any(
                            isinstance(
                                _convert_bytes_to_binary(v),
                                (dict, list, Binary),
                            )
                            for v in pull_values
                        )

                        if has_complex:
                            pull_values_json = [
                                neosqlite_json_dumps(
                                    _convert_bytes_to_binary(v)
                                )
                                for v in pull_values
                            ]
                            conditions = " OR ".join(
                                [
                                    f"json(value) != {v}"
                                    for v in pull_values_json
                                ]
                            )
                            set_clauses.append(
                                f"{json_path}, json((SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE {conditions} OR json(value) IS NULL))"
                            )
                        else:
                            placeholders = ", ".join(["?" for _ in pull_values])
                            converted_values = [
                                _convert_bytes_to_binary(v) for v in pull_values
                            ]
                            set_clauses.append(
                                f"{json_path}, json((SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value NOT IN ({placeholders})))"
                            )
                            params.extend(converted_values)
                case _:
                    return None  # Fallback for unsupported operators

        if not set_clauses:
            return None

        # For $unset, we already returned above
        if "$unset" not in update:
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            return f"data = {func_name}(data, {', '.join(set_clauses)})", params
        else:
            # This case should have been handled above
            return None

    def _build_sql_update_clause(
        self,
        op: str,
        value: Any,
    ) -> tuple[list[str], list[Any]]:
        """
        Build SQL update clause for a single operation.

        Args:
            op (str): The update operation, such as "$set", "$inc", "$mul", etc.
            value (Any): The value associated with the update operation.

        Returns:
            tuple[list[str], list[Any]]: A tuple containing the SQL update clauses
                                         and parameters.
        """
        clauses = []
        params = []

        match op:
            case "$set":
                for field, field_val in value.items():
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    json_path = f"'{parse_json_path(field)}'"
                    if isinstance(converted_val, Binary):
                        clauses.append(f"{json_path}, json(?)")
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(f"{json_path}, ?")
                        params.append(converted_val)
            case "$inc":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + ?"
                        )
                        params.append(converted_val)
            case "$mul":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(converted_val)
            case "$min":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Missing/NULL field must take the new value (scalar
                    # min(NULL, x) is NULL), matching MongoDB (#88).
                    extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                    clauses.append(
                        f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN ? ELSE min({extract_expr}, ?) END"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN json(?) ELSE min({extract_expr}, json(?)) END"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
                        params.append(converted_val)
            case "$max":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    extract_expr = f"{self.jsonb.json_function_prefix}_extract(data, {json_path})"
                    clauses.append(
                        f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN ? ELSE max({extract_expr}, ?) END"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, CASE WHEN {extract_expr} IS NULL THEN json(?) ELSE max({extract_expr}, json(?)) END"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
                        params.append(converted_val)
            case "$push":
                # Optimized $push (with $each, optionally with $position and/or $slice) using [#]
                for field, push_value in value.items():
                    # Handle $each, $position, and $slice
                    values_to_push = []
                    slice_value = None
                    position_value = None
                    if isinstance(push_value, dict) and "$each" in push_value:
                        # Check for $slice and $position
                        if "$slice" in push_value:
                            slice_value = push_value["$slice"]
                        if "$position" in push_value:
                            position_value = push_value["$position"]
                        # SQL optimization supports $each, $position, and $slice
                        values_to_push = push_value["$each"]
                        if not isinstance(values_to_push, list):
                            values_to_push = [values_to_push]
                    else:
                        values_to_push = [push_value]

                    # Check for $slice without $each
                    if (
                        isinstance(push_value, dict)
                        and "$slice" in push_value
                        and "$each" not in push_value
                    ):
                        slice_value = push_value["$slice"]

                    # Check for $position without $each
                    if (
                        isinstance(push_value, dict)
                        and "$position" in push_value
                        and "$each" not in push_value
                    ):
                        position_value = push_value["$position"]

                    json_path = f"'{parse_json_path(field)}'"

                    # Handle $position or $slice: need to reconstruct array
                    if position_value is not None or slice_value is not None:
                        # Convert values to add
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in values_to_push
                        ]
                        # Build JSON array of new values
                        new_values_json = neosqlite_json_dumps(converted_values)

                        # Handle $position / $slice via the shared,
                        # order-correct clause builder (#97, #98).
                        built = self._build_push_position_slice_clause(
                            field,
                            new_values_json,
                            position_value,
                            slice_value,
                        )
                        if built is not None:
                            clause, extra = built
                            clauses.append(clause)
                            params.extend(extra)
                        else:
                            # No $position or $slice - just append values
                            append_path = f"'{parse_json_path(field)}[#]'"
                            for val in values_to_push:
                                # Convert bytes to Binary for proper JSON serialization
                                converted_val = _convert_bytes_to_binary(val)
                                if isinstance(converted_val, Binary):
                                    clauses.append(f"{append_path}, json(?)")
                                    params.append(
                                        neosqlite_json_dumps(converted_val)
                                    )
                                elif isinstance(converted_val, (dict, list)):
                                    clauses.append(f"{append_path}, json(?)")
                                    params.append(
                                        neosqlite_json_dumps(converted_val)
                                    )
                                else:
                                    clauses.append(f"{append_path}, ?")
                                    params.append(converted_val)
            case "$pop":
                # Tier 2: $pop uses json_remove with [0] or [#-1]
                if not _supports_relative_json_indexing():
                    return [], []
                for field, pop_direction in value.items():
                    # 1: remove last, -1: remove first
                    index_path = "[0]" if int(pop_direction) < 0 else "[#-1]"
                    json_path = f"'{parse_json_path(field)}{index_path}'"
                    clauses.append(json_path)
            case "$addToSet":
                # Tier 2: $addToSet (without $each) can use conditional SQL
                for field, val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert value for proper parameter handling
                    converted_val = _convert_bytes_to_binary(val)
                    if isinstance(converted_val, Binary):
                        param_value = neosqlite_json_dumps(converted_val)
                        use_json = True
                    elif isinstance(converted_val, (dict, list)):
                        param_value = neosqlite_json_dumps(converted_val)
                        use_json = True
                    else:
                        param_value = converted_val
                        use_json = False

                    if not use_json:
                        # Build SQL that only inserts if value not in array
                        insert_func = _get_json_function(
                            "insert", self.jsonb.jsonb_supported
                        )
                        array_path = json_path

                        # Use a CASE expression returning the resulting array.
                        # Both branches must yield the *array value* because
                        # this clause is wrapped as json_set(data, ...):
                        # - element exists -> keep current array unchanged
                        # - missing        -> insert into a copy of the array (#89)
                        exists_subquery = f"EXISTS (SELECT 1 FROM json_each(data, {array_path}) WHERE value = ?)"
                        current_array = f"{self.jsonb.json_function_prefix}_extract(data, {array_path})"

                        clauses.append(
                            f"{array_path}, CASE WHEN {exists_subquery} THEN {current_array} ELSE {insert_func}({current_array}, '$[#]', ?) END"
                        )
                        params.extend([param_value, param_value])
                    else:
                        # Complex values - fall back to Python
                        return [], []
            case "$unset":
                # For $unset, we use json_remove
                for field in value:
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(json_path)
            case "$pull":
                # SQL optimization for $pull: filter array elements using json_each
                for field, pull_value in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    converted_val = _convert_bytes_to_binary(pull_value)
                    if isinstance(converted_val, (dict, list, Binary)):
                        # For complex values, serialize to JSON for comparison
                        pull_value_json = neosqlite_json_dumps(converted_val)
                        clauses.append(
                            f"{json_path}, json((SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE json(value) != {pull_value_json} OR json(value) IS NULL))"
                        )
                    else:
                        # For simple values, compare directly
                        clauses.append(
                            f"{json_path}, json((SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value != ?))"
                        )
                        params.append(converted_val)
            case "$pullAll":
                # SQL optimization for $pullAll: filter multiple values from array
                for field, pull_values in value.items():
                    if not isinstance(pull_values, list):
                        pull_values = [pull_values]
                    json_path = f"'{parse_json_path(field)}'"

                    # Check if any values are complex (dict, list, Binary)
                    has_complex = any(
                        isinstance(
                            _convert_bytes_to_binary(v), (dict, list, Binary)
                        )
                        for v in pull_values
                    )

                    if has_complex:
                        # For complex values, serialize each and build JSON comparison
                        pull_values_json = [
                            neosqlite_json_dumps(_convert_bytes_to_binary(v))
                            for v in pull_values
                        ]
                        conditions = " OR ".join(
                            [f"json(value) != {v}" for v in pull_values_json]
                        )
                        clauses.append(
                            f"{json_path}, json((SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE {conditions} OR json(value) IS NULL))"
                        )
                    else:
                        # For simple values, use IN clause
                        placeholders = ", ".join(["?" for _ in pull_values])
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in pull_values
                        ]
                        clauses.append(
                            f"{json_path}, json((SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value NOT IN ({placeholders})))"
                        )
                        params.extend(converted_values)
            case "$currentDate":
                # SQL implementation for $currentDate
                json_obj_func = _get_json_function(
                    "object", self.jsonb.jsonb_supported
                )
                for field in value.keys():
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(
                        f"{json_path}, {json_obj_func}('$date', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
                    )
            case "$setOnInsert":
                # $setOnInsert only applies on upsert (doc_id == 0)
                # For existing documents (doc_id != 0), this is a no-op
                # We can safely skip it - return empty clauses
                pass
            case "$rename":
                # SQL implementation for $rename
                # $rename requires: get value, set at new path, remove old path
                # This is handled in _perform_enhanced_sql_update specially
                # Return empty to trigger fallback for complex cases
                return [], []

        return clauses, params

    def _validate_inc_mul_types_sql(
        db: Any,
        collection_name: str,
        where_clause: str | None,
        where_params: list[Any],
        update: dict[str, Any],
        jsonb_supported: bool,
    ) -> bool:
        """
        Validate that fields in $inc/$mul operations are numeric.

        This checks the JSON type of each field being incremented/multiplied
        to ensure they're numeric types.

        Args:
            db: Database connection
            collection_name: Name of the collection
            where_clause: The translated WHERE clause
            where_params: Parameters for the WHERE clause
            update: The update operations
            jsonb_supported: Whether JSONB is supported

        Returns:
            True if all fields are numeric or don't exist, False if any field is non-numeric
        """
        fields_to_check = []
        for op in ("$inc", "$mul"):
            if op in update:
                fields_to_check.extend(update[op].keys())

        if not fields_to_check:
            return True

        # json_type accepts both JSON text and JSONB blobs (SQLite has no jsonb_type)
        json_func = "json_type"

        # Build a single query to check all fields
        select_expressions = []
        for field in fields_to_check:
            json_path = parse_json_path(field)
            select_expressions.append(f"{json_func}(data, '{json_path}')")

        if not where_clause:
            where_clause = "WHERE 1=1"

        cmd = f"SELECT {', '.join(select_expressions)} FROM {quote_table_name(collection_name)} {where_clause} LIMIT 1"
        try:
            cursor = db.execute(cmd, where_params)
            row = cursor.fetchone()
            if row:
                for field_type in row:
                    if field_type is not None and field_type not in (
                        "number",
                        "null",
                        "integer",
                        "real",
                    ):
                        return False
            # If no row matches, the update will be a no-op anyway, so it's safe to use fast path
            return True
        except Exception as e:
            # Fallback to slow path on any SQL error
            logger.debug(f"Fast path check failed due to SQL error: {e}")
            return False

    def _validate_inc_mul_sql(
        self,
        where_clause: str,
        where_params: list[Any],
        update: dict[str, Any],
        *,
        order_by: str = "",
        limit_one: bool = True,
    ) -> bool:
        """Validate $inc/$mul operand & existing DB field types for one or many rows."""
        fields_to_check: list[str] = []
        for op in ("$inc", "$mul"):
            op_val = update.get(op)
            if isinstance(op_val, dict):
                for field, operand in op_val.items():
                    if "$" in field or not field:
                        return False
                    if not _is_numeric_value(operand):
                        return False
                    fields_to_check.append(field)

        if not fields_to_check:
            return True

        bad_preds = [
            f"(json_type(data, '{parse_json_path(f)}') IS NOT NULL "
            f"AND json_type(data, '{parse_json_path(f)}') NOT IN ('integer', 'real', 'null'))"
            for f in fields_to_check
        ]
        tbl = quote_table_name(self.collection.name)
        if limit_one:
            select_exprs = ", ".join(
                f"json_type(data, '{parse_json_path(f)}')"
                for f in fields_to_check
            )
            sql = (
                f"SELECT {select_exprs} FROM {tbl} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            try:
                row = self.collection.db.execute(sql, where_params).fetchone()
            except Exception as e:
                logger.debug(f"SQL $inc/$mul validation failed: {e}")
                return False
            if row is None:
                return True
            return all(
                ft is None or ft in ("integer", "real", "null", "number")
                for ft in row
            )

        bad_cond = " OR ".join(bad_preds)
        if where_clause.strip():
            check_where = f"{where_clause} AND ({bad_cond})"
        else:
            check_where = f"WHERE ({bad_cond})"
        try:
            row = self.collection.db.execute(
                f"SELECT 1 FROM {tbl} {check_where} LIMIT 1",
                where_params,
            ).fetchone()
            return row is None
        except Exception as e:
            logger.debug(f"SQL $inc/$mul multi-row validation failed: {e}")
            return False

    def _build_positional_set_expr(
        self,
        cur_expr: str,
        cur_params: list[Any],
        parts: list[str],
        value: Any,
        array_filters: list[dict[str, Any]] | None,
        jsonb_supported: bool,
        counter: list[int],
    ) -> tuple[str, list[Any]] | None:
        """Compile a $set field path containing $[] or $[identifier] to SQL."""
        pos_idx = next(
            (
                i
                for i, p in enumerate(parts)
                if p == "$[]" or p.startswith("$[")
            ),
            -1,
        )
        if pos_idx <= 0:
            return None
        for p in parts:
            if p == "$":
                return None

        prefix = ".".join(parts[:pos_idx])
        if "$" in prefix or not prefix:
            return None
        pos_seg = parts[pos_idx]
        tail_parts = parts[pos_idx + 1 :]

        je = f"_je{counter[0]}"
        counter[0] += 1
        match_params: list[Any] = []
        if pos_seg == "$[]":
            match_sql = "1"
        elif (
            pos_seg.startswith("$[")
            and pos_seg.endswith("]")
            and len(pos_seg) > 3
        ):
            identifier = pos_seg[2:-1]
            filter_spec = None
            if array_filters:
                for af in array_filters:
                    if isinstance(af, dict) and identifier in af:
                        filter_spec = af[identifier]
                        break
            if filter_spec is None:
                # Missing arrayFilter binding is a no-op (matches Python Tier 3)
                return cur_expr, cur_params
            pred = _compile_elem_predicate(je, filter_spec)
            if pred is None:
                return None
            match_sql, match_params = pred
        else:
            return None

        set_func = _get_json_function("set", jsonb_supported)
        ga_func = _get_json_function("group_array", jsonb_supported)
        empty_arr = "jsonb('[]')" if jsonb_supported else "json('[]')"
        unmatched_elem = _je_preserved_value_expr(je)

        if not tail_parts:
            matched_elem_sql, matched_elem_params = _format_sql_value_expr(
                value
            )
        elif any(p == "$[]" or p.startswith("$[") for p in tail_parts):
            inner_res = self._build_positional_set_expr(
                f"json({je}.value)",
                [],
                tail_parts,
                value,
                array_filters,
                jsonb_supported,
                counter,
            )
            if inner_res is None:
                return None
            inner_sql, inner_params = inner_res
            matched_elem_sql = (
                f"CASE WHEN {je}.type = 'object' THEN {inner_sql} "
                f"ELSE {unmatched_elem} END"
            )
            matched_elem_params = inner_params
        else:
            tail_path = f"'{parse_json_path('.'.join(tail_parts))}'"
            val_sql, val_params = _format_sql_value_expr(value)
            inter_checks = [f"{je}.type = 'object'"]
            for depth in range(1, len(tail_parts)):
                inter_path = (
                    f"'{parse_json_path('.'.join(tail_parts[:depth]))}'"
                )
                inter_checks.append(
                    f"json_type((CASE WHEN {je}.type = 'object' "
                    f"THEN json({je}.value) ELSE NULL END), {inter_path}) = 'object'"
                )
            matched_elem_sql = (
                f"CASE WHEN {' AND '.join(inter_checks)} "
                f"THEN {set_func}(json({je}.value), {tail_path}, {val_sql}) "
                f"ELSE {unmatched_elem} END"
            )
            matched_elem_params = val_params

        arr_path = f"'{parse_json_path(prefix)}'"

        def _build_body(src: str) -> tuple[str, list[Any]]:
            # WHEN matches SQL CASE order: match_params before matched_elem_params
            rebuilt = (
                f"COALESCE((SELECT {ga_func}(CASE WHEN ({match_sql}) "
                f"THEN {matched_elem_sql} ELSE {unmatched_elem} END) "
                f"FROM json_each({src}, {arr_path}) AS {je}), {empty_arr})"
            )
            sql = (
                f"CASE WHEN json_type({src}, {arr_path}) = 'array' "
                f"THEN {set_func}({src}, {arr_path}, {rebuilt}) "
                f"ELSE {src} END"
            )
            return sql, match_params + matched_elem_params

        return _wrap_single_eval(cur_expr, cur_params, _build_body, counter)

    def _build_pull_expr(
        self,
        cur_expr: str,
        cur_params: list[Any],
        field: str,
        pull_value: Any,
        jsonb_supported: bool,
        counter: list[int],
    ) -> tuple[str, list[Any]] | None:
        """Compile scalar or condition $pull (top-level or dotted) into SQL."""
        if _contains_binary_value(pull_value) or not field or "$" in field:
            return None
        je = f"_je{counter[0]}"
        counter[0] += 1
        pred = _compile_elem_predicate(je, pull_value)
        if pred is None:
            return None
        pred_sql, pred_params = pred
        json_path = f"'{parse_json_path(field)}'"
        set_func = _get_json_function("set", jsonb_supported)
        ga_func = _get_json_function("group_array", jsonb_supported)
        empty_arr = "jsonb('[]')" if jsonb_supported else "json('[]')"

        def _build_body(src: str) -> tuple[str, list[Any]]:
            filtered = (
                f"COALESCE((SELECT {ga_func}({_je_preserved_value_expr(je)}) "
                f"FROM json_each({src}, {json_path}) AS {je} "
                f"WHERE NOT COALESCE(({pred_sql}), 0)), {empty_arr})"
            )
            sql = (
                f"CASE WHEN json_type({src}, {json_path}) = 'array' "
                f"THEN {set_func}({src}, {json_path}, {filtered}) "
                f"ELSE {src} END"
            )
            return sql, list(pred_params)

        return _wrap_single_eval(cur_expr, cur_params, _build_body, counter)

    def _build_pull_all_expr(
        self,
        cur_expr: str,
        cur_params: list[Any],
        field: str,
        pull_values: Any,
        jsonb_supported: bool,
        counter: list[int],
    ) -> tuple[str, list[Any]] | None:
        """Compile $pullAll into a type-safe SQL array filter expression."""
        if _contains_binary_value(pull_values) or not field or "$" in field:
            return None
        if not isinstance(pull_values, (list, tuple)):
            return None
        if not pull_values:
            return cur_expr, cur_params
        je = f"_je{counter[0]}"
        counter[0] += 1
        cond_sqls: list[str] = []
        cond_params: list[Any] = []
        for pv in pull_values:
            eq_res = _compile_json_scalar_eq(f"{je}.type", f"{je}.value", pv)
            if eq_res is None:
                return None
            cond_sqls.append(f"({eq_res[0]})")
            cond_params.extend(eq_res[1])
        combined = " OR ".join(cond_sqls)
        json_path = f"'{parse_json_path(field)}'"
        set_func = _get_json_function("set", jsonb_supported)
        ga_func = _get_json_function("group_array", jsonb_supported)
        empty_arr = "jsonb('[]')" if jsonb_supported else "json('[]')"

        def _build_body(src: str) -> tuple[str, list[Any]]:
            filtered = (
                f"COALESCE((SELECT {ga_func}({_je_preserved_value_expr(je)}) "
                f"FROM json_each({src}, {json_path}) AS {je} "
                f"WHERE NOT COALESCE(({combined}), 0)), {empty_arr})"
            )
            sql = (
                f"CASE WHEN json_type({src}, {json_path}) = 'array' "
                f"THEN {set_func}({src}, {json_path}, {filtered}) "
                f"ELSE {src} END"
            )
            return sql, list(cond_params)

        return _wrap_single_eval(cur_expr, cur_params, _build_body, counter)

    def _build_add_to_set_expr(
        self,
        cur_expr: str,
        cur_params: list[Any],
        field: str,
        val: Any,
        jsonb_supported: bool,
        counter: list[int],
    ) -> tuple[str, list[Any]] | None:
        """Compile $addToSet (scalar or Binary, with optional $each) into SQL."""
        if not field or "$" in field:
            return None
        if isinstance(val, dict) and "$each" in val:
            each_vals = val["$each"]
            values_to_add = (
                each_vals if isinstance(each_vals, list) else [each_vals]
            )
        else:
            values_to_add = [val]
        if len(values_to_add) > 20:
            return None

        json_path = f"'{parse_json_path(field)}'"
        append_path = f"'{parse_json_path(field)}[#]'"
        set_func = _get_json_function("set", jsonb_supported)
        insert_func = _get_json_function("insert", jsonb_supported)
        array_func = _get_json_function("array", jsonb_supported)

        for item in values_to_add:
            converted = _convert_bytes_to_binary(item)
            je = f"_je{counter[0]}"
            counter[0] += 1
            val_sql, val_params = _format_sql_value_expr(converted)
            if isinstance(converted, Binary):
                exists_cond = (
                    f"(CASE WHEN {je}.type = 'object' "
                    f"THEN json({je}.value) = json(?) ELSE 0 END)"
                )
                exists_params: list[Any] = [neosqlite_json_dumps(converted)]
            else:
                eq_res = _compile_json_scalar_eq(
                    f"{je}.type", f"{je}.value", converted
                )
                if eq_res is None:
                    return None
                exists_cond, exists_params = eq_res

            def _build_body(
                src: str,
                vs: str = val_sql,
                vp: list[Any] = val_params,
                ec: str = exists_cond,
                ep: list[Any] = exists_params,
                je_name: str = je,
            ) -> tuple[str, list[Any]]:
                sql = (
                    f"CASE "
                    f"WHEN json_type({src}, {json_path}) IS NULL "
                    f"THEN {set_func}({src}, {json_path}, {array_func}({vs})) "
                    f"WHEN json_type({src}, {json_path}) = 'array' AND NOT EXISTS ("
                    f"  SELECT 1 FROM json_each({src}, {json_path}) AS {je_name} WHERE {ec}"
                    f") THEN {insert_func}({src}, {append_path}, {vs}) "
                    f"ELSE {src} END"
                )
                return sql, vp + ep + vp

            cur_expr, cur_params = _wrap_single_eval(
                cur_expr, cur_params, _build_body, counter
            )
        return cur_expr, cur_params

    def _build_set_data_expression(
        self,
        update_spec: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
    ) -> tuple[str, list[Any], str] | None:
        """Build a composable SQL expression for `SET data = <expr>` + tier label.

        Returns ``(data_expr, params, tier)`` where ``tier`` is ``"tier1"`` or
        ``"tier2"``, or ``None`` if Python fallback is required.
        """
        if not isinstance(update_spec, dict) or not update_spec:
            return None

        supported_ops = {
            "$set",
            "$unset",
            "$inc",
            "$mul",
            "$min",
            "$max",
            "$push",
            "$pop",
            "$addToSet",
            "$bit",
            "$currentDate",
            "$rename",
            "$pull",
            "$pullAll",
            "$setOnInsert",
        }
        if any(op not in supported_ops for op in update_spec):
            return None
        if "$pop" in update_spec and not _supports_relative_json_indexing():
            return None

        jsonb = self.jsonb.jsonb_supported
        set_func = _get_json_function("set", jsonb)
        remove_func = _get_json_function("remove", jsonb)
        extract_func = _get_json_function("extract", jsonb)

        cur_expr = "data"
        cur_params: list[Any] = []
        tier = "tier1"
        counter = [0]
        now_dt: datetime | None = None

        for op, value in update_spec.items():
            if not isinstance(value, dict):
                return None

            if op == "$setOnInsert":
                continue

            if op != "$set":
                for field in value:
                    if not field or "$" in field or field == "_id":
                        return None

            match op:
                case "$set":
                    plain_pairs: list[str] = []
                    plain_params: list[Any] = []
                    pos_fields: list[tuple[str, Any]] = []
                    for field, field_val in value.items():
                        if not field or field == "_id":
                            return None
                        if "$" in field:
                            pos_fields.append((field, field_val))
                        else:
                            jp = f"'{parse_json_path(field)}'"
                            v_sql, v_params = _format_sql_value_expr(field_val)
                            plain_pairs.append(f"{jp}, {v_sql}")
                            plain_params.extend(v_params)
                    if plain_pairs:
                        cur_expr = (
                            f"{set_func}({cur_expr}, {', '.join(plain_pairs)})"
                        )
                        cur_params.extend(plain_params)
                    for field, field_val in pos_fields:
                        tier = "tier2"
                        pos_res = self._build_positional_set_expr(
                            cur_expr,
                            cur_params,
                            field.split("."),
                            field_val,
                            array_filters,
                            jsonb,
                            counter,
                        )
                        if pos_res is None:
                            return None
                        cur_expr, cur_params = pos_res

                case "$unset":
                    paths = [f"'{parse_json_path(f)}'" for f in value]
                    if paths:
                        cur_expr = (
                            f"{remove_func}({cur_expr}, {', '.join(paths)})"
                        )

                case "$inc" | "$mul":
                    arith = "+" if op == "$inc" else "*"
                    for field, operand in value.items():
                        if not _is_numeric_value(operand):
                            return None
                        jp = f"'{parse_json_path(field)}'"

                        def _arith_body(
                            src: str,
                            p: str = jp,
                            a: str = arith,
                            val: Any = operand,
                        ) -> tuple[str, list[Any]]:
                            expr = (
                                f"COALESCE({extract_func}({src}, {p}), 0) {a} ?"
                            )
                            return f"{set_func}({src}, {p}, {expr})", [val]

                        cur_expr, cur_params = _wrap_single_eval(
                            cur_expr, cur_params, _arith_body, counter
                        )

                case "$min" | "$max":
                    fn = "min" if op == "$min" else "max"
                    for field, field_val in value.items():
                        if _contains_binary_value(field_val) or not (
                            isinstance(field_val, str)
                            or _is_numeric_value(field_val)
                        ):
                            return None
                        jp = f"'{parse_json_path(field)}'"

                        def _cmp_body(
                            src: str,
                            p: str = jp,
                            f_name: str = fn,
                            val: Any = field_val,
                        ) -> tuple[str, list[Any]]:
                            ext = f"{extract_func}({src}, {p})"
                            val_expr = (
                                f"CASE WHEN json_type({src}, {p}) IS NULL "
                                f"OR json_type({src}, {p}) = 'null' THEN ? "
                                f"ELSE {f_name}({ext}, ?) END"
                            )
                            return f"{set_func}({src}, {p}, {val_expr})", [
                                val,
                                val,
                            ]

                        cur_expr, cur_params = _wrap_single_eval(
                            cur_expr, cur_params, _cmp_body, counter
                        )

                case "$bit":
                    for field, bit_spec in value.items():
                        if not isinstance(bit_spec, dict):
                            return None
                        jp = f"'{parse_json_path(field)}'"

                        def _bit_body(
                            src: str, p: str = jp, bs: dict[str, Any] = bit_spec
                        ) -> tuple[str, list[Any]]:
                            bit_expr = (
                                f"COALESCE({extract_func}({src}, {p}), 0)"
                            )
                            if "and" in bs:
                                bit_expr = f"({bit_expr} & {int(bs['and'])})"
                            if "or" in bs:
                                bit_expr = f"({bit_expr} | {int(bs['or'])})"
                            if "xor" in bs:
                                xv = int(bs["xor"])
                                bit_expr = f"(({bit_expr} | {xv}) & ~(({bit_expr}) & {xv}))"
                            return f"{set_func}({src}, {p}, {bit_expr})", []

                        cur_expr, cur_params = _wrap_single_eval(
                            cur_expr, cur_params, _bit_body, counter
                        )

                case "$currentDate":
                    if now_dt is None:
                        now_dt = datetime.now(UTC)
                    pairs: list[str] = []
                    dt_params: list[Any] = []
                    v_sql, v_params = _format_sql_value_expr(now_dt)
                    for field in value:
                        jp = f"'{parse_json_path(field)}'"
                        pairs.append(f"{jp}, {v_sql}")
                        dt_params.extend(v_params)
                    if pairs:
                        cur_expr = f"{set_func}({cur_expr}, {', '.join(pairs)})"
                        cur_params.extend(dt_params)

                case "$rename":
                    for old_field, new_field in value.items():
                        if not new_field or "$" in new_field:
                            return None
                        if old_field == new_field:
                            continue
                        old_p = f"'{parse_json_path(old_field)}'"
                        new_p = f"'{parse_json_path(new_field)}'"

                        def _ren_body(
                            src: str, op_p: str = old_p, np_p: str = new_p
                        ) -> tuple[str, list[Any]]:
                            preserved = _extract_preserved_json_expr(
                                extract_func, src, op_p
                            )
                            renamed = (
                                f"{remove_func}({set_func}({src}, {np_p}, "
                                f"{preserved}), {op_p})"
                            )
                            return (
                                f"CASE WHEN json_type({src}, {op_p}) IS NOT NULL "
                                f"THEN {renamed} ELSE {src} END"
                            ), []

                        cur_expr, cur_params = _wrap_single_eval(
                            cur_expr, cur_params, _ren_body, counter
                        )

                case "$pop":
                    pop_paths: list[str] = []
                    for field, pop_dir in value.items():
                        if not _is_numeric_value(pop_dir) or int(
                            pop_dir
                        ) not in (-1, 1):
                            return None
                        idx = "[0]" if int(pop_dir) < 0 else "[#-1]"
                        pop_paths.append(f"'{parse_json_path(field)}{idx}'")
                    if pop_paths:
                        cur_expr = (
                            f"{remove_func}({cur_expr}, {', '.join(pop_paths)})"
                        )

                case "$push":
                    for field, push_val in value.items():
                        slice_val = None
                        pos_val = None
                        if isinstance(push_val, dict) and "$each" in push_val:
                            slice_val = push_val.get("$slice")
                            pos_val = push_val.get("$position")
                            raw_each = push_val["$each"]
                            values_to_push = (
                                raw_each
                                if isinstance(raw_each, list)
                                else [raw_each]
                            )
                        else:
                            if isinstance(push_val, dict) and (
                                "$slice" in push_val or "$position" in push_val
                            ):
                                return None
                            values_to_push = [push_val]

                        if pos_val is not None or slice_val is not None:
                            tier = "tier2"
                            converted = [
                                _convert_bytes_to_binary(v)
                                for v in values_to_push
                            ]
                            new_json = neosqlite_json_dumps(converted)

                            def _push_ps_body(
                                src: str,
                                f_name: str = field,
                                nj: str = new_json,
                                pv: Any = pos_val,
                                sv: Any = slice_val,
                            ) -> tuple[str, list[Any]]:
                                built = self._build_push_position_slice_clause(
                                    f_name, nj, pv, sv, data_expr=src
                                )
                                if built is None:
                                    return "", []
                                return (
                                    f"{set_func}({src}, {built[0]})",
                                    built[1],
                                )

                            # Probe once with src to confirm supported
                            if (
                                self._build_push_position_slice_clause(
                                    field, new_json, pos_val, slice_val
                                )
                                is None
                            ):
                                return None
                            cur_expr, cur_params = _wrap_single_eval(
                                cur_expr, cur_params, _push_ps_body, counter
                            )
                        else:
                            append_path = f"'{parse_json_path(field)}[#]'"
                            pairs = []
                            push_params: list[Any] = []
                            for pv_item in values_to_push:
                                v_sql, v_params = _format_sql_value_expr(
                                    pv_item
                                )
                                pairs.append(f"{append_path}, {v_sql}")
                                push_params.extend(v_params)
                            if pairs:
                                cur_expr = f"{set_func}({cur_expr}, {', '.join(pairs)})"
                                cur_params.extend(push_params)

                case "$addToSet":
                    tier = "tier2"
                    for field, val in value.items():
                        res = self._build_add_to_set_expr(
                            cur_expr, cur_params, field, val, jsonb, counter
                        )
                        if res is None:
                            return None
                        cur_expr, cur_params = res

                case "$pull":
                    tier = "tier2"
                    for field, pull_val in value.items():
                        res = self._build_pull_expr(
                            cur_expr,
                            cur_params,
                            field,
                            pull_val,
                            jsonb,
                            counter,
                        )
                        if res is None:
                            return None
                        cur_expr, cur_params = res

                case "$pullAll":
                    tier = "tier2"
                    for field, pull_vals in value.items():
                        res = self._build_pull_all_expr(
                            cur_expr,
                            cur_params,
                            field,
                            pull_vals,
                            jsonb,
                            counter,
                        )
                        if res is None:
                            return None
                        cur_expr, cur_params = res

        return cur_expr, cur_params, tier
