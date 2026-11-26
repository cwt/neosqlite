"""SQL-based update operations mixin for QueryHelper.

Extracted from update_operations.py.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..._sqlite import sqlite3
from ...binary import Binary
from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
)
from ..json_path_utils import parse_json_path
from ..jsonb_support import json_constructor, json_function, json_literal
from ..type_utils import (
    _is_numeric_value,
    build_bson_type_sql,
    regex_to_sql_pattern,
)
from .utils import (
    _convert_bytes_to_binary,
    _supports_heap_parser_stack,
    _supports_relative_json_indexing,
    _validate_bit_operand,
)

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


def _has_ambiguous_numeric_path(update_spec: dict[str, Any]) -> bool:
    """Return True when a dotted field segment is numeric without brackets.

    ``parse_json_path`` rewrites ``a.1`` to ``$.a[1]``, but MongoDB treats a
    numeric segment as a literal object key when ``a`` is a document.  The SQL
    tier cannot know the runtime shape, so such updates must use the Python
    tier which handles both arrays and documents correctly.

    Args:
        update_spec: MongoDB update specification to inspect.

    Returns:
        bool: True when at least one field path contains a dotted integer.
    """
    for value in update_spec.values():
        if not isinstance(value, dict):
            continue
        for field in value:
            if not isinstance(field, str):
                continue
            for segment in field.split(".")[1:]:
                if segment.isascii() and segment.isdigit():
                    return True
    return False


def _parse_push_payload(
    field: str, push_value: Any
) -> tuple[list[Any], int | None, int | None] | None:
    """Validate and extract ``$push`` values and ``$position``/``$slice`` modifiers.

    Returns ``None`` when ``$sort`` is present so execution delegates to the
    Python tier, and raises ``MalformedQueryException`` when ``$slice``,
    ``$position``, or ``$sort`` appears without ``$each``.
    """
    if isinstance(push_value, dict) and "$each" in push_value:
        if "$sort" in push_value:
            return None
        raw_each = push_value["$each"]
        values_to_push = raw_each if isinstance(raw_each, list) else [raw_each]
        return (
            values_to_push,
            push_value.get("$position"),
            push_value.get("$slice"),
        )
    if isinstance(push_value, dict):
        for modifier in ("$slice", "$position", "$sort"):
            if modifier in push_value:
                raise MalformedQueryException(
                    f"{modifier} modifier in $push for '{field}' requires $each"
                )
    return [push_value], None, None


def _contains_binary_value(obj: Any) -> bool:
    """Return True if ``obj`` contains a ``Binary`` instance.

    Recursively inspects dicts, lists, and ``bytes`` subclasses carrying
    ``encode_for_storage`` so callers can avoid SQL paths that do not
    safely handle encoded binary payloads.

    Args:
        obj: Value, dictionary, or list to inspect.

    Returns:
        bool: True if a ``Binary``-like object is present anywhere inside
            ``obj``, False otherwise.
    """
    if isinstance(obj, bytes) and hasattr(obj, "encode_for_storage"):
        return True
    if isinstance(obj, dict):
        return any(_contains_binary_value(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_binary_value(v) for v in obj)
    return False


def _format_sql_value_expr(value: Any) -> tuple[str, list[Any]]:
    """Bind a Python value as a SQLite JSON or scalar parameter expression.

    Converts ``bytes`` via ``_convert_bytes_to_binary``. Booleans are wrapped
    with ``json(?)`` so SQLite stores JSON booleans rather than integers;
    scalars (``None``, ``int``, ``float``, ``str``) use ``?``; complex
    objects and ``Binary`` instances are JSON-dumped into ``json(?)``.

    Args:
        value: Python value to bind into a SQL expression.

    Returns:
        tuple[str, list[Any]]: Pair of ``(sql_placeholder, bind_params)``.
    """
    converted = _convert_bytes_to_binary(value)
    if isinstance(converted, bool):
        return "json(?)", ["true" if converted else "false"]
    if converted is None or isinstance(converted, (int, float, str)):
        return "?", [converted]
    return "json(?)", [neosqlite_json_dumps(converted)]


def _je_preserved_value_expr(alias: str) -> str:
    """Preserve JSON types (including booleans and sub-objects) from json_each.

    Builds a SQL ``CASE`` expression over ``<alias>.type`` and
    ``<alias>.value`` so ``json_group_array`` or ``json_set`` retains nested
    objects, arrays, and boolean literals instead of coercing them to text
    or integers.

    Args:
        alias: Table alias or name of the ``json_each`` virtual table.

    Returns:
        str: SQL ``CASE`` expression evaluating to the typed JSON value.
    """
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
    """Extract a JSON value preserving booleans, objects, and arrays.

    Wraps ``<extract_func>(data_expr, json_path)`` in a ``CASE`` on
    ``json_type`` so nested containers and booleans remain valid JSON
    fragments when passed to ``json_set``/``jsonb_set``.

    Args:
        extract_func: SQLite extraction function name (e.g., ``json_extract``
            or ``jsonb_extract``).
        data_expr: SQL expression representing the JSON document.
        json_path: Quoted SQL JSON path string literal (e.g., ``'$.a'``).

    Returns:
        str: SQL ``CASE`` expression producing a type-preserving value.
    """
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
    """Evaluate ``cur_expr`` once via a subquery alias during composition.

    When ``cur_expr`` is already a simple column/alias reference (``"data"``
    or ``"_je*.value"``), ``build_fn`` is invoked directly. Otherwise,
    ``cur_expr`` is bound to a fresh ``_d<n>`` subquery column so ``build_fn``
    can reference the intermediate document multiple times without
    re-evaluating ``cur_expr``.

    Args:
        cur_expr: Current SQL expression producing the document JSON.
        cur_params: Bind parameters required by ``cur_expr``.
        build_fn: Callback taking a source column/alias string and returning
            ``(sql_expr, params)``.
        counter: Single-element mutable list used to generate unique aliases.

    Returns:
        tuple[str, list[Any]]: Composed ``(sql_expr, combined_params)``.
    """
    if cur_expr == "data" or (
        cur_expr.startswith("_je") and cur_expr.endswith(".value")
    ):
        body_sql, body_params = build_fn(cur_expr)
        return body_sql, cur_params + body_params
    alias = f"_d{counter[0]}"
    counter[0] += 1
    body_sql, body_params = build_fn(alias)
    return f"(SELECT {body_sql} FROM (SELECT {cur_expr} AS {alias}))", (
        body_params + cur_params
    )


def _compile_json_scalar_eq(
    type_expr: str, val_expr: str, expected: Any
) -> tuple[str, list[Any]] | None:
    """Compile scalar equality predicate on ``(json_type, value)``.

    Supports ``None``, ``bool``, ``int``, ``float``, and ``str`` while
    checking ``bool`` before numeric types. Non-scalar values return ``None``.

    Args:
        type_expr: SQL expression yielding the SQLite ``json_type`` string.
        val_expr: SQL expression yielding the extracted JSON value.
        expected: Expected Python scalar value to match.

    Returns:
        tuple[str, list[Any]] | None: ``(sql_predicate, params)`` if
            ``expected`` is a supported scalar, else ``None``.
    """
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


def _is_sql_exact_json_array(value: Any) -> bool:
    """Return True when a nested list can be compared accurately via JSON text.

    Restricts leaves to ``None``, exact ``int`` (excluding ``bool``),
    ``str``, and nested lists so SQLite JSON text equality matches
    Python/MongoDB semantics (avoiding ``1`` vs ``1.0`` or dict key-order
    ambiguities).

    Args:
        value: Value to check for exact JSON array comparability.

    Returns:
        bool: True if ``value`` is a list containing only supported exact
            leaves or nested lists, False otherwise.
    """
    if not isinstance(value, list):
        return False
    for item in value:
        if isinstance(item, list):
            if not _is_sql_exact_json_array(item):
                return False
        elif not (item is None or type(item) is int or isinstance(item, str)):
            return False
    return True


def _compile_json_value_eq(
    type_expr: str, val_expr: str, expected: Any
) -> tuple[str, list[Any]] | None:
    """Compile scalar or exact nested-array equality on ``(json_type, value)``.

    Args:
        type_expr: SQL expression yielding the SQLite ``json_type`` string.
        val_expr: SQL expression yielding the extracted JSON value.
        expected: Scalar or exact nested list value to match.

    Returns:
        tuple[str, list[Any]] | None: ``(sql_predicate, params)`` if
            compilable, else ``None``.
    """
    scalar_eq = _compile_json_scalar_eq(type_expr, val_expr, expected)
    if scalar_eq is not None:
        return scalar_eq
    if _is_sql_exact_json_array(expected):
        return (
            f"{type_expr} = 'array' AND {val_expr} = json(?)",
            [neosqlite_json_dumps(expected)],
        )
    return None


def _compile_json_value_operators(
    type_expr: str,
    val_expr: str,
    operators: dict[str, Any],
    *,
    exists_always_true: bool = False,
    safe_obj_expr: str | None = None,
) -> tuple[str, list[Any]] | None:
    """Compile MongoDB query operator dict against a SQLite JSON value.

    Supports ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$in``,
    ``$nin``, ``$exists``, ``$size``, ``$regex`` (with optional ``$options``),
    and ``$type``. Returns ``("1", [])`` for an empty dict or ``None`` if any
    operator or operand cannot be translated safely.

    Args:
        type_expr: SQL expression yielding the SQLite ``json_type`` string.
        val_expr: SQL expression yielding the extracted JSON value.
        operators: Dictionary of MongoDB query operators and operands.
        exists_always_true: When True, ``$exists`` evaluates as a constant
            ``1`` or ``0`` (e.g., for existing array elements).
        safe_obj_expr: Optional SQL expression guarded to be an object JSON
            text value for ``$type`` checks.

    Returns:
        tuple[str, list[Any]] | None: ``(sql_predicate, params)`` joined by
            ``AND``, or ``None`` if unsupported.
    """
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
    """Compile an array-element filter for ``$pull`` or ``$[id]`` filters.

    Handles scalar/exact-array equality, empty filter dicts, pure operator
    dicts, and object sub-field match specifications against a ``json_each``
    row alias. Returns ``None`` for mixed ``$``/field keys, ``$`` in sub-field
    names, or unsupported nested filters.

    Args:
        je_alias: SQLite alias of the ``json_each`` virtual table.
        filter_spec: Element value, operator dict, or object-field filter.

    Returns:
        tuple[str, list[Any]] | None: ``(sql_predicate, params)`` or ``None``
            when Python fallback is required.
    """
    type_col = f"{je_alias}.type"
    val_col = f"{je_alias}.value"
    safe_obj = f"(CASE WHEN {type_col} = 'object' THEN {val_col} ELSE NULL END)"

    if not isinstance(filter_spec, dict):
        return _compile_json_value_eq(type_col, val_col, filter_spec)
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
        sub_safe_obj = (
            f"(CASE WHEN {sub_type} = 'object' THEN {sub_val} ELSE NULL END)"
        )
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
                clauses.append(f"{sub_type} IS NOT NULL AND ({ops_res[0]})")
                params.extend(ops_res[1])
        else:
            eq_res = _compile_json_scalar_eq(sub_type, sub_val, expected)
            if eq_res is None:
                return None
            clauses.append(eq_res[0])
            params.extend(eq_res[1])

    return " AND ".join(clauses), params


class SqlUpdatesMixin:
    """Mixin providing SQL-based update operations.

    Designed to be composed into ``UpdateOperationsMixin``. Translates
    MongoDB update specifications into SQLite JSON/JSONB update expressions
    when direct SQL execution is safe and supported.

    Expected attributes from host class:
        - collection: Target ``Collection`` instance.
        - jsonb: ``JSONBContext`` describing connection capabilities.
        - _get_integer_id_for_oid: Resolver mapping document IDs to row IDs.
    """

    collection: Collection
    jsonb: JSONBContext
    _get_integer_id_for_oid: Any

    @staticmethod
    def _build_push_position_slice_clause(
        field: str,
        values_json: str | None,
        position_value: int | None,
        slice_value: int | None,
        data_expr: str = "data",
    ) -> tuple[str, list[Any]] | None:
        """Build the SET-clause fragment for ``$push`` with $position/$slice.

        Assembles the new array from explicit ordered blocks (front /
        inserted values / tail) so no ``LIMIT`` appears before
        ``UNION ALL`` — the previous template was rejected by SQLite (#97).
        ``$slice`` follows MongoDB semantics including negative counts, which
        keep the LAST ``n`` elements instead of everything (#98).

        Args:
            field: Target field name.
            values_json: JSON text array of converted values to insert, or
                ``None`` when only slicing is requested.
            position_value: ``$position`` index (negative counts from end),
                or ``None`` to append at the end.
            slice_value: ``$slice`` limit (negative keeps the tail), or
                ``None`` for no truncation.
            data_expr: SQL expression representing the source JSON document.
                Defaults to ``"data"``.

        Returns:
            tuple[str, list[Any]] | None: ``("<json_path>, <expr>", params)``
                fragment and bind parameters, or ``None`` if Python fallback
                should be used.
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

    def _validate_inc_mul_types_sql(
        db: Any,
        collection_name: str,
        where_clause: str | None,
        where_params: list[Any],
        update: dict[str, Any],
        jsonb_supported: bool,
    ) -> bool:
        """Validate that fields in ``$inc``/``$mul`` operations are numeric.

        Queries ``json_type`` on the first matching row for each field
        referenced by ``$inc`` or ``$mul`` and verifies every existing value is
        numeric or JSON null.

        Args:
            db: SQLite database connection.
            collection_name: Name of the target collection table.
            where_clause: Translated SQL ``WHERE`` clause, or ``None``.
            where_params: Bind parameters for ``where_clause``.
            update: MongoDB update specification dictionary.
            jsonb_supported: Whether JSONB is enabled on ``db``.

        Returns:
            bool: True if all checked fields are numeric, null, or missing
                (or no row matches); False if a non-numeric type or SQL error
                occurs.
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
        """Validate ``$inc``/``$mul``/``$bit`` operands and DB field types via SQL.

        Ensures every target field path is non-empty without ``$``, validates
        operands, and verifies existing database values are compatible with SQL
        arithmetic or bitwise operators.

        Args:
            where_clause: SQL ``WHERE`` clause string (may be empty).
            where_params: Bind parameters for ``where_clause``.
            update: MongoDB update specification dictionary.
            order_by: Optional SQL ``ORDER BY`` clause used when
                ``limit_one`` is True.
            limit_one: If True, inspect only the first matching row; if False,
                verify no matching row has an incompatible field type.

        Returns:
            bool: True if operands and existing DB field types are valid for
                SQL execution, False otherwise.
        """
        num_fields: list[str] = []
        bit_fields: list[str] = []
        for op in ("$inc", "$mul"):
            op_val = update.get(op)
            if isinstance(op_val, dict):
                for field, operand in op_val.items():
                    if "$" in field or not field:
                        return False
                    if not _is_numeric_value(operand):
                        return False
                    num_fields.append(field)

        bit_val = update.get("$bit")
        if isinstance(bit_val, dict):
            for field, bit_op in bit_val.items():
                if "$" in field or not field:
                    return False
                _validate_bit_operand(bit_op)
                bit_fields.append(field)

        if not num_fields and not bit_fields:
            return True

        tbl = quote_table_name(self.collection.name)
        if limit_one:
            all_checks = [
                (f, ("integer", "real", "null", "number")) for f in num_fields
            ] + [(f, ("integer", "null")) for f in bit_fields]
            select_exprs = ", ".join(
                f"json_type(data, '{parse_json_path(f)}')"
                for f, _ in all_checks
            )
            sql = (
                f"SELECT {select_exprs} FROM {tbl} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            try:
                row = self.collection.db.execute(sql, where_params).fetchone()
            except Exception as e:
                logger.debug(f"SQL $inc/$mul/$bit validation failed: {e}")
                return False
            if row is None:
                return True
            return all(
                ft is None or ft in allowed
                for ft, (_, allowed) in zip(row, all_checks)
            )

        bad_preds = [
            f"(json_type(data, '{parse_json_path(f)}') IS NOT NULL "
            f"AND json_type(data, '{parse_json_path(f)}') NOT IN ('integer', 'real', 'null'))"
            for f in num_fields
        ] + [
            f"(json_type(data, '{parse_json_path(f)}') IS NOT NULL "
            f"AND json_type(data, '{parse_json_path(f)}') NOT IN ('integer', 'null'))"
            for f in bit_fields
        ]
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
            logger.debug(f"SQL $inc/$mul/$bit multi-row validation failed: {e}")
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
        """Compile a ``$set`` field path with ``$[]`` or ``$[id]`` to SQL.

        Recursively handles nested positional segments, validates intermediate
        object types for tail paths, and treats missing ``arrayFilters``
        bindings as no-ops to match Python fallback behavior. Returns ``None``
        for unsupported ``$`` positional operators or invalid path segments.

        Args:
            cur_expr: Current SQL expression yielding the document JSON.
            cur_params: Bind parameters associated with ``cur_expr``.
            parts: Dot-split components of the ``$set`` field path.
            value: Target value to assign at matched positions.
            array_filters: Optional list of ``arrayFilters`` binding dicts.
            jsonb_supported: Whether JSONB functions should be emitted.
            counter: Single-element mutable list for unique SQL aliases.

        Returns:
            tuple[str, list[Any]] | None: Updated ``(sql_expr, params)`` or
                ``None`` if Python fallback is needed.
        """
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

        set_func = json_function("set", jsonb_supported)
        ga_func = json_function("group_array", jsonb_supported)
        unmatched_elem = _je_preserved_value_expr(je)

        if not tail_parts:
            val_sql, val_params = _format_sql_value_expr(value)
            if pos_seg == "$[]":
                elem_expr_sql = val_sql
                elem_params = val_params
            else:
                elem_expr_sql = (
                    f"CASE WHEN {match_sql} "
                    f"THEN {val_sql} ELSE {unmatched_elem} END"
                )
                elem_params = match_params + val_params
        elif any(p == "$[]" or p.startswith("$[") for p in tail_parts):
            inner_res = self._build_positional_set_expr(
                f"{je}.value",
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
            elem_cond = (
                f"{je}.type = 'object'"
                if pos_seg == "$[]"
                else f"({match_sql}) AND {je}.type = 'object'"
            )
            elem_expr_sql = (
                f"CASE WHEN {elem_cond} THEN {inner_sql} "
                f"ELSE {unmatched_elem} END"
            )
            elem_params = match_params + inner_params
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
                    f"THEN {je}.value ELSE NULL END), {inter_path}) = 'object'"
                )
            cond_parts = (
                inter_checks
                if pos_seg == "$[]"
                else [f"({match_sql})", *inter_checks]
            )
            elem_expr_sql = (
                f"CASE WHEN {' AND '.join(cond_parts)} "
                f"THEN {set_func}(json({je}.value), {tail_path}, {val_sql}) "
                f"ELSE {unmatched_elem} END"
            )
            elem_params = match_params + val_params

        arr_path = f"'{parse_json_path(prefix)}'"

        def _build_body(src: str) -> tuple[str, list[Any]]:
            """Build SQL rebuilding the target array for positional ``$set``.

            Args:
                src: SQL expression or alias referencing the document JSON.

            Returns:
                tuple[str, list[Any]]: ``(sql_expr, elem_params)``.
            """
            target_src = (
                f"json({src})"
                if (src.startswith("_je") and src.endswith(".value"))
                else src
            )
            rebuilt = json_constructor(
                f"(SELECT {ga_func}({elem_expr_sql}) "
                f"FROM json_each({src}, {arr_path}) AS {je})",
                jsonb_supported,
            )
            sql = (
                f"CASE WHEN json_type({src}, {arr_path}) = 'array' "
                f"THEN {set_func}({target_src}, {arr_path}, {rebuilt}) "
                f"ELSE {target_src} END"
            )
            return sql, elem_params

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
        """Compile scalar or condition ``$pull`` (top-level or dotted) to SQL.

        Leaves non-array target fields untouched and rejects ``Binary``
        payloads or field paths containing ``$``.

        Args:
            cur_expr: Current SQL expression yielding the document JSON.
            cur_params: Bind parameters associated with ``cur_expr``.
            field: Target array field path.
            pull_value: Scalar, exact array, or operator/subdocument filter
                matching elements to remove.
            jsonb_supported: Whether JSONB functions should be emitted.
            counter: Single-element mutable list for unique SQL aliases.

        Returns:
            tuple[str, list[Any]] | None: Updated ``(sql_expr, params)`` or
                ``None`` if Python fallback is needed.
        """
        if _contains_binary_value(pull_value) or not field or "$" in field:
            return None
        je = f"_je{counter[0]}"
        counter[0] += 1
        pred = _compile_elem_predicate(je, pull_value)
        if pred is None:
            return None
        pred_sql, pred_params = pred
        json_path = f"'{parse_json_path(field)}'"
        set_func = json_function("set", jsonb_supported)
        ga_func = json_function("group_array", jsonb_supported)
        empty_arr = json_literal("[]", jsonb_supported)

        def _build_body(src: str) -> tuple[str, list[Any]]:
            """Build SQL filtering matched ``$pull`` elements from an array.

            Args:
                src: SQL expression or alias referencing the document JSON.

            Returns:
                tuple[str, list[Any]]: ``(sql_expr, pred_params)``.
            """
            filtered = json_constructor(
                f"COALESCE((SELECT {ga_func}({_je_preserved_value_expr(je)}) "
                f"FROM json_each({src}, {json_path}) AS {je} "
                f"WHERE NOT COALESCE(({pred_sql}), 0)), {empty_arr})",
                jsonb_supported,
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
        """Compile ``$pullAll`` into a type-safe SQL array filter expression.

        Rejects ``Binary`` payloads, empty/``$`` field paths, and non-list
        ``pull_values``. An empty ``pull_values`` sequence is a no-op.

        Args:
            cur_expr: Current SQL expression yielding the document JSON.
            cur_params: Bind parameters associated with ``cur_expr``.
            field: Target array field path.
            pull_values: List or tuple of element values to remove.
            jsonb_supported: Whether JSONB functions should be emitted.
            counter: Single-element mutable list for unique SQL aliases.

        Returns:
            tuple[str, list[Any]] | None: Updated ``(sql_expr, params)`` or
                ``None`` if Python fallback is needed.
        """
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
            eq_res = _compile_json_value_eq(f"{je}.type", f"{je}.value", pv)
            if eq_res is None:
                return None
            cond_sqls.append(f"({eq_res[0]})")
            cond_params.extend(eq_res[1])
        combined = " OR ".join(cond_sqls)
        json_path = f"'{parse_json_path(field)}'"
        set_func = json_function("set", jsonb_supported)
        ga_func = json_function("group_array", jsonb_supported)
        empty_arr = json_literal("[]", jsonb_supported)

        def _build_body(src: str) -> tuple[str, list[Any]]:
            """Build SQL filtering ``$pullAll`` values from a JSON array.

            Args:
                src: SQL expression or alias referencing the document JSON.

            Returns:
                tuple[str, list[Any]]: ``(sql_expr, cond_params)``.
            """
            filtered = json_constructor(
                f"COALESCE((SELECT {ga_func}({_je_preserved_value_expr(je)}) "
                f"FROM json_each({src}, {json_path}) AS {je} "
                f"WHERE NOT COALESCE(({combined}), 0)), {empty_arr})",
                jsonb_supported,
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
        """Compile ``$addToSet`` (with optional ``$each``) into SQL.

        Supports scalar and ``Binary`` elements, creates a new single-element
        array when the target field is missing, and returns ``None`` for
        empty/``$`` field paths, more than 20 ``$each`` values, or complex
        non-scalar/non-Binary items.

        Args:
            cur_expr: Current SQL expression yielding the document JSON.
            cur_params: Bind parameters associated with ``cur_expr``.
            field: Target array field path.
            val: Value to add, or ``{"$each": ...}`` modifier dict.
            jsonb_supported: Whether JSONB functions should be emitted.
            counter: Single-element mutable list for unique SQL aliases.

        Returns:
            tuple[str, list[Any]] | None: Updated ``(sql_expr, params)`` or
                ``None`` if Python fallback is needed.
        """
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
        set_func = json_function("set", jsonb_supported)
        insert_func = json_function("insert", jsonb_supported)
        array_func = json_function("array", jsonb_supported)

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
                """Build SQL inserting one element when absent from an array.

                Args:
                    src: SQL expression or alias referencing the document.
                    vs: SQL placeholder expression for the candidate value.
                    vp: Bind parameters for ``vs``.
                    ec: SQL existence predicate against ``je_name``.
                    ep: Bind parameters for ``ec``.
                    je_name: Alias of the ``json_each`` virtual table.

                Returns:
                    tuple[str, list[Any]]: ``(sql_expr, params)``.
                """
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
        """Build a composable SQL expression for ``SET data = <expr>``.

        Translates all operators in ``update_spec`` into a nested SQL JSON or
        JSONB expression suitable for single-statement ``UPDATE`` execution,
        tracking whether any tier-2 feature (positional ``$set``, ``$push``
        with ``$position``/``$slice``, ``$addToSet``, ``$pull``, ``$pullAll``)
        was used. Returns ``None`` when Python fallback is required.

        Args:
            update_spec: MongoDB update specification dictionary.
            array_filters: Optional list of ``arrayFilters`` dictionaries used
                by ``$[identifier]`` positional paths in ``$set``.

        Returns:
            tuple[str, list[Any], str] | None: ``(data_expr, params, tier)``
                where ``tier`` is ``"tier1"`` or ``"tier2"``, or ``None`` if
                Python fallback is required.
        """
        if not isinstance(update_spec, dict) or not update_spec:
            return None
        if _has_ambiguous_numeric_path(update_spec):
            return None

        # $push/$addToSet use the JSON append path [#], which requires
        # SQLite 3.42+ (same relative-indexing feature gate as $pop).
        if any(op in update_spec for op in ("$push", "$addToSet")) and not (
            _supports_relative_json_indexing()
        ):
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
        set_func = json_function("set", jsonb)
        remove_func = json_function("remove", jsonb)
        extract_func = json_function("extract", jsonb)

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
                    if (
                        not isinstance(field, str)
                        or not field
                        or "$" in field
                        or field == "_id"
                    ):
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
                            """Build SQL arithmetic update for one field.

                            Args:
                                src: SQL expression or alias for the document.
                                p: Quoted JSON path literal for the field.
                                a: Arithmetic operator (``"+"`` or ``"*"``).
                                val: Numeric operand to bind.

                            Returns:
                                tuple[str, list[Any]]: ``(sql_expr, params)``.
                            """
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
                        allowed_types = (
                            "('integer', 'real')"
                            if _is_numeric_value(field_val)
                            else "('text')"
                        )

                        def _cmp_body(
                            src: str,
                            p: str = jp,
                            f_name: str = fn,
                            val: Any = field_val,
                            types_sql: str = allowed_types,
                        ) -> tuple[str, list[Any]]:
                            """Build SQL ``min``/``max`` conditional update.

                            Args:
                                src: SQL expression or alias for the document.
                                p: Quoted JSON path literal for the field.
                                f_name: SQL aggregate function (``"min"`` or
                                    ``"max"``).
                                val: Comparison scalar value to bind.
                                types_sql: SQL tuple of compatible
                                    ``json_type`` names.

                            Returns:
                                tuple[str, list[Any]]: ``(sql_expr, params)``.
                            """
                            ext = f"{extract_func}({src}, {p})"
                            preserved = _extract_preserved_json_expr(
                                extract_func, src, p
                            )
                            val_expr = (
                                f"CASE WHEN json_type({src}, {p}) IS NULL "
                                f"OR json_type({src}, {p}) = 'null' THEN ? "
                                f"WHEN json_type({src}, {p}) IN {types_sql} "
                                f"THEN {f_name}({ext}, ?) "
                                f"ELSE {preserved} END"
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
                        _validate_bit_operand(bit_spec)
                        jp = f"'{parse_json_path(field)}'"

                        def _bit_body(
                            src: str, p: str = jp, bs: dict[str, Any] = bit_spec
                        ) -> tuple[str, list[Any]]:
                            """Build SQL bitwise update expression for a field.

                            Args:
                                src: SQL expression or alias for the document.
                                p: Quoted JSON path literal for the field.
                                bs: Bitwise spec dict (``and``/``or``/``xor``).

                            Returns:
                                tuple[str, list[Any]]: ``(sql_expr, params)``.
                            """
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
                        if (
                            not isinstance(new_field, str)
                            or not new_field
                            or "$" in new_field
                            or new_field == "_id"
                        ):
                            return None
                        if old_field == new_field:
                            continue
                        old_p = f"'{parse_json_path(old_field)}'"
                        new_p = f"'{parse_json_path(new_field)}'"

                        def _ren_body(
                            src: str, op_p: str = old_p, np_p: str = new_p
                        ) -> tuple[str, list[Any]]:
                            """Build SQL conditional field rename expression.

                            Args:
                                src: SQL expression or alias for the document.
                                op_p: Quoted JSON path of the source field.
                                np_p: Quoted JSON path of the destination
                                    field.

                            Returns:
                                tuple[str, list[Any]]: ``(sql_expr, params)``.
                            """
                            preserved = _extract_preserved_json_expr(
                                extract_func, src, op_p
                            )
                            renamed = (
                                f"{set_func}({remove_func}({src}, {op_p}), "
                                f"{np_p}, {preserved})"
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
                        parsed = _parse_push_payload(field, push_val)
                        if parsed is None:
                            return None
                        values_to_push, pos_val, slice_val = parsed

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
                                """Build SQL ``$push`` modifier expression.

                                Args:
                                    src: SQL expression or alias for the
                                        document.
                                    f_name: Target array field path.
                                    nj: JSON-encoded array of values to push.
                                    pv: Optional ``$position`` modifier value.
                                    sv: Optional ``$slice`` modifier value.

                                Returns:
                                    tuple[str, list[Any]]: ``(sql_expr,
                                        params)``.
                                """
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

        # SQLite before 3.46 uses a fixed 100-entry expression parser stack.
        # A deeply nested chain of json_set()/CASE fragments (for example
        # $rename + $min + $max + $bit + $currentDate in one update) can
        # overflow it and fail at prepare time with "parser stack overflow".
        # Let the real parser decide; if it declines, fall back to the
        # per-document Python update path used by all callers of this method.
        if not _supports_heap_parser_stack():
            try:
                self.collection.db.execute(
                    f"EXPLAIN SELECT ({cur_expr})", cur_params
                )
            except sqlite3.OperationalError as exc:
                if "parser stack overflow" in str(exc).lower():
                    logger.warning(
                        "SQLite update expression exceeds the parser stack "
                        "on %s; falling back to Python updates",
                        sqlite3.sqlite_version,
                    )
                    return None
                logger.debug("Update EXPLAIN probe skipped: %s", exc)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Update EXPLAIN probe failed: %s", exc)

        return cur_expr, cur_params, tier
