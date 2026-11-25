"""SQL converters for object operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...json_path_utils import parse_json_path

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ObjectMixin(BaseSqlMixin):
    """Mixin converting MongoDB object operators to SQL expressions.

    Translates ``$mergeObjects``, ``$getField``, ``$setField``,
    ``$unsetField``, ``$objectToArray``, ``$literal``, ``$rand``, and
    ``$let`` into SQLite JSON/JSONB function calls and bound parameters.
    """

    def _convert_object_literal_to_sql(self, obj: Any) -> tuple[str, list[Any]]:
        """Convert an object expression operand, evaluating nested values.

        A plain dict used as a ``$mergeObjects`` argument is an object
        expression: its field values are themselves expressions and must be
        evaluated (for example ``{"total": {"$sum": "$values"}}``). Single-key
        dicts with a ``$`` operator and field/variable references are handled
        by the normal operand converter.

        Args:
            obj: Object expression operand to convert.

        Returns:
            tuple[str, list[Any]]: SQL expression and bound parameters.
        """
        if (
            isinstance(obj, dict)
            and obj
            and not next(iter(obj)).startswith("$")
            and self._contains_expression_value(obj)
        ):
            parts: list[str] = []
            params: list[Any] = []
            for key, value in obj.items():
                escaped_key = str(key).replace("'", "''")
                value_sql, value_params = self._convert_operand_to_sql(value)
                parts.extend([f"'{escaped_key}'", value_sql])
                params.extend(value_params)
            return f"json_object({', '.join(parts)})", params
        return self._convert_operand_to_sql(obj)

    def _contains_expression_value(self, value: Any) -> bool:
        """Return whether a literal value contains an expression to evaluate."""
        if isinstance(value, str):
            return value.startswith("$")
        if isinstance(value, dict):
            if value and str(next(iter(value))).startswith("$"):
                return True
            return any(
                self._contains_expression_value(v) for v in value.values()
            )
        if isinstance(value, list):
            return any(self._contains_expression_value(v) for v in value)
        return False

    def _convert_object_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert a MongoDB object operator to a SQL expression.

        Supports ``$mergeObjects`` (via ``json_patch``), ``$getField``,
        ``$setField``, ``$unsetField``, ``$objectToArray``, ``$literal``,
        and ``$rand``. Uses ``json_function_prefix`` for extract, set, and
        remove functions, while ``json_patch()`` accepts both JSON and JSONB.

        Args:
            operator: Object operator name (e.g., ``"$getField"``,
                ``"$mergeObjects"``, ``"$literal"``).
            operands: Operator arguments (list, dict, or literal value
                depending on ``operator``).

        Returns:
            tuple[str, list[Any]]: A ``(sql_expression, parameters)`` pair.

        Raises:
            ValueError: If ``operands`` lacks required fields or has an
                invalid type for the operator.
            NotImplementedError: If ``operator`` is not supported in the SQL
                tier.
        """
        json_prefix = self.json_function_prefix

        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list) or len(operands) < 1:
                    raise ValueError("$mergeObjects requires a list of objects")
                mo_id = getattr(self, "_mo_counter", 0) + 1
                self._mo_counter = mo_id
                json_each = self.json_each_function
                operand_ctes: list[str] = []
                entry_unions: list[str] = []
                all_params: list[Any] = []
                for idx, obj in enumerate(operands):
                    obj_sql, obj_params = self._convert_object_literal_to_sql(
                        obj
                    )
                    cte_name = f"_mo_{mo_id}_{idx}"
                    operand_ctes.append(f"{cte_name}(v) AS (SELECT {obj_sql})")
                    valid_expr = (
                        f"typeof({cte_name}.v) IN ('text', 'blob') "
                        f"AND json_valid({cte_name}.v, 5)"
                        if self.jsonb.jsonb_supported
                        else f"typeof({cte_name}.v) = 'text' "
                        f"AND json_valid({cte_name}.v)"
                    )
                    entry_unions.append(
                        f"SELECT {idx} AS idx, j.key AS k, j.value AS val, j.type AS tp "
                        f"FROM {cte_name}, {json_each}(CASE WHEN {valid_expr} "
                        f"AND json_type({cte_name}.v) = 'object' "
                        f"THEN {cte_name}.v ELSE '{{}}' END) AS j"
                    )
                    all_params.extend(obj_params)
                ctes_sql = ", ".join(operand_ctes)
                unions_sql = " UNION ALL ".join(entry_unions)
                sql = (
                    f"(WITH {ctes_sql}, "
                    f"_me_{mo_id} AS ({unions_sql}), "
                    f"_mf_{mo_id} AS ("
                    f"SELECT k, val, tp, "
                    f"ROW_NUMBER() OVER (PARTITION BY k ORDER BY idx DESC) AS rn "
                    f"FROM _me_{mo_id}) "
                    f"SELECT json_group_object(k, "
                    f"CASE WHEN tp = 'true' THEN json('true') "
                    f"WHEN tp = 'false' THEN json('false') "
                    f"WHEN tp = 'null' THEN json('null') "
                    f"WHEN tp IN ('object', 'array') THEN json(val) "
                    f"ELSE val END) "
                    f"FROM _mf_{mo_id} WHERE rn = 1)"
                )
                return sql, all_params
            case "$getField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError("$getField requires 'field' specification")
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                sql = f"{json_prefix}_extract({input_sql}, '{parse_json_path(field)}')"
                return sql, input_params
            case "$setField":
                if not isinstance(operands, dict):
                    raise ValueError("$setField requires a dictionary")
                field = operands.get("field")
                value = operands.get("value")
                input_val = operands.get("input")
                if field is None:
                    raise ValueError("$setField requires 'field'")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                value_sql, value_params = self._convert_operand_to_sql(value)
                sql = f"{json_prefix}_set({input_sql}, '{parse_json_path(field)}', {value_sql})"
                return sql, input_params + value_params
            case "$unsetField":
                if not isinstance(operands, dict) or "field" not in operands:
                    raise ValueError(
                        "$unsetField requires 'field' specification"
                    )
                field = operands["field"]
                input_val = operands.get("input")
                if input_val is not None:
                    input_sql, input_params = self._convert_operand_to_sql(
                        input_val
                    )
                else:
                    input_sql, input_params = self.data_column, []
                # Use json_remove to remove field
                sql = f"{json_prefix}_remove({input_sql}, '{parse_json_path(field)}')"
                return sql, input_params
            case "$objectToArray":
                # Convert object to array of {k, v} objects
                # Syntax: { $objectToArray: <object> }
                sql_input, params = self._convert_operand_to_sql(operands)
                json_group_array = self.json_group_array_function
                json_each = self.json_each_function
                oa_id = getattr(self, "_oa_counter", 0) + 1
                self._oa_counter = oa_id
                valid_expr = (
                    f"typeof(_oa_{oa_id}.v) IN ('text', 'blob') "
                    f"AND json_valid(_oa_{oa_id}.v, 5)"
                    if self.jsonb.jsonb_supported
                    else f"typeof(_oa_{oa_id}.v) = 'text' "
                    f"AND json_valid(_oa_{oa_id}.v)"
                )

                sql = (
                    f"(WITH _oa_{oa_id}(v) AS (SELECT {sql_input}) "
                    f"SELECT json((SELECT {json_group_array}(json_object('k', j.key, 'v', "
                    f"CASE WHEN j.type = 'true' THEN json('true') "
                    f"WHEN j.type = 'false' THEN json('false') "
                    f"WHEN j.type IN ('object', 'array') THEN json(j.value) "
                    f"ELSE j.value END)) "
                    f"FROM _oa_{oa_id}, {json_each}(CASE WHEN {valid_expr} "
                    f"AND json_type(_oa_{oa_id}.v) = 'object' "
                    f"THEN _oa_{oa_id}.v ELSE '{{}}' END) AS j)))"
                )
                return sql, params
            case "$literal":
                # Return operand as-is without interpretation.
                # Handles $ prefix strings and expression-looking dicts correctly.
                if isinstance(operands, str) and operands.startswith("$"):
                    return "?", [operands]
                if isinstance(operands, (dict, list)):
                    from neosqlite.collection.json_helpers import (
                        neosqlite_json_dumps,
                    )

                    return "json(?)", [neosqlite_json_dumps(operands)]
                return "?", [operands]
            case "$rand":
                # SQLite random() returns integer -64..64 bits.
                # Divide to get float in [0, 1) matching Python's random.random().
                return (
                    "(ABS(RANDOM() % 1000000000)) / 1000000000.0",
                    [],
                )
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in SQL tier"
                )

    def _convert_let_operator(self, operands: Any) -> tuple[str, list[Any]]:
        """Convert a ``$let`` operator to SQL by inlining scoped variables.

        Evaluates each binding in ``vars`` against the active aggregation
        context, stores ``$$<name>`` SQL/parameter pairs in a cloned nested
        context, and translates the ``in`` expression with that context
        temporarily active.

        Args:
            operands: Dictionary with optional ``"vars"`` mapping and
                required ``"in"`` expression (matching
                ``{ $let: { vars: { ... }, in: <expr> } }``).

        Returns:
            tuple[str, list[Any]]: A ``(sql_expression, parameters)`` pair
            for the evaluated ``in`` expression.

        Raises:
            ValueError: If ``operands`` is not a dictionary or omits ``"in"``.
            NotImplementedError: If ``_current_context`` is missing or
                ``None``.
        """
        if not isinstance(operands, dict):
            raise ValueError("$let requires a dictionary")

        vars_spec = operands.get("vars", {})
        in_expr = operands.get("in")

        if in_expr is None:
            raise ValueError("$let requires 'in' expression")

        # We need the current context to store variables for inlining
        if (
            not hasattr(self, "_current_context")
            or self._current_context is None
        ):
            # Fallback to Python if no context is available
            raise NotImplementedError(
                "$let requires an aggregation context in SQL tier"
            )

        context = self._current_context
        # Create a new context for nested scoping
        nested_context = context.clone()

        for var_name, var_expr in vars_spec.items():
            # Evaluate the variable expression to SQL
            var_sql, var_params = self._convert_operand_to_sql(var_expr)
            # Store the SQL and params in the nested context
            # var_name should be prefixed with $$
            nested_context.set_variable("$$" + var_name, (var_sql, var_params))

        # Now evaluate 'in' using the nested context by temporarily swapping it
        old_context = self._current_context
        self._current_context = nested_context
        try:
            return self._convert_operand_to_sql(in_expr)
        finally:
            self._current_context = old_context
