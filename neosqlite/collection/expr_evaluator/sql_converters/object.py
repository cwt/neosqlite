"""SQL converters for object operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...json_path_utils import parse_json_path

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ObjectMixin(BaseSqlMixin):
    """$mergeObjects / $getField / $setField / $unsetField / $objectToArray / $let → SQL."""

    def _convert_object_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert object operators to SQL.

        Note: json_patch() works with both JSON and JSONB data types.
        Only json_extract/jsonb_extract, json_set/jsonb_set have JSONB variants.
        """
        json_prefix = self.json_function_prefix

        match operator:
            case "$mergeObjects":
                if not isinstance(operands, list) or len(operands) < 1:
                    raise ValueError("$mergeObjects requires a list of objects")
                sql_parts = []
                all_params = []
                for obj in operands:
                    obj_sql, obj_params = self._convert_operand_to_sql(obj)
                    sql_parts.append(obj_sql)
                    all_params.extend(obj_params)
                # Use json_patch to merge objects (works with both JSON and JSONB)
                if len(sql_parts) == 1:
                    sql = sql_parts[0]
                else:
                    sql = f"json_patch({sql_parts[0]}, {sql_parts[1]})"
                    for part in sql_parts[2:]:
                        sql = f"json_patch({sql}, {part})"
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

                # Use a subquery with json_each to build the array
                sql = f"(SELECT json({json_group_array}(json_object('k', key, 'v', value))) FROM {json_each}({sql_input}))"
                return sql, params
            case _:
                raise NotImplementedError(
                    f"Object operator {operator} not supported in SQL tier"
                )

    def _convert_let_operator(self, operands: Any) -> tuple[str, list[Any]]:
        """
        Convert $let operator to SQL by inlining variables.

        MongoDB syntax: { $let: { vars: { <var1>: <expr1>, ... }, in: <expr> } }
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
