"""SQL converters for array operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...json_path_utils import parse_json_path

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class ArrayMixin(BaseSqlMixin):
    """$size / $in / $isArray / $slice / $indexOfArray / $sum / $avg → SQL."""

    def _convert_array_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert array operators to SQL."""
        # Get the appropriate function names based on SQLite version
        json_each = self.json_each_function
        json_group_array = self.json_group_array_function

        # Normalize operands for operators that accept single values
        if operator in (
            "$size",
            "$isArray",
            "$sum",
            "$avg",
            "$min",
            "$max",
            "$first",
            "$last",
        ) and not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$size":
                if len(operands) != 1:
                    raise ValueError("$size requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"json_array_length({array_sql})"
                return sql, array_params
            case "$in":
                if len(operands) != 2:
                    raise ValueError("$in requires exactly 2 operands")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[1]
                )
                self._je_counter = getattr(self, "_je_counter", 0) + 1
                in_alias = f"_inj_{self._je_counter}"
                sql = (
                    f"EXISTS (SELECT 1 FROM {json_each}({array_sql}) AS {in_alias} "
                    f"WHERE {in_alias}.value = {value_sql})"
                )
                # Bind in SQL order: array placeholders come first.
                return sql, array_params + value_params
            case "$isArray":
                if len(operands) != 1:
                    raise ValueError("$isArray requires exactly 1 operand")

                operand = operands[0]
                if isinstance(operand, str) and operand.startswith("$"):
                    field_path = operand[1:]
                    sql = f"CASE WHEN json_type({self.data_column}, '{parse_json_path(field_path)}') = 'array' THEN 1 ELSE 0 END"
                    return sql, []
                else:
                    value_sql, value_params = self._convert_operand_to_sql(
                        operand
                    )
                    sql = f"CASE WHEN json_type({value_sql}) = 'array' THEN 1 ELSE 0 END"
                    return sql, value_params
            case "$sum" | "$avg" | "$min" | "$max":
                if len(operands) != 1:
                    raise ValueError(f"{operator} requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql_agg = operator[1:].upper()
                if operator in ("$sum", "$avg"):
                    sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}) WHERE typeof(value) IN ('integer', 'real'))"
                else:
                    sql = f"(SELECT {sql_agg}(value) FROM {json_each}({array_sql}))"
                return sql, array_params
            case "$slice":
                if not isinstance(operands, list) or len(operands) < 2:
                    raise ValueError("$slice requires array and count/position")
                array_operand = operands[0]
                array_sql, array_params = self._convert_operand_to_sql(
                    array_operand
                )
                count_operand = operands[1]
                skip_operand = operands[2] if len(operands) > 2 else 0
                has_skip_arg = len(operands) > 2
                elem_expr = "CASE WHEN type IN ('object', 'array') THEN json(value) ELSE value END"
                is_arr_sql = self._array_type_check_sql(
                    array_operand, "raw_arr"
                )
                self._slice_counter = getattr(self, "_slice_counter", 0) + 1
                s_id = self._slice_counter

                # Fast path for non-negative literal integers on 2 or 3 args
                if (
                    not isinstance(count_operand, bool)
                    and isinstance(count_operand, int)
                    and not isinstance(skip_operand, bool)
                    and isinstance(skip_operand, int)
                ):
                    if not has_skip_arg and count_operand >= 0:
                        safe_arr = f"(CASE WHEN {self._array_type_check_sql(array_operand, 'raw_arr')} THEN raw_arr ELSE json('[]') END)"
                        sql = (
                            f"json((WITH _sr_{s_id}(raw_arr) AS (SELECT {array_sql}) "
                            f"SELECT {json_group_array}({elem_expr} ORDER BY CAST(key AS INTEGER) ASC) "
                            f"FROM (SELECT value, key, type FROM {json_each}((SELECT {safe_arr} FROM _sr_{s_id})) LIMIT {count_operand})))"
                        )
                        return sql, array_params
                    if not has_skip_arg and count_operand < 0:
                        safe_arr = f"(CASE WHEN {is_arr_sql} THEN raw_arr ELSE json('[]') END)"
                        sql = (
                            f"json((WITH _sr_{s_id}(raw_arr) AS (SELECT {array_sql}) "
                            f"SELECT {json_group_array}({elem_expr} ORDER BY CAST(key AS INTEGER) ASC) "
                            f"FROM (SELECT value, key, type FROM {json_each}((SELECT {safe_arr} FROM _sr_{s_id})) "
                            f"ORDER BY CAST(key AS INTEGER) DESC LIMIT {abs(count_operand)})))"
                        )
                        return sql, array_params
                    if has_skip_arg and count_operand >= 0 and skip_operand >= 0:
                        safe_arr = f"(CASE WHEN {is_arr_sql} THEN raw_arr ELSE json('[]') END)"
                        sql = (
                            f"json((WITH _sr_{s_id}(raw_arr) AS (SELECT {array_sql}) "
                            f"SELECT {json_group_array}({elem_expr} ORDER BY CAST(key AS INTEGER) ASC) "
                            f"FROM (SELECT value, key, type FROM {json_each}((SELECT {safe_arr} FROM _sr_{s_id})) "
                            f"LIMIT {count_operand} OFFSET {skip_operand})))"
                        )
                        return sql, array_params

                # General / dynamic / negative slice via single-pass CTEs matching
                # Python semantics: 2-arg [:c] / [c:], 3-arg [s : s + c]
                count_sql, count_params = self._convert_operand_to_sql(
                    count_operand
                )
                if has_skip_arg:
                    skip_sql, skip_params = self._convert_operand_to_sql(
                        skip_operand
                    )
                    # Avoid duplicating skip_params by computing c, s in CTE first
                    bounds_cte = (
                        f"_sp_{s_id}(c, s, n) AS ("
                        f"SELECT CAST({count_sql} AS INTEGER), "
                        f"CAST({skip_sql} AS INTEGER), "
                        f"(SELECT COUNT(*) FROM _se_{s_id})"
                        f")"
                    )
                    where_slice = (
                        "k >= (CASE WHEN s < 0 THEN MAX(n + s, 0) ELSE MIN(s, n) END) "
                        "AND k < (CASE WHEN (s + c) < 0 THEN MAX(n + s + c, 0) ELSE MIN(s + c, n) END)"
                    )
                    slice_params = array_params + count_params + skip_params
                else:
                    bounds_cte = (
                        f"_sp_{s_id}(c, n) AS ("
                        f"SELECT CAST({count_sql} AS INTEGER), "
                        f"(SELECT COUNT(*) FROM _se_{s_id})"
                        f")"
                    )
                    where_slice = (
                        "(c >= 0 AND k < c) OR (c < 0 AND k >= MAX(n + c, 0))"
                    )
                    slice_params = array_params + count_params

                safe_arr = f"(CASE WHEN {is_arr_sql} THEN raw_arr ELSE json('[]') END)"
                sql = (
                    f"json((WITH "
                    f"_sr_{s_id}(raw_arr) AS (SELECT {array_sql}), "
                    f"_se_{s_id}(value, k, type) AS ("
                    f"SELECT value, CAST(key AS INTEGER), type "
                    f"FROM {json_each}((SELECT {safe_arr} FROM _sr_{s_id}))), "
                    f"{bounds_cte} "
                    f"SELECT {json_group_array}({elem_expr} ORDER BY k ASC) "
                    f"FROM _se_{s_id}, _sp_{s_id} "
                    f"WHERE {where_slice}))"
                )
                return sql, slice_params
            case "$indexOfArray":
                if len(operands) != 2:
                    raise ValueError(
                        "$indexOfArray requires exactly 2 operands"
                    )
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[1]
                )
                sql = f"(SELECT COALESCE((SELECT key FROM {json_each}({array_sql}) WHERE value = {value_sql} LIMIT 1), -1))"
                return sql, array_params + value_params
            case "$arrayElemAt":
                if not isinstance(operands, list) or len(operands) != 2:
                    raise ValueError("$arrayElemAt requires exactly 2 operands")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # Literal index: use SQLite JSON path $[N] / $[#-N] directly
                jfx = self.json_function_prefix + "_extract"
                if isinstance(operands[1], int):
                    idx = operands[1]
                    if idx >= 0:
                        sql = f"{jfx}({array_sql}, '$[{idx}]')"
                    else:
                        sql = f"{jfx}({array_sql}, '$[#-{abs(idx)}]')"
                    return sql, array_params
                # Dynamic index: use json_each to avoid parameter duplication
                idx_sql, idx_params = self._convert_operand_to_sql(operands[1])
                json_each = self.json_each_function
                sql = (
                    f"(SELECT value FROM {json_each}({array_sql})"
                    f" WHERE CAST(key AS INTEGER) = CAST({idx_sql} AS INTEGER)"
                    f" LIMIT 1)"
                )
                return sql, array_params + idx_params
            case "$first":
                if len(operands) != 1:
                    raise ValueError("$first requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                jfx = self.json_function_prefix + "_extract"
                sql = f"{jfx}({array_sql}, '$[0]')"
                return sql, array_params
            case "$last":
                if len(operands) != 1:
                    raise ValueError("$last requires exactly 1 operand")
                array_sql, array_params = self._convert_operand_to_sql(
                    operands[0]
                )
                jfx = self.json_function_prefix + "_extract"
                # Negative index -1 for last element (SQLite JSON path supports #-1)
                sql = f"{jfx}({array_sql}, '$[#-1]')"
                return sql, array_params
            case "$firstN":
                # MongoDB: { $firstN: { input: <array>, n: <number> } }
                # Also accepts positional: [<array>, <number>]
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$firstN requires input array and n count")
                array_sql, array_params = self._convert_operand_to_sql(
                    array_operand
                )
                # Use literal n if available, otherwise parameterize
                if isinstance(n_operand, int):
                    n_value: int = n_operand
                    n_clause = str(n_value)
                    n_params: list[Any] = []
                else:
                    n_sql, n_params = self._convert_operand_to_sql(n_operand)
                    n_clause = n_sql
                json_ga = self.json_group_array_function
                # Wrap json_group_array subquery with json() so that the outer
                # caller (e.g. json_object) treats it as a JSON array rather than
                # raw text (required on SQLite >= 3.54 where subqueries drop subtype).
                # Include 'key' in the inner SELECT so the ordered aggregate
                # can reference it. Re-parse object/array elements with json().
                elem_expr = "CASE WHEN type IN ('object', 'array') THEN json(value) ELSE value END"
                sql = (
                    f"json((SELECT {json_ga}({elem_expr} ORDER BY CAST(key AS INTEGER) ASC) FROM"
                    f" (SELECT value, key, type FROM {json_each}({array_sql}) LIMIT {n_clause})))"
                )
                return sql, array_params + n_params
            case "$lastN":
                # MongoDB: { $lastN: { input: <array>, n: <number> } }
                if isinstance(operands, dict):
                    array_operand = operands.get("input")
                    n_operand = operands.get("n")
                elif isinstance(operands, list) and len(operands) == 2:
                    array_operand = operands[0]
                    n_operand = operands[1]
                else:
                    raise ValueError("$lastN requires input array and n count")
                array_sql, array_params = self._convert_operand_to_sql(
                    array_operand
                )
                if isinstance(n_operand, int):
                    n_value = n_operand
                    n_clause = str(n_value)
                    n_params = []
                else:
                    n_sql, n_params = self._convert_operand_to_sql(n_operand)
                    n_clause = n_sql
                json_ga = self.json_group_array_function
                # Get last N in original order.
                # Inner: select last N elements (DESC by key).
                # Outer: use ordered aggregate to re-sort by original key ASC.
                elem_expr = "CASE WHEN type IN ('object', 'array') THEN json(value) ELSE value END"
                sql = (
                    f"json((SELECT {json_ga}({elem_expr} ORDER BY CAST(key AS INTEGER) ASC)"
                    f" FROM (SELECT value, key, type"
                    f" FROM {json_each}({array_sql})"
                    f" ORDER BY CAST(key AS INTEGER) DESC LIMIT {n_clause})))"
                )
                return sql, array_params + n_params
            case "$sortArray":
                # MongoDB: { $sortArray: { input: <array>, sortBy: <spec> } }
                # where <spec> is { <field>: <dir> }, 1, or -1.
                if not isinstance(operands, dict):
                    raise ValueError("$sortArray requires a dictionary")
                array_operand = operands.get("input")
                sort_by = operands.get("sortBy")
                if array_operand is None:
                    raise ValueError("$sortArray requires 'input' field")
                array_sql, array_params = self._convert_operand_to_sql(
                    array_operand
                )
                json_ga = self.json_group_array_function
                if sort_by is None or (
                    isinstance(sort_by, (int, float))
                    and not isinstance(sort_by, bool)
                    and sort_by in (1, -1)
                ):
                    # Sort by value.  SQLite json_each converts JSON
                    # true/false to integers, so boolean vs number
                    # distinction is not preserved (same as the
                    # json_group_array approach used by $firstN etc.).
                    order = "DESC" if sort_by == -1 else "ASC"
                    order_expr = "value"
                elif isinstance(sort_by, dict):
                    field = next(iter(sort_by.keys()))
                    direction = sort_by[field]
                    order = "DESC" if direction == -1 else "ASC"
                    # Escape the field path like every other emitter (#119)
                    order_expr = (
                        f"{self.json_function_prefix}_extract("
                        f"value, '{parse_json_path(field)}')"
                    )
                else:
                    raise ValueError(
                        "$sortArray sortBy must be a dict, 1, -1, or null"
                    )
                elem_expr = "CASE WHEN type IN ('object', 'array') THEN json(value) ELSE value END"
                sql = (
                    f"json((SELECT {json_ga}({elem_expr} ORDER BY {order_expr} {order}) FROM"
                    f" {json_each}({array_sql})))"
                )
                return sql, array_params
            case "$maxN":
                # MongoDB: { $maxN: { input: <array>, n: <number> } }
                return self._convert_extreme_n(operands, "DESC")
            case "$minN":
                # MongoDB: { $minN: { input: <array>, n: <number> } }
                return self._convert_extreme_n(operands, "ASC")
            case (
                "$setEquals"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
            ):
                return self._convert_set_operator(operator, operands)
            case _:
                raise NotImplementedError(
                    f"Array operator {operator} not supported in SQL tier"
                )

    def _convert_array_transform_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $filter / $map / $reduce to SQL.

        Uses json_each to iterate array elements and the AggregationContext
        variable mechanism to substitute $$var references with the current
        element column (\"value\") in the SQL expression.
        """
        if not isinstance(operands, dict):
            raise ValueError(f"{operator} requires a dictionary")

        input_operand = operands.get("input")
        as_var = operands.get("as", "this")

        if input_operand is None:
            raise ValueError(f"{operator} requires 'input' field")

        input_sql, input_params = self._convert_operand_to_sql(input_operand)

        json_ga = self.json_group_array_function

        # Build a temporary context where $$var -> aliased json_each column
        # and document fields still reference the outer data column.
        from ..context import AggregationContext

        self._je_counter = getattr(self, "_je_counter", 0) + 1
        je_alias = f"_je_{self._je_counter}"

        old_ctx = self._current_context
        ctx = old_ctx.clone() if old_ctx is not None else AggregationContext()
        ctx.set_variable(f"$${as_var}", (f"{je_alias}.value", []))
        self._current_context = ctx

        try:
            if operator == "$filter":
                cond = operands.get("cond")
                if cond is None:
                    raise ValueError("$filter requires 'cond' expression")
                cond_sql, cond_params = self._convert_expr_to_sql(cond)

                elem_expr = (
                    f"CASE WHEN {je_alias}.type IN ('object', 'array') "
                    f"THEN json({je_alias}.value) ELSE {je_alias}.value END"
                )
                sql = (
                    f"json((SELECT {json_ga}({elem_expr} ORDER BY"
                    f" CAST({je_alias}.key AS INTEGER)) FROM"
                    f" {self.json_each_function}({input_sql}) AS {je_alias}"
                    f" WHERE {cond_sql}))"
                )
                return sql, input_params + cond_params

            elif operator == "$map":
                in_expr = operands.get("in")
                if in_expr is None:
                    raise ValueError("$map requires 'in' expression")
                in_sql, in_params = self._convert_operand_to_sql(in_expr)

                sql = (
                    f"json((SELECT {json_ga}({in_sql} ORDER BY"
                    f" CAST({je_alias}.key AS INTEGER)) FROM"
                    f" {self.json_each_function}({input_sql}) AS {je_alias}))"
                )
                return sql, input_params + in_params

            elif operator == "$reduce":
                initial_value = operands.get("initialValue")
                in_expr = operands.get("in")
                if in_expr is None:
                    raise ValueError("$reduce requires 'in' expression")

                # Evaluate initialValue in outer context (no $$value/$$this yet)
                self._current_context = old_ctx
                try:
                    init_sql, init_params = self._convert_operand_to_sql(
                        initial_value
                    )
                finally:
                    self._current_context = ctx

                self._reduce_counter = getattr(self, "_reduce_counter", 0) + 1
                r_id = self._reduce_counter
                raw_cte = f"_red_raw_{r_id}"
                in_cte = f"_red_in_{r_id}"
                rec_cte = f"_red_{r_id}"

                is_json_acc = self._is_json_container_expr(
                    initial_value
                ) or self._is_json_container_expr(in_expr)
                ctx.set_variable(
                    "$$value", ("json(r.acc)" if is_json_acc else "r.acc", [])
                )
                ctx.set_variable("$$index", ("r.idx", []))
                rje = f"_rje_{r_id}"
                elem_sql = (
                    f"(SELECT CASE WHEN {rje}.type IN ('object', 'array') "
                    f"THEN json({rje}.value) ELSE {rje}.value END "
                    f"FROM {self.json_each_function}({in_cte}.arr) AS {rje} "
                    f"WHERE CAST({rje}.key AS INTEGER) = r.idx LIMIT 1)"
                )
                ctx.set_variable("$$this", (elem_sql, []))

                in_sql, in_params = self._convert_operand_to_sql(in_expr)
                is_arr_sql = self._array_type_check_sql(
                    input_operand, "raw_val"
                )
                acc_out = "json(acc)" if is_json_acc else "acc"
                subq = (
                    f"(WITH RECURSIVE "
                    f"{raw_cte}(raw_val) AS (SELECT {input_sql}), "
                    f"{in_cte}(arr, init_val, is_arr) AS ("
                    f"SELECT CASE WHEN ({is_arr_sql}) THEN raw_val ELSE json('[]') END, "
                    f"{init_sql}, "
                    f"CASE WHEN ({is_arr_sql}) THEN 1 ELSE 0 END "
                    f"FROM {raw_cte}), "
                    f"{rec_cte}(idx, acc, len, is_arr) AS ("
                    f"SELECT 0, init_val, "
                    f"CASE WHEN is_arr = 1 THEN json_array_length(arr) ELSE 0 END, "
                    f"is_arr FROM {in_cte} "
                    f"UNION ALL "
                    f"SELECT r.idx + 1, ({in_sql}), r.len, r.is_arr "
                    f"FROM {rec_cte} r, {in_cte} "
                    f"WHERE r.is_arr = 1 AND r.idx < r.len) "
                    f"SELECT CASE WHEN is_arr = 1 THEN {acc_out} ELSE NULL END "
                    f"FROM {rec_cte} WHERE idx = len)"
                )
                sql = f"json({subq})" if is_json_acc else subq
                return sql, input_params + init_params + in_params

            else:
                raise NotImplementedError(
                    f"Array transform operator {operator}"
                    f" not supported in SQL tier"
                )
        finally:
            self._current_context = old_ctx

    def _array_type_check_sql(self, operand: Any, col_ref: str) -> str:
        """Return a safe SQL predicate checking whether operand evaluates to a JSON array."""
        if (
            isinstance(operand, str)
            and operand.startswith("$")
            and not operand.startswith("$$")
        ):
            return (
                f"json_type({self.data_column}, "
                f"'{parse_json_path(operand[1:])}') = 'array'"
            )
        if isinstance(operand, list):
            return "1"
        if isinstance(operand, dict) or (
            isinstance(operand, str) and operand.startswith("$$")
        ):
            if self.jsonb.jsonb_supported:
                return (
                    f"((typeof({col_ref}) = 'blob' AND json_valid({col_ref}, 4) "
                    f"AND json_type({col_ref}) = 'array') OR "
                    f"(typeof({col_ref}) = 'text' AND json_valid({col_ref}) "
                    f"AND json_type({col_ref}) = 'array'))"
                )
            return (
                f"(typeof({col_ref}) = 'text' AND json_valid({col_ref}) "
                f"AND json_type({col_ref}) = 'array')"
            )
        return "0"

    def _is_json_container_expr(self, expr: Any) -> bool:
        """Return True if expr is statically known to evaluate to an array or object."""
        if isinstance(expr, list):
            return True
        if isinstance(expr, dict):
            if len(expr) == 1:
                op = next(iter(expr.keys()))
                if op.startswith("$"):
                    return self._get_operator_return_type(op) in (
                        "array",
                        "object",
                    )
            return True
        return False

    def _convert_extreme_n(
        self, operands: Any, direction: str
    ) -> tuple[str, list[Any]]:
        """Convert $maxN / $minN to SQL (shared implementation)."""
        if isinstance(operands, dict):
            array_operand = operands.get("input")
            n_operand = operands.get("n")
        elif isinstance(operands, list) and len(operands) == 2:
            array_operand = operands[0]
            n_operand = operands[1]
        else:
            raise ValueError("$maxN/$minN requires input array and n count")
        array_sql, array_params = self._convert_operand_to_sql(array_operand)
        if isinstance(n_operand, int):
            n_clause = str(n_operand)
            n_params: list[Any] = []
        else:
            n_sql, n_params = self._convert_operand_to_sql(n_operand)
            n_clause = n_sql
        json_ga = self.json_group_array_function
        elem_expr = "CASE WHEN type IN ('object', 'array') THEN json(value) ELSE value END"
        sql = (
            f"json((SELECT {json_ga}({elem_expr} ORDER BY value {direction})"
            f" FROM (SELECT value, type FROM {self.json_each_function}({array_sql})"
            f" ORDER BY value {direction} LIMIT {n_clause})))"
        )
        return sql, array_params + n_params
