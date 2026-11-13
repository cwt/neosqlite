"""Stage-builder mixin for SQLTierAggregator.

Contains all _build_*_sql methods extracted from sql_tier_aggregator.py.
These methods translate individual aggregation pipeline stages ($match,
$group, $sort, $lookup, etc.) into SQL CTE expressions.

Designed as a mixin to be composed into SQLTierAggregator.
"""

from __future__ import annotations

from typing import Any

from ..sql_utils import quote_table_name
from .expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_aggregation_variable,
    _is_expression,
)
from .json_helpers import neosqlite_json_dumps
from .json_path_utils import parse_json_path
from .pipeline_context import PipelineContext
from .query_helper.utils import _get_json_function


def _graph_lookup_scalar_or_oid(expr_sql: str) -> str:
    """Normalize encoded ObjectId JSON dicts to their 24-hex id string."""
    return (
        f"CASE WHEN typeof({expr_sql}) = 'text' AND json_valid({expr_sql}) "
        f"AND json_extract({expr_sql}, '$.__neosqlite_objectid__') = 1 "
        f"THEN json_extract({expr_sql}, '$.id') ELSE {expr_sql} END"
    )


def build_redact_where_sql(
    evaluator: Any, spec: Any
) -> tuple[str | None, list[Any]]:
    """Translate $redact specs using only $$KEEP / $$PRUNE to a SQL WHERE clause.

    Returns (where_clause, params) where where_clause is '' for keep-all or
    'WHERE ...', or (None, []) when Python fallback (e.g. $$DESCEND) is needed.
    """
    if spec == "$$KEEP":
        return "", []
    if spec == "$$PRUNE":
        return "WHERE 0", []
    if not isinstance(spec, dict) or "$cond" not in spec:
        return None, []

    cond = spec["$cond"]
    if isinstance(cond, dict):
        if "if" not in cond or "then" not in cond or "else" not in cond:
            return None, []
        if_expr, then_val, else_val = cond["if"], cond["then"], cond["else"]
    elif isinstance(cond, list) and len(cond) == 3:
        if_expr, then_val, else_val = cond
    else:
        return None, []

    valid_vals = ("$$KEEP", "$$PRUNE")
    if then_val not in valid_vals or else_val not in valid_vals:
        return None, []

    if then_val == else_val:
        return ("" if then_val == "$$KEEP" else "WHERE 0"), []

    expr_sql, expr_params = evaluator.evaluate_for_aggregation(if_expr)
    if not expr_sql:
        return None, []

    if then_val == "$$KEEP":
        return f"WHERE COALESCE(({expr_sql}), 0) != 0", list(expr_params)
    return f"WHERE NOT COALESCE(({expr_sql}), 0)", list(expr_params)


def build_graph_lookup_parts(
    collection: Any,
    jsonb_supported: bool,
    source_table: str,
    recurse_cte: str,
    spec: dict[str, Any],
    has_root: bool = False,
) -> tuple[str, str, list[Any]] | None:
    """Build SQL pieces (recursive CTE query, final SELECT, params) for $graphLookup."""
    if not isinstance(spec, dict):
        return None

    from_collection = spec.get("from")
    start_with = spec.get("startWith")
    connect_from_field = spec.get("connectFromField")
    connect_to_field = spec.get("connectToField")
    as_field = spec.get("as")
    max_depth = spec.get("maxDepth")
    depth_field = spec.get("depthField")
    restrict_search = spec.get("restrictSearchWithMatch")

    if not (
        isinstance(from_collection, str)
        and from_collection
        and start_with is not None
        and isinstance(connect_from_field, str)
        and connect_from_field
        and isinstance(connect_to_field, str)
        and connect_to_field
        and isinstance(as_field, str)
        and as_field
    ):
        return None

    if max_depth is not None and (
        not isinstance(max_depth, int)
        or isinstance(max_depth, bool)
        or max_depth < 0
    ):
        return None

    if restrict_search is not None and not isinstance(restrict_search, dict):
        return None

    quoted_target = quote_table_name(str(from_collection))

    # Build startWith expression using json_extract (reads both JSON and JSONB as SQL types)
    start_params: list[Any] = []
    if start_with == "$_id":
        start_sql = "p._id"
        is_arr_expr = "0"
    elif (
        isinstance(start_with, str)
        and start_with.startswith("$")
        and not start_with.startswith("$$")
    ):
        sw_path = parse_json_path(start_with[1:])
        start_sql = f"json_extract(p.data, '{sw_path}')"
        is_arr_expr = f"(CASE WHEN json_type(p.data, '{sw_path}') = 'array' THEN 1 ELSE 0 END)"
    elif isinstance(start_with, list):
        start_sql = "?"
        start_params = [neosqlite_json_dumps(start_with)]
        is_arr_expr = "1"
    else:
        p_eval = ExprEvaluator(data_column="p.data", db_connection=None)
        compiled_sw, sw_params = p_eval.build_select_expression(start_with)
        if compiled_sw is None:
            return None
        start_sql = compiled_sw
        start_params = list(sw_params)
        is_arr_expr = (
            "(CASE WHEN typeof(_sw_val) = 'text' AND json_valid(_sw_val) "
            "AND json_type(_sw_val) = 'array' THEN 1 ELSE 0 END)"
        )

    restrict_where = ""
    restrict_params: list[Any] = []
    if restrict_search:
        from .query_helper import QueryHelper

        target_coll = collection.database.get_collection(from_collection)
        helper = QueryHelper(target_coll)
        query_result = helper._build_simple_where_clause(restrict_search)
        if not query_result:
            return None
        r_sql, r_params, _ = query_result
        r_sql = r_sql.strip()
        if r_sql.upper().startswith("WHERE "):
            r_sql = r_sql[6:]
        for func in (
            "json_extract",
            "jsonb_extract",
            "json_type",
            "json_each",
            "jsonb_each",
        ):
            r_sql = r_sql.replace(f"{func}(data", f"{func}(t.data")
        r_sql = r_sql.replace(f"{quoted_target}._id", "t._id")
        r_sql = r_sql.replace(f"{quoted_target}.id", "t.id")
        restrict_where = f"AND ({r_sql})"
        restrict_params = list(r_params)

    def target_match(val_sql: str) -> str:
        norm_val = _graph_lookup_scalar_or_oid(val_sql)
        if connect_to_field == "_id":
            return f"t._id = {norm_val}"
        to_path = parse_json_path(connect_to_field)
        scalar_to = _graph_lookup_scalar_or_oid(
            f"json_extract(t.data, '{to_path}')"
        )
        return (
            f"(CASE WHEN json_type(t.data, '{to_path}') = 'array' "
            f"THEN EXISTS (SELECT 1 FROM json_each(t.data, '{to_path}') _te "
            f"WHERE {_graph_lookup_scalar_or_oid('_te.value')} = {norm_val}) "
            f"ELSE {scalar_to} = {norm_val} END)"
        )

    if start_with == "$_id" or (
        isinstance(start_with, str) and start_with.startswith("$")
    ) or isinstance(start_with, list):
        p_subquery = (
            f"SELECT p.id AS _pid, ({start_sql}) AS _sw, "
            f"{is_arr_expr} AS _sw_arr FROM {source_table} p"
        )
    else:
        p_subquery = (
            f"SELECT _pid, _sw_val AS _sw, {is_arr_expr} AS _sw_arr "
            f"FROM (SELECT p.id AS _pid, ({start_sql}) AS _sw_val FROM {source_table} p)"
        )

    start_points_sql = f"""
        SELECT
            _p._pid AS original_id,
            t._id AS found_id,
            t.data AS found_data,
            0 AS depth,
            ',' || CAST(t.id AS TEXT) || ',' AS visited
        FROM ({p_subquery}) _p
        JOIN {quoted_target} t ON (
            CASE WHEN _p._sw_arr = 1
            THEN EXISTS (
                SELECT 1 FROM json_each(_p._sw) _se
                WHERE _se.value IS NOT NULL AND {target_match('_se.value')}
            )
            ELSE _p._sw IS NOT NULL AND {target_match('_p._sw')}
            END
        )
        WHERE 1=1 {restrict_where}
    """

    if connect_from_field == "_id":
        rec_join_cond = f"r.found_id IS NOT NULL AND {target_match('r.found_id')}"
    else:
        from_path = parse_json_path(connect_from_field)
        scalar_from = f"json_extract(r.found_data, '{from_path}')"
        rec_join_cond = (
            f"CASE WHEN json_type(r.found_data, '{from_path}') = 'array' "
            f"THEN EXISTS (SELECT 1 FROM json_each(r.found_data, '{from_path}') _re "
            f"WHERE _re.value IS NOT NULL AND {target_match('_re.value')}) "
            f"ELSE {scalar_from} IS NOT NULL AND {target_match(scalar_from)} END"
        )

    max_depth_cond = "AND r.depth < ?" if max_depth is not None else ""
    recursive_step_sql = f"""
        SELECT
            r.original_id,
            t._id AS found_id,
            t.data AS found_data,
            r.depth + 1 AS depth,
            r.visited || CAST(t.id AS TEXT) || ',' AS visited
        FROM {recurse_cte} r
        JOIN {quoted_target} t ON ({rec_join_cond})
        WHERE INSTR(r.visited, ',' || CAST(t.id AS TEXT) || ',') = 0
        {max_depth_cond} {restrict_where}
    """

    recurse_sql = f"{start_points_sql} UNION ALL {recursive_step_sql}"
    rec_params = (
        start_params
        + restrict_params
        + ([max_depth] if max_depth is not None else [])
        + restrict_params
    )

    json_set_func = "jsonb_set" if jsonb_supported else "json_set"
    inner_doc = (
        f"{json_set_func}(sub.found_data, '{parse_json_path(str(depth_field))}', sub.depth)"
        if depth_field
        else "sub.found_data"
    )
    elem_expr = (
        f"json(CASE WHEN json_type(sub.found_data, '$._id') IS NOT NULL "
        f"THEN {inner_doc} ELSE {json_set_func}({inner_doc}, '$._id', sub.found_id) END)"
    )
    group_expr = (
        f"json(COALESCE((SELECT json_group_array({elem_expr}) "
        f"FROM (SELECT found_id, found_data, MIN(depth) AS depth "
        f"FROM {recurse_cte} WHERE original_id = p.id GROUP BY found_id) sub), '[]'))"
    )
    data_expr = (
        f"json({json_set_func}(p.data, '{parse_json_path(str(as_field))}', {group_expr}))"
    )
    select_parts = ["p.id AS id", "p._id AS _id", f"{data_expr} AS data"]
    if has_root:
        select_parts.append("p.root_data")
    select_sql = f"SELECT {', '.join(select_parts)} FROM {source_table} p"

    return recurse_sql, select_sql, rec_params


class StageBuildersMixin:
    """Mixin providing _build_*_sql methods for SQLTierAggregator."""

    # Attributes supplied by SQLTierAggregator
    evaluator: Any
    collection: Any
    jsonb: Any

    def _get_json_extract(self, path: str | None = None) -> str: ...  # type: ignore[empty-body]
    def _get_json_set(self) -> str: ...  # type: ignore[empty-body]
    def can_optimize_pipeline(self, pipeline: list[dict[str, Any]]) -> bool: ...  # type: ignore[empty-body]
    def _pipeline_needs_root(self, pipeline: list[dict[str, Any]]) -> bool: ...  # type: ignore[empty-body]

    def _build_stage_sequence(
        self,
        pipeline: list[dict[str, Any]],
        prev_stage: str,
        cte_prefix: str,
        context: PipelineContext,
        needs_root: bool = False,
    ) -> tuple[str | None, list[tuple[str, str, list[Any]]]]:
        """Compile a sequence of stages into ordered (cte_name, cte_sql, cte_params) triples."""
        ctes: list[tuple[str, str, list[Any]]] = []
        for i, stage in enumerate(pipeline):
            stage_name = next(iter(stage.keys()))
            stage_spec = stage[stage_name]
            context.stage_index = i
            cte_name = f"{cte_prefix}{i}"

            stage_sql, stage_params = self._build_stage_sql(
                stage_name,
                stage_spec,
                prev_stage,
                context,
                preserve_root=(i == 0 and needs_root),
            )
            if stage_sql is None:
                return None, []

            for aux_name, aux_sql, aux_p in context.drain_pre_ctes():
                ctes.append((aux_name, aux_sql, aux_p))

            ctes.append((cte_name, stage_sql, stage_params))
            prev_stage = cte_name

        return prev_stage, ctes

    def _build_stage_sql(
        self,
        stage_name: str,
        stage_spec: Any,
        prev_stage: str,
        context: PipelineContext,
        preserve_root: bool = False,
    ) -> tuple[str | None, list[Any]]:
        """Build SQL for a single pipeline stage."""
        match stage_name:
            case "$addFields":
                return self._build_addfields_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$project":
                return self._build_project_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$group":
                return self._build_group_sql(stage_spec, prev_stage, context)
            case "$match":
                return self._build_match_sql(stage_spec, prev_stage, context)
            case "$sort":
                return self._build_sort_sql(stage_spec, prev_stage, context)
            case "$skip":
                return self._build_skip_sql(stage_spec, prev_stage, context)
            case "$limit":
                return self._build_limit_sql(stage_spec, prev_stage, context)
            case "$count":
                return self._build_count_sql(stage_spec, prev_stage, context)
            case "$unset":
                return self._build_unset_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$replaceRoot":
                return self._build_replace_root_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$replaceWith":
                return self._build_replace_root_sql(
                    {"newRoot": stage_spec}, prev_stage, context, preserve_root
                )
            case "$setWindowFields":
                return self._build_set_window_fields_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$sample":
                return self._build_sample_sql(stage_spec, prev_stage, context)
            case "$bucket":
                return self._build_bucket_sql(stage_spec, prev_stage, context)
            case "$bucketAuto":
                return self._build_bucket_auto_sql(
                    stage_spec, prev_stage, context
                )
            case "$unionWith":
                return self._build_union_with_sql(
                    stage_spec, prev_stage, context
                )
            case "$redact":
                return self._build_redact_sql(stage_spec, prev_stage, context)
            case "$lookup":
                return self._build_lookup_sql(stage_spec, prev_stage, context)
            case "$merge":
                return self._build_merge_sql(stage_spec, prev_stage, context)
            case "$densify":
                return self._build_densify_sql(stage_spec, prev_stage, context)
            case "$graphLookup":
                return self._build_graph_lookup_sql(
                    stage_spec, prev_stage, context
                )
            case "$facet":
                return self._build_facet_sql(stage_spec, prev_stage, context)
            case _:
                return None, []

    def _build_addfields_sql(self, spec, prev_stage, context, preserve_root):
        all_params = []
        json_set_args = []
        for field, expr in spec.items():
            if _is_aggregation_variable(expr):
                json_set_func = self._get_json_set()
                if expr == "$$CURRENT":
                    expr_sql = f"json({json_set_func}(data, '$._id', _id))"
                elif expr == "$$ROOT":
                    context.preserve_root()
                    expr_sql = f"json({json_set_func}(root_data, '$._id', _id))"
                else:
                    return None, []
            else:
                agg_ctx = AggregationContext()
                agg_ctx.stage_index = context.stage_index
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr, context=agg_ctx
                )
                all_params.extend(expr_params)
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(expr_sql)
            context.add_computed_field(field, expr_sql)

        json_set_func = self._get_json_set()
        if json_set_args:
            data_expr = (
                f"json({json_set_func}(data, {', '.join(json_set_args)}))"
            )
        else:
            data_expr = "data"

        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_project_sql(self, spec, prev_stage, context, preserve_root):
        all_params = []
        select_parts = ["id"]

        # MongoDB includes _id by default in $project unless explicitly excluded (_id: 0) (#144)
        include_id = spec.get("_id", 1) != 0
        select_parts.append("_id" if include_id else "NULL AS _id")

        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")

        json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
        json_obj_func = f"{self.jsonb.json_function_prefix}_object"

        is_exclusion = not any(
            value == 1
            or _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for field, value in spec.items()
            if field != "_id"
        )

        if is_exclusion:
            fields_to_remove = [
                field
                for field, value in spec.items()
                if value == 0 and (field != "_id" or not include_id)
            ]
            if fields_to_remove:
                json_remove_func = f"{self.jsonb.json_function_prefix}_remove"
                path_args = ", ".join(
                    f"'{parse_json_path(f)}'" for f in fields_to_remove
                )
                data_expr = f"json({json_remove_func}(data, {path_args}))"
            else:
                data_expr = "data"
        else:
            json_parts = []
            missing_removals = []

            for field, value in spec.items():
                if field == "_id":
                    continue

                if _is_expression(value):
                    agg_ctx = AggregationContext()
                    agg_ctx.stage_index = context.stage_index
                    expr_sql, expr_params = (
                        self.evaluator.build_select_expression(
                            value, context=agg_ctx
                        )
                    )
                    if expr_sql is None:
                        return None, []
                    all_params.extend(expr_params)
                    json_parts.append(f"'{field}'")
                    json_parts.append(expr_sql)
                    context.add_computed_field(field, expr_sql)
                elif isinstance(value, str) and value.startswith("$"):
                    source_field = value[1:]
                    if source_field == "_id":
                        json_parts.append(f"'{field}'")
                        json_parts.append("_id")
                    else:
                        json_parts.append(f"'{field}'")
                        json_parts.append(
                            f"{json_extract_func}(data, '{parse_json_path(source_field)}')"
                        )
                        missing_removals.append(
                            f"CASE WHEN json_type(data, '{parse_json_path(source_field)}') IS NULL "
                            f"THEN '{parse_json_path(field)}' ELSE '$.__none__' END"
                        )
                elif value == 1:
                    json_parts.append(f"'{field}'")
                    json_parts.append(
                        f"{json_extract_func}(data, '{parse_json_path(field)}')"
                    )
                    missing_removals.append(
                        f"CASE WHEN json_type(data, '{parse_json_path(field)}') IS NULL "
                        f"THEN '{parse_json_path(field)}' ELSE '$.__none__' END"
                    )

            if json_parts:
                base_obj = f"{json_obj_func}({', '.join(json_parts)})"
                if missing_removals:
                    json_remove_func = (
                        f"{self.jsonb.json_function_prefix}_remove"
                    )
                    base_obj = f"{json_remove_func}({base_obj}, {', '.join(missing_removals)})"
                data_expr = f"json({base_obj})"
            else:
                data_expr = "json('{}')"

        select_parts.append(f"{data_expr} AS data")

        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_unset_sql(self, spec, prev_stage, context, preserve_root):
        fields = [spec] if isinstance(spec, str) else spec
        json_remove_func = _get_json_function(
            "remove", self.jsonb.jsonb_supported
        )
        json_set_func = self._get_json_set()
        json_paths = [f"'{parse_json_path(f)}'" for f in fields]
        data_expr = f"json({json_set_func}({json_remove_func}(data, {', '.join(json_paths)}), '$._id', _id))"
        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", []

    def _build_replace_root_sql(self, spec, prev_stage, context, preserve_root):
        new_root = spec.get("newRoot")
        all_params = []
        json_set_func = self._get_json_set()
        if (
            isinstance(new_root, str)
            and new_root.startswith("$")
            and not new_root.startswith("$$")
        ):
            field = new_root[1:]
            json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
            expr_sql = f"{json_extract_func}(data, '{parse_json_path(field)}')"
        elif new_root == "$$ROOT":
            if context.has_root:
                expr_sql = f"json({json_set_func}(root_data, '$._id', _id))"
            else:
                expr_sql = f"json({json_set_func}(data, '$._id', _id))"
        elif new_root == "$$CURRENT":
            expr_sql = f"json({json_set_func}(data, '$._id', _id))"
        else:
            expr_sql, expr_params = self.evaluator.build_select_expression(
                new_root
            )
            all_params.extend(expr_params)

        data_expr = f"json({json_set_func}({expr_sql}, '$._id', _id))"
        select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            select_parts.append("data AS root_data")
        elif context.has_root:
            select_parts.append("root_data")
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_sample_sql(self, spec, prev_stage, context):
        size = spec.get("size", 1)
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY RANDOM() LIMIT ?"
        return sql, [size]

    def _build_bucket_sql(self, spec, prev_stage, context):
        group_by = spec.get("groupBy")
        boundaries = spec.get("boundaries")
        default = spec.get("default", "Other")
        output = spec.get("output", {"count": {"$sum": 1}})

        if not group_by or not boundaries:
            return None, []

        all_params = []
        key_sql, key_params = self.evaluator.build_select_expression(group_by)
        all_params.extend(key_params)

        # Build CASE for boundaries
        case_parts = []
        for i in range(len(boundaries) - 1):
            lower = boundaries[i]
            upper = boundaries[i + 1]
            case_parts.append(f"WHEN {key_sql} >= ? AND {key_sql} < ? THEN ?")
            all_params.extend([lower, upper, lower])

        if default is not None:
            case_parts.append("ELSE ?")
            all_params.append(default)

        bucket_id_sql = f"CASE {' '.join(case_parts)} END"

        # Build accumulators into an inner SELECT; the outer projection
        # references those aliases to satisfy the (id, _id, data) invariant
        # without self-referencing them at the same SELECT level (#94).
        # The bucket key alias must not collide with the input's own _id
        # column, so it is named __bucket_id and projected as _id outside.
        sub_parts = [f"{bucket_id_sql} AS __bucket_id"]
        output_fields: list[str] = []
        for field, acc_spec in output.items():
            op, expr = next(iter(acc_spec.items()))
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            sub_parts.append(f"{sql_agg}({distinct_str}{expr_sql}) AS {field}")
            output_fields.append(field)

        json_args = ", ".join(
            ["'_id', __bucket_id"]
            + [
                f"'{f.replace(chr(39), chr(39) * 2)}', {f}"
                for f in output_fields
            ]
        )

        # GROUP BY __bucket_id (an unambiguous name), so the CASE expression
        # — and its bound parameters — appear exactly once.
        sql = (
            f"SELECT NULL AS id, __bucket_id AS _id, "
            f"json_object({json_args}) AS data "
            f"FROM (SELECT {', '.join(sub_parts)} FROM {prev_stage} "
            f"GROUP BY __bucket_id)"
        )
        return sql, all_params

    def _build_bucket_auto_sql(self, spec, prev_stage, context):
        group_by = spec.get("groupBy")
        buckets = spec.get("buckets", 10)
        output = spec.get("output", {"count": {"$sum": 1}})
        granularity = spec.get("granularity")

        if not group_by:
            return None, []

        if granularity:
            return None, []

        all_params = []
        key_sql, key_params = self.evaluator.build_select_expression(group_by)
        all_params.extend(key_params)

        min_max_sql = f"""
            SELECT
                MIN({key_sql}) as min_val,
                MAX({key_sql}) as max_val
            FROM {prev_stage}
        """

        bucket_calc_sql = f"""
            SELECT
                min_val,
                max_val,
                CASE
                    WHEN max_val = min_val THEN 0
                    ELSE (max_val - min_val) * 1.0 / {buckets}
                END as bucket_width
            FROM ({min_max_sql})
        """

        grouped_parts: list[str] = [
            # MongoDB's $bucketAuto reports each bucket's boundaries as an
            # {_id: {min, max}} document computed over the bucketed values.
            "MIN(val) AS __bmin",
            "MAX(val) AS __bmax",
        ]
        output_fields: list[str] = []
        for field, acc_spec in output.items():
            op, expr = next(iter(acc_spec.items()))
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            grouped_parts.append(
                f"{sql_agg}({distinct_str}{expr_sql}) AS {field}"
            )
            output_fields.append(field)

        # Wrap the grouped SELECT in an outer projection: json_object must
        # reference the accumulator aliases from an enclosing level (#94).
        # The bucket key is aliased __bucket_id to avoid colliding with the
        # input's own _id column.
        json_args = ", ".join(
            ["'_id', json_object('min', __bmin, 'max', __bmax)"]
            + [
                f"'{f.replace(chr(39), chr(39) * 2)}', {f}"
                for f in output_fields
            ]
        )

        sql = f"""
            SELECT
                NULL AS id,
                json_object('min', __bmin, 'max', __bmax) AS _id,
                json_object({json_args}) AS data
            FROM (
                SELECT
                    i.bucket_id AS __bucket_id,
                    MIN(i.val) AS __bmin,
                    MAX(i.val) AS __bmax,
                    {', '.join(grouped_parts)}
                FROM (
                    SELECT
                        __src.id AS __rid,
                        {key_sql} as val,
                        CASE
                            WHEN max_val = min_val THEN 0
                            ELSE MIN(
                                CAST(
                                    ({key_sql} - min_val) * 1.0
                                    / ((max_val - min_val) * 1.0 / {buckets})
                                    AS INTEGER
                                ),
                                {buckets} - 1
                            )
                        END as bucket_id
                    FROM {prev_stage} AS __src, ({bucket_calc_sql})
                    WHERE min_val IS NOT NULL AND max_val IS NOT NULL
                ) AS i
                JOIN {prev_stage} AS __src2
                    ON __src2.id = i.__rid
                GROUP BY i.bucket_id
            )
            ORDER BY _id
        """
        return sql, all_params

    def _build_union_with_sql(self, spec, prev_stage, context):
        coll_name: str | None
        if isinstance(spec, str):
            coll_name = spec
            pipeline: list[dict[str, Any]] = []
        elif isinstance(spec, dict):
            coll_name = spec.get("coll")
            pipeline = spec.get("pipeline", [])
        else:
            return None, []

        if (
            not isinstance(coll_name, str)
            or not coll_name
            or not isinstance(pipeline, list)
        ):
            return None, []

        from ..sql_utils import quote_table_name
        from .sql_tier_aggregator import SQLTierAggregator

        target_coll = self.collection.database.get_collection(coll_name)
        if pipeline:
            other_agg = SQLTierAggregator(target_coll, self.evaluator)
            union_sql, union_params = other_agg.build_pipeline_sql(pipeline)
            if not union_sql:
                return None, []
            union_from = f"({union_sql})"
        else:
            union_from = quote_table_name(target_coll.name)
            union_params = []

        select_parts = ["id", "_id", "data"]
        union_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
            union_parts.append("data AS root_data")

        cols = ", ".join(select_parts)
        union_cols = ", ".join(union_parts)
        sql = f"SELECT {cols} FROM {prev_stage} UNION ALL SELECT {union_cols} FROM {union_from}"
        return sql, union_params

    def _build_redact_sql(self, spec, prev_stage, context):
        """
        Build SQL for $redact stage.
        Optimizes KEEP/PRUNE patterns into a WHERE clause.
        Falls back to Python for DESCEND pattern as it requires recursive JSON modification.
        """
        where_clause, expr_params = build_redact_where_sql(
            self.evaluator, spec
        )
        if where_clause is None:
            return None, []

        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")

        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage}"
        if where_clause:
            sql = f"{sql} {where_clause}"
        return sql, expr_params

    def _build_lookup_sql(self, spec, prev_stage, context):
        from ..sql_utils import quote_table_name

        from_coll = spec.get("from")
        local_field = spec.get("localField")
        foreign_field = spec.get("foreignField")
        as_field = spec.get("as")
        pipeline = spec.get("pipeline", [])

        if not from_coll or not as_field:
            return None, []

        json_set_func = self._get_json_set()

        if pipeline:
            if not local_field or not foreign_field:
                return None, []

            target_coll = self.collection.database.get_collection(from_coll)
            from .sql_tier_aggregator import SQLTierAggregator

            other_agg = SQLTierAggregator(target_coll, self.evaluator)

            if not other_agg.can_optimize_pipeline(pipeline):
                return None, []

            pipeline_sql, pipeline_params = other_agg.build_pipeline_sql(
                pipeline
            )
            if not pipeline_sql:
                return None, []

            local_expr = (
                "t1._id"
                if local_field == "_id"
                else _graph_lookup_scalar_or_oid(
                    f"json_extract(t1.data, '{parse_json_path(local_field)}')"
                )
            )
            foreign_expr = (
                "pipeline_result._id"
                if foreign_field == "_id"
                else _graph_lookup_scalar_or_oid(
                    f"json_extract(pipeline_result.data, '{parse_json_path(foreign_field)}')"
                )
            )

            joined_data_expr = f"""
                (SELECT json_group_array(
                    json(CASE
                        WHEN json_type(pipeline_result.data, '$._id') IS NOT NULL
                        THEN pipeline_result.data
                        ELSE json_set(pipeline_result.data, '$._id', pipeline_result._id)
                    END)
                 )
                 FROM ({pipeline_sql}) AS pipeline_result
                 WHERE {foreign_expr} = {local_expr})
            """

            data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

            select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
            if context.has_root:
                select_parts.append("t1.root_data")

            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM {prev_stage} AS t1
            """
            return sql, pipeline_params

        if not all([local_field, foreign_field]):
            return None, []

        local_expr = (
            "t1._id"
            if local_field == "_id"
            else _graph_lookup_scalar_or_oid(
                f"json_extract(t1.data, '{parse_json_path(local_field)}')"
            )
        )
        foreign_expr = (
            "t2._id"
            if foreign_field == "_id"
            else _graph_lookup_scalar_or_oid(
                f"json_extract(t2.data, '{parse_json_path(foreign_field)}')"
            )
        )

        joined_data_expr = (
            f"json_group_array("
            f"json(CASE "
            f"WHEN json_type(t2.data, '$._id') IS NOT NULL THEN t2.data "
            f"ELSE {json_set_func}(t2.data, '$._id', t2._id) END)"
            f") FILTER (WHERE t2.id IS NOT NULL)"
        )
        data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

        select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
        if context.has_root:
            select_parts.append("t1.root_data")

        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM {prev_stage} AS t1
            LEFT JOIN {quote_table_name(from_coll)} AS t2 ON {local_expr} = {foreign_expr}
            GROUP BY t1.id
        """
        return sql, []

    def _build_merge_sql(self, spec, prev_stage, context):
        return None, []

    def _build_densify_sql(self, spec, prev_stage, context):
        """
        Build SQL for $densify stage - fills gaps in numeric sequences.

        Algorithm:
        1. Extract existing distinct values from the field into CTE "existing"
        2. Calculate min/max bounds from existing values in CTE "bounds_calc"
        3. Generate series of values from min to max with given step using
           CROSS JOIN with a numbers table (0-50) in CTE "series"
        4. UNION ALL:
           - Original documents from prev_stage
           - New documents with densified field values (where value not in existing)
        5. Use NOT EXISTS to filter out values that already exist

        Example: field=[1,3,5], step=1 produces [1,2,3,4,5]
        - existing: {1,3,5}
        - series: {1,2,3,4,5}
        - result: original docs + new docs for {2,4}

        Limitations:
        - Only supports numeric fields (int/float)
        - Does not support partitionBy/partitionByFields (requires group-by)
        - Does not support date fields or granularity
        - Max gap filling limited to 1000 values (falls back to Python for larger)
        """
        field = spec.get("field")
        range_spec = spec.get("range")
        partition_by = spec.get("partitionBy") or spec.get("partitionByFields")

        if not field:
            return None, []

        if partition_by:
            return None, []

        if not range_spec:
            return None, []

        step = range_spec.get("step")
        bounds = range_spec.get("bounds")
        unit = range_spec.get("unit")

        if (
            step is not None
            and isinstance(step, (int, float))
            and not isinstance(step, bool)
            and step <= 0
        ):
            raise ValueError(
                f"$densify step must be greater than 0, got {step}"
            )

        if unit is not None or "." in field:
            return None, []

        if (
            not step
            or not isinstance(step, (int, float))
            or isinstance(step, bool)
        ):
            return None, []

        if not isinstance(bounds, list) or len(bounds) != 2:
            return None, []

        lower_bound, upper_bound = bounds[0], bounds[1]
        if (
            isinstance(lower_bound, bool)
            or isinstance(upper_bound, bool)
            or not isinstance(lower_bound, (int, float))
            or not isinstance(upper_bound, (int, float))
        ):
            return None, []

        all_params: list[Any] = []
        key_sql, key_params = self.evaluator.build_select_expression(
            f"${field}"
        )
        all_params.extend(key_params)
        all_params.extend(
            [
                lower_bound,
                lower_bound,
                upper_bound,
                lower_bound,
                step,
                lower_bound,
                step,
                upper_bound,
                field,
            ]
        )

        sql = f"""
            WITH RECURSIVE existing AS (
                SELECT DISTINCT {key_sql} as val
                FROM {prev_stage}
                WHERE {key_sql} IS NOT NULL
            ),
            series(val, idx) AS (
                SELECT ?, 0
                WHERE ? <= ?
                UNION ALL
                SELECT ? + (idx + 1) * ?, idx + 1
                FROM series
                WHERE idx < 1000 AND ? + (idx + 1) * ? <= ?
            )
            SELECT t.id, t._id, t.data
            FROM {prev_stage} t
            UNION ALL
            SELECT NULL as id, NULL as _id, json_object(?, s.val) as data
            FROM series s
            WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.val = s.val)
        """
        return sql, all_params

    def _build_group_sql(self, spec, prev_stage, context):
        """Build $group SQL preserving the (id, _id, data) CTE invariant (#94).

        Downstream stages read fields via json_extract(data, ...) and the
        final wrapper selects id/_id/data, so the aggregate SELECT is wrapped
        in an outer projection that materializes those columns.
        """
        all_params = []
        agg_select_parts = []
        group_by_parts = []
        group_id = spec.get("_id")
        if group_id is not None:
            key_sql, key_params = self.evaluator.build_select_expression(
                group_id
            )
            agg_select_parts.append(f"{key_sql} AS _id")
            group_by_parts.append(key_sql)
            all_params.extend(key_params)
        else:
            agg_select_parts.append("NULL AS _id")

        output_fields: list[str] = []
        array_fields: set[str] = set()
        for field, accumulator in spec.items():
            if field == "_id":
                continue
            op, expr = next(iter(accumulator.items()))
            # Object literals / array literals as accumulator input (e.g.
            # $push of a sub-document) are not compiled by the SQL evaluator;
            # bail out so the pipeline falls back to the Python tier rather
            # than emitting unexpanded field references (#94).
            if isinstance(expr, list):
                return None, []
            if isinstance(expr, dict) and not any(
                k.startswith("$") for k in expr
            ):
                return None, []
            agg_result = self._map_accumulator_to_sql(op)
            if not agg_result:
                return None, []
            sql_agg, use_distinct = agg_result
            expr_sql, expr_params = self.evaluator.build_select_expression(expr)
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            agg_select_parts.append(
                f"{sql_agg}({distinct_str}{expr_sql}) AS {field}"
            )
            output_fields.append(field)
            if sql_agg == "json_group_array":
                # json_group_array yields a JSON *string*; re-parse it so
                # data holds a real JSON array like MongoDB (#94).
                array_fields.add(field)

        group_by_clause = (
            f"GROUP BY {', '.join(group_by_parts)}" if group_by_parts else ""
        )

        # Reference the inner aliases from the wrapping SELECT level.
        json_args = ", ".join(
            ["'_id', _id"]
            + [
                (
                    f"'{f.replace(chr(39), chr(39) * 2)}', json({f})"
                    if f in array_fields
                    else f"'{f.replace(chr(39), chr(39) * 2)}', {f}"
                )
                for f in output_fields
            ]
        )
        select_parts = [
            "ROW_NUMBER() OVER () AS id",
            "_id AS _id",
            f"json_object({json_args}) AS data",
        ]
        if context.has_root:
            # $group cannot preserve $$ROOT, but keeping the column present
            # keeps the final wrapper's SELECT executable.
            select_parts.append("NULL AS root_data")

        if not group_by_parts:
            # A constant-key aggregate without GROUP BY always emits exactly
            # one row in SQLite — even over zero input rows. Compute the row
            # count alongside the aggregates and filter outside so empty
            # input yields zero documents, as in MongoDB.
            agg_select_parts.append("COUNT(*) AS __cnt")
            having = "WHERE __cnt > 0"
        else:
            having = ""

        return (
            f"SELECT {', '.join(select_parts)} FROM "
            f"(SELECT {', '.join(agg_select_parts)} FROM {prev_stage} "
            f"{group_by_clause}) {having}",
            all_params,
        )

    def _map_accumulator_to_sql(self, op):
        mapping = {
            "$sum": ("SUM", False),
            "$avg": ("AVG", False),
            "$min": ("MIN", False),
            "$max": ("MAX", False),
            "$count": ("COUNT", False),
            "$push": ("json_group_array", False),
            "$addToSet": ("json_group_array", True),
        }
        return mapping.get(op)

    def _get_match_sql_translator(self):
        """Return a cached SQLTranslator matching this aggregator's JSONB context."""
        translator = getattr(self, "_match_sql_translator", None)
        if translator is None:
            from .sql_translator_unified import SQLTranslator

            translator = SQLTranslator(
                self.collection.name,
                "data",
                "id",
                self.jsonb.jsonb_supported,
                self.jsonb.json_each_function,
                db_connection=self.collection.db,
            )
            self._match_sql_translator = translator
        return translator

    def _translate_match_spec(
        self, spec: dict[str, Any]
    ) -> tuple[list[str] | None, list[Any]]:
        """Translate a $match query dictionary into SQL WHERE predicates."""
        import re

        from ..objectid import ObjectId
        from .type_utils import (
            build_json_containment_clause,
            is_encoded_json_value,
            json_type_guard_sql,
            regex_to_sql_pattern,
        )

        if not isinstance(spec, dict) or "$text" in spec:
            return None, []

        translator = self._get_match_sql_translator()
        op_translator = translator.operator_translator
        all_params: list[Any] = []
        where_clauses: list[str] = []

        def _wrap_for_where(expr_sql: str) -> str:
            """Wrap expression to work in WHERE clause if it returns JSON booleans."""
            if "json('true')" in expr_sql or 'json("true")' in expr_sql:
                return f"({expr_sql} = json('true'))"
            return expr_sql

        for field, value in spec.items():
            if field == "$jsonSchema":
                from .query_helper.schema_compiler import (
                    compile_query_schema_to_sql,
                )

                schema_res = compile_query_schema_to_sql(
                    value,
                    data_column="data",
                    jsonb=self.jsonb.jsonb_supported,
                )
                if schema_res is None:
                    return None, []
                schema_clause, schema_params = schema_res
                where_clauses.append(schema_clause)
                all_params.extend(schema_params)
            elif field == "$expr":
                expr_sql, expr_params = self.evaluator.evaluate(value)
                if not expr_sql:
                    return None, []
                where_clauses.append(_wrap_for_where(expr_sql))
                all_params.extend(expr_params)
            elif field in ("$and", "$or", "$nor"):
                if not isinstance(value, list) or not value:
                    return None, []
                sub_clauses: list[str] = []
                for sub_spec in value:
                    if not isinstance(sub_spec, dict) or not sub_spec:
                        return None, []
                    parts, sub_params = self._translate_match_spec(sub_spec)
                    if parts is None:
                        return None, []
                    sub_clauses.append(
                        " AND ".join(f"({p})" for p in parts) if parts else "1"
                    )
                    all_params.extend(sub_params)
                if field == "$and":
                    where_clauses.append(
                        "(" + " AND ".join(f"({c})" for c in sub_clauses) + ")"
                    )
                elif field == "$or":
                    where_clauses.append(
                        "("
                        + " OR ".join(
                            f"COALESCE(({c}), 0)" for c in sub_clauses
                        )
                        + ")"
                    )
                else:
                    where_clauses.append(
                        "NOT ("
                        + " OR ".join(
                            f"COALESCE(({c}), 0)" for c in sub_clauses
                        )
                        + ")"
                    )
            elif field == "$not":
                if not isinstance(value, dict) or not value:
                    return None, []
                parts, not_params = self._translate_match_spec(value)
                if parts is None or not parts:
                    return None, []
                inner = " AND ".join(f"({p})" for p in parts)
                where_clauses.append(f"NOT COALESCE(({inner}), 0)")
                all_params.extend(not_params)
            elif field.startswith("$") and _is_expression({field: value}):
                # Handle direct expression (no $expr wrapper)
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}
                )
                if not expr_sql:
                    return None, []
                where_clauses.append(_wrap_for_where(expr_sql))
                all_params.extend(expr_params)
            elif field.startswith("$"):
                return None, []
            else:
                if field == "_id":
                    field_sql = "_id"
                else:
                    json_extract_func = (
                        f"{self.jsonb.json_function_prefix}_extract"
                    )
                    field_sql = (
                        f"{json_extract_func}(data, '{parse_json_path(field)}')"
                    )

                if isinstance(value, dict):
                    options = value.get("$options", "")
                    if "$options" in value and (
                        "$regex" not in value or not isinstance(options, str)
                    ):
                        return None, []

                    range_ops = {
                        "$gt": ">",
                        "$lt": "<",
                        "$gte": ">=",
                        "$lte": "<=",
                    }
                    json_each_func = self.jsonb.json_each_function
                    for op, arg in value.items():
                        if op == "$options":
                            continue
                        if op == "$not":
                            if not isinstance(arg, (dict, re.Pattern)) or (
                                isinstance(arg, dict) and "$not" in arg
                            ):
                                return None, []
                            sub_parts, sub_params = self._translate_match_spec(
                                {field: arg}
                            )
                            if sub_parts is None or not sub_parts:
                                return None, []
                            inner = " AND ".join(f"({p})" for p in sub_parts)
                            where_clauses.append(f"NOT COALESCE(({inner}), 0)")
                            all_params.extend(sub_params)
                            continue
                        if op in range_ops:
                            if isinstance(
                                arg, (list, tuple)
                            ) or is_encoded_json_value(arg):
                                return None, []
                            if field == "_id" or op_translator._is_datetime_value(
                                arg
                            ):
                                sql, params = op_translator.translate_operator(
                                    field_sql, op, arg
                                )
                                if sql is None:
                                    return None, []
                                where_clauses.append(sql)
                                all_params.extend(params)
                            elif arg is None:
                                if op in ("$gt", "$lt"):
                                    where_clauses.append("0")
                                else:
                                    where_clauses.append(f"{field_sql} IS NULL")
                            else:
                                sql_op = range_ops[op]
                                json_path = parse_json_path(field)
                                json_path_q = f"'{json_path}'"
                                guard = json_type_guard_sql(json_path_q, arg)
                                where_clauses.append(
                                    f"((json_type(data, {json_path_q}) != 'array' AND {guard} AND {field_sql} {sql_op} ?) OR "
                                    f"(json_type(data, {json_path_q}) = 'array' AND "
                                    f"EXISTS (SELECT 1 FROM {json_each_func}(data, {json_path_q}) AS json_each "
                                    f"WHERE json_each.value {sql_op} ?)))"
                                )
                                all_params.extend([arg, arg])
                        else:
                            norm_arg = (
                                list(arg)
                                if op in ("$in", "$nin", "$all")
                                and isinstance(arg, tuple)
                                else arg
                            )
                            if op == "$regex" and options:
                                sql, params = op_translator.translate_operator(
                                    field_sql,
                                    op,
                                    norm_arg,
                                    regex_options=options,
                                )
                            else:
                                sql, params = op_translator.translate_operator(
                                    field_sql, op, norm_arg
                                )
                            if sql is None:
                                return None, []
                            where_clauses.append(sql)
                            all_params.extend(params)
                elif isinstance(value, re.Pattern):
                    pattern = regex_to_sql_pattern(value)
                    if pattern is None:
                        return None, []
                    if field == "_id":
                        where_clauses.append(
                            f"({field_sql} IS NOT NULL AND {field_sql} REGEXP ?)"
                        )
                    else:
                        json_path_q = f"'{parse_json_path(field)}'"
                        where_clauses.append(
                            f"(json_type(data, {json_path_q}) IS NOT NULL AND "
                            f"json_type(data, {json_path_q}) != 'null' AND "
                            f"{field_sql} REGEXP ?)"
                        )
                    all_params.append(pattern)
                elif op_translator._is_datetime_value(value):
                    sql, params = op_translator.translate_operator(
                        field_sql, "$eq", value
                    )
                    if sql is None:
                        return None, []
                    where_clauses.append(sql)
                    all_params.extend(params)
                elif value is None:
                    # MongoDB: null matches null-or-missing (#90)
                    where_clauses.append(f"{field_sql} IS NULL")
                elif field == "_id":
                    bound_val = (
                        str(value) if isinstance(value, ObjectId) else value
                    )
                    where_clauses.append(f"{field_sql} = ?")
                    all_params.append(bound_val)
                elif isinstance(value, (dict, list)):
                    where_clauses.append(f"{field_sql} = ?")
                    all_params.append(value)
                else:
                    clause, params = build_json_containment_clause(
                        "data",
                        field,
                        field_sql,
                        value,
                        self.jsonb.json_each_function,
                    )
                    where_clauses.append(clause)
                    all_params.extend(params)

        return where_clauses, all_params

    def _build_match_sql(self, spec, prev_stage, context):
        where_clauses, all_params = self._translate_match_spec(spec)
        if where_clauses is None:
            return None, []

        where_clause = (
            f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        )
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} {where_clause}",
            all_params,
        )

    def _build_sort_sql(self, spec, prev_stage, context):
        order_parts = []
        for field, direction in spec.items():
            order_dir = "DESC" if direction == -1 else "ASC"
            if field == "_id":
                field_sql = "_id"
            else:
                json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
                field_sql = (
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )
            order_parts.append(f"{field_sql} {order_dir}")
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY {', '.join(order_parts)}",
            [],
        )

    def _build_skip_sql(self, spec, prev_stage, context):
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT -1 OFFSET ?",
            [int(spec)],
        )

    def _build_limit_sql(self, spec, prev_stage, context):
        if not isinstance(spec, int) or isinstance(spec, bool) or spec < 0:
            raise ValueError(
                f"The $limit stage requires a non-negative integer, got {spec}"
            )
        select_parts = ["id", "_id", "data"]
        if context.has_root:
            select_parts.append("root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT ?",
            [int(spec)],
        )

    def _build_count_sql(self, spec, prev_stage, context):
        return (
            f"SELECT NULL as id, NULL as _id, json_object(?, COUNT(*)) as data FROM {prev_stage}",
            [spec],
        )

    def _build_set_window_fields_sql(
        self, spec, prev_stage, context, preserve_root
    ):
        partition_by = spec.get("partitionBy")
        sort_by = spec.get("sortBy", {})
        output = spec.get("output", {})

        all_params = []
        partition_clause = ""
        if partition_by:
            expr_sql, expr_params = self.evaluator.build_select_expression(
                partition_by
            )
            partition_clause = f"PARTITION BY {expr_sql}"
            all_params.extend(expr_params)

        sort_clause = ""
        if sort_by:
            sort_parts = []
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                if field == "_id":
                    field_sql = "_id"
                else:
                    field_sql = f"{self.jsonb.json_function_prefix}_extract(data, '{parse_json_path(field)}')"
                sort_parts.append(f"{field_sql} {order}")
            sort_clause = f"ORDER BY {', '.join(sort_parts)}"

        # 1. Build inner SELECT with window functions
        window_select_parts = ["id", "_id", "data"]
        if preserve_root:
            window_select_parts.append("data AS root_data")
        elif context.has_root:
            window_select_parts.append("root_data")

        json_set_args = []
        for i, (field, window_op) in enumerate(output.items()):
            op_name, op_spec = next(iter(window_op.items()))
            sql_func = self._map_window_op_to_sql(op_name)
            if not sql_func:
                return None, []

            # Use a unique alias for the window function result
            win_alias = f"win_func_{i}"

            if op_name in ("$rank", "$denseRank", "$documentNumber"):
                expr_sql = (
                    f"{sql_func}() OVER ({partition_clause} {sort_clause})"
                )
            elif op_name in ("$first", "$last"):
                inner_sql, inner_params = (
                    self.evaluator.build_select_expression(op_spec)
                )
                all_params.extend(inner_params)
                frame = (
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                )
                expr_sql = f"{sql_func}({inner_sql}) OVER ({partition_clause} {sort_clause} {frame})"
            else:
                return None, []

            window_select_parts.append(f"{expr_sql} AS {win_alias}")
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(win_alias)

        # 2. Build final SELECT that merges window results into JSON data
        json_set_func = self._get_json_set()
        data_expr = (
            f"json({json_set_func}(data, {', '.join(json_set_args)}))"
            if json_set_args
            else "data"
        )

        final_select_parts = ["id", "_id", f"{data_expr} AS data"]
        if preserve_root:
            final_select_parts.append("root_data")
        elif context.has_root:
            final_select_parts.append("root_data")

        inner_sql = f"SELECT {', '.join(window_select_parts)} FROM {prev_stage}"
        sql = f"SELECT {', '.join(final_select_parts)} FROM ({inner_sql})"

        return sql, all_params

    def _map_window_op_to_sql(self, op):
        mapping = {
            "$rank": "RANK",
            "$denseRank": "DENSE_RANK",
            "$documentNumber": "ROW_NUMBER",
            "$first": "FIRST_VALUE",
            "$last": "LAST_VALUE",
        }
        return mapping.get(op)

    def _build_graph_lookup_sql(self, spec, prev_stage, context):
        """Build Tier-1 CTE SQL for $graphLookup using top-level recursive CTE."""
        uid = context.next_aux_id()
        src_cte = f"{context.cte_prefix}_glsrc_{uid}"
        rec_cte = f"{context.cte_prefix}_glrec_{uid}"

        src_cols = "id, _id, data, root_data" if context.has_root else "id, _id, data"
        context.add_pre_cte(src_cte, f"SELECT {src_cols} FROM {prev_stage}", [])

        parts = build_graph_lookup_parts(
            self.collection,
            self.jsonb.jsonb_supported,
            src_cte,
            rec_cte,
            spec,
            has_root=context.has_root,
        )
        if parts is None:
            return None, []

        recurse_sql, select_sql, rec_params = parts
        context.add_pre_cte(rec_cte, recurse_sql, rec_params)
        context.mark_recursive()
        return select_sql, []

    def _build_facet_sql(self, spec, prev_stage, context):
        """Build Tier-1 CTE SQL for $facet by compiling sub-pipelines over shared source CTE."""
        if not isinstance(spec, dict):
            return None, []

        uid = context.next_aux_id()
        src_cte = f"{context.cte_prefix}_fsrc_{uid}"
        context.add_pre_cte(src_cte, f"SELECT id, _id, data FROM {prev_stage}", [])

        if not spec:
            select_parts = ["NULL AS id", "NULL AS _id", "json('{}') AS data"]
            if context.has_root:
                select_parts.append("NULL AS root_data")
            return f"SELECT {', '.join(select_parts)}", []

        json_set_func = self._get_json_set()
        doc_expr = (
            f"json(CASE "
            f"WHEN json_type(data, '$._id') IS NOT NULL THEN data "
            f"WHEN _id IS NULL THEN data "
            f"ELSE {json_set_func}(data, '$._id', _id) END)"
        )

        json_obj_args: list[str] = []
        all_params: list[Any] = []
        for f_idx, (facet_name, sub_pipeline) in enumerate(spec.items()):
            if not isinstance(facet_name, str) or not isinstance(
                sub_pipeline, list
            ):
                return None, []

            if sub_pipeline:
                if not self.can_optimize_pipeline(sub_pipeline):
                    return None, []
                sub_ctx = PipelineContext()
                sub_ctx.cte_prefix = f"{context.cte_prefix}_f{uid}_{f_idx}"
                sub_needs_root = self._pipeline_needs_root(sub_pipeline)
                if sub_needs_root:
                    sub_ctx.preserve_root()
                sub_final, sub_ctes = self._build_stage_sequence(
                    sub_pipeline,
                    src_cte,
                    f"{sub_ctx.cte_prefix}_s",
                    sub_ctx,
                    needs_root=sub_needs_root,
                )
                if sub_final is None:
                    return None, []
                for cname, csql, cparams in sub_ctes:
                    context.add_pre_cte(cname, csql, cparams)
                if sub_ctx.needs_recursive:
                    context.mark_recursive()
            else:
                sub_final = src_cte

            arr_expr = (
                f"json(COALESCE((SELECT json_group_array({doc_expr}) "
                f"FROM {sub_final} WHERE EXISTS (SELECT 1 FROM {src_cte})), '[]'))"
            )
            json_obj_args.append("?")
            json_obj_args.append(arr_expr)
            all_params.append(facet_name)

        data_expr = f"json(json_object({', '.join(json_obj_args)}))"
        select_parts = ["NULL AS id", "NULL AS _id", f"{data_expr} AS data"]
        if context.has_root:
            select_parts.append("NULL AS root_data")
        return f"SELECT {', '.join(select_parts)}", all_params
