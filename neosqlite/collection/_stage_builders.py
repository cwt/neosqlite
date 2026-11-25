"""Stage-builder mixin for SQLTierAggregator.

Contains all _build_*_sql methods extracted from sql_tier_aggregator.py.
These methods translate individual aggregation pipeline stages ($match,
$group, $sort, $lookup, etc.) into SQL CTE expressions.

Designed as a mixin to be composed into SQLTierAggregator.
"""

from __future__ import annotations

import re
from itertools import pairwise
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
from .jsonb_support import JSONBContext
from .pipeline_context import PipelineContext
from .query_helper.utils import _check_sqlite_version


def _append_root_column(
    select_parts: list[str],
    context: PipelineContext,
    preserve_root: bool,
    table_alias: str = "",
) -> None:
    """Append the ``root_data`` projection when ``$$ROOT`` is active.

    Projects ``data AS root_data`` when ``preserve_root`` is ``True`` on the
    first root-preserving stage, or propagates ``root_data`` when
    ``context.has_root`` is already ``True``.

    Args:
        select_parts: Mutable list of SQL ``SELECT`` projection items to
            extend in place.
        context: Active ``PipelineContext`` tracking ``has_root`` state.
        preserve_root: Whether this stage initializes ``root_data`` from
            ``data``.
        table_alias: Optional SQL table alias prefix (without trailing dot).
            Defaults to ``""``.
    """
    prefix = f"{table_alias}." if table_alias else ""
    if preserve_root:
        select_parts.append(f"{prefix}data AS root_data")
    elif context.has_root:
        select_parts.append(f"{prefix}root_data")


def _graph_lookup_scalar_or_oid(expr_sql: str) -> str:
    """Normalize encoded ObjectId JSON dicts to their 24-hex id string.

    Wraps ``expr_sql`` in a SQL ``CASE`` expression that extracts ``$.id``
    when the expression evaluates to valid JSON text with
    ``$.__neosqlite_objectid__ = 1``, leaving all other values unchanged.

    Args:
        expr_sql: SQL expression whose value may be a scalar or an encoded
            NeoSQLite ``ObjectId`` JSON string.

    Returns:
        str: SQL expression yielding the normalized scalar or ObjectId string.
    """
    return (
        f"CASE WHEN typeof({expr_sql}) = 'text' AND json_valid({expr_sql}) "
        f"AND json_extract({expr_sql}, '$.__neosqlite_objectid__') = 1 "
        f"THEN json_extract({expr_sql}, '$.id') ELSE {expr_sql} END"
    )


def build_redact_where_sql(
    evaluator: Any, spec: Any
) -> tuple[str | None, list[Any]]:
    """Translate ``$redact`` specs using ``$$KEEP``/``$$PRUNE`` to SQL WHERE.

    Supports ``"$$KEEP"``, ``"$$PRUNE"``, and ``{"$cond": ...}`` (dict or
    3-element list forms) whose ``then`` and ``else`` branches evaluate solely
    to ``"$$KEEP"`` or ``"$$PRUNE"``. Returns ``(None, [])`` when Python
    fallback is required (for example, when ``$$DESCEND`` is used or the
    condition cannot be compiled).

    Args:
        evaluator: Expression evaluator exposing
            ``evaluate_for_aggregation``.
        spec: MongoDB ``$redact`` expression specification.

    Returns:
        tuple[str | None, list[Any]]: ``(where_clause, params)`` where
            ``where_clause`` is ``""`` for keep-all, ``"WHERE ..."`` for
            filtered execution, or ``None`` when SQL compilation is not
            possible.
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
    jsonb: JSONBContext,
    source_table: str,
    recurse_cte: str,
    spec: dict[str, Any],
    has_root: bool = False,
) -> tuple[str, str, list[Any]] | None:
    """Build recursive CTE and SELECT SQL parts for ``$graphLookup``.

    Validates the ``$graphLookup`` specification (``from``, ``startWith``,
    ``connectFromField``, ``connectToField``, ``as``, and optional
    ``maxDepth``, ``depthField``, ``restrictSearchWithMatch``), constructs the
    ``UNION ALL`` recursive CTE with cycle prevention via ``visited`` row IDs,
    and builds the outer ``SELECT`` grouping deduplicated matches per source
    document.

    Args:
        collection: Source ``Collection`` instance providing database access
            for resolving ``restrictSearchWithMatch`` on the target collection.
        jsonb: Active ``JSONBContext`` supplying JSON function names.
        source_table: Name of the source table or CTE feeding ``$graphLookup``.
        recurse_cte: Name assigned to the generated recursive CTE.
        spec: MongoDB ``$graphLookup`` stage specification dictionary.
        has_root: Whether ``root_data`` must be preserved in the output
            projection. Defaults to ``False``.

    Returns:
        tuple[str, str, list[Any]] | None: ``(recurse_sql, select_sql,
            rec_params)`` when compilable to SQL, or ``None`` when Python
            fallback is required.
    """
    if not isinstance(spec, dict):
        return None
    _json_prefix = jsonb.json_function_prefix
    _json_each = jsonb.json_each_function

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
        start_sql = f"{_json_prefix}_extract(p.data, '{sw_path}')"
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
        helper._disable_indexed_union = True
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
        """Build a SQL join predicate matching a value to ``connectToField``.

        Normalizes encoded ObjectId values on both sides and checks scalar or
        array membership on target table alias ``t``.

        Args:
            val_sql: SQL expression yielding the lookup key value.

        Returns:
            str: SQL boolean predicate matching ``t`` against ``val_sql``.
        """
        norm_val = _graph_lookup_scalar_or_oid(val_sql)
        if connect_to_field == "_id":
            return f"t._id = {norm_val}"
        to_path = parse_json_path(connect_to_field)
        scalar_to = _graph_lookup_scalar_or_oid(
            f"{_json_prefix}_extract(t.data, '{to_path}')"
        )
        return (
            f"(CASE WHEN json_type(t.data, '{to_path}') = 'array' "
            f"THEN EXISTS (SELECT 1 FROM {_json_each}(t.data, '{to_path}') _te "
            f"WHERE {_graph_lookup_scalar_or_oid('_te.value')} = {norm_val}) "
            f"ELSE {scalar_to} = {norm_val} END)"
        )

    if (
        start_with == "$_id"
        or (isinstance(start_with, str) and start_with.startswith("$"))
        or isinstance(start_with, list)
    ):
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
                SELECT 1 FROM {_json_each}(_p._sw) _se
                WHERE _se.value IS NOT NULL AND {target_match('_se.value')}
            )
            ELSE _p._sw IS NOT NULL AND {target_match('_p._sw')}
            END
        )
        WHERE 1=1 {restrict_where}
    """

    if connect_from_field == "_id":
        rec_join_cond = (
            f"r.found_id IS NOT NULL AND {target_match('r.found_id')}"
        )
    else:
        from_path = parse_json_path(connect_from_field)
        scalar_from = f"{_json_prefix}_extract(r.found_data, '{from_path}')"
        rec_join_cond = (
            f"CASE WHEN json_type(r.found_data, '{from_path}') = 'array' "
            f"THEN EXISTS (SELECT 1 FROM {_json_each}(r.found_data, '{from_path}') _re "
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

    json_set_func = jsonb.function("set")
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
    data_expr = f"json({json_set_func}(p.data, '{parse_json_path(str(as_field))}', {group_expr}))"
    select_parts = ["p.id AS id", "p._id AS _id", f"{data_expr} AS data"]
    if has_root:
        select_parts.append("p.root_data")
    select_sql = f"SELECT {', '.join(select_parts)} FROM {source_table} p"

    return recurse_sql, select_sql, rec_params


class StageBuildersMixin:
    """Mixin providing ``_build_*_sql`` stage compilers for SQLTierAggregator.

    Translates individual MongoDB aggregation pipeline stages into SQLite CTE
    queries preserving the ``(id, _id, data[, root_data])`` column contract.
    Expects the host class to supply ``evaluator``, ``collection``,
    ``jsonb``, ``_get_json_extract``, ``_get_json_set``,
    ``can_optimize_pipeline``, and ``_pipeline_needs_root``.
    """

    # Attributes supplied by SQLTierAggregator
    evaluator: Any
    collection: Any
    jsonb: Any

    def _get_json_extract(  # type: ignore[empty-body]
        self, path: str | None = None
    ) -> str:
        """Return the SQL JSON extract function name or call expression.

        Args:
            path: Optional JSON path suffix (without leading ``"$"``).
                Defaults to ``None``.

        Returns:
            str: Function name (``json_extract`` or ``jsonb_extract``), or a
                call expression against ``data`` when ``path`` is given.
        """
        ...

    def _get_json_set(self) -> str:  # type: ignore[empty-body]
        """Return the SQL JSON set function name for this connection.

        Returns:
            str: ``"json_set"`` or ``"jsonb_set"`` based on JSONB support.
        """
        ...

    def can_optimize_pipeline(  # type: ignore[empty-body]
        self, pipeline: list[dict[str, Any]]
    ) -> bool:
        """Check whether an aggregation pipeline can run in the SQL tier.

        Args:
            pipeline: List of MongoDB aggregation stage dictionaries.

        Returns:
            bool: ``True`` if all stages can be compiled to SQL, ``False``
                otherwise.
        """
        ...

    def _pipeline_needs_root(  # type: ignore[empty-body]
        self, pipeline: list[dict[str, Any]]
    ) -> bool:
        """Check whether any stage in a pipeline references ``$$ROOT``.

        Args:
            pipeline: List of MongoDB aggregation stage dictionaries.

        Returns:
            bool: ``True`` if ``$$ROOT`` must be preserved across stages,
                ``False`` otherwise.
        """
        ...

    def _make_agg_context(
        self, context: PipelineContext, preserve_root: bool = False
    ) -> AggregationContext:
        """Create an AggregationContext scoped to the current pipeline stage.

        Copies the current stage index from ``context`` and configures
        ``root_column = "root_data"`` when root preservation is active and not
        initialized by the current stage.

        Args:
            context: Active ``PipelineContext`` for the pipeline being built.
            preserve_root: Whether the current stage is the first stage
                initializing ``root_data`` from ``data``. Defaults to
                ``False``.

        Returns:
            AggregationContext: Context instance configured for evaluating
                stage expressions.
        """
        agg_ctx = AggregationContext()
        agg_ctx.stage_index = context.stage_index
        if context.has_root and not preserve_root:
            agg_ctx.root_column = "root_data"
        return agg_ctx

    def _preserved_id_json_expr(self) -> str:
        """Build SQL expression preserving original document ``$._id`` type (e.g., ObjectId)."""
        json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
        return (
            f"CASE "
            f"WHEN json_type(data, '$._id') IN ('object', 'array') THEN json({json_extract_func}(data, '$._id')) "
            f"WHEN json_type(data, '$._id') = 'true' THEN json('true') "
            f"WHEN json_type(data, '$._id') = 'false' THEN json('false') "
            f"WHEN json_type(data, '$._id') IS NOT NULL THEN {json_extract_func}(data, '$._id') "
            f"ELSE _id END"
        )

    def _select_expr_preserving_bool(
        self, expr: Any, agg_ctx: AggregationContext
    ) -> tuple[str | None, list[Any]]:
        """Compile ``expr`` for JSON document construction while preserving booleans."""
        if isinstance(expr, bool):
            return ("json('true')" if expr else "json('false')"), []
        if isinstance(expr, dict) and len(expr) == 1:
            op_name = next(iter(expr))
            if op_name == "$literal" and isinstance(expr["$literal"], bool):
                return (
                    "json('true')" if expr["$literal"] else "json('false')"
                ), []
        expr_sql, expr_params = self.evaluator.build_select_expression(
            expr, context=agg_ctx
        )
        if expr_sql is None:
            return None, []
        if (
            isinstance(expr, dict)
            and len(expr) == 1
            and self.evaluator._get_operator_return_type(next(iter(expr)))
            == "bool"
        ):
            expr_sql = (
                f"(WITH _b(v) AS (SELECT ({expr_sql})) "
                f"SELECT CASE WHEN v = json('true') THEN json('true') "
                f"WHEN v = json('false') THEN json('false') "
                f"WHEN COALESCE(v, 0) != 0 THEN json('true') "
                f"ELSE json('false') END FROM _b)"
            )
        return expr_sql, expr_params

    def _build_stage_sequence(
        self,
        pipeline: list[dict[str, Any]],
        prev_stage: str,
        cte_prefix: str,
        context: PipelineContext,
        needs_root: bool = False,
    ) -> tuple[str | None, list[tuple[str, str, list[Any]]]]:
        """Compile a sequence of stages into ordered CTE definitions.

        Iterates through ``pipeline``, builds SQL for each stage, drains any
        auxiliary pre-CTEs registered on ``context`` before appending the stage
        CTE, and aborts with ``(None, [])`` if any stage requires fallback.

        Args:
            pipeline: Ordered list of MongoDB aggregation stage dictionaries.
            prev_stage: Name of the initial input table or CTE.
            cte_prefix: Prefix used to generate sequential CTE names.
            context: Mutable ``PipelineContext`` tracking stage state and
                auxiliary CTEs.
            needs_root: Whether the pipeline requires preserving ``$$ROOT``
                from the first stage onward. Defaults to ``False``.

        Returns:
            tuple[str | None, list[tuple[str, str, list[Any]]]]: Pair of the
                final CTE/table name (or ``None`` on fallback) and ordered
                ``(cte_name, cte_sql, cte_params)`` tuples.
        """
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
        """Dispatch a single aggregation stage to its SQL builder method.

        Args:
            stage_name: MongoDB stage operator name (e.g. ``"$match"``).
            stage_spec: Stage configuration payload from the pipeline dict.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext`` for the enclosing pipeline.
            preserve_root: Whether this stage should capture ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: Compiled SQL query and parameter
                list, or ``(None, [])`` if the stage is unsupported in SQL.
        """
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
                return self._build_group_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$match":
                return self._build_match_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$sort":
                return self._build_sort_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$skip":
                return self._build_skip_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$limit":
                return self._build_limit_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
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
                return self._build_sample_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$bucket":
                return self._build_bucket_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$bucketAuto":
                return self._build_bucket_auto_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$unionWith":
                return self._build_union_with_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$redact":
                return self._build_redact_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$lookup":
                return self._build_lookup_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$densify":
                return self._build_densify_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$graphLookup":
                return self._build_graph_lookup_sql(
                    stage_spec, prev_stage, context, preserve_root
                )
            case "$facet":
                return self._build_facet_sql(stage_spec, prev_stage, context)
            case _:
                return None, []

    def _build_addfields_sql(self, spec, prev_stage, context, preserve_root):
        """Build SQL for the ``$addFields`` (and ``$set``) stage.

        Updates or inserts fields in ``data`` via ``json_set``/``jsonb_set``,
        supporting ``$$CURRENT``, ``$$ROOT``, and compiled expressions while
        registering computed fields on ``context``.

        Args:
            spec: Dictionary mapping field paths to field expressions.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext`` for tracking computed fields.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when an unsupported aggregation variable is
                referenced.
        """
        all_params = []
        json_set_args = []
        for field, expr in spec.items():
            if _is_aggregation_variable(expr):
                json_set_func = self._get_json_set()
                if expr == "$$CURRENT":
                    expr_sql = (
                        f"json(CASE WHEN json_type(data, '$._id') IS NOT NULL "
                        f"OR _id IS NULL THEN data "
                        f"ELSE {json_set_func}(data, '$._id', _id) END)"
                    )
                elif expr == "$$ROOT":
                    root_source = (
                        "root_data"
                        if (context.has_root and not preserve_root)
                        else "data"
                    )
                    expr_sql = (
                        f"json(CASE WHEN json_type({root_source}, '$._id') IS NOT NULL "
                        f"OR _id IS NULL THEN {root_source} "
                        f"ELSE {json_set_func}({root_source}, '$._id', _id) END)"
                    )
                elif expr.startswith("$$ROOT.") or expr.startswith(
                    "$$CURRENT."
                ):
                    agg_ctx = self._make_agg_context(context, preserve_root)
                    maybe_sql, expr_params = (
                        self.evaluator.build_select_expression(
                            expr, context=agg_ctx
                        )
                    )
                    if maybe_sql is None:
                        return None, []
                    expr_sql = maybe_sql
                    all_params.extend(expr_params)
                else:
                    return None, []
            else:
                agg_ctx = self._make_agg_context(context, preserve_root)
                maybe_sql, expr_params = self._select_expr_preserving_bool(
                    expr, agg_ctx
                )
                if maybe_sql is None:
                    return None, []
                expr_sql = maybe_sql
                all_params.extend(expr_params)
            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(expr_sql)

        json_set_func = self._get_json_set()
        if json_set_args:
            data_expr = (
                f"json({json_set_func}(data, {', '.join(json_set_args)}))"
            )
        else:
            data_expr = "data"

        select_parts = ["id", "_id", f"{data_expr} AS data"]
        _append_root_column(select_parts, context, preserve_root)
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_project_sql(self, spec, prev_stage, context, preserve_root):
        """Build SQL for the ``$project`` stage.

        Supports exclusion projections (via ``json_remove``/``jsonb_remove``)
        and inclusion or computed-field projections (via
        ``json_object``/``jsonb_object`` with missing-field cleanup), keeping
        ``_id`` unless ``_id: 0`` is specified. Returns ``(None, [])`` for
        dotted non-``_id`` keys or uncompilable expressions.

        Args:
            spec: Projection specification dictionary.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext`` for tracking computed fields.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when Python fallback is required.
        """
        if any("." in k for k in spec if k != "_id"):
            return None, []
        all_params = []
        id_spec = spec.get("_id", 1)
        id_is_flag = isinstance(id_spec, (int, bool)) and id_spec in (0, 1)
        include_id = not (id_is_flag and id_spec in (0, False))
        has_computed_id = "_id" in spec and not id_is_flag

        json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
        json_obj_func = f"{self.jsonb.json_function_prefix}_object"
        json_set_func = self._get_json_set()

        is_exclusion = not has_computed_id and not any(
            value == 1
            or _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for field, value in spec.items()
            if field != "_id"
        )

        def _compile_proj_field(
            field_name: str,
            val: Any,
            parts: list[str],
            removals: list[str],
        ) -> tuple[bool, str | None]:
            """Append a projected field to ``parts`` and ``removals``."""
            escaped = field_name.replace("'", "''")
            if _is_expression(val):
                agg_ctx = self._make_agg_context(context, preserve_root)
                e_sql, e_params = self._select_expr_preserving_bool(
                    val, agg_ctx
                )
                if e_sql is None:
                    return False, None
                all_params.extend(e_params)
                parts.append(f"'{escaped}'")
                parts.append(e_sql)
                return True, e_sql
            if val in ("$$ROOT", "$$CURRENT"):
                src_col = (
                    "root_data"
                    if (
                        val == "$$ROOT"
                        and context.has_root
                        and not preserve_root
                    )
                    else "data"
                )
                doc_sql = (
                    f"json(CASE WHEN json_type({src_col}, '$._id') IS NOT NULL "
                    f"OR _id IS NULL THEN {src_col} "
                    f"ELSE {json_set_func}({src_col}, '$._id', _id) END)"
                )
                parts.append(f"'{escaped}'")
                parts.append(doc_sql)
                return True, doc_sql
            if isinstance(val, str) and (
                val.startswith("$$ROOT.") or val.startswith("$$CURRENT.")
            ):
                is_root_ref = val.startswith("$$ROOT.")
                sub_path = val.split(".", 1)[1]
                src_col = (
                    "root_data"
                    if (is_root_ref and context.has_root and not preserve_root)
                    else "data"
                )
                sub_jpath = parse_json_path(sub_path, preserve_id=False)
                extract_sql = f"{json_extract_func}({src_col}, '{sub_jpath}')"
                parts.append(f"'{escaped}'")
                parts.append(extract_sql)
                removals.append(
                    f"CASE WHEN json_type({src_col}, '{sub_jpath}') IS NULL "
                    f"THEN '{parse_json_path(field_name, preserve_id=False)}' ELSE '$.__none__' END"
                )
                return True, extract_sql
            if val == "$$REMOVE":
                return True, None
            if isinstance(val, str) and val.startswith("$$"):
                return False, None
            if isinstance(val, str) and val.startswith("$"):
                src_field = val[1:]
                if src_field == "_id":
                    id_sql = self._preserved_id_json_expr()
                    parts.append(f"'{escaped}'")
                    parts.append(id_sql)
                    removals.append(
                        f"CASE WHEN json_type(data, '$._id') IS NULL AND _id IS NULL "
                        f"THEN '{parse_json_path(field_name, preserve_id=False)}' ELSE '$.__none__' END"
                    )
                    return True, "_id"
                src_jpath = parse_json_path(src_field)
                extract_sql = f"{json_extract_func}(data, '{src_jpath}')"
                parts.append(f"'{escaped}'")
                parts.append(extract_sql)
                removals.append(
                    f"CASE WHEN json_type(data, '{src_jpath}') IS NULL "
                    f"THEN '{parse_json_path(field_name, preserve_id=False)}' ELSE '$.__none__' END"
                )
                return True, extract_sql
            if val == 1 or (isinstance(val, bool) and val):
                jpath = parse_json_path(field_name)
                extract_sql = f"{json_extract_func}(data, '{jpath}')"
                parts.append(f"'{escaped}'")
                parts.append(extract_sql)
                removals.append(
                    f"CASE WHEN json_type(data, '{jpath}') IS NULL "
                    f"THEN '{jpath}' ELSE '$.__none__' END"
                )
                return True, extract_sql
            if val == 0 or (isinstance(val, bool) and not val):
                return True, None
            # Literal value for _id (e.g. {"_id": "const"})
            agg_ctx = self._make_agg_context(context, preserve_root)
            lit_sql, lit_params = self._select_expr_preserving_bool(
                val, agg_ctx
            )
            if lit_sql is None:
                return False, None
            all_params.extend(lit_params)
            parts.append(f"'{escaped}'")
            parts.append(lit_sql)
            return True, lit_sql

        id_col_sql = "_id" if include_id else "NULL AS _id"
        if is_exclusion:
            fields_to_remove = [
                field
                for field, value in spec.items()
                if value == 0 and (field != "_id" or not include_id)
            ]
            if not include_id and "_id" not in fields_to_remove:
                fields_to_remove.append("_id")
            if fields_to_remove:
                json_remove_func = f"{self.jsonb.json_function_prefix}_remove"
                path_args = ", ".join(
                    f"'{parse_json_path(f, preserve_id=False)}'"
                    for f in fields_to_remove
                )
                data_expr = f"json({json_remove_func}(data, {path_args}))"
            else:
                data_expr = "data"
            select_parts = ["id", id_col_sql]
            _append_root_column(select_parts, context, preserve_root)
            select_parts.append(f"{data_expr} AS data")
            return (
                f"SELECT {', '.join(select_parts)} FROM {prev_stage}",
                all_params,
            )

        json_parts: list[str] = []
        missing_removals: list[str] = []
        if has_computed_id:
            ok, _ = _compile_proj_field(
                "_id", id_spec, json_parts, missing_removals
            )
            if not ok:
                return None, []
        elif include_id:
            json_parts.append("'_id'")
            json_parts.append(self._preserved_id_json_expr())
            missing_removals.append(
                "CASE WHEN json_type(data, '$._id') IS NULL AND _id IS NULL "
                "THEN '$._id' ELSE '$.__none__' END"
            )

        for field, value in spec.items():
            if field == "_id":
                continue
            ok, _ = _compile_proj_field(
                field, value, json_parts, missing_removals
            )
            if not ok:
                return None, []

        if json_parts:
            base_obj = f"{json_obj_func}({', '.join(json_parts)})"
            if missing_removals:
                json_remove_func = f"{self.jsonb.json_function_prefix}_remove"
                base_obj = f"{json_remove_func}({base_obj}, {', '.join(missing_removals)})"
            data_expr = f"json({base_obj})"
        else:
            data_expr = "json('{}')"

        if has_computed_id:
            # Compute data once in subquery so computed _id params aren't duplicated
            inner_parts = ["id", f"{data_expr} AS _pdata"]
            _append_root_column(inner_parts, context, preserve_root)
            outer_parts = [
                "id",
                f"{self.jsonb.json_function_prefix}_extract(_pdata, '$._id') AS _id",
                "_pdata AS data",
            ]
            if preserve_root or context.has_root:
                outer_parts.append("root_data")
            return (
                f"SELECT {', '.join(outer_parts)} "
                f"FROM (SELECT {', '.join(inner_parts)} FROM {prev_stage})"
            ), all_params

        select_parts = ["id", id_col_sql]
        _append_root_column(select_parts, context, preserve_root)
        select_parts.append(f"{data_expr} AS data")

        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", all_params

    def _build_unset_sql(self, spec, prev_stage, context, preserve_root):
        """Build SQL for the ``$unset`` stage.

        Removes one or more specified field paths from ``data`` using
        ``json_remove``/``jsonb_remove`` and ensures ``$._id`` remains set.

        Args:
            spec: Single field name string or list of field names to remove.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``.

        Returns:
            tuple[str, list[Any]]: SQL ``SELECT`` query and empty parameter
                list.
        """
        fields = [spec] if isinstance(spec, str) else spec
        json_remove_func = self.jsonb.function("remove")
        json_set_func = self._get_json_set()
        json_paths = [
            f"'{parse_json_path(f, preserve_id=False)}'" for f in fields
        ]
        if "_id" in fields:
            data_expr = (
                f"json({json_remove_func}(data, {', '.join(json_paths)}))"
            )
            select_parts = ["id", "NULL AS _id", f"{data_expr} AS data"]
        else:
            data_expr = f"json({json_set_func}({json_remove_func}(data, {', '.join(json_paths)}), '$._id', _id))"
            select_parts = ["id", "_id", f"{data_expr} AS data"]
        _append_root_column(select_parts, context, preserve_root)
        return f"SELECT {', '.join(select_parts)} FROM {prev_stage}", []

    def _build_replace_root_sql(self, spec, prev_stage, context, preserve_root):
        """Build SQL for ``$replaceRoot`` and ``$replaceWith`` stages.

        Replaces each document's ``data`` column with ``spec["newRoot"]``
        (supporting ``$field`` references, ``$$ROOT``, ``$$CURRENT``, dotted
        ``$$ROOT.<path>`` / ``$$CURRENT.<path>``, or compiled expressions)
        while preserving a child document's own ``_id`` when present and falling
        back to the outer ``_id`` only when absent.

        Args:
            spec: Stage dictionary containing the ``"newRoot"`` expression.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``.

        Returns:
            tuple[str, list[Any]]: SQL ``SELECT`` query and bound parameter
                list.
        """
        new_root = spec.get("newRoot")
        all_params: list[Any] = []
        json_set_func = self._get_json_set()
        json_extract_func = f"{self.jsonb.json_function_prefix}_extract"
        if (
            isinstance(new_root, str)
            and new_root.startswith("$")
            and not new_root.startswith("$$")
        ):
            field = new_root[1:]
            expr_sql = f"{json_extract_func}(data, '{parse_json_path(field)}')"
        elif new_root == "$$ROOT":
            src = (
                "root_data"
                if (context.has_root and not preserve_root)
                else "data"
            )
            expr_sql = src
        elif new_root == "$$CURRENT":
            expr_sql = "data"
        else:
            agg_ctx = self._make_agg_context(context, preserve_root)
            expr_sql, expr_params = self.evaluator.build_select_expression(
                new_root, context=agg_ctx
            )
            if expr_sql is None:
                return None, []
            all_params.extend(expr_params)

        inner_parts = ["id", "_id", f"{expr_sql} AS _new_data"]
        if preserve_root:
            inner_parts.append("data AS root_data")
        elif context.has_root:
            inner_parts.append("root_data")

        data_expr = (
            f"json(CASE "
            f"WHEN json_type(_new_data, '$._id') IS NOT NULL OR _id IS NULL THEN _new_data "
            f"ELSE {json_set_func}(_new_data, '$._id', _id) END)"
        )
        id_expr = (
            "CASE WHEN json_type(_new_data, '$._id') IS NOT NULL "
            f"THEN {self.jsonb.json_function_prefix}_extract(_new_data, '$._id') ELSE _id END AS _id"
        )
        outer_parts = ["id", id_expr, f"{data_expr} AS data"]
        if preserve_root or context.has_root:
            outer_parts.append("root_data")
        return (
            f"SELECT {', '.join(outer_parts)} "
            f"FROM (SELECT {', '.join(inner_parts)} FROM {prev_stage})"
        ), all_params

    def _build_sample_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$sample`` stage using ``ORDER BY RANDOM()``.

        Args:
            spec: Stage specification dictionary with optional ``"size"`` key
                (default ``1``).
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str, list[Any]]: SQL query and single-element parameter list
                containing the sample size.
        """
        size = spec.get("size", 1)
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError(
                f"The $sample size must be a non-negative integer, got {size}"
            )
        select_parts = ["id", "_id", "data"]
        _append_root_column(select_parts, context, preserve_root)
        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY RANDOM() LIMIT ?"
        return sql, [size]

    def _compile_bucket_accumulators(
        self,
        output: dict[str, Any],
        agg_ctx: AggregationContext,
    ) -> tuple[list[str] | None, list[Any], list[tuple[str, str]], set[str]]:
        """Compile ``$bucket``/``$bucketAuto`` output accumulators to SQL.

        Args:
            output: Mapping of output field names to accumulator specs.
            agg_ctx: Active aggregation expression context.

        Returns:
            tuple: ``(acc_select_parts, acc_params, output_aliases, array_fields)``
                where ``acc_select_parts`` is ``None`` when any accumulator is
                unsupported in SQL Tier 1.
        """
        acc_parts: list[str] = []
        acc_params: list[Any] = []
        output_aliases: list[tuple[str, str]] = []
        array_fields: set[str] = set()

        for field, acc_spec in output.items():
            if not isinstance(acc_spec, dict) or len(acc_spec) != 1:
                return None, [], [], set()
            op, expr = next(iter(acc_spec.items()))
            sql_alias = f"__acc_{len(output_aliases)}"
            if op == "$count":
                acc_parts.append(f"COUNT(*) AS {sql_alias}")
            elif op == "$sum" and expr == 1 and not isinstance(expr, bool):
                acc_parts.append(f"COUNT(*) AS {sql_alias}")
            else:
                agg_result = self._map_accumulator_to_sql(op)
                if not agg_result:
                    return None, [], [], set()
                sql_agg, use_distinct = agg_result
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    expr, context=agg_ctx
                )
                if expr_sql is None:
                    return None, [], [], set()
                acc_params.extend(expr_params)
                distinct_str = "DISTINCT " if use_distinct else ""
                agg_call = f"{sql_agg}({distinct_str}{expr_sql})"
                if op == "$sum":
                    agg_call = f"COALESCE({agg_call}, 0)"
                acc_parts.append(f"{agg_call} AS {sql_alias}")
                if sql_agg == "json_group_array":
                    array_fields.add(field)
            output_aliases.append((field, sql_alias))

        return acc_parts, acc_params, output_aliases, array_fields

    def _build_bucket_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$bucket`` stage.

        Categorizes documents into half-open ``[lower, upper)`` intervals
        defined by ``boundaries`` (with an optional ``default`` bucket
        defaulting to ``"Other"``) and computes ``output`` accumulators per
        bucket while preserving the ``(id, _id, data)`` CTE column contract.
        ``groupBy`` is precomputed once in an inner subquery so bound
        parameters are never duplicated across boundary ``CASE`` branches.

        Args:
            spec: Bucket specification containing ``"groupBy"``,
                ``"boundaries"``, optional ``"default"``, and optional
                ``"output"``.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` if required keys are missing or an accumulator
                is unsupported.
        """
        group_by = spec.get("groupBy")
        boundaries = spec.get("boundaries")
        default = spec.get("default", "Other")
        output = spec.get("output", {"count": {"$sum": 1}})

        if group_by is None or group_by == "" or not boundaries:
            return None, []

        try:
            sorted_boundaries = sorted(boundaries)
        except TypeError:
            return None, []

        agg_ctx = self._make_agg_context(context, preserve_root)
        key_sql, key_params = self.evaluator.build_select_expression(
            group_by, context=agg_ctx
        )
        if key_sql is None:
            return None, []

        acc_parts, acc_params, output_aliases, array_fields = (
            self._compile_bucket_accumulators(output, agg_ctx)
        )
        if acc_parts is None:
            return None, []

        case_parts = []
        case_params: list[Any] = []
        for lower, upper in pairwise(sorted_boundaries):
            case_parts.append("WHEN __gval >= ? AND __gval < ? THEN ?")
            case_params.extend([lower, upper, lower])

        if not case_parts:
            if default is not None:
                bucket_id_sql = "?"
                case_params.append(default)
            else:
                bucket_id_sql = "NULL"
        else:
            if default is not None:
                case_parts.append("ELSE ?")
                case_params.append(default)
            bucket_id_sql = f"CASE {' '.join(case_parts)} END"

        json_args = ", ".join(
            ["'_id', __bucket_id"]
            + [
                (
                    f"'{field.replace(chr(39), chr(39) * 2)}', json({alias})"
                    if field in array_fields
                    else f"'{field.replace(chr(39), chr(39) * 2)}', {alias}"
                )
                for field, alias in output_aliases
            ]
        )

        outer_cols = [
            "NULL AS id",
            "__bucket_id AS _id",
            f"json_object({json_args}) AS data",
        ]
        if context.has_root:
            outer_cols.append(f"json_object({json_args}) AS root_data")

        sub_parts = ["__bucket_id"] + acc_parts
        all_params = acc_params + case_params + list(key_params)
        sql = (
            f"SELECT {', '.join(outer_cols)} "
            f"FROM ("
            f"SELECT {', '.join(sub_parts)} "
            f"FROM ("
            f"SELECT __bucket_src.*, {bucket_id_sql} AS __bucket_id "
            f"FROM (SELECT *, ({key_sql}) AS __gval FROM {prev_stage}) AS __bucket_src"
            f") "
            f"GROUP BY __bucket_id"
            f") "
            f"ORDER BY __bucket_id"
        )
        return sql, all_params

    def _build_bucket_auto_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$bucketAuto`` stage using equal-count bins.

        Partitions documents across up to ``buckets`` (default ``10``) groups
        ordered by ``groupBy`` using ``DENSE_RANK`` so identical values remain
        in the same bucket, emitting ``{_id: {min, max}, ...}`` documents.
        Falls back to ``(None, [])`` when ``groupBy`` is missing,
        ``granularity`` is specified, SQLite is older than 3.25.0, or an
        accumulator cannot be mapped to SQL.

        Args:
            spec: Auto-bucket specification with ``"groupBy"``, optional
                ``"buckets"``, optional ``"output"``, and optional
                ``"granularity"``.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when Python fallback is required.

        Raises:
            ValueError: If ``buckets`` is not a positive non-boolean integer.
        """
        group_by = spec.get("groupBy")
        buckets = spec.get("buckets", 10)
        output = spec.get("output", {"count": {"$sum": 1}})
        granularity = spec.get("granularity")

        if (
            not isinstance(buckets, int)
            or isinstance(buckets, bool)
            or buckets <= 0
        ):
            raise ValueError(
                "The $bucketAuto 'buckets' option must be a positive integer, "
                f"got {buckets!r}"
            )

        if group_by is None or group_by == "" or granularity:
            return None, []

        if not _check_sqlite_version("3.25.0"):
            return None, []

        agg_ctx = self._make_agg_context(context, preserve_root)
        key_sql, key_params = self.evaluator.build_select_expression(
            group_by, context=agg_ctx
        )
        if key_sql is None:
            return None, []

        acc_parts, grouped_params, output_aliases, array_fields = (
            self._compile_bucket_accumulators(output, agg_ctx)
        )
        if acc_parts is None:
            return None, []

        json_args = ", ".join(
            ["'_id', json_object('min', __bmin, 'max', __bmax)"]
            + [
                (
                    f"'{field.replace(chr(39), chr(39) * 2)}', json({alias})"
                    if field in array_fields
                    else f"'{field.replace(chr(39), chr(39) * 2)}', {alias}"
                )
                for field, alias in output_aliases
            ]
        )

        root_select_col = (
            f",\n                json_object({json_args}) AS root_data"
            if context.has_root
            else ""
        )
        acc_select_sql = f", {', '.join(acc_parts)}" if acc_parts else ""
        sql = f"""
            SELECT
                NULL AS id,
                json_object('min', __bmin, 'max', __bmax) AS _id,
                json_object({json_args}) AS data{root_select_col}
            FROM (
                SELECT
                    __bucket_id,
                    CASE WHEN MAX(CASE WHEN __gval IS NULL THEN 1 ELSE 0 END) = 1 THEN NULL ELSE MIN(__gval) END AS __bmin,
                    MAX(__gval) AS __bmax{acc_select_sql}
                FROM (
                    SELECT
                        __dr.*,
                        ((__dr.__drank - 1) * ?) / NULLIF(MAX(__dr.__drank) OVER (), 0) AS __bucket_id
                    FROM (
                        SELECT
                            __bsrc.*,
                            DENSE_RANK() OVER (ORDER BY __bsrc.__gval) AS __drank
                        FROM (
                            SELECT
                                *,
                                ({key_sql}) AS __gval
                            FROM {prev_stage}
                        ) AS __bsrc
                    ) AS __dr
                )
                GROUP BY __bucket_id
            )
            ORDER BY __bmin
        """
        # Bind parameters in textual SQL order:
        # 1) grouped accumulator params
        # 2) NTILE-equivalent bucket count divisor (?)
        # 3) single inner groupBy expression key_params
        all_params = grouped_params + [buckets] + list(key_params)
        return sql, all_params

    def _build_union_with_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$unionWith`` stage using ``UNION ALL``.

        Accepts either a collection name string or a ``{"coll": ...,
        "pipeline": ...}`` dictionary, compiling any sub-pipeline via
        ``SQLTierAggregator.build_pipeline_sql`` and aligning ``root_data``
        columns when ``context.has_root`` is active.

        Args:
            spec: Collection name string or specification dictionary with
                ``"coll"`` and optional ``"pipeline"``.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: ``UNION ALL`` SQL query and bound
                parameters, or ``(None, [])`` when invalid or uncompilable.
        """
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
        _append_root_column(select_parts, context, preserve_root)
        if context.has_root:
            union_parts.append("data AS root_data")

        cols = ", ".join(select_parts)
        union_cols = ", ".join(union_parts)
        sql = f"SELECT {cols} FROM {prev_stage} UNION ALL SELECT {union_cols} FROM {union_from}"
        return sql, union_params

    def _build_redact_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$redact`` stage.

        Optimizes ``$$KEEP``/``$$PRUNE`` patterns into a ``WHERE`` clause via
        ``build_redact_where_sql`` and returns ``(None, [])`` to fall back to
        Python when ``$$DESCEND`` or unsupported expressions appear.

        Args:
            spec: MongoDB ``$redact`` expression specification.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when Python fallback is required.
        """
        agg_ctx = self._make_agg_context(context, preserve_root)
        old_ctx = self.evaluator._current_context
        self.evaluator._current_context = agg_ctx
        try:
            where_clause, expr_params = build_redact_where_sql(
                self.evaluator, spec
            )
        finally:
            self.evaluator._current_context = old_ctx
        if where_clause is None:
            return None, []

        select_parts = ["id", "_id", "data"]
        _append_root_column(select_parts, context, preserve_root)

        sql = f"SELECT {', '.join(select_parts)} FROM {prev_stage}"
        if where_clause:
            sql = f"{sql} {where_clause}"
        return sql, expr_params

    def _build_lookup_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$lookup`` left outer join stage.

        Supports equality joins on ``localField``/``foreignField`` against a
        foreign collection or a SQL-optimizable sub-``pipeline``. Aggregates
        matched documents into a JSON array under ``as`` using
        ``FILTER (WHERE ...)`` on SQLite >= 3.30.0 or a correlated subquery on
        older SQLite versions.

        Args:
            spec: Lookup specification containing ``"from"``, ``"as"``,
                ``"localField"``, ``"foreignField"``, and optional
                ``"pipeline"``.
            prev_stage: SQL table or CTE name supplying left-side rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when required fields are missing or the nested
                pipeline cannot be SQL-optimized.
        """
        from ..sql_utils import quote_table_name

        from_coll = spec.get("from")
        local_field = spec.get("localField")
        foreign_field = spec.get("foreignField")
        as_field = spec.get("as")
        pipeline = spec.get("pipeline", [])
        let_spec = spec.get("let")

        if not from_coll or not as_field or let_spec is not None:
            return None, []

        def _lookup_join_condition(
            l_field: str, l_expr: str, f_expr: str
        ) -> str:
            """Build scalar or array-expanding equality condition."""
            if l_field == "_id":
                return f"{f_expr} = {l_expr}"
            local_path = parse_json_path(l_field)
            elem_expr = (
                "CASE "
                "WHEN _je.type = 'object' "
                "AND json_extract(_je.value, '$.__neosqlite_objectid__') = 1 "
                "THEN json_extract(_je.value, '$.id') "
                "ELSE _je.value END"
            )
            return (
                f"("
                f"(json_type(t1.data, '{local_path}') = 'array' AND EXISTS ("
                f"SELECT 1 FROM json_each(t1.data, '{local_path}') AS _je "
                f"WHERE _je.value IS NOT NULL AND ({elem_expr}) = {f_expr}"
                f")) OR ("
                f"json_type(t1.data, '{local_path}') IS NOT 'array' "
                f"AND {f_expr} = {l_expr}"
                f"))"
            )

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
                    f"{self.jsonb.json_function_prefix}_extract(t1.data, '{parse_json_path(local_field)}')"
                )
            )
            foreign_expr = (
                "pipeline_result._id"
                if foreign_field == "_id"
                else _graph_lookup_scalar_or_oid(
                    f"{self.jsonb.json_function_prefix}_extract(pipeline_result.data, '{parse_json_path(foreign_field)}')"
                )
            )
            join_cond = _lookup_join_condition(
                local_field, local_expr, foreign_expr
            )

            joined_data_expr = f"""
                COALESCE((SELECT json_group_array(
                    json(CASE
                        WHEN json_type(pipeline_result.data, '$._id') IS NOT NULL
                            OR pipeline_result._id IS NULL
                        THEN pipeline_result.data
                        ELSE {json_set_func}(pipeline_result.data, '$._id', pipeline_result._id)
                    END)
                 )
                 FROM ({pipeline_sql}) AS pipeline_result
                 WHERE {join_cond}), '[]')
            """

            data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

            select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
            _append_root_column(
                select_parts, context, preserve_root, table_alias="t1"
            )

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
        assert local_field is not None
        join_cond = _lookup_join_condition(
            local_field, local_expr, foreign_expr
        )

        if _check_sqlite_version("3.30.0"):
            joined_data_expr = (
                f"COALESCE(json_group_array("
                f"json(CASE "
                f"WHEN json_type(t2.data, '$._id') IS NOT NULL OR t2._id IS NULL THEN t2.data "
                f"ELSE {json_set_func}(t2.data, '$._id', t2._id) END)"
                f") FILTER (WHERE t2.id IS NOT NULL), '[]')"
            )
            data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

            select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
            _append_root_column(
                select_parts, context, preserve_root, table_alias="t1"
            )

            # Use ROW_NUMBER() per row so rows sharing t1.id after $unionWith/$densify never collapse
            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM (SELECT *, ROW_NUMBER() OVER () AS __lkp_rn FROM {prev_stage}) AS t1
                LEFT JOIN {quote_table_name(from_coll)} AS t2 ON {join_cond}
                GROUP BY t1.__lkp_rn
            """
            return sql, []

        # SQLite < 3.30 has no aggregate FILTER clause. A correlated
        # subquery naturally excludes non-matching rows and is equivalent.
        joined_data_expr = (
            f"COALESCE((SELECT json_group_array("
            f"json(CASE "
            f"WHEN json_type(t2.data, '$._id') IS NOT NULL OR t2._id IS NULL THEN t2.data "
            f"ELSE {json_set_func}(t2.data, '$._id', t2._id) END)"
            f") FROM {quote_table_name(from_coll)} AS t2 "
            f"WHERE {join_cond}), '[]')"
        )
        data_expr = f"json({json_set_func}(t1.data, '{parse_json_path(as_field)}', json({joined_data_expr})))"

        select_parts = ["t1.id", "t1._id", f"{data_expr} AS data"]
        _append_root_column(
            select_parts, context, preserve_root, table_alias="t1"
        )

        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM {prev_stage} AS t1
        """
        return sql, []

    def _build_densify_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for ``$densify`` to fill numeric sequence gaps.

        Uses ``WITH RECURSIVE`` CTEs (``existing`` for distinct non-null field
        values and ``series`` generating up to 1001 stepped points across
        ``[lower_bound, upper_bound]``) and ``UNION ALL`` with ``NOT EXISTS``
        to synthesize missing documents.

        Limitations:
        - Only supports non-dotted numeric fields with numeric ``step > 0``
          and two numeric ``bounds``.
        - Does not support ``partitionBy``, ``partitionByFields``, or ``unit``.
        - Series generation is capped at ``idx < 1000`` steps.

        Args:
            spec: Densify stage dictionary with ``"field"`` and ``"range"``.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: Recursive CTE ``SELECT`` query and
                bound parameters, or ``(None, [])`` when fallback is required.
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

        orig_cols = ["t.id", "t._id", "t.data"]
        _append_root_column(orig_cols, context, preserve_root, table_alias="t")
        synth_root_col = ", NULL AS root_data" if context.has_root else ""
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
            SELECT {', '.join(orig_cols)}
            FROM {prev_stage} t
            UNION ALL
            SELECT NULL as id, NULL as _id, json_object(?, s.val) as data{synth_root_col}
            FROM series s
            WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.val = s.val)
        """
        return sql, all_params

    def _build_group_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build ``$group`` SQL preserving the ``(id, _id, data)`` invariant.

        Wraps the aggregate ``SELECT`` in an outer projection that materializes
        ``id``, ``_id``, and ``data`` (re-parsing ``json_group_array`` outputs
        so array accumulators store JSON arrays rather than strings) and adds a
        ``COUNT(*) AS __cnt`` guard when grouping without ``GROUP BY`` so
        empty input yields zero rows. Bails out with ``(None, [])`` on literal
        list/object accumulator inputs or unsupported accumulators.

        Args:
            spec: Group specification containing ``"_id"`` and accumulator
                fields.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when Python fallback is required.
        """
        agg_ctx = self._make_agg_context(context, preserve_root)
        all_params = []
        agg_select_parts = []
        group_by_parts = []
        group_id = spec.get("_id")
        if group_id is not None:
            key_sql, key_params = self.evaluator.build_select_expression(
                group_id, context=agg_ctx
            )
            agg_select_parts.append(f"{key_sql} AS _id")
            group_by_parts.append(key_sql)
            all_params.extend(key_params)
        else:
            agg_select_parts.append("NULL AS _id")

        output_aliases: list[tuple[str, str]] = []
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
            expr_sql, expr_params = self.evaluator.build_select_expression(
                expr, context=agg_ctx
            )
            all_params.extend(expr_params)
            distinct_str = "DISTINCT " if use_distinct else ""
            sql_alias = f"__acc_{len(output_aliases)}"
            agg_select_parts.append(
                f"{sql_agg}({distinct_str}{expr_sql}) AS {sql_alias}"
            )
            output_aliases.append((field, sql_alias))
            if sql_agg == "json_group_array":
                # json_group_array yields a JSON *string*; re-parse it so
                # data holds a real JSON array like MongoDB (#94).
                array_fields.add(field)

        group_by_clause = (
            f"GROUP BY {', '.join(group_by_parts)}" if group_by_parts else ""
        )

        # Reference the inner aliases from the wrapping SELECT level.
        if self.jsonb.jsonb_supported:
            id_json_arg = (
                "'_id', CASE WHEN typeof(_id) = 'blob' AND json_valid(_id, 4) "
                "THEN json(_id) "
                "WHEN typeof(_id) = 'text' AND json_valid(_id) "
                "AND json_type(_id) IN ('array', 'object') THEN json(_id) "
                "ELSE _id END"
            )
        else:
            id_json_arg = (
                "'_id', CASE WHEN typeof(_id) = 'text' AND json_valid(_id) "
                "AND json_type(_id) IN ('array', 'object') THEN json(_id) "
                "ELSE _id END"
            )
        json_args = ", ".join(
            [id_json_arg]
            + [
                (
                    f"'{field.replace(chr(39), chr(39) * 2)}', json({alias})"
                    if field in array_fields
                    else f"'{field.replace(chr(39), chr(39) * 2)}', {alias}"
                )
                for field, alias in output_aliases
            ]
        )
        data_sql = f"json_object({json_args})"
        select_parts = [
            "ROW_NUMBER() OVER () AS id",
            "_id AS _id",
            f"{data_sql} AS data",
        ]
        if context.has_root:
            select_parts.append(f"{data_sql} AS root_data")

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
        """Map a MongoDB ``$group`` accumulator operator to a SQL aggregate.

        Args:
            op: MongoDB accumulator operator name (such as ``"$sum"`` or
                ``"$addToSet"``).

        Returns:
            tuple[str, bool] | None: ``(sql_agg_func, use_distinct)`` if
                supported, or ``None`` otherwise.
        """
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
        """Return a cached SQLTranslator matching this aggregator's JSONB mode.

        Lazily initializes and caches ``self._match_sql_translator`` bound to
        ``self.collection.name``, ``data``/``id`` columns,
        ``self.collection.db``, and ``self.jsonb``.

        Returns:
            SQLTranslator: Cached translator for ``$match`` field operators.
        """
        translator = getattr(self, "_match_sql_translator", None)
        if translator is None:
            from .sql_translator_unified import SQLTranslator

            translator = SQLTranslator(
                self.collection.name,
                "data",
                "id",
                db_connection=self.collection.db,
                jsonb_context=self.jsonb,
            )
            self._match_sql_translator = translator
        return translator

    def _translate_match_spec(
        self,
        spec: dict[str, Any],
        agg_ctx: AggregationContext | None = None,
    ) -> tuple[list[str] | None, list[Any]]:
        """Translate a ``$match`` query dictionary into SQL WHERE predicates.

        Compiles aggregation ``$expr`` / direct ``$`` expressions via
        ``self.evaluator``, recurses on logical combinators (``$and``, ``$or``,
        ``$nor``, ``$not``) so nested expressions stay supported, emits BSON-type
        and array-safe range predicates via ``build_bson_range_sql``, and
        delegates ``$jsonSchema``, operator dicts, regex, datetime, null,
        ``_id``, and scalar containment predicates to ``SQLTranslator``.
        Returns ``(None, [])`` for ``$text`` or unsupported predicates.

        Args:
            spec: MongoDB ``$match`` filter specification dictionary.
            agg_ctx: Optional ``AggregationContext`` used when compiling
                direct aggregation expressions. Defaults to ``None``.

        Returns:
            tuple[list[str] | None, list[Any]]: List of SQL predicate strings
                (or ``None`` on fallback) and bound parameters.
        """
        from .type_utils import build_bson_range_sql, expr_truthiness_sql

        if not isinstance(spec, dict) or "$text" in spec:
            return None, []

        translator = self._get_match_sql_translator()
        op_translator = translator.operator_translator
        clause_builder = translator.clause_builder
        all_params: list[Any] = []
        where_clauses: list[str] = []
        range_ops = {
            "$gt": (">", False),
            "$lt": ("<", False),
            "$gte": (">=", True),
            "$lte": ("<=", True),
        }

        for field, value in spec.items():
            if field == "$expr":
                expr_sql, expr_params = self.evaluator.evaluate(value)
                if not expr_sql:
                    return None, []
                where_clauses.append(expr_truthiness_sql(expr_sql))
                all_params.extend(expr_params)
            elif field in ("$and", "$or", "$nor"):
                if not isinstance(value, list) or not value:
                    return None, []
                sub_clauses: list[str] = []
                for sub_spec in value:
                    if not isinstance(sub_spec, dict) or not sub_spec:
                        return None, []
                    parts, sub_params = self._translate_match_spec(
                        sub_spec, agg_ctx=agg_ctx
                    )
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
                parts, not_params = self._translate_match_spec(
                    value, agg_ctx=agg_ctx
                )
                if parts is None or not parts:
                    return None, []
                inner = " AND ".join(f"({p})" for p in parts)
                where_clauses.append(f"NOT COALESCE(({inner}), 0)")
                all_params.extend(not_params)
            elif (
                field != "$jsonSchema"
                and field.startswith("$")
                and _is_expression({field: value})
            ):
                # Handle direct expression (no $expr wrapper)
                expr_sql, expr_params = self.evaluator.build_select_expression(
                    {field: value}, context=agg_ctx
                )
                if not expr_sql:
                    return None, []
                where_clauses.append(expr_truthiness_sql(expr_sql))
                all_params.extend(expr_params)
            elif field.startswith("$") and field != "$jsonSchema":
                return None, []
            elif isinstance(value, dict) and field != "$jsonSchema":
                options = value.get("$options", "")
                if "$options" in value and (
                    "$regex" not in value or not isinstance(options, str)
                ):
                    return None, []

                rest_ops: dict[str, Any] = {}
                for op, arg in value.items():
                    if op == "$options":
                        continue
                    if op == "$not":
                        if not isinstance(arg, (dict, re.Pattern)) or (
                            isinstance(arg, dict) and "$not" in arg
                        ):
                            return None, []
                        sub_parts, sub_params = self._translate_match_spec(
                            {field: arg}, agg_ctx=agg_ctx
                        )
                        if sub_parts is None or not sub_parts:
                            return None, []
                        inner = " AND ".join(f"({p})" for p in sub_parts)
                        where_clauses.append(f"NOT COALESCE(({inner}), 0)")
                        all_params.extend(sub_params)
                    elif (
                        op in range_ops
                        and field != "_id"
                        and not op_translator._is_datetime_value(arg)
                    ):
                        sql_op, allow_null = range_ops[op]
                        res = build_bson_range_sql(
                            sql_op,
                            f"'{parse_json_path(field)}'",
                            arg,
                            json_prefix=self.jsonb.json_function_prefix,
                            json_each_function=self.jsonb.json_each_function,
                            allow_null=allow_null,
                        )
                        if res is None:
                            return None, []
                        where_clauses.append(res[0])
                        all_params.extend(res[1])
                    else:
                        rest_ops[op] = (
                            list(arg)
                            if op in ("$in", "$nin", "$all")
                            and isinstance(arg, tuple)
                            else arg
                        )

                if rest_ops:
                    if "$regex" in rest_ops and "$options" in value:
                        rest_ops["$options"] = options
                    sql_clause, sub_params = clause_builder.build_where_clause(
                        {field: rest_ops},
                        "direct",
                        is_nested=True,
                        query_param=spec,
                    )
                    if sql_clause is None:
                        return None, []
                    if sql_clause:
                        where_clauses.append(sql_clause)
                        all_params.extend(sub_params)
            else:
                sql_clause, sub_params = clause_builder.build_where_clause(
                    {field: value},
                    "direct",
                    is_nested=True,
                    query_param=spec,
                )
                if sql_clause is None:
                    return None, []
                if sql_clause:
                    where_clauses.append(sql_clause)
                    all_params.extend(sub_params)

        return where_clauses, all_params

    def _build_match_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$match`` stage.

        Temporarily sets ``self.evaluator._current_context`` while compiling
        ``spec`` via ``_translate_match_spec``, restoring it in a ``finally``
        block.

        Args:
            spec: MongoDB ``$match`` query filter dictionary.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: Filtered ``SELECT`` query and bound
                parameters, or ``(None, [])`` when Python fallback is needed.
        """
        agg_ctx = self._make_agg_context(context, preserve_root)
        old_ctx = self.evaluator._current_context
        self.evaluator._current_context = agg_ctx
        try:
            where_clauses, all_params = self._translate_match_spec(
                spec, agg_ctx=agg_ctx
            )
        finally:
            self.evaluator._current_context = old_ctx
        if where_clauses is None:
            return None, []

        where_clause = (
            f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        )
        select_parts = ["id", "_id", "data"]
        _append_root_column(select_parts, context, preserve_root)
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} {where_clause}",
            all_params,
        )

    def _build_sort_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$sort`` stage using ``ORDER BY``.

        Args:
            spec: Dictionary mapping field paths to sort directions (``-1`` for
                descending, anything else for ascending).
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str, list[Any]]: Ordered ``SELECT`` query and empty
                parameter list.
        """
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
        _append_root_column(select_parts, context, preserve_root)
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} ORDER BY {', '.join(order_parts)}",
            [],
        )

    def _build_skip_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$skip`` stage using ``LIMIT -1 OFFSET ?``.

        Args:
            spec: Integer count of documents to skip.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str, list[Any]]: SQL query and single-element parameter list
                ``[int(spec)]``.
        """
        select_parts = ["id", "_id", "data"]
        _append_root_column(select_parts, context, preserve_root)
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT -1 OFFSET ?",
            [int(spec)],
        )

    def _build_limit_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build SQL for the ``$limit`` stage using ``LIMIT ?``.

        Validates that ``spec`` is a non-negative integer (not ``bool``) and
        raises ``ValueError`` otherwise.

        Args:
            spec: Non-negative integer maximum number of rows to return.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str, list[Any]]: SQL query and single-element parameter list
                ``[int(spec)]``.
        """
        if not isinstance(spec, int) or isinstance(spec, bool) or spec < 0:
            raise ValueError(
                f"The $limit stage requires a non-negative integer, got {spec}"
            )
        select_parts = ["id", "_id", "data"]
        _append_root_column(select_parts, context, preserve_root)
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage} LIMIT ?",
            [int(spec)],
        )

    def _build_count_sql(self, spec, prev_stage, context):
        """Build SQL for the ``$count`` stage.

        Emits a single row whose ``data`` JSON object maps ``spec`` to
        ``COUNT(*)`` with ``NULL`` ``id`` and ``_id``.

        Args:
            spec: Output field name for the document count.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.

        Returns:
            tuple[str, list[Any]]: Aggregate ``SELECT`` query and ``[spec]``
                parameter list.
        """
        select_parts = [
            "NULL as id",
            "NULL as _id",
            "json_object(?, COUNT(*)) as data",
        ]
        if context.has_root:
            select_parts.append("NULL AS root_data")
        return (
            f"SELECT {', '.join(select_parts)} FROM {prev_stage}",
            [spec],
        )

    def _build_set_window_fields_sql(
        self, spec, prev_stage, context, preserve_root
    ):
        """Build SQL for the ``$setWindowFields`` stage using window functions.

        Supports ``$rank``, ``$denseRank``, ``$documentNumber``, ``$first``,
        and ``$last`` over optional ``partitionBy`` and ``sortBy`` clauses,
        merging computed window columns into ``data`` via
        ``json_set``/``jsonb_set``. Returns ``(None, [])`` for compound
        ``partitionBy`` objects, explicit ``window`` frames, multi-operator
        output specs, or unsupported operators.

        Args:
            spec: Window specification containing optional ``"partitionBy"``,
                ``"sortBy"``, and ``"output"`` mappings.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext``.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``.

        Returns:
            tuple[str | None, list[Any]]: SQL query and bound parameters, or
                ``(None, [])`` when Python fallback is required.
        """
        partition_by = spec.get("partitionBy")
        sort_by = spec.get("sortBy", {})
        output = spec.get("output", {})
        agg_ctx = self._make_agg_context(context, preserve_root)

        all_params = []
        partition_clause = ""
        partition_params: list[Any] = []
        if partition_by:
            if isinstance(partition_by, dict) and not (
                len(partition_by) == 1
                and next(iter(partition_by)).startswith("$")
            ):
                return None, []
            expr_sql, expr_params = self.evaluator.build_select_expression(
                partition_by, context=agg_ctx
            )
            if expr_sql is None:
                return None, []
            partition_clause = f"PARTITION BY {expr_sql}"
            partition_params = list(expr_params)

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
            if "window" in window_op or len(window_op) != 1:
                return None, []
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
                all_params.extend(partition_params)
            elif op_name in ("$first", "$last"):
                inner_sql, inner_params = (
                    self.evaluator.build_select_expression(
                        op_spec, context=agg_ctx
                    )
                )
                if inner_sql is None:
                    return None, []
                all_params.extend(inner_params)
                all_params.extend(partition_params)
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
        """Map a MongoDB window operator name to a SQLite window function.

        Args:
            op: Window operator name (``"$rank"``, ``"$denseRank"``,
                ``"$documentNumber"``, ``"$first"``, or ``"$last"``).

        Returns:
            str | None: SQLite window function name, or ``None`` if
                unsupported.
        """
        mapping = {
            "$rank": "RANK",
            "$denseRank": "DENSE_RANK",
            "$documentNumber": "ROW_NUMBER",
            "$first": "FIRST_VALUE",
            "$last": "LAST_VALUE",
        }
        return mapping.get(op)

    def _build_graph_lookup_sql(
        self, spec, prev_stage, context, preserve_root: bool = False
    ):
        """Build Tier-1 CTE SQL for ``$graphLookup`` via recursive CTEs.

        Materializes ``prev_stage`` into a pre-CTE, registers the recursive
        lookup CTE generated by ``build_graph_lookup_parts``, marks
        ``context.mark_recursive()``, and returns the outer ``SELECT``.

        Args:
            spec: MongoDB ``$graphLookup`` stage specification dictionary.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext`` for registering pre-CTEs.
            preserve_root: Whether this stage captures ``data`` as
                ``root_data``. Defaults to ``False``.

        Returns:
            tuple[str | None, list[Any]]: Outer ``SELECT`` SQL and empty
                parameter list (recursive parameters live on the pre-CTE), or
                ``(None, [])`` when fallback is required.
        """
        uid = context.next_aux_id()
        src_cte = f"{context.cte_prefix}_glsrc_{uid}"
        rec_cte = f"{context.cte_prefix}_glrec_{uid}"

        src_col_parts = ["id", "_id", "data"]
        _append_root_column(src_col_parts, context, preserve_root)
        src_cols = ", ".join(src_col_parts)

        parts = build_graph_lookup_parts(
            self.collection,
            self.jsonb,
            src_cte,
            rec_cte,
            spec,
            has_root=context.has_root,
        )
        if parts is None:
            return None, []

        recurse_sql, select_sql, rec_params = parts
        context.add_pre_cte(src_cte, f"SELECT {src_cols} FROM {prev_stage}", [])
        context.add_pre_cte(rec_cte, recurse_sql, rec_params)
        context.mark_recursive()
        return select_sql, []

    def _build_facet_sql(self, spec, prev_stage, context):
        """Build Tier-1 CTE SQL for ``$facet`` over a shared source CTE.

        Captures ``prev_stage`` once into an auxiliary source CTE, compiles
        each facet sub-pipeline into auxiliary pre-CTEs using an isolated
        ``PipelineContext``, and aggregates each facet's output array into a
        single ``json_object`` document (or empty ``json('{}')`` if ``spec`` is
        empty).

        Args:
            spec: Dictionary mapping facet output field names to sub-pipeline
                lists.
            prev_stage: SQL table or CTE name supplying input rows.
            context: Active ``PipelineContext`` receiving sub-pipeline
                pre-CTEs.

        Returns:
            tuple[str | None, list[Any]]: Single-row ``SELECT`` query and
                facet-name parameters, or ``(None, [])`` when invalid or a
                sub-pipeline cannot be SQL-optimized.
        """
        if not isinstance(spec, dict):
            return None, []

        uid = context.next_aux_id()
        src_cte = f"{context.cte_prefix}_fsrc_{uid}"
        context.add_pre_cte(
            src_cte, f"SELECT id, _id, data FROM {prev_stage}", []
        )

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
            select_parts.append(f"{data_expr} AS root_data")
        return f"SELECT {', '.join(select_parts)}", all_params
