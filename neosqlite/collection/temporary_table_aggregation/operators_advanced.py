from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any, Callable

from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from ..jsonb_support import (
    _get_json_group_array_function,
)
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)


class OperatorsAdvancedMixin(OperatorsBaseMixin):
    def _process_densify_stage(self, create_temp, current_table, densify_spec):
        """
        Process $densify stage - fills in missing values in a sequence.

        MongoDB syntax:
        {
          $densify: {
            field: <field_name>,
            range: {
              step: <number>,
              bounds: [<lower>, <upper>]
            },
            partitionBy: <expression>  // optional
          }
        }
        """
        field = densify_spec.get("field")
        range_spec = densify_spec.get("range")
        partition_by = densify_spec.get("partitionBy") or densify_spec.get(
            "partitionByFields"
        )

        if not field or not range_spec:
            raise NotImplementedError(
                "$densify requires field and range - use force_fallback or simplify pipeline"
            )

        if partition_by:
            raise NotImplementedError(
                "$densify with partitionBy not supported - use force_fallback"
            )

        print(
            f"DEBUG DENSIFY: field={field}, range_spec={range_spec}, partition_by={partition_by}"
        )

        if not field or not range_spec:
            raise NotImplementedError(
                "$densify requires field and range - use force_fallback or simplify pipeline"
            )

        if partition_by:
            print(
                "DEBUG DENSIFY: partition_by is truthy, raising NotImplementedError"
            )
            raise NotImplementedError(
                "$densify with partitionBy not supported - use force_fallback"
            )

        step = range_spec.get("step")
        bounds = range_spec.get("bounds")

        if not step or not bounds:
            raise NotImplementedError(
                "$densify requires step and bounds - use force_fallback or simplify pipeline"
            )

        if not isinstance(bounds, list) or len(bounds) != 2:
            raise NotImplementedError(
                "$densify with non-array bounds not supported - use force_fallback"
            )

        lower_bound = bounds[0]
        upper_bound = bounds[1]

        if not isinstance(step, (int, float)):
            raise NotImplementedError(
                "$densify with non-numeric step not supported - use force_fallback"
            )

        if not isinstance(lower_bound, (int, float)) or not isinstance(
            upper_bound, (int, float)
        ):
            raise NotImplementedError(
                "$densify with non-numeric bounds not supported - use force_fallback"
            )

        json_extract = f"{self._json_function_prefix}_extract"

        densify_key = f"{field}:{step}:{lower_bound}:{upper_bound}"
        densify_hash = hashlib.sha256(densify_key.encode()).hexdigest()[:8]
        series_table = f"_densify_series_{densify_hash}"

        step_series = []
        current = float(lower_bound)
        while current <= upper_bound:
            step_series.append(current)
            current += step
            if len(step_series) > 1000:
                break

        if not step_series:
            return current_table

        try:
            self.collection.db.execute(
                f"CREATE TEMP TABLE {series_table} (val REAL)"
            )
            self.collection.db.execute(
                f"INSERT INTO {series_table} (val) VALUES "
                + "("
                + "),((".join([str(v) for v in step_series])
                + ")"
            )

            json_set_func = f"{self._json_function_prefix}_set"

            select_clause = f"""
                SELECT id, _id,
                json({json_set_func}(data, '{field}', s.val)) as data
                FROM {current_table}, {series_table} s
                WHERE s.val >= {lower_bound} AND s.val <= {upper_bound}
                AND NOT EXISTS (
                    SELECT 1 FROM {current_table} c
                    WHERE {json_extract}(c.data, '{field}') = s.val
                )
            """

            new_table = create_temp({"$densify": densify_spec}, select_clause)

            return new_table
        finally:
            try:
                self.collection.db.execute(
                    f"DROP TABLE IF EXISTS {series_table}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop series table '{series_table}': {e}"
                )
                pass

    def _process_facet_stage(self, create_temp, current_table, facet_spec):
        """
        Process $facet stage - processes multiple sub-pipelines and combines results.

        MongoDB syntax:
        {
          $facet: {
            <output_field1>: [<pipeline stages>],
            <output_field2>: [<pipeline stages>],
            ...
          }
        }

        This method:
        1. Extracts input documents from the current temp table
        2. For each sub-pipeline, executes it using normal aggregation (Tier 1/2/3)
        3. Combines all results into a single document with facet fields
        4. Returns a temp table containing that combined result
        """

        from neosqlite.collection.json_helpers import (
            neosqlite_json_dumps,
            neosqlite_json_loads,
        )

        # Extract input documents from current temp table
        # IMPORTANT: _id is stored as a separate column, not in the data JSON
        # When JSONB is supported, data column stores JSONB BLOB — convert to text
        if self._jsonb_supported:
            cursor = self.db.execute(
                f"SELECT _id, json(data) FROM {current_table}"
            )
        else:
            cursor = self.db.execute(f"SELECT _id, data FROM {current_table}")

        input_docs = []
        skipped_count = 0
        for row in cursor.fetchall():
            doc_id, doc_data = row
            try:
                doc = neosqlite_json_loads(doc_data)
                # Merge _id from the column into the document
                doc["_id"] = doc_id
                input_docs.append(doc)
            except (UnicodeDecodeError, ValueError, TypeError) as e:
                skipped_count += 1
                logger.warning(
                    f"Skipping corrupted document in $facet stage (id={doc_id}): {e}"
                )

        if skipped_count > 0:
            logger.warning(
                f"$facet stage skipped {skipped_count} corrupted document(s) "
                f"out of {skipped_count + len(input_docs)} total"
            )

        # Process each sub-pipeline and store results
        facet_results = {}
        result_tables = []

        try:
            for facet_name, sub_pipeline in facet_spec.items():
                # If no input documents, sub-pipeline results should be empty
                # (to match Tier 3 Python fallback behavior)
                if not input_docs:
                    facet_results[facet_name] = []
                    continue

                # Create a temporary in-memory collection for this sub-pipeline
                temp_collection_name = f"_facet_sub_{uuid.uuid4().hex[:12]}"
                from .. import Collection

                temp_collection = Collection(
                    db=self.collection.db,
                    name=temp_collection_name,
                    create=True,
                    database=self.collection._database,
                )

                try:
                    # Insert input documents into temp collection, preserving _id
                    if input_docs:
                        # Use insert_many which should preserve _id if present
                        temp_collection.insert_many(input_docs)

                    # Run sub-pipeline through normal aggregation (uses Tier 1/2/3)
                    sub_results = list(temp_collection.aggregate(sub_pipeline))

                    # Store results
                    facet_results[facet_name] = sub_results

                finally:
                    # Clean up temp collection table
                    try:
                        self.db.execute(
                            f"DROP TABLE IF EXISTS {temp_collection_name}"
                        )
                    except Exception as e:
                        logger.debug(
                            f"Failed to drop facet temp table '{temp_collection_name}': {e}"
                        )
                        pass

            # Create a single result document with all facet fields
            result_doc = facet_results

            # Create a temp table with the combined result
            result_table_name = f"_facet_combined_{uuid.uuid4().hex[:12]}"
            self.db.execute(f"""
                CREATE TEMP TABLE {result_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    _id INTEGER,
                    data TEXT
                )
            """)

            # Insert the result document
            self.db.execute(
                f"INSERT INTO {result_table_name} (_id, data) VALUES (?, ?)",
                (0, neosqlite_json_dumps(result_doc)),
            )

            return result_table_name

        except Exception as e:
            # Clean up any result tables on error
            for table_name in result_tables:
                try:
                    self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception as cleanup_error:
                    logger.debug(
                        f"Failed to cleanup result table {table_name}: {cleanup_error}"
                    )
            raise e

    def _process_union_with_stage(self, create_temp, current_table, union_spec):
        """
        Process $unionWith stage - combines documents from another collection.

        MongoDB syntax:
        {
          $unionWith: {
            coll: <collection_name>,
            pipeline: [<pipeline stages>]  // optional
          }
        }
        """
        coll_name = union_spec.get("coll")
        pipeline = union_spec.get("pipeline", [])

        if not coll_name:
            return current_table

        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Build SELECT clause for current table based on available columns
        if has_id and has_underscore_id:
            current_select = f"SELECT id, _id, data FROM {current_table}"
        elif has_id:
            current_select = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id:
            current_select = f"SELECT _id, data FROM {current_table}"
        else:
            current_select = f"SELECT data FROM {current_table}"

        # Get documents from the other collection with matching columns
        other_columns = []
        if has_id:
            other_columns.append("id")
        if has_underscore_id:
            other_columns.append("_id")
        other_columns.append("data")

        other_select_cols = (
            ", ".join(other_columns) if other_columns else "data"
        )

        if pipeline:
            # If pipeline specified, process it
            # For simplicity, just get all documents
            other_table = create_temp(
                {"$unionWith": union_spec},
                f"SELECT {other_select_cols} FROM {coll_name}",
            )
        else:
            other_table = create_temp(
                {"$unionWith": union_spec},
                f"SELECT {other_select_cols} FROM {coll_name}",
            )

        # Union the two tables with explicit column lists
        result_table = create_temp(
            {"$unionWith": union_spec},
            f"{current_select} UNION ALL SELECT {other_select_cols} FROM {other_table}",
        )

        return result_table

    def _process_merge_stage(self, create_temp, current_table, merge_spec):
        """
        Process $merge stage - writes results to a collection.

        MongoDB syntax:
        {
          $merge: {
            into: <collection_name>,
            on: <field>,  // optional
            whenMatched: <action>,  // optional
            whenNotMatched: <action>  // optional
          }
        }
        """
        into = merge_spec.get("into")
        if isinstance(into, dict):
            db_name = into.get("db") or ""
            coll_name = into.get("coll") or ""
            into = db_name + "." + coll_name

        if not into:
            return current_table

        # For now, just return current table (actual merge would write to collection)
        # This is a placeholder - full implementation would INSERT/UPDATE the target
        return current_table

    def _process_redact_stage(self, create_temp, current_table, redact_spec):
        """
        Process $redact stage - field-level redaction based on conditions.

        MongoDB syntax:
        {
          $redact: {
            $cond: {
              if: <condition>,
              then: <level>,
              else: <level>
            }
          }
        }

        Levels:
        - $$DESCEND: Include the field and process sub-fields
        - $$PRUNE: Exclude the field
        - $$KEEP: Include the field as-is
        """
        # For now, this is a placeholder - full redaction requires complex expression evaluation
        # Return current table unchanged
        return current_table

    def _process_set_window_fields_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """
        Process $setWindowFields stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by: dict[str, int] = spec.get("sortBy", {})
        output: dict[str, Any] = spec.get("output", {})
        all_params: list[Any] = []

        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names
        has_data = "data" in column_names

        # 1. Build PARTITION BY clause
        partition_parts = []
        if partition_by is not None:
            # Handle _id specially - it's a column, not in JSON
            if partition_by == "_id":
                if has_underscore_id:
                    partition_parts.append("_id")
                elif has_id:
                    partition_parts.append("id")
                else:
                    # Can't partition by _id if column doesn't exist
                    partition_by = None
            else:
                sql, params = self.expr_evaluator.build_select_expression(
                    partition_by
                )
                partition_parts.append(sql)
                all_params.extend(params)

        partition_clause = ""
        if partition_parts:
            partition_clause = f"PARTITION BY {', '.join(partition_parts)}"

        # 2. Build ORDER BY clause
        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                # Handle _id specially
                if field == "_id":
                    if has_underscore_id:
                        sort_parts.append(f"_id {order}")
                    elif has_id:
                        sort_parts.append(f"id {order}")
                    # else skip this sort field
                else:
                    sql, params = self.expr_evaluator.build_select_expression(
                        f"${field}"
                    )
                    sort_parts.append(f"{sql} {order}")
                    all_params.extend(params)

        sort_clause = ""
        if sort_parts:
            sort_clause = f"ORDER BY {', '.join(sort_parts)}"

        # 3. Build output fields with window functions
        json_set_args = []
        for field, op_spec in output.items():
            # Skip _id field - it's a separate column, not in JSON data
            if field == "_id":
                # For _id, we need to handle it separately in the SELECT clause
                continue

            if not isinstance(op_spec, dict) or not op_spec:
                # Skip invalid op_spec
                continue

            op_name = next(iter(op_spec.keys()))
            op_val = op_spec[op_name]
            window_spec = op_spec.get("window")

            sql_func, sql_operand, sql_params = (
                self._map_window_operator_to_sql(op_name, op_val)
            )
            if sql_func is None:
                # Fall back to Python if operator not supported in SQL
                raise NotImplementedError(
                    f"Window operator {op_name} not supported in SQL tier"
                )

            all_params.extend(sql_params)
            # Only include frame clause if we have ORDER BY (required by SQLite)
            frame_clause = ""
            if sort_parts and window_spec:
                frame_clause = self._build_window_frame_sql(window_spec)

            # Build window SQL - ensure proper spacing
            window_parts = []
            if partition_clause:
                window_parts.append(partition_clause)
            if sort_clause:
                window_parts.append(sort_clause)
            if frame_clause:
                window_parts.append(frame_clause)

            if window_parts:
                window_sql = (
                    f"{sql_func}({sql_operand}) OVER ({' '.join(window_parts)})"
                )
            else:
                window_sql = f"{sql_func}({sql_operand}) OVER ()"

            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(window_sql)

        # 4. Create the temporary table
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        # Build SELECT clause based on available columns
        if has_id and has_underscore_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT id, _id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT id, _id, data FROM {current_table}"
        elif has_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id and has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT _id, json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT _id, data FROM {current_table}"
        elif has_data:
            if json_set_args:
                args_str = ", ".join(json_set_args)
                sql = f"SELECT json({json_set_func}(data, {args_str})) AS data FROM {current_table}"
            else:
                sql = f"SELECT data FROM {current_table}"
        else:
            # No data column - can't process this stage
            logger.warning(
                f"Table {current_table} has no data column for setWindowFields"
            )
            return current_table

        return create_temp({"$setWindowFields": spec}, sql, all_params)

    def _map_window_operator_to_sql(
        self, op_name: str, op_val: Any
    ) -> tuple[str | None, str, list[Any]]:
        """Map MongoDB window operator to SQL function and operand."""
        match op_name:
            case "$rank":
                return "RANK", "", []
            case "$denseRank":
                return "DENSE_RANK", "", []
            case "$documentNumber":
                return "ROW_NUMBER", "", []
            case "$first":
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return "FIRST_VALUE", sql, params
            case "$last":
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return "LAST_VALUE", sql, params
            case "$shift":
                output_expr = op_val.get("output")
                by = op_val.get("by", 0)
                default = op_val.get("default")

                if by >= 0:
                    func = "LEAD"
                    offset = by
                else:
                    func = "LAG"
                    offset = -by

                sql, params = self.expr_evaluator.build_select_expression(
                    output_expr
                )
                if default is not None:
                    return f"{func}", f"{sql}, {offset}, ?", params + [default]
                return f"{func}", f"{sql}, {offset}", params

            case "$sum" | "$avg" | "$min" | "$max":
                func = op_name[1:].upper()
                sql, params = self.expr_evaluator.build_select_expression(
                    op_val
                )
                return func, sql, params

            case _:
                return None, "", []

    def _build_window_frame_sql(
        self, window_spec: dict[str, Any] | None
    ) -> str:
        """Build SQL window frame clause (ROWS BETWEEN ...)."""
        if not window_spec:
            return ""

        if "documents" in window_spec:
            lower, upper = window_spec["documents"]

            def map_bound(val: Any, is_upper: bool = False) -> str:
                if val == "unbounded":
                    return (
                        "UNBOUNDED FOLLOWING"
                        if is_upper
                        else "UNBOUNDED PRECEDING"
                    )
                if val == "current":
                    return "CURRENT ROW"
                if isinstance(val, int):
                    if val < 0:
                        return f"{-val} PRECEDING"
                    if val > 0:
                        return f"{val} FOLLOWING"
                return "CURRENT ROW"

            l_bound = map_bound(lower, is_upper=False)
            u_bound = map_bound(upper, is_upper=True)

            # Validate bounds - if either is empty, return empty string
            if not l_bound or not u_bound:
                return ""

            return f"ROWS BETWEEN {l_bound} AND {u_bound}"

        return ""

    def _process_graph_lookup_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """
        Process $graphLookup stage.
        """
        from_collection = spec.get("from")
        start_with = spec.get("startWith")
        connect_from_field = spec.get("connectFromField")
        connect_to_field = spec.get("connectToField")
        as_field = spec.get("as")
        max_depth = spec.get("maxDepth")
        depth_field = spec.get("depthField")
        restrict_search = spec.get("restrictSearchWithMatch")

        if not all(
            [
                from_collection,
                start_with,
                connect_from_field,
                connect_to_field,
                as_field,
            ]
        ):
            return current_table

        all_params: list[Any] = []

        # 1. Build startWith expression
        start_with_sql, start_with_params = (
            self.expr_evaluator.build_select_expression(start_with)
        )
        # Prefix with p. to avoid ambiguity during JOIN
        start_with_sql = start_with_sql.replace(
            "json_extract(data", "json_extract(p.data"
        )
        start_with_sql = start_with_sql.replace(
            "jsonb_extract(data", "jsonb_extract(p.data"
        )
        all_params.extend(start_with_params)

        # 2. Build restrictSearchWithMatch if present
        restrict_where = ""
        restrict_params = []
        if restrict_search:
            from ..query_helper import QueryHelper

            target_coll = self.collection.database.get_collection(
                from_collection
            )
            helper = QueryHelper(target_coll)
            query_result = helper._build_simple_where_clause(restrict_search)
            if query_result:
                r_sql, r_params, _ = query_result
                # Remove leading "WHERE " if present since we're adding it to existing WHERE clause
                r_sql = r_sql.strip()
                if r_sql.upper().startswith("WHERE "):
                    r_sql = r_sql[6:]  # Remove "WHERE " prefix
                r_sql = r_sql.replace(
                    "json_extract(data", "json_extract(t.data"
                )
                r_sql = r_sql.replace(
                    "jsonb_extract(data", "jsonb_extract(t.data"
                )
                restrict_where = f"AND ({r_sql})"
                restrict_params = (
                    r_params * 2
                )  # Used twice: in start_points_sql and recursive_step_sql
                all_params.extend(restrict_params)

        # 3. Build recursive search
        recurse_cte = "graph_recurse_tier2"

        # Build field access SQL
        def get_field_sql(table_alias, field_name, is_recursive_table=False):
            if field_name == "_id":
                return (
                    f"{table_alias}.found_id"
                    if is_recursive_table
                    else f"{table_alias}._id"
                )
            data_col = "found_data" if is_recursive_table else "data"
            return f"json_extract({table_alias}.{data_col}, '$.{field_name}')"

        target_to_sql = get_field_sql("t", connect_to_field)
        recurse_from_sql = get_field_sql(
            "r", connect_from_field, is_recursive_table=True
        )

        start_points_sql = f"""
            SELECT
                p.id as original_id,
                t.id as found_id,
                t.data as found_data,
                0 as depth
            FROM {current_table} p
            JOIN {from_collection} t ON {target_to_sql} = {start_with_sql}
            WHERE 1=1 {restrict_where}
        """

        max_depth_cond = (
            f"AND r.depth < {max_depth}" if max_depth is not None else ""
        )
        recursive_step_sql = f"""
            SELECT
                r.original_id,
                t.id as found_id,
                t.data as found_data,
                r.depth + 1
            FROM {recurse_cte} r
            JOIN {from_collection} t ON {target_to_sql} = {recurse_from_sql}
            WHERE 1=1 {max_depth_cond} {restrict_where}
        """

        # 4. Combine into stage SQL
        # Move WITH clause to top level (SQLite doesn't allow nested WITH)
        depth_json_sql = ""
        if depth_field:
            depth_json_sql = f", '{parse_json_path(str(depth_field))}', depth"

        json_group_func = _get_json_group_array_function(self._jsonb_supported)
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        as_field_str = str(as_field)
        stage_sql = f"""
            WITH RECURSIVE {recurse_cte} AS (
                {start_points_sql}
                UNION ALL
                {recursive_step_sql}
            )
            SELECT
                p.id AS id,
                json({json_set_func}({json_set_func}(p.data, '$._id', p.id), '{parse_json_path(as_field_str)}',
                    COALESCE((
                        SELECT {json_group_func}(
                            json({json_set_func}(sub.found_data, '$._id', sub.found_id {depth_json_sql}))
                        )
                        FROM (
                            SELECT found_id, found_data, depth FROM {recurse_cte}
                            WHERE original_id = p.id
                            GROUP BY found_id
                        ) sub
                    ), json('[]'))
                )) as data
            FROM {current_table} p
        """

        return create_temp({"$graphLookup": spec}, stage_sql, all_params)

    def _process_fill_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """
        Process $fill stage.
        """
        partition_by = spec.get("partitionBy")
        sort_by: dict[str, int] = spec.get("sortBy", {})
        output: dict[str, Any] = spec.get("output", {})
        all_params: list[Any] = []

        # Check for 'linear' method
        for fill_spec in output.values():
            if fill_spec.get("method") == "linear":
                raise NotImplementedError(
                    "$fill method 'linear' not supported in SQL tier"
                )

        # 1. Build PARTITION BY and ORDER BY clauses
        partition_parts = []
        if partition_by is not None:
            sql, params = self.expr_evaluator.build_select_expression(
                partition_by
            )
            partition_parts.append(sql)
            all_params.extend(params)
        partition_clause = (
            f"PARTITION BY {', '.join(partition_parts)}"
            if partition_parts
            else ""
        )

        sort_parts = []
        if sort_by:
            for field, direction in sort_by.items():
                order = "ASC" if direction == 1 else "DESC"
                sql, params = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                sort_parts.append(f"{sql} {order}")
                all_params.extend(params)
        sort_clause = f"ORDER BY {', '.join(sort_parts)}" if sort_parts else ""

        # 2. Process output fields
        has_locf = any(fs.get("method") == "locf" for fs in output.values())
        json_set_func = "jsonb_set" if self._jsonb_supported else "json_set"

        if not has_locf:
            # Simple constant value fill
            json_set_args = []
            for field, fill_spec in output.items():
                value = fill_spec.get("value")
                field_sql, field_params = (
                    self.expr_evaluator.build_select_expression(f"${field}")
                )
                all_params.extend(field_params)

                fill_expr = f"COALESCE({field_sql}, ?)"
                all_params.append(value)

                json_set_args.append(f"'{parse_json_path(field)}'")
                json_set_args.append(fill_expr)

            args_str = ", ".join(json_set_args)
            data_expr = f"json({json_set_func}(data, {args_str}))"
            sql = f"SELECT id, _id, {data_expr} AS data FROM {current_table}"
            return create_temp({"$fill": spec}, sql, all_params)

        # Complex locf fill
        block_id_selects = ["id", "_id", "data"]
        for field, fill_spec in output.items():
            if fill_spec.get("method") == "locf":
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                block_id_selects.append(
                    f"COUNT({field_sql}) OVER ({partition_clause} {sort_clause}) AS block_id_{parse_json_path(field).replace('.', '_')}"
                )

        subquery_alias = "fill_blocks_tier2"
        final_json_args = []
        for field, fill_spec in output.items():
            field_path = parse_json_path(field)
            if fill_spec.get("method") == "locf":
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                block_col = f"block_id_{field_path.replace('.', '_')}"
                block_partition = (
                    f"PARTITION BY {', '.join(partition_parts + [block_col])}"
                    if partition_parts
                    else f"PARTITION BY {block_col}"
                )
                locf_expr = f"FIRST_VALUE({field_sql}) OVER ({block_partition} {sort_clause})"
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(locf_expr)
            else:
                value = fill_spec.get("value")
                field_sql, _ = self.expr_evaluator.build_select_expression(
                    f"${field}"
                )
                final_json_args.append(f"'{field_path}'")
                final_json_args.append(f"COALESCE({field_sql}, ?)")
                all_params.append(value)

        args_str = ", ".join(final_json_args)
        data_expr = f"json({json_set_func}(data, {args_str}))"

        stage_sql = f"""
            SELECT id, _id, {data_expr} AS data
            FROM (
                SELECT {", ".join(block_id_selects)}
                FROM {current_table}
            ) {subquery_alias}
        """
        return create_temp({"$fill": spec}, stage_sql, all_params)
