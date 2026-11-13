from __future__ import annotations

import logging
import uuid
from typing import Any, Callable

from ...sql_utils import quote_table_name
from .._stage_builders import (
    _graph_lookup_scalar_or_oid,
    build_graph_lookup_parts,
    build_redact_where_sql,
)
from ..json_path_utils import parse_json_path
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)


class OperatorsAdvancedMixin(OperatorsBaseMixin):
    def _process_densify_stage(self, create_temp, current_table, densify_spec):
        """Process $densify stage — numeric series with bounds array/'full'/'partition' and partitionBy/partitionByFields."""
        field = densify_spec.get("field")
        range_spec = densify_spec.get("range")
        raw_partition = (
            densify_spec.get("partitionByFields")
            or densify_spec.get("partitionBy")
        )

        if not field or not isinstance(range_spec, dict):
            raise NotImplementedError(
                "$densify requires field and range - use force_fallback or simplify pipeline"
            )

        if raw_partition is None:
            partition_fields: list[str] = []
        elif isinstance(raw_partition, str):
            partition_fields = [raw_partition.lstrip("$")]
        elif isinstance(raw_partition, list) and all(
            isinstance(p, str) and p for p in raw_partition
        ):
            partition_fields = [p.lstrip("$") for p in raw_partition]
        else:
            raise NotImplementedError(
                "$densify with non-field partitionBy expression requires Python fallback"
            )

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

        if unit is not None:
            raise NotImplementedError(
                "$densify with date 'unit' requires Python fallback"
            )

        if not step or bounds is None:
            raise NotImplementedError(
                "$densify requires step and bounds - use force_fallback or simplify pipeline"
            )

        if not isinstance(step, (int, float)) or isinstance(step, bool):
            raise NotImplementedError(
                "$densify with non-numeric step not supported - use force_fallback"
            )

        if isinstance(bounds, str):
            if bounds not in ("full", "partition"):
                raise NotImplementedError(
                    f"$densify bounds '{bounds}' not supported - use force_fallback"
                )
        elif isinstance(bounds, list) and len(bounds) == 2:
            if (
                not isinstance(bounds[0], (int, float))
                or isinstance(bounds[0], bool)
                or not isinstance(bounds[1], (int, float))
                or isinstance(bounds[1], bool)
            ):
                raise NotImplementedError(
                    "$densify with non-numeric bounds not supported - use force_fallback"
                )
        else:
            raise NotImplementedError(
                "$densify bounds must be 'full', 'partition', or [lower, upper]"
            )

        json_set_func = f"{self.jsonb.json_function_prefix}_set"
        json_path = parse_json_path(field)
        columns = {
            col[1]
            for col in self.db.execute(
                f"PRAGMA table_info({quote_table_name(current_table)})"
            ).fetchall()
        }
        has_id = "id" in columns
        has_underscore_id = "_id" in columns

        orig_cols = [
            c for c in ("id", "_id", "data") if c in columns or c == "data"
        ]
        orig_select = f"SELECT {', '.join(orig_cols)} FROM {current_table}"

        part_paths = [parse_json_path(pf) for pf in partition_fields]
        part_cols = [f"_p{i}" for i in range(len(part_paths))]
        part_extracts = [
            f"json_extract(data, '{p}') AS _p{i}"
            for i, p in enumerate(part_paths)
        ]

        params: list[Any] = []
        if not partition_fields:
            if isinstance(bounds, list):
                bounds_sql = "SELECT ? AS _lo, ? AS _hi"
                params.extend([bounds[0], bounds[1]])
            else:
                bounds_sql = (
                    f"SELECT MIN(json_extract(data, '{json_path}')) AS _lo, "
                    f"MAX(json_extract(data, '{json_path}')) AS _hi "
                    f"FROM {current_table} "
                    f"WHERE json_extract(data, '{json_path}') IS NOT NULL"
                )
            series_sql = (
                "SELECT _lo AS _val, 0 AS _idx, _lo AS _base, _hi AS _limit "
                "FROM _densify_bounds WHERE _lo IS NOT NULL AND _hi IS NOT NULL AND _lo <= _hi "
                "UNION ALL "
                "SELECT _base + (_idx + 1) * ?, _idx + 1, _base, _limit "
                "FROM _densify_series WHERE _idx < 1000 AND _base + (_idx + 1) * ? <= _limit"
            )
            params.extend([step, step])
            gen_data_expr = f"json({json_set_func}('{{}}', '{json_path}', _s._val))"
            exists_cond = f"json_extract(c.data, '{json_path}') = _s._val"
        else:
            p_cols_str = ", ".join(part_cols)
            p_extract_str = ", ".join(part_extracts)
            if isinstance(bounds, list):
                bounds_sql = (
                    f"SELECT {p_cols_str}, ? AS _lo, ? AS _hi "
                    f"FROM (SELECT DISTINCT {p_extract_str} FROM {current_table})"
                )
                params.extend([bounds[0], bounds[1]])
            else:
                bounds_sql = (
                    f"SELECT {p_cols_str}, MIN(_v) AS _lo, MAX(_v) AS _hi "
                    f"FROM (SELECT {p_extract_str}, json_extract(data, '{json_path}') AS _v "
                    f"FROM {current_table}) WHERE _v IS NOT NULL "
                    f"GROUP BY {p_cols_str}"
                )
            series_sql = (
                f"SELECT {p_cols_str}, _lo AS _val, 0 AS _idx, _lo AS _base, _hi AS _limit "
                f"FROM _densify_bounds WHERE _lo IS NOT NULL AND _hi IS NOT NULL AND _lo <= _hi "
                f"UNION ALL "
                f"SELECT {', '.join('_s.' + c for c in part_cols)}, "
                f"_s._base + (_s._idx + 1) * ?, _s._idx + 1, _s._base, _s._limit "
                f"FROM _densify_series _s WHERE _s._idx < 1000 AND _s._base + (_s._idx + 1) * ? <= _s._limit"
            )
            params.extend([step, step])
            set_args = []
            match_parts = []
            for i, p_path in enumerate(part_paths):
                set_args.append(f"'{p_path}'")
                set_args.append(f"_s._p{i}")
                match_parts.append(
                    f"json_extract(c.data, '{p_path}') IS _s._p{i}"
                )
            set_args.extend([f"'{json_path}'", "_s._val"])
            match_parts.append(f"json_extract(c.data, '{json_path}') = _s._val")
            gen_data_expr = f"json({json_set_func}('{{}}', {', '.join(set_args)}))"
            exists_cond = " AND ".join(match_parts)

        gen_parts = []
        if has_id:
            gen_parts.append(
                f"ROW_NUMBER() OVER () + COALESCE((SELECT MAX(id) FROM {current_table}), 0) AS id"
            )
        if has_underscore_id:
            gen_parts.append("NULL AS _id")
        gen_parts.append(f"{gen_data_expr} AS data")

        select_clause = f"""
            WITH RECURSIVE
                _densify_bounds AS ({bounds_sql}),
                _densify_series AS ({series_sql})
            {orig_select}
            UNION ALL
            SELECT {", ".join(gen_parts)}
            FROM _densify_series _s
            WHERE NOT EXISTS (
                SELECT 1 FROM {current_table} c WHERE {exists_cond}
            )
        """
        return create_temp({"$densify": densify_spec}, select_clause, params)

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
        if self.jsonb.jsonb_supported:
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
        facet_results: dict[str, Any] = {}
        result_tables: list[str] = []

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

            # Create the combined result table through create_temp so the
            # pipeline context owns it — a raw CREATE TEMP TABLE here leaked
            # one _facet_combined_* table per aggregation (#124).
            result_table = create_temp(
                {"$facet": "_combined"},
                "SELECT NULL AS id, NULL AS _id, '{}' AS data WHERE 0",
            )
            data_val = neosqlite_json_dumps(result_doc)
            if self.jsonb.jsonb_supported:
                self.db.execute(
                    f"INSERT INTO {result_table} (_id, data) VALUES (?, jsonb(?))",
                    (None, data_val),
                )
            else:
                self.db.execute(
                    f"INSERT INTO {result_table} (_id, data) VALUES (?, ?)",
                    (None, data_val),
                )

            return result_table

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
        if isinstance(union_spec, str):
            coll_name = union_spec
            pipeline = []
        else:
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
            # A sub-pipeline changes which foreign rows are unioned; the
            # previous code ignored it and unioned everything (#170).
            # Raise so the pipeline falls back to the Python tier, which
            # applies the sub-pipeline correctly.
            raise NotImplementedError(
                "$unionWith with a sub-pipeline is not supported in the "
                "SQL tier - use force_fallback or simplify the pipeline"
            )
        other_table = create_temp(
            {"$unionWith": union_spec},
            f"SELECT {other_select_cols} FROM {quote_table_name(coll_name)}",
        )

        # Union the two tables with explicit column lists
        result_table = create_temp(
            {"$unionWith": union_spec},
            f"{current_select} UNION ALL SELECT {other_select_cols} FROM {other_table}",
        )

        return result_table

    def _insert_stage_rows_into_collection(
        self,
        current_table: str,
        quoted_target: str,
        where_clause: str = "",
    ) -> None:
        """Insert documents from current_table into quoted_target(data, _id) synchronizing _id and $._id."""
        cols = {
            col[1]
            for col in self.db.execute(
                f"PRAGMA table_info({quote_table_name(current_table)})"
            ).fetchall()
        }
        col_id_expr = "src._id" if "_id" in cols else "NULL"
        insert_sql = f"""
            INSERT INTO {quoted_target} (data, _id)
            SELECT
                CASE
                    WHEN json_type(_raw_data, '$._id') IS NOT NULL THEN json(_raw_data)
                    WHEN _col_id IS NOT NULL THEN json(json_set(_raw_data, '$._id', _col_id))
                    ELSE json(json_set(_raw_data, '$._id', json_object('__neosqlite_objectid__', json('true'), 'id', _gen_oid)))
                END,
                COALESCE(
                    _col_id,
                    CASE
                        WHEN _doc_id IS NOT NULL THEN _graph_doc_id
                        ELSE _gen_oid
                    END
                )
            FROM (
                SELECT
                    src.data AS _raw_data,
                    {col_id_expr} AS _col_id,
                    json_extract(src.data, '$._id') AS _doc_id,
                    CASE
                        WHEN json_extract(src.data, '$._id.__neosqlite_objectid__') = 1
                        THEN json_extract(src.data, '$._id.id')
                        ELSE json_extract(src.data, '$._id')
                    END AS _graph_doc_id,
                    lower(hex(randomblob(12))) AS _gen_oid
                FROM {current_table} src
                {where_clause}
            )
        """
        self.db.execute(insert_sql)

    def _ensure_target_collection(self, target_coll_name: str):
        """Return target collection, creating its backing SQLite table if missing."""
        target_coll = self.collection._database[target_coll_name]
        if hasattr(target_coll, "_initialize_table"):
            target_coll._initialize_table()
        return target_coll

    def _process_out_stage(self, create_temp, current_table, out_spec):
        """$out writes all pipeline documents into a collection, replacing existing contents."""
        if isinstance(out_spec, str):
            target_coll_name = out_spec
        elif isinstance(out_spec, dict):
            coll_name = out_spec.get("coll")
            db_name = out_spec.get("db")
            current_db_name = getattr(self.collection._database, "name", None)
            if db_name and db_name != current_db_name:
                raise NotImplementedError(
                    "$out to a different database requires Python fallback"
                )
            if not isinstance(coll_name, str) or not coll_name:
                raise ValueError("$out requires non-empty collection name")
            target_coll_name = coll_name
        else:
            raise ValueError("$out requires string or dict specification")

        target_coll = self._ensure_target_collection(target_coll_name)
        quoted_target = quote_table_name(target_coll.name)
        self.db.execute(f"DELETE FROM {quoted_target}")
        self._insert_stage_rows_into_collection(current_table, quoted_target)

        return create_temp(
            {"$out": out_spec},
            f"SELECT id, _id, data FROM {current_table} WHERE 0",
            [],
        )

    def _process_merge_stage(self, create_temp, current_table, merge_spec):
        """$merge writes pipeline results into a target collection using SQL."""
        if isinstance(merge_spec, str):
            target_coll_name = merge_spec
            on_spec: Any = "_id"
            when_matched: Any = "replace"
            when_not_matched: Any = "insert"
        elif isinstance(merge_spec, dict):
            into = merge_spec.get("into", "")
            if isinstance(into, dict):
                db_name = into.get("db")
                current_db_name = getattr(
                    self.collection._database, "name", None
                )
                if db_name and db_name != current_db_name:
                    raise NotImplementedError(
                        "$merge into a different database requires Python fallback"
                    )
                target_coll_name = into.get("coll", "")
            elif isinstance(into, str):
                target_coll_name = into
            else:
                raise NotImplementedError("$merge 'into' must be string or dict")
            if "." in str(target_coll_name):
                raise NotImplementedError(
                    "$merge cross-database namespace requires Python fallback"
                )
            on_spec = merge_spec.get("on", "_id")
            when_matched = merge_spec.get("whenMatched", "replace")
            when_not_matched = merge_spec.get("whenNotMatched", "insert")
        else:
            target_coll_name = "merged"
            on_spec = "_id"
            when_matched = "replace"
            when_not_matched = "insert"

        if not isinstance(target_coll_name, str) or not target_coll_name:
            raise NotImplementedError("$merge requires target collection name")

        if when_matched not in ("replace", "merge", "keepExisting", "fail"):
            raise NotImplementedError(
                f"$merge whenMatched={when_matched!r} requires Python fallback"
            )
        if when_not_matched not in ("insert", "discard", "fail"):
            raise NotImplementedError(
                f"$merge whenNotMatched={when_not_matched!r} requires Python fallback"
            )

        if isinstance(on_spec, str):
            on_fields = [on_spec]
        elif isinstance(on_spec, list) and on_spec and all(
            isinstance(f, str) and f for f in on_spec
        ):
            on_fields = on_spec
        else:
            raise NotImplementedError(
                f"$merge unsupported 'on' specification: {on_spec!r}"
            )

        src_cols = {
            col[1]
            for col in self.db.execute(
                f"PRAGMA table_info({quote_table_name(current_table)})"
            ).fetchall()
        }
        src_has_underscore_id = "_id" in src_cols

        target_coll = self._ensure_target_collection(target_coll_name)
        quoted_target = quote_table_name(target_coll.name)

        cond_parts = []
        for f in on_fields:
            if f == "_id":
                s_key = (
                    "COALESCE(src._id, "
                    + _graph_lookup_scalar_or_oid("json_extract(src.data, '$._id')")
                    + ")"
                    if src_has_underscore_id
                    else _graph_lookup_scalar_or_oid(
                        "json_extract(src.data, '$._id')"
                    )
                )
                t_key = "tgt._id"
            else:
                jp = parse_json_path(f)
                s_key = _graph_lookup_scalar_or_oid(
                    f"json_extract(src.data, '{jp}')"
                )
                t_key = _graph_lookup_scalar_or_oid(
                    f"json_extract(tgt.data, '{jp}')"
                )
            cond_parts.append(f"({s_key}) IS NOT NULL AND ({t_key}) = ({s_key})")

        match_cond = " AND ".join(cond_parts)

        # Pre-check fail modes before mutating target_coll so fallback stays atomic
        if when_matched == "fail":
            hit = self.db.execute(
                f"SELECT 1 FROM {current_table} src "
                f"WHERE EXISTS (SELECT 1 FROM {quoted_target} tgt WHERE {match_cond}) LIMIT 1"
            ).fetchone()
            if hit is not None:
                raise Exception(
                    f"$merge failed: matching document already exists on {on_fields}"
                )

        if when_not_matched == "fail":
            miss = self.db.execute(
                f"SELECT 1 FROM {current_table} src "
                f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_target} tgt WHERE {match_cond}) LIMIT 1"
            ).fetchone()
            if miss is not None:
                raise Exception(
                    f"$merge failed: no matching document found on {on_fields}"
                )

        if when_matched == "replace":
            src_lookup = f"(SELECT src.data FROM {current_table} src WHERE {match_cond} ORDER BY src.id DESC LIMIT 1)"
            # Rebuild replacing keys using json_set for scalar _id vs json() for ObjectId dict
            src_kv = (
                f"SELECT key AS k, "
                f"CASE WHEN type = 'true' THEN 'true' WHEN type = 'false' THEN 'false' "
                f"WHEN type = 'null' THEN 'null' WHEN type IN ('object', 'array') THEN value "
                f"ELSE json_quote(value) END AS jv "
                f"FROM json_each({src_lookup}) WHERE key != '_id' "
                f"UNION ALL "
                f"SELECT '_id' AS k, "
                f"CASE WHEN json_type(tgt.data, '$._id') IN ('object', 'array') THEN json_extract(tgt.data, '$._id') "
                f"WHEN json_type(tgt.data, '$._id') IS NOT NULL THEN json_quote(json_extract(tgt.data, '$._id')) "
                f"WHEN tgt._id IS NOT NULL THEN json_quote(tgt._id) END AS jv "
                f"WHERE json_type(tgt.data, '$._id') IS NOT NULL OR tgt._id IS NOT NULL"
            )
            update_sql = f"""
                UPDATE {quoted_target} AS tgt
                SET data = json((SELECT json_group_object(k, json(jv)) FROM ({src_kv})))
                WHERE EXISTS (SELECT 1 FROM {current_table} src WHERE {match_cond})
            """
            self.db.execute(update_sql)
        elif when_matched == "merge":
            src_lookup = f"(SELECT src.data FROM {current_table} src WHERE {match_cond} ORDER BY src.id DESC LIMIT 1)"
            def _kv_select(src_expr: str) -> str:
                return (
                    f"SELECT key AS k, "
                    f"CASE WHEN type = 'true' THEN 'true' WHEN type = 'false' THEN 'false' "
                    f"WHEN type = 'null' THEN 'null' WHEN type IN ('object', 'array') THEN value "
                    f"ELSE json_quote(value) END AS jv "
                    f"FROM json_each({src_expr}) _je"
                )
            merge_kv = (
                f"{_kv_select('tgt.data')} "
                f"WHERE _je.key = '_id' OR NOT EXISTS ("
                f"SELECT 1 FROM json_each({src_lookup}) _se WHERE _se.key = _je.key"
                f") UNION ALL "
                f"{_kv_select(src_lookup)} WHERE _je.key != '_id'"
            )
            update_sql = f"""
                UPDATE {quoted_target} AS tgt
                SET data = json((SELECT json_group_object(k, json(jv)) FROM ({merge_kv})))
                WHERE EXISTS (SELECT 1 FROM {current_table} src WHERE {match_cond})
            """
            self.db.execute(update_sql)

        if when_not_matched == "insert":
            self._insert_stage_rows_into_collection(
                current_table,
                quoted_target,
                where_clause=f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_target} tgt WHERE {match_cond})",
            )

        return current_table

    def _process_redact_stage(self, create_temp, current_table, redact_spec):
        """Process $redact stage in SQLite for top-level $$KEEP / $$PRUNE patterns."""
        where_clause, params = build_redact_where_sql(
            self.expr_evaluator, redact_spec
        )
        if where_clause is None:
            raise NotImplementedError(
                "$redact pattern (e.g. $$DESCEND) is not supported in the temporary-table tier - "
                "the pipeline will fall back to the Python tier"
            )

        columns = {
            col[1]
            for col in self.db.execute(
                f"PRAGMA table_info({quote_table_name(current_table)})"
            ).fetchall()
        }
        select_cols = [c for c in ("id", "_id", "data") if c in columns or c == "data"]
        sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"
        if where_clause:
            sql = f"{sql} {where_clause}"
        return create_temp({"$redact": redact_spec}, sql, params)

    def _process_set_window_fields_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """Process $setWindowFields stage."""
        partition_by = spec.get("partitionBy")
        sort_by: dict[str, int] = spec.get("sortBy", {})
        output: dict[str, Any] = spec.get("output", {})
        all_params: list[Any] = []

        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names
        has_data = "data" in column_names

        if not has_data:
            logger.warning(
                f"Table {current_table} has no data column for setWindowFields"
            )
            return current_table

        base_cols = [c for c in ("id", "_id", "data") if c in column_names]
        inner_selects = list(base_cols)

        # 1. Compute PARTITION BY once in inner subquery
        partition_aliases: list[str] = []
        if partition_by is not None:
            if partition_by == "_id":
                if has_underscore_id:
                    inner_selects.append("_id AS _wp_0")
                    partition_aliases.append("_wp_0")
                elif has_id:
                    inner_selects.append("id AS _wp_0")
                    partition_aliases.append("_wp_0")
            else:
                sql, params = self.expr_evaluator.build_select_expression(
                    partition_by
                )
                inner_selects.append(f"({sql}) AS _wp_0")
                partition_aliases.append("_wp_0")
                all_params.extend(params)
        partition_clause = (
            f"PARTITION BY {', '.join(partition_aliases)}"
            if partition_aliases
            else ""
        )

        # 2. Compute ORDER BY keys once in inner subquery
        sort_aliases: list[str] = []
        if sort_by:
            for idx, (field, direction) in enumerate(sort_by.items()):
                order = "ASC" if direction == 1 else "DESC"
                if field == "_id":
                    if has_underscore_id:
                        inner_selects.append(f"_id AS _ws_{idx}")
                        sort_aliases.append(f"_ws_{idx} {order}")
                    elif has_id:
                        inner_selects.append(f"id AS _ws_{idx}")
                        sort_aliases.append(f"_ws_{idx} {order}")
                else:
                    sql, params = self.expr_evaluator.build_select_expression(
                        f"${field}"
                    )
                    inner_selects.append(f"({sql}) AS _ws_{idx}")
                    sort_aliases.append(f"_ws_{idx} {order}")
                    all_params.extend(params)
        sort_clause = f"ORDER BY {', '.join(sort_aliases)}" if sort_aliases else ""

        # 3. Compile output operators (operands evaluated once inside inner subquery)
        json_set_args: list[str] = []
        op_index = 0
        for field, op_spec in output.items():
            if field == "_id" or not isinstance(op_spec, dict) or not op_spec:
                continue

            op_name = next(iter(op_spec.keys()))
            op_val = op_spec[op_name]
            window_spec = op_spec.get("window")

            frame_clause = ""
            if window_spec:
                if "range" in window_spec:
                    if window_spec.get("unit") is not None:
                        raise NotImplementedError(
                            "window.range with 'unit' not supported in SQL tier"
                        )
                    if len(sort_aliases) != 1:
                        raise NotImplementedError(
                            "window.range requires single sortBy field in SQL tier"
                        )
                if sort_aliases:
                    frame_clause = self._build_window_frame_sql(window_spec)

            win_parts = [p for p in (partition_clause, sort_clause) if p]
            if frame_clause:
                win_parts.append(frame_clause)
            elif sort_clause:
                win_parts.append(
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                )
            over_clause = f"OVER ({' '.join(win_parts)})" if win_parts else "OVER ()"

            op_col = f"_wop_{op_index}"
            op_index += 1

            match op_name:
                case "$rank":
                    win_expr = f"RANK() {over_clause}"
                case "$denseRank":
                    win_expr = f"DENSE_RANK() {over_clause}"
                case "$documentNumber":
                    win_expr = f"ROW_NUMBER() {over_clause}"
                case "$count":
                    win_expr = f"COUNT(*) {over_clause}"
                case "$first":
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        op_val
                    )
                    inner_selects.append(f"({op_sql}) AS {op_col}")
                    all_params.extend(op_params)
                    win_expr = f"FIRST_VALUE({op_col}) {over_clause}"
                case "$last":
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        op_val
                    )
                    inner_selects.append(f"({op_sql}) AS {op_col}")
                    all_params.extend(op_params)
                    win_expr = f"LAST_VALUE({op_col}) {over_clause}"
                case "$shift":
                    output_expr = op_val.get("output")
                    by = op_val.get("by", 0)
                    default = op_val.get("default")
                    func = "LEAD" if by >= 0 else "LAG"
                    offset = by if by >= 0 else -by
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        output_expr
                    )
                    inner_selects.append(f"({op_sql}) AS {op_col}")
                    all_params.extend(op_params)
                    if default is not None:
                        win_expr = f"{func}({op_col}, {offset}, ?) {over_clause}"
                        all_params.append(default)
                    else:
                        win_expr = f"{func}({op_col}, {offset}) {over_clause}"
                case "$sum" | "$avg" | "$min" | "$max":
                    func = op_name[1:].upper()
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        op_val
                    )
                    inner_selects.append(f"({op_sql}) AS {op_col}")
                    all_params.extend(op_params)
                    win_expr = f"{func}({op_col}) {over_clause}"
                case "$push":
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        op_val
                    )
                    inner_selects.append(f"({op_sql}) AS {op_col}")
                    all_params.extend(op_params)
                    win_expr = (
                        f"json(json_group_array({op_col}) "
                        f"FILTER (WHERE {op_col} IS NOT NULL) {over_clause})"
                    )
                case "$stdDevPop" | "$stdDevSamp":
                    op_sql, op_params = self.expr_evaluator.build_select_expression(
                        op_val
                    )
                    inner_selects.append(
                        f"CASE WHEN typeof(({op_sql})) IN ('integer', 'real') "
                        f"THEN CAST(({op_sql}) AS REAL) ELSE NULL END AS {op_col}"
                    )
                    all_params.extend(op_params)
                    all_params.extend(op_params)
                    cnt_w = f"COUNT({op_col}) {over_clause}"
                    sum_w = f"SUM({op_col}) {over_clause}"
                    sum2_w = f"SUM({op_col} * {op_col}) {over_clause}"
                    if op_name == "$stdDevPop":
                        win_expr = (
                            f"(CASE WHEN {cnt_w} > 0 THEN "
                            f"SQRT(MAX(0.0, ({sum2_w} * 1.0 / {cnt_w}) "
                            f"- (({sum_w} * 1.0 / {cnt_w}) * ({sum_w} * 1.0 / {cnt_w})))) "
                            f"ELSE NULL END)"
                        )
                    else:
                        win_expr = (
                            f"(CASE WHEN {cnt_w} > 1 THEN "
                            f"SQRT(MAX(0.0, ({sum2_w} - ({sum_w} * {sum_w} * 1.0 / {cnt_w})) "
                            f"/ ({cnt_w} - 1))) ELSE NULL END)"
                        )
                case _:
                    raise NotImplementedError(
                        f"Window operator {op_name} not supported in SQL tier"
                    )

            json_set_args.append(f"'{parse_json_path(field)}'")
            json_set_args.append(win_expr)

        json_set_func = (
            "jsonb_set" if self.jsonb.jsonb_supported else "json_set"
        )
        outer_cols = [c for c in ("id", "_id") if c in column_names]
        if json_set_args:
            data_expr = f"json({json_set_func}(data, {', '.join(json_set_args)})) AS data"
        else:
            data_expr = "data"
        outer_cols.append(data_expr)

        sql = (
            f"SELECT {', '.join(outer_cols)} "
            f"FROM (SELECT {', '.join(inner_selects)} FROM {current_table})"
        )
        return create_temp({"$setWindowFields": spec}, sql, all_params)

    def _build_window_frame_sql(
        self, window_spec: dict[str, Any] | None
    ) -> str:
        """Build SQL window frame clause (ROWS BETWEEN ... or RANGE BETWEEN ...)."""
        if not window_spec:
            return ""

        def map_bound(val: Any, is_upper: bool, is_range: bool) -> str | None:
            if val == "unbounded":
                return (
                    "UNBOUNDED FOLLOWING" if is_upper else "UNBOUNDED PRECEDING"
                )
            if val == "current":
                return "CURRENT ROW"
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                if not is_range and not isinstance(val, int):
                    return None
                if val < 0:
                    return f"{-val} PRECEDING"
                if val > 0:
                    return f"{val} FOLLOWING"
                return "CURRENT ROW"
            return None

        if "documents" in window_spec:
            lower, upper = window_spec["documents"]
            l_bound = map_bound(lower, False, False)
            u_bound = map_bound(upper, True, False)
            if l_bound and u_bound:
                return f"ROWS BETWEEN {l_bound} AND {u_bound}"

        if "range" in window_spec:
            lower, upper = window_spec["range"]
            l_bound = map_bound(lower, False, True)
            u_bound = map_bound(upper, True, True)
            if not l_bound or not u_bound:
                raise NotImplementedError(
                    f"Unsupported window.range bounds: {window_spec['range']!r}"
                )
            return f"RANGE BETWEEN {l_bound} AND {u_bound}"

        return ""

    def _process_graph_lookup_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """Process $graphLookup stage."""
        rec_cte = "graph_recurse_tier2"
        parts = build_graph_lookup_parts(
            self.collection,
            self.jsonb.jsonb_supported,
            current_table,
            rec_cte,
            spec,
            has_root=False,
        )
        if parts is None:
            return current_table

        recurse_sql, select_sql, rec_params = parts
        stage_sql = (
            f"WITH RECURSIVE {rec_cte} AS ({recurse_sql}) {select_sql}"
        )
        return create_temp({"$graphLookup": spec}, stage_sql, rec_params)

    def _process_fill_stage(
        self,
        create_temp: Callable[[dict[str, Any], str, list[Any]], str],
        current_table: str,
        spec: dict[str, Any],
    ) -> str:
        """Process $fill stage including constant value, locf, and linear interpolation."""
        partition_by = spec.get("partitionBy")
        sort_by: dict[str, int] = spec.get("sortBy", {})
        output: dict[str, Any] = spec.get("output", {})
        if not output:
            return current_table

        inner_selects = ["id", "_id", "data"]
        all_params: list[Any] = []

        # 1. Partition expressions (evaluated once in _f0)
        part_aliases: list[str] = []
        if partition_by is not None:
            if isinstance(partition_by, dict) and not any(
                k.startswith("$") for k in partition_by
            ):
                for idx, val_expr in enumerate(partition_by.values()):
                    psql, pparams = self.expr_evaluator.build_select_expression(
                        val_expr
                    )
                    alias = f"_fp_{idx}"
                    inner_selects.append(f"({psql}) AS {alias}")
                    part_aliases.append(alias)
                    all_params.extend(pparams)
            else:
                psql, pparams = self.expr_evaluator.build_select_expression(
                    partition_by
                )
                inner_selects.append(f"({psql}) AS _fp_0")
                part_aliases.append("_fp_0")
                all_params.extend(pparams)

        part_clause = (
            f"PARTITION BY {', '.join(part_aliases)}" if part_aliases else ""
        )

        # 2. Sort expressions (evaluated once in _f0)
        sort_aliases: list[str] = []
        primary_sort_col: str | None = None
        if sort_by:
            for idx, (sfield, direction) in enumerate(sort_by.items()):
                order = "ASC" if direction == 1 else "DESC"
                if sfield == "_id":
                    ssql, sparams = "_id", []
                else:
                    ssql, sparams = self.expr_evaluator.build_select_expression(
                        f"${sfield}"
                    )
                salias = f"_fs_{idx}"
                inner_selects.append(f"({ssql}) AS {salias}")
                sort_aliases.append(f"{salias} {order}")
                if idx == 0:
                    primary_sort_col = salias
                all_params.extend(sparams)

        order_clause = (
            f"ORDER BY {', '.join(sort_aliases)}, id ASC"
            if sort_aliases
            else "ORDER BY id ASC"
        )
        locf_order_clause = (
            f"ORDER BY {', '.join(sort_aliases)}" if sort_aliases else ""
        )

        # 3. Extract each output field once in _f0
        f0_fields: list[tuple[int, str, str, dict[str, Any]]] = []
        has_locf = False
        has_linear = False
        for idx, (field, fill_spec) in enumerate(output.items()):
            method = fill_spec.get("method")
            if method == "locf":
                has_locf = True
            elif method == "linear":
                has_linear = True
            fsql, fparams = self.expr_evaluator.build_select_expression(
                f"${field}"
            )
            inner_selects.append(f"({fsql}) AS _fv_{idx}")
            all_params.extend(fparams)
            f0_fields.append((idx, field, parse_json_path(field), fill_spec))

        json_set_func = (
            "jsonb_set" if self.jsonb.jsonb_supported else "json_set"
        )

        if not has_locf and not has_linear:
            # Simple constant value fill
            set_args: list[str] = []
            for idx, _, fpath, fill_spec in f0_fields:
                set_args.append(f"'{fpath}'")
                set_args.append(f"COALESCE(_fv_{idx}, ?)")
                all_params.append(fill_spec.get("value"))
            data_expr = f"json({json_set_func}(data, {', '.join(set_args)}))"
            sql = (
                f"SELECT id, _id, {data_expr} AS data "
                f"FROM (SELECT {', '.join(inner_selects)} FROM {current_table}) _f0"
            )
            return create_temp({"$fill": spec}, sql, all_params)

        # Two-pass block window layers for locf + linear
        f1_selects = ["*"]
        f2_selects = ["*"]
        win_base = f"{part_clause} {order_clause}".strip()

        for idx, _, _, fill_spec in f0_fields:
            method = fill_spec.get("method")
            if method == "locf":
                f1_selects.append(
                    f"COUNT(_fv_{idx}) OVER ({win_base} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _lblk_{idx}"
                )
                lpart = (
                    f"PARTITION BY {', '.join(part_aliases + [f'_lblk_{idx}'])}"
                )
                lwin = f"{lpart} {locf_order_clause}".strip()
                f2_selects.append(
                    f"FIRST_VALUE(_fv_{idx}) OVER ({lwin}) AS _lval_{idx}"
                )
            elif method == "linear" and primary_sort_col is not None:
                f1_selects.append(
                    f"SUM(CASE WHEN _fv_{idx} IS NOT NULL THEN 1 ELSE 0 END) "
                    f"OVER ({win_base} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _fblk_{idx}"
                )
                f1_selects.append(
                    f"SUM(CASE WHEN _fv_{idx} IS NOT NULL THEN 1 ELSE 0 END) "
                    f"OVER ({win_base} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS _bblk_{idx}"
                )
                f_part = f"PARTITION BY {', '.join(part_aliases + [f'_fblk_{idx}'])}"
                b_part = f"PARTITION BY {', '.join(part_aliases + [f'_bblk_{idx}'])}"
                f2_selects.append(
                    f"MAX(_fv_{idx}) OVER ({f_part}) AS _vs_{idx}"
                )
                f2_selects.append(
                    f"MAX(CASE WHEN _fv_{idx} IS NOT NULL THEN {primary_sort_col} END) OVER ({f_part}) AS _ts_{idx}"
                )
                f2_selects.append(
                    f"MAX(_fv_{idx}) OVER ({b_part}) AS _ve_{idx}"
                )
                f2_selects.append(
                    f"MAX(CASE WHEN _fv_{idx} IS NOT NULL THEN {primary_sort_col} END) OVER ({b_part}) AS _te_{idx}"
                )

        outer_set_args: list[str] = []
        for idx, _, fpath, fill_spec in f0_fields:
            method = fill_spec.get("method")
            outer_set_args.append(f"'{fpath}'")
            if method == "locf":
                outer_set_args.append(f"COALESCE(_fv_{idx}, _lval_{idx})")
            elif method == "linear":
                if primary_sort_col is None:
                    outer_set_args.append(f"_fv_{idx}")
                else:
                    outer_set_args.append(
                        f"(CASE "
                        f"WHEN _fv_{idx} IS NOT NULL THEN _fv_{idx} "
                        f"WHEN typeof(_vs_{idx}) IN ('integer', 'real') "
                        f"AND typeof(_ve_{idx}) IN ('integer', 'real') "
                        f"AND typeof(_ts_{idx}) IN ('integer', 'real') "
                        f"AND typeof(_te_{idx}) IN ('integer', 'real') "
                        f"AND typeof({primary_sort_col}) IN ('integer', 'real') "
                        f"AND _ts_{idx} != _te_{idx} "
                        f"THEN CAST(_vs_{idx} AS REAL) + (CAST(_ve_{idx} AS REAL) - CAST(_vs_{idx} AS REAL)) "
                        f"* (CAST({primary_sort_col} AS REAL) - CAST(_ts_{idx} AS REAL)) "
                        f"/ (CAST(_te_{idx} AS REAL) - CAST(_ts_{idx} AS REAL)) "
                        f"ELSE NULL END)"
                    )
            else:
                outer_set_args.append(f"COALESCE(_fv_{idx}, ?)")
                all_params.append(fill_spec.get("value"))

        data_expr = f"json({json_set_func}(data, {', '.join(outer_set_args)}))"
        stage_sql = (
            f"SELECT id, _id, {data_expr} AS data FROM ("
            f"SELECT {', '.join(f2_selects)} FROM ("
            f"SELECT {', '.join(f1_selects)} FROM ("
            f"SELECT {', '.join(inner_selects)} FROM {current_table}"
            f") _f0) _f1) _f2"
        )
        return create_temp({"$fill": spec}, stage_sql, all_params)
