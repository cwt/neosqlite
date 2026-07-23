from __future__ import annotations

import logging
import re
from typing import Any, Callable

from ...sql_utils import quote_table_name
from ..expr_evaluator import (
    AggregationContext,
)
from ..expr_evaluator import (
    _is_expression as _is_expression_module,
)
from ..json_path_utils import parse_json_path
from ..jsonb_support import (
    json_data_column,
)
from .operators_base import OperatorsBaseMixin

logger = logging.getLogger(__name__)


class OperatorsSortProjMixin(OperatorsBaseMixin):
    def _process_sort_skip_limit_stage(
        self,
        create_temp: Callable,
        current_table: str,
        sort_spec: dict[str, Any] | None,
        skip_value: int = 0,
        limit_value: int | None = None,
    ) -> str:
        """
        Process sort/skip/limit stages using temporary tables.

        This method handles the $sort, $skip, and $limit aggregation stages, which
        can be used individually or in combination. It creates a temporary table
        with the results sorted and/or paginated according to the specifications.

        The method supports sorting on both regular fields (using json_extract)
        and the special _id field (using the id column directly). It handles
        ascending and descending sort orders, as well as skip and limit operations
        with proper OFFSET and LIMIT clauses in the SQL query.

        When multiple sort/skip/limit stages are consecutive in a pipeline, they
        are processed together in a single operation for efficiency.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            sort_spec (dict[str, Any] | None): The $sort stage specification, mapping
                                              field names to sort directions (1
                                              for ascending, -1 for descending)
            skip_value (int): The number of documents to skip (from $skip stage)
            limit_value (int | None): The maximum number of documents to return
                                      (from $limit stage)

        Returns:
            str: Name of the newly created temporary table with sorted/skipped/limited results
        """
        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Use SQLTranslator to build ORDER BY clause
        order_clause = ""
        if sort_spec:
            order_clause = self.sql_translator.translate_sort(sort_spec)
            # If sorting by _id but table doesn't have _id column, fix references
            if "_id" in sort_spec and not has_underscore_id:
                if has_id:
                    # Replace ORDER BY _id with ORDER BY id
                    # Handle various forms: "_id ASC", "_id DESC", "._id", etc.
                    order_clause = re.sub(r"\b_id\b", "id", order_clause)
                else:
                    # Neither id nor _id column exists - extract _id from JSON
                    # Replace references to _id with json_extract(data, '$._id')
                    json_func = self.jsonb.json_function_prefix
                    order_clause = order_clause.replace(
                        "_id ASC", f"{json_func}_extract(data, '$._id') ASC"
                    )
                    order_clause = order_clause.replace(
                        "_id DESC", f"{json_func}_extract(data, '$._id') DESC"
                    )
                    # Handle case without explicit direction
                    order_clause = order_clause.replace(
                        "ORDER BY _id",
                        f"ORDER BY {json_func}_extract(data, '$._id')",
                    )

        # Use SQLTranslator to build LIMIT/OFFSET clause
        limit_clause = self.sql_translator.translate_skip_limit(
            limit_value, skip_value
        )

        # Create a stage spec for naming (use the first non-null stage type)
        stage_spec: dict[str, Any] = {}
        if sort_spec:
            stage_spec["$sort"] = sort_spec
        elif skip_value > 0:
            stage_spec["$skip"] = skip_value
        elif limit_value is not None:
            stage_spec["$limit"] = limit_value
        else:
            # Default case if all are None/default values
            stage_spec["$sort"] = {}

        # Build SELECT clause based on available columns
        # Always preserve id and _id columns if they exist
        if has_id and has_underscore_id:
            select_clause = f"SELECT id, _id, data FROM {current_table}"
        elif has_id:
            select_clause = f"SELECT id, data FROM {current_table}"
        elif has_underscore_id:
            select_clause = f"SELECT _id, data FROM {current_table}"
        else:
            select_clause = f"SELECT data FROM {current_table}"

        # Create sorted/skipped/limited temporary table
        new_table = create_temp(
            stage_spec,
            f"{select_clause} {order_clause} {limit_clause}",
        )
        return new_table

    def _process_add_fields_stage(
        self,
        create_temp: Callable,
        current_table: str,
        add_fields_spec: dict[str, Any],
    ) -> str:
        """
        Process an $addFields stage using temporary tables.

        This method implements the $addFields aggregation stage which adds new fields
        to documents. It uses SQLite's json_set function to add fields to the JSON data.

        Supports:
        - Simple field copying: {"newField": "$existingField"}
        - $replaceOne: {$replaceOne: {input: "$text", find: "old", replacement: "new"}}
        - Literal values: {"field": "value"}

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            add_fields_spec (dict[str, Any]): The $addFields stage specification mapping
                                              new field names to source field paths

        Returns:
            str: Name of the newly created temporary table with added fields
        """
        # Build json_set expressions for each field to add
        # We'll construct a nested json_set call for each field
        data_expr = "data"  # Start with the original data
        params: list[Any] = []
        has_complex_expression = False

        # Process each field to add
        for new_field, source_field in add_fields_spec.items():
            # Handle $replaceOne operator
            if isinstance(source_field, dict) and "$replaceOne" in source_field:
                replace_spec = source_field["$replaceOne"]
                if isinstance(replace_spec, dict):
                    input_expr = replace_spec.get("input", "")
                    find_str = replace_spec.get("find", "")
                    replacement_str = replace_spec.get("replacement", "")

                    # Escape single quotes for SQL
                    find_str_escaped = find_str.replace("'", "''")
                    replacement_str_escaped = replacement_str.replace("'", "''")

                    # Build SQL for $replaceOne using instr() and substr()
                    json_extract = f"{self.jsonb.json_function_prefix}_extract"
                    json_set_func = f"{self.jsonb.json_function_prefix}_set"
                    if isinstance(input_expr, str) and input_expr.startswith(
                        "$"
                    ):
                        input_field = input_expr[1:]
                        # SQL: substr(data, 1, instr-1) || replacement || substr(data, instr+len(find))
                        data_expr = (
                            f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', "
                            f"CASE "
                            f"WHEN instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') > 0 THEN "
                            f"substr({json_extract}(data, '{parse_json_path(input_field)}'), 1, "
                            f"instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') - 1) || "
                            f"'{replacement_str_escaped}' || "
                            f"substr({json_extract}(data, '{parse_json_path(input_field)}'), "
                            f"instr({json_extract}(data, '{parse_json_path(input_field)}'), '{find_str_escaped}') + length('{find_str_escaped}')) "
                            f"ELSE {json_extract}(data, '{parse_json_path(input_field)}') END)"
                        )
                    else:
                        # For non-field input, use Python hybrid approach
                        has_complex_expression = True
                        break

            # Handle simple field copying (e.g., {"newField": "$existingField"})
            elif isinstance(source_field, str) and source_field.startswith("$"):
                source_field_name = source_field[1:]  # Remove leading $
                json_set_func = f"{self.jsonb.json_function_prefix}_set"
                if source_field_name == "_id":
                    # Special handling for _id field
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', id)"
                else:
                    # Use json_extract/jsonb_extract to get the source field value
                    json_extract = f"{self.jsonb.json_function_prefix}_extract"
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', {json_extract}(data, '{parse_json_path(source_field_name)}'))"

            # Handle literal values
            elif not isinstance(source_field, dict):
                # For literal values, use json_set with parameterized value
                json_set_func = f"{self.jsonb.json_function_prefix}_set"
                data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', json(?))"
                params.append(source_field)

            # Handle complex expressions (dict with operators like $filter, $map, etc.)
            elif isinstance(source_field, dict):
                # Check if it's a supported operator
                supported_operators = {"$replaceOne"}
                has_supported_op = any(
                    op in supported_operators for op in source_field.keys()
                )

                if not has_supported_op:
                    # Complex expression like $filter, $map, $reduce, etc.
                    # Use Python hybrid approach to stay in Tier 2
                    has_complex_expression = True
                    break

            # For other complex expressions, use Python hybrid approach
            else:
                has_complex_expression = True
                break

        # If we have complex expressions, use Python hybrid approach
        # This loads from temp table, processes in Python, and creates a new temp table
        # This keeps us in Tier 2 (temp tables) rather than falling back to Tier 3
        if has_complex_expression:
            return self._process_add_fields_stage_python_hybrid(
                create_temp, current_table, add_fields_spec
            )

        # Create addFields temporary table
        add_fields_stage = {"$addFields": add_fields_spec}

        # Check what columns the current table has (similar to _process_sort_skip_limit_stage)
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Build SELECT clause based on available columns
        if has_id and has_underscore_id:
            select_cols = "id, _id"
        elif has_id:
            select_cols = "id"
        elif has_underscore_id:
            select_cols = "_id"
        else:
            select_cols = ""

        # When using JSONB, we need to convert final output to text JSON for Python
        jsonb = self.jsonb.jsonb_supported

        # If we have parameters, use a subquery to avoid duplicating
        # placeholder expressions in json_data_column()'s CASE statement.
        if params:
            if select_cols:
                inner_query = f"SELECT {select_cols}, {data_expr} as data FROM {current_table}"
            else:
                inner_query = f"SELECT {data_expr} as data FROM {current_table}"
            outer_data = json_data_column(jsonb, "data")
            if select_cols:
                query = f"SELECT {select_cols}, {outer_data} as data FROM ({inner_query})"
            else:
                query = f"SELECT {outer_data} as data FROM ({inner_query})"
        else:
            if select_cols:
                query = f"SELECT {select_cols}, {json_data_column(jsonb, data_expr)} as data FROM {current_table}"
            else:
                query = f"SELECT {json_data_column(jsonb, data_expr)} as data FROM {current_table}"

        new_table = create_temp(
            add_fields_stage,
            query,
            params if params else None,
        )
        return new_table

    def _process_add_fields_stage_python_hybrid(
        self,
        create_temp: Callable,
        current_table: str,
        add_fields_spec: dict[str, Any],
    ) -> str:
        """
        Process $addFields stage with complex expressions using Python hybrid approach.

        This method loads documents from the current temp table, applies the $addFields
        expressions in Python (using ExprEvaluator), and creates a new temp table.
        This allows us to stay in Tier 2 (temp tables) while still supporting complex
        expressions like $filter, $map, $reduce, etc.

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table
            add_fields_spec (dict[str, Any]): The $addFields stage specification

        Returns:
            str: Name of the newly created temporary table with added fields
        """
        from neosqlite.collection.expr_evaluator import (
            AggregationContext,
            ExprEvaluator,
        )
        from neosqlite.collection.json_helpers import (
            neosqlite_json_dumps,
            neosqlite_json_loads,
        )

        # Check what columns the current table has
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

        # Load all documents from the current temp table
        select_clause = "id"
        if has_underscore_id:
            select_clause += ", _id"
        select_clause += ", data"

        cursor = self.db.execute(f"SELECT {select_clause} FROM {current_table}")
        rows = cursor.fetchall()

        # Process each document with $addFields
        processed_docs = []
        evaluator = ExprEvaluator(data_column="data", db_connection=self.db)

        for row in rows:
            doc_id = row[0]
            doc_underscore_id = row[1] if has_underscore_id else None
            doc_data = row[-1]

            # Parse the document
            doc = neosqlite_json_loads(doc_data)

            # Ensure _id is in the document
            if "_id" not in doc and doc_underscore_id is not None:
                doc["_id"] = doc_underscore_id
            elif "_id" not in doc and has_id:
                doc["_id"] = doc_id

            # Create context for expression evaluation
            ctx = AggregationContext()
            ctx.bind_document(doc.copy())  # $$ROOT
            ctx.update_current(doc.copy())  # $$CURRENT

            # Apply each field in the addFields spec
            for new_field, expr in add_fields_spec.items():
                if self._is_expression(expr):
                    # Evaluate expression in Python
                    value = evaluator._evaluate_expr_python(expr, doc)
                    doc[new_field] = value
                elif isinstance(expr, str) and expr.startswith("$"):
                    if expr.startswith("$$"):
                        # Aggregation variable
                        if expr == "$$ROOT":
                            doc[new_field] = doc.copy()
                        elif expr == "$$CURRENT":
                            doc[new_field] = doc.copy()
                        else:
                            doc[new_field] = None
                    else:
                        # Regular field reference
                        source_field_name = expr[1:]
                        doc[new_field] = self.collection._get_val(
                            doc, source_field_name
                        )
                else:
                    # Literal value
                    doc[new_field] = expr

            processed_docs.append((doc_id, doc_underscore_id, doc))

        # Create a new temp table with the processed documents
        add_fields_stage = {"$addFields": add_fields_spec}

        # Use CREATE TABLE with proper schema, then INSERT
        new_table = create_temp(
            add_fields_stage,
            "SELECT 1 as id, 1 as _id, '{}' as data WHERE 0",
        )

        # Insert processed documents
        for doc_id, doc_underscore_id, doc in processed_docs:
            self.db.execute(
                f"INSERT INTO {new_table} (id, _id, data) VALUES (?, ?, ?)",
                (doc_id, doc_underscore_id, neosqlite_json_dumps(doc)),
            )

        return new_table

    def _is_expression(self, expr: Any) -> bool:
        """Check if an expression is a complex expression (not a simple field reference or literal)."""
        if isinstance(expr, dict):
            # Check if it looks like an expression (has operators starting with $)
            if len(expr) == 1:
                key = next(iter(expr.keys()))
                return key.startswith("$")
            return True
        return False

    def _process_project_stage(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: dict[str, Any],
    ) -> str:
        """
        Process a $project stage using temporary tables.

        This method implements the $project aggregation stage which reshapes
        each document in the stream, by adding new fields, removing existing
        fields, or renaming fields. It reconstructs a unified ``data`` column
        using ``json_object`` / ``jsonb_object`` so that downstream stages
        (especially FTS5 text search via ``json_tree``) continue to work
        without modification.

        Supports:
        - Simple inclusion: ``{"field": 1}``
        - Exclusion: ``{"field": 0}``
        - Field references: ``{"alias": "$some.path"}``
        - Expression projections: ``{"alias": {$concat: [...]}}``
        - ``_id`` inclusion/exclusion

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table
            project_spec (dict[str, Any]): The $project stage specification

        Returns:
            str: Name of the newly created temporary table
        """
        # Check kill switch FIRST — force Python fallback
        from ..query_helper import get_force_fallback

        if get_force_fallback():
            raise NotImplementedError(
                "Force fallback - use Tier 3 Python evaluation"
            )

        include_id = project_spec.get("_id", 1) == 1

        # Determine mode: inclusion vs exclusion
        # Inclusion mode if any value == 1 or is an expression/field ref
        # Exclusion mode if all non-_id values are 0
        non_id_values = {k: v for k, v in project_spec.items() if k != "_id"}
        is_exclusion_mode = all(v == 0 for v in non_id_values.values())

        if is_exclusion_mode:
            # Exclusion mode: use json_remove to strip fields
            return self._process_project_exclusion(
                create_temp, current_table, project_spec, include_id
            )
        else:
            # Inclusion mode: reconstruct data via json_object
            return self._process_project_inclusion(
                create_temp, current_table, project_spec, include_id
            )

    def _process_project_exclusion(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: dict[str, Any],
        include_id: bool,
    ) -> str:
        """Handle exclusion-mode projection by removing fields via json_remove."""
        fields_to_remove = [
            k
            for k, v in project_spec.items()
            if v == 0 and k != "_id"  # _id is a separate column, not in data
        ]

        select_cols = ["id"]
        if include_id:
            select_cols.append("_id")

        if fields_to_remove:
            json_remove = f"{self.jsonb.json_function_prefix}_remove"
            # SQLite's json_remove supports multiple paths in a single call:
            #   json_remove(data, p1, p2, ...)  -- more efficient than nesting
            path_args = ", ".join(
                f"'{parse_json_path(f)}'" for f in fields_to_remove
            )
            data_expr = f"{json_remove}(data, {path_args})"
        else:
            data_expr = "data"

        select_cols.append(
            f"{json_data_column(self.jsonb.jsonb_supported, data_expr)} AS data"
        )

        sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"
        project_stage = {"$project": project_spec}
        return create_temp(project_stage, sql)

    def _process_project_inclusion(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: dict[str, Any],
        include_id_default: bool,
    ) -> str:
        """Handle inclusion-mode projection by reconstructing data via json_object.

        Handles:
        - Simple inclusion: ``{"field": 1}``
        - Field references: ``{"alias": "$some.path"}``
        - Expression projections: ``{"alias": {$concat: [...]}}``
        """
        jsonb = self.jsonb.jsonb_supported
        json_obj_func = "jsonb_object" if jsonb else "json_object"
        json_extract_func = f"{self.jsonb.json_function_prefix}_extract"

        # Determine if projection uses expressions or field references.
        # When it does, _id is only included if explicitly specified
        # (matches Python _apply_projection behavior).
        # For simple inclusion ({field: 1}), _id is included by default.
        has_expressions_or_refs = any(
            _is_expression_module(value)
            or (isinstance(value, str) and value.startswith("$"))
            for value in project_spec.values()
        )

        if has_expressions_or_refs:
            # Expression/field reference mode: _id included by default unless explicitly excluded
            # (matches MongoDB behavior)
            include_id = (
                "_id" not in project_spec or project_spec.get("_id") != 0
            )
        else:
            # Simple inclusion mode: _id included by default
            include_id = include_id_default

        # Build key-value pairs for json_object
        json_parts = []
        all_params: list[Any] = []

        for field, value in project_spec.items():
            if field == "_id":
                continue

            if _is_expression_module(value):
                # Check for $meta: "textScore" - native FTS5 BM25 relevance scoring
                if (
                    isinstance(value, dict)
                    and "$meta" in value
                    and value["$meta"] == "textScore"
                ):
                    # Use FTS5 bm25() function for relevance scoring
                    bm25_score = self._generate_text_score_sql()
                    json_parts.append(f"'{field}'")
                    json_parts.append(bm25_score)
                else:
                    # Expression projection: use ExprEvaluator
                    agg_ctx = AggregationContext()
                    expr_sql, expr_params = (
                        self.expr_evaluator.build_select_expression(
                            value, context=agg_ctx
                        )
                    )
                    # If expr_sql is None, the operator can't be translated to SQL
                    # — trigger Python fallback
                    if expr_sql is None:
                        raise NotImplementedError(
                            f"Expression {value} not supported in SQL tier"
                        )
                    all_params.extend(expr_params)
                    json_parts.append(f"'{field}'")
                    json_parts.append(expr_sql)

            elif isinstance(value, str) and value.startswith("$"):
                # Field reference: "$some.path"
                source_field = value[1:]
                if source_field == "_id":
                    json_parts.append(f"'{field}'")
                    json_parts.append("_id")
                else:
                    json_parts.append(f"'{field}'")
                    json_parts.append(
                        f"{json_extract_func}(data, '{parse_json_path(source_field)}')"
                    )

            elif value == 1:
                # Simple inclusion: copy field from data
                json_parts.append(f"'{field}'")
                json_parts.append(
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )

            # value == 0 is exclusion — skip in inclusion mode

        # Build the reconstructed data column
        if json_parts:
            data_expr = f"{json_obj_func}({', '.join(json_parts)})"
        else:
            # No fields projected — empty object
            data_expr = f"{json_obj_func}()"

        select_cols = ["id"]
        if include_id:
            select_cols.append("_id")

        # If we have parameters, use a subquery to avoid duplicating
        # placeholder expressions in json_data_column()'s CASE statement.
        # json_data_column() may wrap data_expr in CASE WHEN typeof(...)='blob'...
        # which would duplicate the expression (and its placeholders) multiple times.
        if all_params:
            # Subquery: compute the data column once, then apply json_data_column wrapper
            inner_cols = select_cols.copy()
            inner_cols.append(f"{data_expr} AS data")
            inner_sql = f"SELECT {', '.join(inner_cols)} FROM {current_table}"
            # Outer query: apply json_data_column wrapper to the pre-computed data
            outer_data = json_data_column(jsonb, "data")
            sql = (
                f"SELECT id{', _id' if include_id else ''}, {outer_data} AS data "
                f"FROM ({inner_sql})"
            )
        else:
            select_cols.append(f"{json_data_column(jsonb, data_expr)} AS data")
            sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"

        project_stage = {"$project": project_spec}
        return create_temp(
            project_stage, sql, all_params if all_params else None
        )

    def _generate_text_score_sql(self) -> str:
        """
        Generate SQL for $meta: "textScore" using stored BM25 score.

        During $text search stages, the FTS5 BM25 relevance score is captured
        and stored in the document's JSON data as `_textScore`. This method
        extracts that score for use in $project/$addFields stages.

        Returns:
            SQL expression that returns the BM25 relevance score (positive value)
        """
        json_extract = f"{self.jsonb.json_function_prefix}_extract"
        return f"COALESCE({json_extract}(data, '$._textScore'), 0.0)"

    def _process_replace_root_stage(
        self,
        create_temp: Callable,
        current_table: str,
        replace_spec: Any,
    ) -> str:
        """
        Process a $replaceRoot or $replaceWith stage using temporary tables.

        This method implements the $replaceRoot/$replaceWith aggregation stage which
        replaces the root document with a specified field or expression.

        MongoDB syntax:
            {$replaceRoot: {newRoot: "$field"}}
            {$replaceWith: "$field"}

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            replace_spec (Any): The replace specification (field path or expression)

        Returns:
            str: Name of the newly created temporary table with replaced root documents
        """
        # Handle both $replaceRoot ({newRoot: ...}) and $replaceWith (direct value)
        if isinstance(replace_spec, dict) and "newRoot" in replace_spec:
            new_root_expr = replace_spec["newRoot"]
        else:
            new_root_expr = replace_spec

        # Handle field reference (e.g., "$field")
        if isinstance(new_root_expr, str) and new_root_expr.startswith("$"):
            field_name = new_root_expr[1:]  # Remove leading $

            # Create replaceRoot temporary table
            replace_stage = {"$replaceRoot": {"newRoot": new_root_expr}}
            json_extract = f"{self.jsonb.json_function_prefix}_extract"

            # Extract the field and use it as the new root document
            new_table = create_temp(
                replace_stage,
                f"SELECT id, _id, {json_extract}(data, '{parse_json_path(field_name)}') as data FROM {current_table}",
            )
            return new_table
        else:
            # For complex expressions, fall back to Python evaluation
            # This handles cases like {$replaceRoot: {newRoot: {$mergeObjects: [...]}}}
            raise NotImplementedError(
                f"$replaceRoot with expression {new_root_expr} not supported in SQL tier"
            )
