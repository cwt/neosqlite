from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import Any

from ...sql_utils import quote_table_name
from ..expr_evaluator import (
    AggregationContext,
)
from ..expr_evaluator import (
    _is_expression as _is_expression_module,
)
from ..json_path_utils import parse_json_path
from .operators_base import OperatorsBaseMixin
from .operators_hybrid import python_hybrid_fallback

logger = logging.getLogger(__name__)


class OperatorsSortProjMixin(OperatorsBaseMixin):
    """Mixin for sort, skip, limit, addFields, project, and replaceRoot.

    Provides temporary-table SQL stage processors (with Python-hybrid support
    for complex ``$addFields`` expressions) for ``$sort``, ``$skip``,
    ``$limit``, ``$addFields``, ``$project``, and ``$replaceRoot`` /
    ``$replaceWith`` stages.
    """

    def _process_sort_skip_limit_stage(
        self,
        create_temp: Callable,
        current_table: str,
        sort_spec: dict[str, Any] | None,
        skip_value: int = 0,
        limit_value: int | None = None,
    ) -> str:
        """Process ``$sort``, ``$skip``, and ``$limit`` using temporary tables.

        Inspects ``current_table`` columns to adapt ``_id`` sorting (using
        ``_id``, ``id``, or JSON extraction as available), builds ``ORDER BY``
        and ``LIMIT``/``OFFSET`` clauses via ``sql_translator``, and preserves
        existing ``id`` and ``_id`` columns in the output table.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            sort_spec: ``$sort`` specification mapping field names to sort
                directions (``1`` ascending, ``-1`` descending), or ``None``.
            skip_value: Number of documents to skip from ``$skip``.
            limit_value: Maximum number of documents to return from ``$limit``,
                or ``None`` if unbounded.

        Returns:
            str: Name of the newly created temporary table with sorted and
                paginated results.
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
        """Process an ``$addFields`` stage using temporary tables.

        Constructs nested ``json_set`` / ``jsonb_set`` expressions for
        ``$replaceOne`` on field references, simple ``$field`` copies
        (mapping ``$_id`` to ``id``), literal values bound via ``json(?)``,
        and SQL-translatable expressions via ``expr_evaluator``. Falls back to
        :meth:`_process_add_fields_stage_python_hybrid` when any field uses an
        unsupported expression, literal subdocument, or non-field
        ``$replaceOne`` input.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            add_fields_spec: ``$addFields`` specification mapping new field
                names to expressions, field references, or literal values.

        Returns:
            str: Name of the newly created temporary table with added fields.
        """
        # Check available table columns before building field expressions
        columns = self.db.execute(
            f"PRAGMA table_info({quote_table_name(current_table)})"
        ).fetchall()
        column_names = {col[1] for col in columns}
        has_id = "id" in column_names
        has_underscore_id = "_id" in column_names

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
                if source_field.startswith("$$"):
                    has_complex_expression = True
                    break
                source_field_name = source_field[1:]  # Remove leading $
                json_set_func = f"{self.jsonb.json_function_prefix}_set"
                json_extract = f"{self.jsonb.json_function_prefix}_extract"
                if source_field_name == "_id":
                    # Preserve ObjectId / JSON _id from data when present, or _id column (never row PK `id`)
                    id_val_expr = (
                        f"CASE WHEN json_type(data, '$._id') IN ('object', 'array') THEN json({json_extract}(data, '$._id')) "
                        f"WHEN json_type(data, '$._id') = 'true' THEN json('true') "
                        f"WHEN json_type(data, '$._id') = 'false' THEN json('false') "
                        f"WHEN json_type(data, '$._id') IS NOT NULL THEN {json_extract}(data, '$._id') "
                        f"ELSE {'_id' if has_underscore_id else 'NULL'} END"
                    )
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', {id_val_expr})"
                else:
                    # Use json_extract/jsonb_extract to get the source field value
                    data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', {json_extract}(data, '{parse_json_path(source_field_name)}'))"

            # Handle literal values (strings, numbers, booleans, lists, None)
            elif not isinstance(source_field, dict):
                agg_ctx = AggregationContext()
                if isinstance(source_field, bool):
                    expr_sql = (
                        "json('true')" if source_field else "json('false')"
                    )
                    expr_params: list[Any] = []
                else:
                    expr_sql, expr_params = (
                        self.expr_evaluator.build_select_expression(
                            source_field, context=agg_ctx
                        )
                    )
                json_set_func = f"{self.jsonb.json_function_prefix}_set"
                data_expr = f"{json_set_func}({data_expr}, '{parse_json_path(new_field)}', {expr_sql})"
                params.extend(expr_params)

            # Handle complex expressions (dict with operators like $add, $filter, etc.)
            elif isinstance(source_field, dict):
                if _is_expression_module(source_field):
                    try:
                        agg_ctx = AggregationContext()
                        expr_sql, expr_params = (
                            self.expr_evaluator.build_select_expression(
                                source_field, context=agg_ctx
                            )
                        )
                    except (NotImplementedError, ValueError, TypeError):
                        expr_sql, expr_params = None, []

                    if expr_sql is not None:
                        json_set_func = f"{self.jsonb.json_function_prefix}_set"
                        data_expr = (
                            f"{json_set_func}({data_expr}, "
                            f"'{parse_json_path(new_field)}', {expr_sql})"
                        )
                        params.extend(expr_params)
                        continue

                # Unsupported expression or literal subdocument -
                # use Python hybrid approach to stay in Tier 2
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
        jsonb = self.jsonb

        # If we have parameters, use a subquery to avoid duplicating
        # placeholder expressions in json_data_column()'s CASE statement.
        if params:
            if select_cols:
                inner_query = f"SELECT {select_cols}, {data_expr} as data FROM {current_table}"
            else:
                inner_query = f"SELECT {data_expr} as data FROM {current_table}"
            outer_data = jsonb.data_column("data")
            if select_cols:
                query = f"SELECT {select_cols}, {outer_data} as data FROM ({inner_query})"
            else:
                query = f"SELECT {outer_data} as data FROM ({inner_query})"
        else:
            if select_cols:
                query = f"SELECT {select_cols}, {jsonb.data_column(data_expr)} as data FROM {current_table}"
            else:
                query = f"SELECT {jsonb.data_column(data_expr)} as data FROM {current_table}"

        new_table = create_temp(
            add_fields_stage,
            query,
            params or None,
        )
        return new_table

    def _process_add_fields_stage_python_hybrid(
        self,
        create_temp: Callable,
        current_table: str,
        add_fields_spec: dict[str, Any],
    ) -> str:
        """Process ``$addFields`` with complex expressions via a Python hybrid.

        Loads rows from ``current_table``, preserves or restores ``_id``,
        evaluates complex expressions in Python via
        ``ExprEvaluator._evaluate_expr_python`` (along with ``$$ROOT``,
        ``$$CURRENT``, ``$field`` references, and literals), and writes the
        updated documents into a newly created temporary table so processing
        remains in Tier 2.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            add_fields_spec: ``$addFields`` specification mapping target fields
                to expressions, field references, or literals.

        Returns:
            str: Name of the newly created temporary table with added fields.
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
        select_clause += f", {self.jsonb.data_column()} AS data"

        cursor = self.db.execute(f"SELECT {select_clause} FROM {current_table}")
        rows = cursor.fetchall()

        # Process each document with $addFields
        processed_docs = []
        evaluator = ExprEvaluator(data_column="data", db_connection=self.db)

        for row in rows:
            doc_id = row[0]
            doc_data = row[-1]

            # Parse the document
            doc = neosqlite_json_loads(doc_data)

            if has_underscore_id:
                doc_underscore_id = row[1]
            elif isinstance(doc, dict) and "_id" in doc:
                # Source table has no _id column (e.g. post-$group): the
                # logical _id lives only inside data — carry it forward so
                # downstream stages don't overwrite it with NULL (#108)
                doc_underscore_id = doc.get("_id")
            else:
                doc_underscore_id = None

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
        """Check whether ``expr`` is evaluated as an expression dictionary.

        Returns ``True`` when ``expr`` is a ``dict`` whose single key starts
        with ``"$"`` or when ``expr`` is a ``dict`` with zero or multiple keys.

        Args:
            expr: Candidate field value or expression specification.

        Returns:
            bool: ``True`` if ``expr`` is treated as an expression dict,
                ``False`` otherwise.
        """
        if isinstance(expr, dict):
            # Check if it looks like an expression (has operators starting with $)
            if len(expr) == 1:
                key = next(iter(expr.keys()))
                return key.startswith("$")
            return True
        return False

    @python_hybrid_fallback("$project")
    def _process_project_stage(
        self,
        create_temp: Callable,
        current_table: str,
        project_spec: dict[str, Any],
    ) -> str:
        """Process a ``$project`` stage using temporary tables.

        Dispatches to exclusion mode (using ``json_remove`` /
        ``jsonb_remove``) when all non-``_id`` values equal ``0``, or to
        inclusion mode (rebuilding ``data`` via ``json_object`` /
        ``jsonb_object``) otherwise.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            project_spec: The ``$project`` stage specification dictionary.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            NotImplementedError: If ``get_force_fallback()`` is enabled or any
                non-``_id`` key contains a dotted path.
        """
        # Check kill switch FIRST — force Python fallback
        from ..query_helper import get_force_fallback

        if get_force_fallback():
            raise NotImplementedError(
                "Force fallback - use Tier 3 Python evaluation"
            )

        id_spec = project_spec.get("_id", 1)
        id_is_flag = isinstance(id_spec, (int, bool)) and id_spec in (0, 1)
        include_id = not (id_is_flag and id_spec in (0, False))
        has_computed_id = "_id" in project_spec and not id_is_flag

        if has_computed_id:
            raise NotImplementedError(
                "Computed _id in Tier-2 $project requires Python tier"
            )

        # Determine mode: inclusion vs exclusion
        # Inclusion mode if any value == 1 or is an expression/field ref
        # Dotted field paths produce nested dicts; decline to Tier-3 Python
        if any("." in k for k in project_spec if k != "_id"):
            raise NotImplementedError(
                "Dotted field paths in $project require Python tier"
            )

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
        """Handle exclusion-mode ``$project`` via ``json_remove``.

        Strips non-``_id`` fields marked ``0`` from ``data`` in a single
        ``json_remove`` / ``jsonb_remove`` call and optionally drops ``_id``
        from the projected columns.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            project_spec: ``$project`` specification with exclusion flags.
            include_id: Whether to include the ``_id`` column in the output
                table.

        Returns:
            str: Name of the newly created temporary table.
        """
        fields_to_remove = [
            k
            for k, v in project_spec.items()
            if v == 0 and (k != "_id" or not include_id)
        ]
        if not include_id and "_id" not in fields_to_remove:
            fields_to_remove.append("_id")

        select_cols = ["id"]
        if include_id:
            select_cols.append("_id")

        if fields_to_remove:
            json_remove = f"{self.jsonb.json_function_prefix}_remove"
            # SQLite's json_remove supports multiple paths in a single call:
            #   json_remove(data, p1, p2, ...)  -- more efficient than nesting
            path_args = ", ".join(
                f"'{parse_json_path(f, preserve_id=False)}'"
                for f in fields_to_remove
            )
            data_expr = f"{json_remove}(data, {path_args})"
        else:
            data_expr = "data"

        select_cols.append(f"{self.jsonb.data_column(data_expr)} AS data")

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
        """Handle inclusion-mode ``$project`` by rebuilding ``data``.

        Reconstructs documents with ``json_object`` / ``jsonb_object`` for
        ``$meta: "textScore"``, SQL-translatable expressions via
        ``ExprEvaluator``, ``$$CURRENT``, ``$field`` references, and simple
        ``1`` inclusions, pruning absent fields with ``json_remove`` /
        ``jsonb_remove``. Wraps parameterized queries in a subquery to avoid
        duplicating SQL placeholders inside ``jsonb.data_column``.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            project_spec: ``$project`` specification dictionary.
            include_id_default: Default ``_id`` inclusion flag when no
                expression or field-reference projections are present.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            NotImplementedError: If ``$$ROOT`` is projected or an expression
                cannot be translated to SQL.
        """
        jsonb = self.jsonb
        json_obj_func = self.jsonb.function("object")
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
        missing_removals = []
        all_params: list[Any] = []

        if include_id:
            json_parts.append("'_id'")
            json_parts.append(
                f"CASE "
                f"WHEN json_type(data, '$._id') IN ('object', 'array') THEN json({json_extract_func}(data, '$._id')) "
                f"WHEN json_type(data, '$._id') = 'true' THEN json('true') "
                f"WHEN json_type(data, '$._id') = 'false' THEN json('false') "
                f"WHEN json_type(data, '$._id') IS NOT NULL THEN {json_extract_func}(data, '$._id') "
                f"ELSE _id END"
            )
            missing_removals.append(
                "CASE WHEN json_type(data, '$._id') IS NULL AND _id IS NULL "
                "THEN '$._id' ELSE '$.__none__' END"
            )

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
                    json_parts.append(
                        f"'{field.replace(chr(39), chr(39) * 2)}'"
                    )
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
                    json_parts.append(
                        f"'{field.replace(chr(39), chr(39) * 2)}'"
                    )
                    json_parts.append(expr_sql)

            elif value == "$$ROOT":
                raise NotImplementedError(
                    "$$ROOT in Tier-2 $project requires Python tier"
                )
            elif value == "$$CURRENT":
                json_set_func = self.jsonb.function("set")
                json_parts.append(f"'{field.replace(chr(39), chr(39) * 2)}'")
                json_parts.append(
                    f"json(CASE WHEN json_type(data, '$._id') IS NOT NULL "
                    f"OR _id IS NULL THEN data "
                    f"ELSE {json_set_func}(data, '$._id', _id) END)"
                )
            elif isinstance(value, str) and value.startswith("$"):
                # Field reference: "$some.path"
                source_field = value[1:]
                escaped_field = field.replace("'", "''")
                if source_field == "_id":
                    json_parts.append(f"'{escaped_field}'")
                    json_parts.append("_id")
                else:
                    json_parts.append(f"'{escaped_field}'")
                    json_parts.append(
                        f"{json_extract_func}(data, '{parse_json_path(source_field)}')"
                    )
                    missing_removals.append(
                        f"CASE WHEN json_type(data, '{parse_json_path(source_field)}') IS NULL "
                        f"THEN '{parse_json_path(field)}' ELSE '$.__none__' END"
                    )

            elif value == 1:
                # Simple inclusion: copy field from data
                json_parts.append(f"'{field.replace(chr(39), chr(39) * 2)}'")
                json_parts.append(
                    f"{json_extract_func}(data, '{parse_json_path(field)}')"
                )
                missing_removals.append(
                    f"CASE WHEN json_type(data, '{parse_json_path(field)}') IS NULL "
                    f"THEN '{parse_json_path(field)}' ELSE '$.__none__' END"
                )

            # value == 0 is exclusion — skip in inclusion mode

        # Build the reconstructed data column
        if json_parts:
            data_expr = f"{json_obj_func}({', '.join(json_parts)})"
            if missing_removals:
                json_remove_func = f"{self.jsonb.json_function_prefix}_remove"
                data_expr = f"{json_remove_func}({data_expr}, {', '.join(missing_removals)})"
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
            outer_data = jsonb.data_column("data")
            sql = (
                f"SELECT id{', _id' if include_id else ''}, {outer_data} AS data "
                f"FROM ({inner_sql})"
            )
        else:
            select_cols.append(f"{jsonb.data_column(data_expr)} AS data")
            sql = f"SELECT {', '.join(select_cols)} FROM {current_table}"

        project_stage = {"$project": project_spec}
        return create_temp(project_stage, sql, all_params or None)

    def _generate_text_score_sql(self) -> str:
        """Generate SQL for ``$meta: "textScore"`` using stored BM25 score.

        During ``$text`` search stages, the FTS5 BM25 relevance score is
        stored in the document JSON as ``_textScore``. Extracts that score
        with a ``0.0`` fallback when absent.

        Returns:
            str: SQL expression evaluating to the stored relevance score.
        """
        json_extract = f"{self.jsonb.json_function_prefix}_extract"
        return f"COALESCE({json_extract}(data, '$._textScore'), 0.0)"

    @python_hybrid_fallback(
        lambda spec: (
            "$replaceRoot"
            if isinstance(spec, dict) and "newRoot" in spec
            else "$replaceWith"
        )
    )
    def _process_replace_root_stage(
        self,
        create_temp: Callable,
        current_table: str,
        replace_spec: Any,
    ) -> str:
        """Process a ``$replaceRoot`` or ``$replaceWith`` stage in SQL.

        Supports field references (for example, ``{"newRoot": "$field"}`` or
        ``"$field"``) by extracting the target subdocument into ``data``.
        Complex expressions raise ``NotImplementedError`` to trigger Python
        fallback.

        Args:
            create_temp: Callback used to materialize the next temporary table.
            current_table: Name of the temporary table holding input rows.
            replace_spec: Replacement specification (``{"newRoot": ...}`` or
                direct ``$replaceWith`` value).

        Returns:
            str: Name of the newly created temporary table with replaced root
                documents.

        Raises:
            NotImplementedError: If the replacement target is not a ``$field``
                reference string.
        """
        # Handle both $replaceRoot ({newRoot: ...}) and $replaceWith (direct value)
        if isinstance(replace_spec, dict) and "newRoot" in replace_spec:
            new_root_expr = replace_spec["newRoot"]
        else:
            new_root_expr = replace_spec

        # Handle field reference (e.g., "$field") or $$CURRENT (never treat $$ROOT as $.$ROOT)
        if isinstance(new_root_expr, str) and new_root_expr.startswith("$"):
            if new_root_expr == "$$ROOT" or new_root_expr.startswith("$$ROOT."):
                raise NotImplementedError(
                    "$$ROOT in $replaceRoot requires Python tier"
                )
            json_extract = f"{self.jsonb.json_function_prefix}_extract"
            json_set_func = f"{self.jsonb.json_function_prefix}_set"
            if new_root_expr == "$$CURRENT":
                extract_expr = "data"
            elif new_root_expr.startswith("$$CURRENT."):
                sub_path = new_root_expr[len("$$CURRENT.") :]
                extract_expr = f"{json_extract}(data, '{parse_json_path(sub_path, preserve_id=False)}')"
            else:
                field_name = new_root_expr[1:]  # Remove leading $
                extract_expr = (
                    f"{json_extract}(data, '{parse_json_path(field_name)}')"
                )

            replace_stage = {"$replaceRoot": {"newRoot": new_root_expr}}
            # Preserve child _id when present in replacement object, else fallback to outer _id
            new_table = create_temp(
                replace_stage,
                f"SELECT _rr.id, "
                f"CASE WHEN json_type(_rr._new_data, '$._id') IS NOT NULL "
                f"THEN json_extract(_rr._new_data, '$._id') ELSE _rr._id END AS _id, "
                f"{self.jsonb.data_column(f'CASE WHEN json_type(_rr._new_data, \'$._id\') IS NOT NULL OR _rr._id IS NULL THEN _rr._new_data ELSE {json_set_func}(_rr._new_data, \'$._id\', _rr._id) END')} AS data "
                f"FROM (SELECT id, _id, {extract_expr} AS _new_data FROM {current_table}) AS _rr",
            )
            return new_table
        else:
            # For complex expressions, fall back to Python evaluation
            # This handles cases like {$replaceRoot: {newRoot: {$mergeObjects: [...]}}}
            raise NotImplementedError(
                f"$replaceRoot with expression {new_root_expr} not supported in SQL tier"
            )
