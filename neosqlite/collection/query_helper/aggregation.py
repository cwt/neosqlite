"""
Aggregation pipeline methods for NeoSQLite.

This module contains the AggregationMixin class, which provides Python-based
aggregation pipeline processing.  SQL-based aggregation lives in
_sql_aggregation.py (SqlAggregationMixin).
"""

import json
import logging
import math
import uuid
from copy import deepcopy
from itertools import batched
from typing import TYPE_CHECKING, Any

from ..._sqlite import sqlite3
from ..expr_evaluator import (
    ExprEvaluator,
    _is_expression,
)

logger = logging.getLogger(__name__)

from ._sql_aggregation import SqlAggregationMixin

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext


from ..type_utils import _hashable_group_key


def _addtoset_key(value: Any) -> Any:
    """Return a hashable canonical key for ``$addToSet`` membership.

    Tags values by type so booleans, numbers, dicts, lists, and strings do not
    collide in Python sets, and normalizes numeric types via ``float`` (#155).

    Args:
        value: Document field or evaluated expression value to key.

    Returns:
        Any: Hashable tagged tuple suitable for set membership checks.
    """
    if isinstance(value, dict):
        return ("d", json.dumps(value, sort_keys=True, default=str))
    if isinstance(value, list):
        return ("l", tuple(_addtoset_key(v) for v in value))
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, (int, float)):
        return ("n", repr(float(value)))
    if isinstance(value, str):
        return ("s", value)
    return ("o", str(value))


class AggregationMixin(SqlAggregationMixin):
    """Mixin class providing aggregation pipeline methods.

    This mixin assumes it will be used with a class that has the following:

    Attributes:
        self.collection: A collection instance with:
            - db: Database connection
            - name: Collection name
            - _load: Method to load documents
            - _get_val: Method to get values from documents
            - _set_val: Method to set values in documents
        self.jsonb.jsonb_supported: Whether JSONB is supported
        self.jsonb.json_function_prefix: "json" or "jsonb"
        self.jsonb.json_each_function: "json_each" or "jsonb_each"
        self._build_simple_where_clause: Method to build WHERE clauses
        self._reorder_pipeline_for_indexes: Method to reorder pipelines
        self._estimate_pipeline_cost: Method to estimate costs
        self._optimize_match_pushdown: Method to optimize match pushdown
        self._is_datetime_indexed_field: Method to check datetime indexes
        self._build_group_query: Method to build group queries
        self._apply_query: Method to apply queries to documents
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _build_simple_where_clause: Any
    _reorder_pipeline_for_indexes: Any
    _estimate_pipeline_cost: Any
    _optimize_match_pushdown: Any
    _is_datetime_indexed_field: Any
    _apply_query: Any

    def _process_group_stage(
        self,
        group_query: dict[str, Any],
        docs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Process the ``$group`` stage of an aggregation pipeline in Python.

        Groups documents by ``_id`` (literal ``None``, expression, or field
        path) while preserving the original unhashed ``_id`` value in output
        documents. Supports ``$count``, ``$sum``, ``$avg``, ``$min``,
        ``$max``, ``$push``, ``$addToSet``, ``$first``, ``$last``,
        ``$mergeObjects``, ``$stdDevPop``, ``$stdDevSamp``, ``$firstN``,
        ``$lastN``, ``$minN``, and ``$maxN``.

        Args:
            group_query: Dictionary representing the ``$group`` stage spec.
            docs: Input documents to group and aggregate.

        Returns:
            list[dict[str, Any]]: Grouped documents with evaluated
                accumulator fields.

        Raises:
            NotImplementedError: If the ``$accumulator`` operator is used.
        """

        grouped_docs: dict[Any, dict[str, Any]] = {}
        group_id_key = group_query.get("_id")

        # Create a copy of group_query without _id for processing accumulator operations
        accumulators = {k: v for k, v in group_query.items() if k != "_id"}

        # Create expression evaluator for evaluating expressions in accumulators
        evaluator = ExprEvaluator(
            data_column="data", db_connection=self.collection.db
        )

        for doc in docs:
            if group_id_key is None:
                group_id = None
            elif _is_expression(group_id_key):
                # Evaluate expression for group key
                group_id = evaluator._evaluate_expr_python(group_id_key, doc)
            else:
                group_id = self.collection._get_val(doc, group_id_key)

            key = _hashable_group_key(group_id)
            group = grouped_docs.get(key)
            if group is None:
                # Keep the original (possibly list/dict) value as _id (#103)
                group = {"_id": group_id}
                grouped_docs[key] = group

            for field, accumulator in accumulators.items():
                # Check if accumulator is a valid dictionary format
                if not isinstance(accumulator, dict) or len(accumulator) != 1:
                    # Invalid accumulator format, skip this field
                    continue

                op, key = next(iter(accumulator.items()))

                # Check for unsupported operators
                if op == "$accumulator":
                    raise NotImplementedError(
                        "The '$accumulator' operator is not supported in NeoSQLite. "
                        "Please use built-in accumulators ($sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last), "
                        "or post-process results in Python."
                    )

                if op == "$count":
                    group[field] = group.get(field, 0) + 1
                    continue

                # Handle expressions in accumulators
                if _is_expression(key):
                    # Evaluate expression for each document
                    value = evaluator._evaluate_expr_python(key, doc)
                # Handle literal values (e.g., $sum: 1 for counting)
                elif isinstance(key, (int, float, bool)) or key is None:
                    value = key
                elif isinstance(key, dict):
                    # Check if this is one of our new N-value operators
                    if op in {"$firstN", "$lastN", "$minN", "$maxN"}:
                        # These operators use dict format with "input" field
                        # Extract the input field and get its value
                        input_field = key.get("input", key.get("values", ""))
                        if input_field in ("$$ROOT", "$$CURRENT"):
                            value = deepcopy(doc)
                        elif isinstance(
                            input_field, str
                        ) and input_field.startswith("$$ROOT."):
                            value = self.collection._get_val(
                                doc, "$" + input_field[7:]
                            )
                        elif isinstance(
                            input_field, str
                        ) and input_field.startswith("$$CURRENT."):
                            value = self.collection._get_val(
                                doc, "$" + input_field[10:]
                            )
                        elif input_field:
                            value = self.collection._get_val(doc, input_field)
                        else:
                            value = None
                    else:
                        # Template dict or literal dict
                        resolved_dict = {}
                        for dk, dv in key.items():
                            if isinstance(dv, str) and dv in (
                                "$$ROOT",
                                "$$CURRENT",
                            ):
                                resolved_dict[dk] = deepcopy(doc)
                            elif isinstance(dv, str) and dv.startswith(
                                "$$ROOT."
                            ):
                                resolved_dict[dk] = self.collection._get_val(
                                    doc, "$" + dv[7:]
                                )
                            elif isinstance(dv, str) and dv.startswith(
                                "$$CURRENT."
                            ):
                                resolved_dict[dk] = self.collection._get_val(
                                    doc, "$" + dv[10:]
                                )
                            elif isinstance(dv, str) and dv.startswith("$"):
                                resolved_dict[dk] = self.collection._get_val(
                                    doc, dv
                                )
                            else:
                                resolved_dict[dk] = deepcopy(dv)
                        value = resolved_dict
                elif isinstance(key, list):
                    value = deepcopy(key)
                elif isinstance(key, str) and key in ("$$ROOT", "$$CURRENT"):
                    value = deepcopy(doc)
                elif isinstance(key, str) and key.startswith("$$ROOT."):
                    value = self.collection._get_val(doc, "$" + key[7:])
                elif isinstance(key, str) and key.startswith("$$CURRENT."):
                    value = self.collection._get_val(doc, "$" + key[10:])
                elif isinstance(key, str) and key.startswith("$"):
                    value = self.collection._get_val(doc, key)
                else:
                    # Remaining scalar values are literals (#94 parity:
                    # $push/$addToSet of a constant must push the constant,
                    # not look it up as a field path)
                    value = key

                match op:
                    case "$sum":
                        num_val = (
                            value
                            if isinstance(value, (int, float))
                            and not isinstance(value, bool)
                            else 0
                        )
                        group[field] = (group.get(field, 0) or 0) + num_val
                    case "$avg":
                        if field not in group:
                            group[field] = {
                                "__neosqlite_avg__": True,
                                "sum": 0,
                                "count": 0,
                            }
                        if isinstance(value, (int, float)) and not isinstance(
                            value, bool
                        ):
                            group[field]["sum"] += value
                            group[field]["count"] += 1
                    case "$min":
                        current = group.get(field, value)
                        if current is not None and value is not None:
                            group[field] = min(current, value)
                        elif value is not None:
                            group[field] = value
                        elif current is not None:
                            group[field] = current
                        else:
                            group[field] = None
                    case "$max":
                        current = group.get(field, value)
                        if current is not None and value is not None:
                            group[field] = max(current, value)
                        elif value is not None:
                            group[field] = value
                        elif current is not None:
                            group[field] = current
                        else:
                            group[field] = None
                    case "$push":
                        group.setdefault(field, []).append(value)
                    case "$addToSet":
                        bucket = group.setdefault(field, [])
                        seen_keys = group.setdefault(f"__keys_{field}", set())
                        k = _addtoset_key(value)
                        if k not in seen_keys:
                            seen_keys.add(k)
                            bucket.append(value)
                    case "$first":
                        # Only set the value if it hasn't been set yet (first document in group)
                        if field not in group:
                            group[field] = value
                    case "$last":
                        # Always update with the latest value (last document in group)
                        group[field] = value
                    case "$mergeObjects":
                        # Merge objects from all documents in the group
                        # Last value wins for conflicting fields
                        if field not in group:
                            group[field] = {}
                        if isinstance(value, dict):
                            group[field] |= value
                    case "$stdDevPop":
                        # Track sum, sum of squares, and count for population standard deviation
                        if field not in group:
                            group[field] = {
                                "sum": 0,
                                "sum_squares": 0,
                                "count": 0,
                                "type": "stdDevPop",
                            }
                        if isinstance(value, (int, float)) and not isinstance(
                            value, bool
                        ):
                            group[field]["sum"] += value
                            group[field]["sum_squares"] += value * value
                            group[field]["count"] += 1
                    case "$stdDevSamp":
                        # Track sum, sum of squares, and count for sample standard deviation
                        if field not in group:
                            group[field] = {
                                "sum": 0,
                                "sum_squares": 0,
                                "count": 0,
                                "type": "stdDevSamp",
                            }
                        if isinstance(value, (int, float)) and not isinstance(
                            value, bool
                        ):
                            group[field]["sum"] += value
                            group[field]["sum_squares"] += value * value
                            group[field]["count"] += 1
                    case "$firstN" | "$lastN" | "$minN" | "$maxN":
                        # Handle N-value operators
                        if not isinstance(key, dict) or "n" not in key:
                            continue

                        n_value = key["n"]

                        if field not in group:
                            group[field] = {
                                "type": op,
                                "n": n_value,
                                "values": [],
                            }

                        # Add value to the list
                        if value is not None:
                            group[field]["values"].append(value)

                            # Keep only the top N values based on operator type
                            if len(group[field]["values"]) > n_value:
                                if op == "$firstN":
                                    # Keep first N values (already in order)
                                    group[field]["values"] = group[field][
                                        "values"
                                    ][:n_value]
                                elif op == "$lastN":
                                    # Keep last N values
                                    group[field]["values"] = group[field][
                                        "values"
                                    ][-n_value:]
                                elif op == "$minN":
                                    # Keep N smallest values
                                    group[field]["values"] = sorted(
                                        group[field]["values"]
                                    )[:n_value]
                                elif op == "$maxN":
                                    # Keep N largest values
                                    group[field]["values"] = sorted(
                                        group[field]["values"], reverse=True
                                    )[:n_value]

        # Finalize $avg calculations
        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                # Skip if this is a std dev calculation (has "type" key)
                if isinstance(value, dict) and value.get("type") in {
                    "stdDevPop",
                    "stdDevSamp",
                }:
                    continue
                # Finalize tagged $avg accumulators without touching user dicts
                if (
                    isinstance(value, dict)
                    and value.get("__neosqlite_avg__") is True
                ):
                    if value["count"] > 0:
                        group[field] = value["sum"] / value["count"]
                    else:
                        group[field] = None

        # Finalize standard deviation calculations
        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                if isinstance(value, dict) and value.get("type") in {
                    "stdDevPop",
                    "stdDevSamp",
                }:
                    n = value["count"]
                    if n == 0 or (value["type"] == "stdDevSamp" and n < 2):
                        group[field] = None
                    else:
                        mean = value["sum"] / n
                        variance = (value["sum_squares"] / n) - (mean * mean)
                        if value["type"] == "stdDevSamp":
                            # Sample standard deviation uses Bessel's correction
                            variance = (
                                value["sum_squares"] - (value["sum"] ** 2) / n
                            ) / (n - 1)
                        if variance < 0:
                            # Handle floating point errors
                            variance = 0
                        group[field] = math.sqrt(variance)

        # Finalize N-value operators
        for group in grouped_docs.values():
            for field, value in group.items():
                if field == "_id":
                    continue
                if isinstance(value, dict) and value.get("type") in {
                    "$firstN",
                    "$lastN",
                    "$minN",
                    "$maxN",
                }:
                    if value["type"] == "$minN":
                        # Sort in ascending order and take first N values
                        sorted_values = sorted(value["values"])
                        group[field] = sorted_values[: value["n"]]
                    elif value["type"] == "$maxN":
                        # Sort in descending order and take first N values
                        sorted_values = sorted(value["values"], reverse=True)
                        group[field] = sorted_values[: value["n"]]
                    else:
                        # For firstN and lastN, values are already in correct order
                        group[field] = value["values"]

        # Strip internal $addToSet bookkeeping keys (#155)
        for group in grouped_docs.values():
            for k in [
                k
                for k in group
                if isinstance(k, str) and k.startswith("__keys_")
            ]:
                del group[k]
        return list(grouped_docs.values())

    def _run_subpipeline(
        self,
        sub_pipeline: list[dict[str, Any]],
        docs: list[dict[str, Any]],
        batch_size: int = 101,
    ) -> str:
        """Run a sub-pipeline (e.g. for ``$facet``) on a list of documents.

        Creates a temporary ``Collection`` and temporary result table,
        processes ``docs`` in batches via ``Collection.aggregate`` so each
        sub-pipeline uses normal tier optimization, and stores serialized
        results in the temporary SQLite table.

        Args:
            sub_pipeline: List of pipeline stage dictionaries to execute.
            docs: Input documents to process (unwrapping ``__doc__`` if set).
            batch_size: Maximum number of documents per processing batch.

        Returns:
            str: Name of the temporary SQLite table holding result rows.
        """
        # Create a temporary in-memory collection to run the sub-pipeline
        # This allows each sub-pipeline to use Tier-1/Tier-2 optimization
        from .. import Collection

        # Create temp collection for processing this batch
        temp_collection_name = f"_facet_batch_{uuid.uuid4().hex[:12]}"
        temp_collection = Collection(
            db=self.collection.db,
            name=temp_collection_name,
            create=True,
            database=self.collection._database,
        )

        # Create result temp table to store sub-pipeline results
        result_table = f"_facet_result_{uuid.uuid4().hex[:12]}"
        self.collection.db.execute(f"""
            CREATE TEMP TABLE {result_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT
            )
        """)

        try:
            all_docs = [
                (
                    deepcopy(doc["__doc__"])
                    if isinstance(doc, dict) and "__doc__" in doc
                    else deepcopy(doc)
                )
                for doc in docs
            ]
            if not all_docs:
                return result_table

            def _has_unique_ids(items: list[dict[str, Any]]) -> bool:
                """Check whether all items have unique non-null _id keys."""
                seen: set[str] = set()
                for item in items:
                    if (
                        not isinstance(item, dict)
                        or "_id" not in item
                        or item["_id"] is None
                    ):
                        return False
                    k = str(item["_id"])
                    if k in seen:
                        return False
                    seen.add(k)
                return True

            if not _has_unique_ids(all_docs):
                from ..query_engine.python_aggregation_engine import (
                    execute_python_aggregation,
                )

                result = execute_python_aggregation(
                    self.collection.query_engine,
                    sub_pipeline,
                    session=None,
                    initial_docs=all_docs,
                )
            else:
                try:
                    for batch in batched(all_docs, batch_size):
                        temp_collection.insert_many(deepcopy(list(batch)))
                    result = list(
                        temp_collection.aggregate(
                            sub_pipeline, batchSize=batch_size
                        )
                    )
                except sqlite3.IntegrityError:
                    from ..query_engine.python_aggregation_engine import (
                        execute_python_aggregation,
                    )

                    temp_collection.delete_many({})
                    result = execute_python_aggregation(
                        self.collection.query_engine,
                        sub_pipeline,
                        session=None,
                        initial_docs=all_docs,
                    )

            # Insert results into result temp table (#154: executemany)
            from neosqlite.collection.json_helpers import (
                neosqlite_json_dumps,
            )

            self.collection.db.executemany(
                f"INSERT INTO {result_table} (data) VALUES (?)",
                [(neosqlite_json_dumps(doc),) for doc in result],
            )

            return result_table

        finally:
            # Clean up temporary collection
            try:
                temp_collection.drop()
            except Exception as e:
                logger.debug(
                    f"Failed to drop temporary collection '{temp_collection.name}': {e}"
                )
                pass  # Ignore cleanup errors

    def _field_exists(self, doc: dict[str, Any], field_path: str) -> bool:
        """Check whether a field or dotted nested path exists in a document.

        Args:
            doc: Document dictionary to inspect.
            field_path: Field name or dot-separated path (e.g. ``a.b.c``).

        Returns:
            bool: True if ``doc`` is a dict and every path segment is present,
                False otherwise.
        """
        if not isinstance(doc, dict):
            return False
        curr: Any = doc
        for part in field_path.split("."):
            if not isinstance(curr, dict) or part not in curr:
                return False
            curr = curr[part]
        return True

    def _apply_projection(
        self,
        projection: dict[str, Any],
        document: dict[str, Any],
        root: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply a MongoDB-style ``$project`` specification to a document.

        Supports empty projections (returned unchanged), inclusion mode with
        computed expressions and variables (``$$ROOT``, ``$$CURRENT``,
        ``$$REMOVE``), simple inclusion mode, and exclusion mode (including
        dotted nested paths).

        Args:
            projection: Mapping specifying fields or expressions to project.
            document: Input document to project.
            root: Optional original root document used for ``$$ROOT``.

        Returns:
            dict[str, Any]: Projected document containing selected fields.
        """
        from ..expr_evaluator import (
            REMOVE_SENTINEL,
        )

        if not projection:
            return document

        doc = deepcopy(document)
        root_doc = root if root is not None else document
        projected_doc: dict[str, Any] = {}
        id_spec = projection.get("_id", 1)
        id_is_flag = isinstance(id_spec, (int, bool)) and id_spec in (0, 1)
        include_id = not (id_is_flag and id_spec in (0, False))
        has_computed_id = "_id" in projection and not id_is_flag

        def _assign(target: dict[str, Any], field_key: str, val: Any) -> None:
            """Assign a projected value to a top-level or dotted target field.

            Args:
                target: Destination document dictionary to mutate.
                field_key: Field name or dot-separated nested path.
                val: Projected value to store at ``field_key``.
            """
            if "." in field_key:
                self.collection._set_val(target, field_key, val)
            else:
                target[field_key] = val

        # Check if this is an inclusion projection with expressions, variables,
        # field references, or a computed/aliased _id specification
        has_expressions = has_computed_id or any(
            _is_expression(value)
            or (isinstance(value, str) and value.startswith("$"))
            for value in projection.values()
        )

        evaluator = (
            ExprEvaluator(data_column="data", db_connection=self.collection.db)
            if has_expressions
            else None
        )
        eval_doc = (
            {**document, "$$ROOT": root_doc, "$$CURRENT": document}
            if has_expressions
            else None
        )

        def _evaluate_proj_value(
            value: Any, dest: dict[str, Any], key: str
        ) -> None:
            if _is_expression(value):
                assert evaluator is not None and eval_doc is not None
                projected_value = evaluator._evaluate_expr_python(
                    value, eval_doc
                )
                if projected_value is not REMOVE_SENTINEL:
                    _assign(dest, key, projected_value)
            elif isinstance(value, str) and value.startswith("$"):
                if value == "$$ROOT":
                    _assign(dest, key, deepcopy(root_doc))
                elif value == "$$CURRENT":
                    _assign(dest, key, deepcopy(document))
                elif value == "$$REMOVE":
                    return
                elif value.startswith("$$ROOT."):
                    sub_path = value[len("$$ROOT.") :]
                    if self._field_exists(root_doc, sub_path):
                        _assign(
                            dest,
                            key,
                            deepcopy(
                                self.collection._get_val(root_doc, sub_path)
                            ),
                        )
                elif value.startswith("$$CURRENT."):
                    sub_path = value[len("$$CURRENT.") :]
                    if self._field_exists(document, sub_path):
                        _assign(
                            dest,
                            key,
                            deepcopy(
                                self.collection._get_val(document, sub_path)
                            ),
                        )
                elif value.startswith("$$"):
                    _assign(dest, key, None)
                else:
                    field_name = value[1:]
                    if self._field_exists(document, field_name):
                        _assign(
                            dest,
                            key,
                            self.collection._get_val(document, field_name),
                        )
            elif value == 1:
                if self._field_exists(document, key):
                    _assign(dest, key, self.collection._get_val(document, key))
            elif key == "_id" and has_computed_id:
                _assign(dest, key, value)

        is_exclusion_mode = not any(
            v == 1
            or _is_expression(v)
            or (isinstance(v, str) and v.startswith("$"))
            for k, v in projection.items()
            if k != "_id"
        ) and any(v == 0 for k, v in projection.items() if k != "_id")

        if not is_exclusion_mode and has_expressions:
            for key, value in projection.items():
                if key == "_id":
                    if has_computed_id:
                        _evaluate_proj_value(value, projected_doc, "_id")
                    elif include_id and "_id" in doc:
                        projected_doc["_id"] = doc["_id"]
                    continue
                _evaluate_proj_value(value, projected_doc, key)

            if not has_computed_id and include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]

            return projected_doc

        # Inclusion mode (no expressions)
        if not is_exclusion_mode and (
            any(v == 1 for k, v in projection.items() if k != "_id")
            or (list(projection.keys()) == ["_id"] and include_id)
        ):
            for key, value in projection.items():
                if key == "_id":
                    continue
                if value == 1 and self._field_exists(document, key):
                    _assign(
                        projected_doc,
                        key,
                        self.collection._get_val(document, key),
                    )
            if include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]
            return projected_doc

        # Exclusion mode
        for key, value in projection.items():
            if value == 0:
                if "." in key:
                    parts = key.split(".")
                    curr: Any = doc
                    for part in parts[:-1]:
                        if isinstance(curr, dict) and part in curr:
                            curr = curr[part]
                        else:
                            curr = None
                            break
                    if isinstance(curr, dict):
                        curr.pop(parts[-1], None)
                elif key in doc:
                    doc.pop(key, None)
        if has_computed_id:
            _evaluate_proj_value(id_spec, doc, "_id")
        elif not include_id and "_id" in doc:
            doc.pop("_id", None)
        return doc
