"""
Python fallback aggregation engine for NeoSQLite.

This module implements the Tier-3 (Python-based) aggregation pipeline
execution.  It is used only when the SQL-based tiers (Tier 1 and Tier 2)
cannot optimize a pipeline.

The logic was extracted from QueryEngine.aggregate_with_constraints()
to keep that class at a manageable size.
"""

from __future__ import annotations

import logging
import random
from copy import deepcopy
from datetime import timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any

from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..cursor import DESCENDING
from ..expr_evaluator import (
    ExprEvaluator,
    _is_expression,
)
from ..type_utils import _hashable_group_key, bson_sort_key

if TYPE_CHECKING:
    from . import QueryEngine

logger = logging.getLogger(__name__)


def _resolve_let_value(var_expr: str, let_vars: dict[str, Any]) -> Any:
    """Resolve ``$$var`` or dotted ``$$var.field`` against ``let_vars``.

    Args:
        var_expr: Variable expression starting with ``$$``.
        let_vars: Mapping of ``"$$name"`` to evaluated bound values.

    Returns:
        tuple[bool, Any]: ``(matched, value)`` where ``matched`` is True when
        the base variable exists in ``let_vars``.
    """
    if "." in var_expr:
        base_var, suffix = var_expr.split(".", 1)
        if base_var not in let_vars:
            return False, None
        val = let_vars[base_var]
        for part in suffix.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return True, None
        return True, val
    if var_expr in let_vars:
        return True, let_vars[var_expr]
    return False, None


def _substitute_let_vars(
    obj: Any,
    let_vars: dict[str, Any],
    shadowed: frozenset[str] = frozenset(),
) -> Any:
    """Substitute ``$lookup`` ``let`` variables into a sub-pipeline template.

    Preserves inner lexical bindings from ``$let``, ``$map``, ``$filter``, and
    ``$reduce`` when variable names shadow outer ``let`` variables.

    Args:
        obj: Pipeline stage, expression dict, list, or scalar value.
        let_vars: Mapping of ``"$$name"`` to resolved outer document values.
        shadowed: Set of ``"$$name"`` bindings currently shadowed in scope.

    Returns:
        Any: Rewritten copy with matched ``$$var`` expressions replaced by
        ``{"$literal": ...}``.
    """
    if isinstance(obj, str) and obj.startswith("$$"):
        base_var = obj.split(".", 1)[0]
        if base_var in shadowed:
            return obj
        matched, val = _resolve_let_value(obj, let_vars)
        if matched:
            return {"$literal": deepcopy(val)}
        return obj
    if isinstance(obj, dict):
        if "$let" in obj and isinstance(obj["$let"], dict):
            let_spec = obj["$let"]
            vars_spec = let_spec.get("vars", {})
            new_vars = (
                {
                    k: _substitute_let_vars(v, let_vars, shadowed)
                    for k, v in vars_spec.items()
                }
                if isinstance(vars_spec, dict)
                else vars_spec
            )
            new_shadowed = shadowed | (
                frozenset(f"$${k}" for k in vars_spec)
                if isinstance(vars_spec, dict)
                else frozenset()
            )
            new_let = dict(let_spec)
            new_let["vars"] = new_vars
            if "in" in new_let:
                new_let["in"] = _substitute_let_vars(
                    new_let["in"], let_vars, new_shadowed
                )
            res = {
                k: (
                    new_let
                    if k == "$let"
                    else _substitute_let_vars(v, let_vars, shadowed)
                )
                for k, v in obj.items()
            }
            return res
        if "$map" in obj and isinstance(obj["$map"], dict):
            ms = obj["$map"]
            as_name = ms.get("as", "this")
            new_ms = {
                k: _substitute_let_vars(
                    v,
                    let_vars,
                    shadowed | {f"$${as_name}"} if k == "in" else shadowed,
                )
                for k, v in ms.items()
            }
            return {
                k: (
                    new_ms
                    if k == "$map"
                    else _substitute_let_vars(v, let_vars, shadowed)
                )
                for k, v in obj.items()
            }
        if "$filter" in obj and isinstance(obj["$filter"], dict):
            fs = obj["$filter"]
            as_name = fs.get("as", "this")
            new_fs = {
                k: _substitute_let_vars(
                    v,
                    let_vars,
                    shadowed | {f"$${as_name}"} if k == "cond" else shadowed,
                )
                for k, v in fs.items()
            }
            return {
                k: (
                    new_fs
                    if k == "$filter"
                    else _substitute_let_vars(v, let_vars, shadowed)
                )
                for k, v in obj.items()
            }
        if "$reduce" in obj and isinstance(obj["$reduce"], dict):
            rs = obj["$reduce"]
            new_rs = {
                k: _substitute_let_vars(
                    v,
                    let_vars,
                    shadowed | {"$$value", "$$this"} if k == "in" else shadowed,
                )
                for k, v in rs.items()
            }
            return {
                k: (
                    new_rs
                    if k == "$reduce"
                    else _substitute_let_vars(v, let_vars, shadowed)
                )
                for k, v in obj.items()
            }
        return {
            k: _substitute_let_vars(v, let_vars, shadowed)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_substitute_let_vars(item, let_vars, shadowed) for item in obj]
    return obj


def _resolve_target_collection(
    query_engine: QueryEngine, coll_name: str, db_name: str | None = None
) -> Any:
    """Resolve a target Collection for ``$merge``/``$out`` within NeoSQLite.

    When ``db_name`` is provided and ``query_engine.collection._database`` has
    a distinct ``client`` object, resolves the named database on that client
    before indexing ``coll_name``; otherwise falls back to the current
    database.

    Args:
        query_engine: ``QueryEngine`` executing the aggregation stage.
        coll_name: Name of the target collection.
        db_name: Optional target database name. Defaults to None.

    Returns:
        Any: Target collection object resolved from the database or client.
    """
    db = query_engine.collection._database
    if db_name:
        client = getattr(db, "client", None)
        if client is not None and client is not db:
            db_obj = client[db_name]
            if not isinstance(db_obj, type(query_engine.collection)):
                return db_obj[coll_name]
    return db[coll_name]


def _bucket_eval_doc(
    doc: dict[str, Any], root_doc: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build evaluation dict with ``$$ROOT`` and ``$$CURRENT`` bindings."""
    eval_doc = dict(doc)
    eval_doc["$$CURRENT"] = doc
    eval_doc["$$ROOT"] = root_doc if root_doc is not None else doc
    return eval_doc


def _compute_bucket_output(
    query_engine: QueryEngine,
    bucket_id: Any,
    bucket_docs: list[dict[str, Any]],
    output_spec: dict[str, Any],
    evaluator: ExprEvaluator | None = None,
    doc_contexts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute output for a single ``$bucket`` or ``$bucketAuto`` bucket.

    Supports ``$sum``, ``$avg``, ``$count``, ``$min``, ``$max``, ``$first``,
    ``$last``, ``$push``, ``$addToSet``, ``$stdDevPop``, and ``$stdDevSamp``
    accumulators via ``ExprEvaluator._evaluate_operand_python`` (supporting
    both field paths and expression dicts), defaulting unknown specs to count.

    Args:
        query_engine: ``QueryEngine`` providing collection context.
        bucket_id: Identifier value assigned to the output ``_id`` field.
        bucket_docs: Documents grouped into this bucket.
        output_spec: Mapping of output field names to accumulator specs.
        evaluator: Optional shared ``ExprEvaluator`` instance.
        doc_contexts: Optional wrapped ``{"__doc__", "__root__"}`` contexts
            corresponding to ``bucket_docs``.

    Returns:
        dict[str, Any]: Output document containing ``_id`` and each computed
        accumulator field.
    """
    import math

    if evaluator is None:
        evaluator = ExprEvaluator()
    eval_docs = (
        [_bucket_eval_doc(dc["__doc__"], dc["__root__"]) for dc in doc_contexts]
        if doc_contexts is not None
        else [_bucket_eval_doc(d) for d in bucket_docs]
    )

    output_doc: dict[str, Any] = {"_id": bucket_id}
    for field_name, accumulator in output_spec.items():
        if not isinstance(accumulator, dict):
            output_doc[field_name] = len(bucket_docs)
            continue
        if "$sum" in accumulator:
            sum_expr = accumulator["$sum"]
            if sum_expr == 1 and not isinstance(sum_expr, bool):
                output_doc[field_name] = len(bucket_docs)
            else:
                total: int | float = 0
                for d in eval_docs:
                    val = evaluator._evaluate_operand_python(sum_expr, d)
                    if isinstance(val, (int, float)) and not isinstance(
                        val, bool
                    ):
                        total += val
                output_doc[field_name] = total
        elif "$avg" in accumulator:
            avg_expr = accumulator["$avg"]
            values = [
                v
                for d in eval_docs
                if isinstance(
                    (v := evaluator._evaluate_operand_python(avg_expr, d)),
                    (int, float),
                )
                and not isinstance(v, bool)
            ]
            output_doc[field_name] = (
                sum(values) / len(values) if values else None
            )
        elif "$count" in accumulator:
            output_doc[field_name] = len(bucket_docs)
        elif "$min" in accumulator:
            min_expr = accumulator["$min"]
            min_vals = [
                v
                for d in eval_docs
                if (v := evaluator._evaluate_operand_python(min_expr, d))
                is not None
            ]
            output_doc[field_name] = (
                min(min_vals, key=bson_sort_key) if min_vals else None
            )
        elif "$max" in accumulator:
            max_expr = accumulator["$max"]
            max_vals = [
                v
                for d in eval_docs
                if (v := evaluator._evaluate_operand_python(max_expr, d))
                is not None
            ]
            output_doc[field_name] = (
                max(max_vals, key=bson_sort_key) if max_vals else None
            )
        elif "$first" in accumulator:
            first_expr = accumulator["$first"]
            output_doc[field_name] = (
                evaluator._evaluate_operand_python(first_expr, eval_docs[0])
                if eval_docs
                else None
            )
        elif "$last" in accumulator:
            last_expr = accumulator["$last"]
            output_doc[field_name] = (
                evaluator._evaluate_operand_python(last_expr, eval_docs[-1])
                if eval_docs
                else None
            )
        elif "$push" in accumulator:
            push_expr = accumulator["$push"]
            output_doc[field_name] = [
                v
                for d in eval_docs
                if (v := evaluator._evaluate_operand_python(push_expr, d))
                is not None
            ]
        elif "$addToSet" in accumulator:
            set_expr = accumulator["$addToSet"]
            unique_values: list[Any] = []
            seen_keys: set[tuple] = set()
            for d in eval_docs:
                v = evaluator._evaluate_operand_python(set_expr, d)
                if v is not None:
                    k = _hashable_group_key(v)
                    if k not in seen_keys:
                        seen_keys.add(k)
                        unique_values.append(v)
            output_doc[field_name] = unique_values
        elif "$stdDevPop" in accumulator or "$stdDevSamp" in accumulator:
            is_samp = "$stdDevSamp" in accumulator
            sd_expr = accumulator["$stdDevSamp" if is_samp else "$stdDevPop"]
            nums = [
                v
                for d in eval_docs
                if isinstance(
                    (v := evaluator._evaluate_operand_python(sd_expr, d)),
                    (int, float),
                )
                and not isinstance(v, bool)
            ]
            n = len(nums)
            if n == 0 or (is_samp and n < 2):
                output_doc[field_name] = None
            else:
                mean = sum(nums) / n
                denom = n - 1 if is_samp else n
                var = sum((x - mean) ** 2 for x in nums) / denom
                output_doc[field_name] = math.sqrt(max(0.0, var))
        else:
            output_doc[field_name] = len(bucket_docs)
    return output_doc


def execute_python_aggregation(
    query_engine: QueryEngine,
    pipeline: list[dict[str, Any]],
    session: Any = None,
    initial_docs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Execute an aggregation pipeline entirely in Python (Tier-3 fallback).

    Wraps each input document with ``__doc__`` and a deep-copied ``__root__``
    context for ``$$ROOT`` variable resolution, applies each pipeline stage
    sequentially (raising ``MalformedQueryException`` on empty or unsupported
    stages), and strips internal ``_textScore`` metadata if not originally
    present in ``__root__``.

    Args:
        query_engine: The ``QueryEngine`` instance that owns this pipeline.
        pipeline: List of aggregation stage dictionaries to execute in order.
        session: Optional client session forwarded when querying collections.
            Defaults to None.
        initial_docs: Optional pre-loaded documents (e.g., for nested
            ``$facet`` sub-pipelines running purely in Python). Defaults to
            None.

    Returns:
        list[dict[str, Any]]: Resulting documents after applying all pipeline
        stages.
    """
    docs: list[dict[str, Any]] = (
        initial_docs
        if initial_docs is not None
        else list(query_engine.find(session=session))
    )

    # Store original documents for $$ROOT variable support
    # Each document is wrapped with metadata for variable scoping
    docs_with_context = [
        {"__doc__": doc, "__root__": deepcopy(doc)} for doc in docs
    ]

    for stage_index, stage in enumerate(pipeline):
        if not stage:
            raise MalformedQueryException("Empty pipeline stage")
        stage_name = next(iter(stage.keys())).strip()
        if stage_name == "$indexStats" and stage_index != 0:
            raise MalformedQueryException(
                "$indexStats is only valid as the first stage in a pipeline"
            )
        match stage_name:
            case "$indexStats":
                from ..index_stats import build_index_stats_documents

                stats_docs = build_index_stats_documents(
                    query_engine.collection
                )
                docs_with_context = [
                    {"__doc__": doc, "__root__": deepcopy(doc)}
                    for doc in stats_docs
                ]
            case "$match":
                query = stage["$match"]
                match_context: dict[str, Any] = {}
                docs_with_context = [
                    dc
                    for dc in docs_with_context
                    if query_engine.helpers._apply_query(
                        query, dc["__doc__"], _context=match_context
                    )
                ]
            case "$sort":
                sort_spec = stage["$sort"]
                for key, direction in reversed(list(sort_spec.items())):
                    is_text_score = direction == {"$meta": "textScore"} or (
                        isinstance(direction, dict)
                        and direction.get("$meta") == "textScore"
                    )
                    if is_text_score:

                        def make_text_score_key(k=key):
                            """Build a text-score sort key function.

                            Args:
                                k: Sort field name used as a fallback key when
                                    ``_textScore`` is absent.

                            Returns:
                                Callable[[dict[str, Any]], float]: Key
                                extractor returning the float text score for a
                                document context.
                            """

                            def score_sort_key(dc: dict[str, Any]) -> Any:
                                """Extract float text score from doc.

                                Args:
                                    dc: Document context wrapping
                                        ``__doc__`` and ``__root__``.

                                Returns:
                                    float: Text search score, defaulting to
                                    ``0.0``.
                                """
                                doc = dc["__doc__"]
                                return float(
                                    doc.get("_textScore", doc.get(k, 0.0))
                                )

                            return score_sort_key

                        docs_with_context.sort(
                            key=make_text_score_key(),
                            reverse=True,
                        )
                        continue

                    def make_sort_key(key, dir):
                        """Create a sort key function for the given field.

                        Args:
                            key: Dot-separated field name to extract from each
                                document.
                            dir: Sort direction specification (kept for
                                signature compatibility; ordering is applied by
                                the caller).

                        Returns:
                            Callable[[dict[str, Any]], Any]: Key function
                            returning a BSON-comparable sort key.
                        """

                        def sort_key(dc):
                            """Extract BSON-ordered sort key from doc context.

                            Args:
                                dc: Document context dict containing
                                    ``__doc__``.

                            Returns:
                                Any: BSON sort key produced by
                                ``bson_sort_key()``.
                            """
                            # BSON-ordered key: missing/null sorts first
                            # ascending (last when reversed), mixed types
                            # never raise (#102).
                            val = query_engine.collection._get_val(
                                dc["__doc__"], key
                            )
                            return bson_sort_key(val)

                        return sort_key

                    sort_key_func = make_sort_key(key, direction)
                    docs_with_context.sort(
                        key=sort_key_func,
                        reverse=direction == DESCENDING,
                    )
            case "$skip":
                count = stage["$skip"]
                docs_with_context = docs_with_context[count:]
            case "$limit":
                count = stage["$limit"]
                if (
                    not isinstance(count, int)
                    or isinstance(count, bool)
                    or count < 0
                ):
                    raise ValueError(
                        f"The $limit stage requires a non-negative integer, got {count}"
                    )
                docs_with_context = docs_with_context[:count]
            case "$project":
                projection = stage["$project"]
                docs_with_context = [
                    {
                        "__doc__": query_engine.helpers._apply_projection(
                            projection, dc["__doc__"], dc["__root__"]
                        ),
                        "__root__": dc["__root__"],
                    }
                    for dc in docs_with_context
                ]
            case "$replaceRoot" | "$replaceWith":
                # Handle $replaceRoot and $replaceWith
                if stage_name == "$replaceRoot":
                    new_root_expr = stage["$replaceRoot"].get("newRoot")
                else:  # $replaceWith
                    new_root_expr = stage["$replaceWith"]

                evaluator = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                new_docs_with_context = []
                for dc in docs_with_context:
                    eval_doc = {
                        **dc["__doc__"],
                        "$$ROOT": dc["__root__"],
                        "$$CURRENT": dc["__doc__"],
                    }
                    # Use _evaluate_operand_python to get the actual value (not forced to bool)
                    try:
                        new_doc = evaluator._evaluate_operand_python(
                            new_root_expr, eval_doc
                        )
                    except Exception as e:
                        # Fallback if evaluation fails
                        logger.debug(f"$replaceRoot evaluation failed: {e}")
                        new_doc = dc["__doc__"]

                    # MongoDB requirement: result MUST be an object
                    if not isinstance(new_doc, dict):
                        raise MalformedQueryException(
                            f"$replaceRoot requires an object, got {type(new_doc).__name__}"
                        )

                    new_doc = deepcopy(new_doc)
                    # Ensure _id is preserved if it was present in the original
                    if (
                        "_id" not in new_doc
                        and "__doc__" in dc
                        and "_id" in dc["__doc__"]
                    ):
                        new_doc["_id"] = dc["__doc__"]["_id"]

                    new_docs_with_context.append(
                        {"__doc__": new_doc, "__root__": dc["__root__"]}
                    )
                docs_with_context = new_docs_with_context
            case "$unset":
                # Handle $unset aggregation stage
                unset_spec = stage["$unset"]
                if isinstance(unset_spec, str):
                    fields_to_unset = [unset_spec]
                elif isinstance(unset_spec, list):
                    fields_to_unset = unset_spec
                else:
                    raise MalformedQueryException(
                        "$unset requires a string or a list of strings"
                    )

                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    for field in fields_to_unset:
                        # Use collection._get_val logic to navigate and pop
                        if "." in field:
                            parts = field.split(".")
                            target: dict[str, Any] | None = doc
                            for part in parts[:-1]:
                                if isinstance(target, dict) and part in target:
                                    target = target[part]
                                else:
                                    target = None
                                    break
                            if isinstance(target, dict):
                                target.pop(parts[-1], None)
                        else:
                            doc.pop(field, None)
            case "$group":
                group_spec = stage["$group"]
                # For $group, we don't preserve __root__ since grouping creates new documents
                grouped_docs = query_engine.helpers._process_group_stage(
                    group_spec, [dc["__doc__"] for dc in docs_with_context]
                )
                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in grouped_docs
                ]
            case "$unwind":
                # Handle both string and object forms of $unwind
                unwind_spec = stage["$unwind"]
                if isinstance(unwind_spec, str):
                    # Legacy string form
                    field_path = unwind_spec.lstrip("$")
                    include_array_index = None
                    preserve_null_and_empty = False
                elif isinstance(unwind_spec, dict):
                    # New object form with advanced options
                    field_path = unwind_spec["path"].lstrip("$")
                    include_array_index = unwind_spec.get("includeArrayIndex")
                    preserve_null_and_empty = unwind_spec.get(
                        "preserveNullAndEmptyArrays", False
                    )
                else:
                    raise MalformedQueryException(
                        f"Invalid $unwind specification: {unwind_spec}"
                    )

                unwound_docs_with_context = []
                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    root = dc["__root__"]
                    array_to_unwind = query_engine.collection._get_val(
                        doc, field_path
                    )

                    # For nested fields, check if parent exists when not preserving
                    field_parts = field_path.split(".")
                    process_document = True
                    if len(field_parts) > 1 and not preserve_null_and_empty:
                        # This is a nested field
                        parent_path = ".".join(field_parts[:-1])
                        parent_value = query_engine.collection._get_val(
                            doc, parent_path
                        )
                        if parent_value is None:
                            # Parent is None or missing, don't process this document
                            process_document = False

                    if not process_document:
                        continue

                    if isinstance(array_to_unwind, list):
                        # Handle array values
                        if array_to_unwind:
                            # Non-empty array - unwind normally
                            for idx, item in enumerate(array_to_unwind):
                                new_doc = deepcopy(doc)
                                query_engine.collection._set_val(
                                    new_doc, field_path, item
                                )
                                # Add array index if requested
                                if include_array_index:
                                    new_doc[include_array_index] = idx
                                # Preserve __root__ for $$ROOT variable
                                unwound_docs_with_context.append(
                                    {"__doc__": new_doc, "__root__": root}
                                )
                        elif preserve_null_and_empty:
                            # Empty array but preserve is requested
                            new_doc = deepcopy(doc)
                            # Remove the field entirely (MongoDB behavior)
                            # For nested fields, we need to navigate to parent
                            if "." in field_path:
                                # Handle nested field removal
                                parts = field_path.split(".")
                                current = new_doc
                                for part in parts[:-1]:
                                    if part in current:
                                        current = current[part]
                                    else:
                                        break
                                else:
                                    # Remove the final field
                                    if parts[-1] in current:
                                        del current[parts[-1]]
                            else:
                                # Simple field removal
                                if field_path in new_doc:
                                    del new_doc[field_path]
                            # Add array index if requested
                            if include_array_index:
                                new_doc[include_array_index] = None
                            unwound_docs_with_context.append(
                                {"__doc__": new_doc, "__root__": root}
                            )
                        # If empty array and preserve is False, don't add any documents
                    elif array_to_unwind is None and preserve_null_and_empty:
                        # Null/missing field with preserve requested: emit
                        # the document without the field (MongoDB behavior)
                        new_doc = deepcopy(doc)
                        if include_array_index:
                            new_doc[include_array_index] = None
                        unwound_docs_with_context.append(
                            {"__doc__": new_doc, "__root__": root}
                        )
                    elif array_to_unwind is not None and not isinstance(
                        array_to_unwind, list
                    ):
                        # MongoDB: non-null scalar fields unwind as a single
                        # element regardless of preserveNullAndEmptyArrays;
                        # nested paths resolve via _get_val above (#96).
                        new_doc = deepcopy(doc)
                        # Keep the value as-is
                        # Add array index if requested
                        if include_array_index:
                            new_doc[include_array_index] = 0
                        unwound_docs_with_context.append(
                            {"__doc__": new_doc, "__root__": root}
                        )
                    # Remaining case: missing/null without preserve — dropped.
                docs_with_context = unwound_docs_with_context
            case "$lookup":
                # Python fallback implementation for $lookup (including let & pipeline)
                lookup_spec = stage["$lookup"]
                from_collection_name = lookup_spec["from"]
                local_field = lookup_spec.get("localField")
                foreign_field = lookup_spec.get("foreignField")
                as_field = lookup_spec["as"]
                let_spec = lookup_spec.get("let")
                sub_pipeline = lookup_spec.get("pipeline")

                # Get the from collection from the database
                from_collection = query_engine.collection._database[
                    from_collection_name
                ]

                from ..type_utils import _hashable_group_key

                # Element-wise local values (arrays expand like MongoDB)
                def _local_candidates(doc: dict[str, Any]) -> list[Any]:
                    """Collect join candidate values for ``local_field``.

                    Walks ``local_field`` across nested dicts and arrays,
                    flattening non-None array elements per MongoDB
                    ``$lookup`` semantics.

                    Args:
                        doc: Input document from the local pipeline.

                    Returns:
                        list[Any]: Non-None scalar or object candidate values
                        resolved at ``local_field``, or an empty list if the
                        path does not exist.
                    """
                    assert local_field is not None
                    parts = local_field.split(".")
                    current: list[Any] = [doc]
                    for part in parts:
                        nxt: list[Any] = []
                        found = False
                        for node in current:
                            if isinstance(node, dict) and part in node:
                                found = True
                                v = node[part]
                                if isinstance(v, list):
                                    nxt.extend(v)
                                elif v is not None:
                                    nxt.append(v)
                            elif isinstance(node, list):
                                for el in node:
                                    if isinstance(el, dict) and part in el:
                                        found = True
                                        ev = el[part]
                                        if isinstance(ev, list):
                                            nxt.extend(ev)
                                        elif ev is not None:
                                            nxt.append(ev)
                        if not found:
                            return []
                        current = nxt
                    return current

                if local_field is not None and foreign_field is not None:
                    foreign_index: dict[Any, list[dict[str, Any]]] = {}
                    for match_doc in from_collection.find(session=session):
                        foreign_value = from_collection._get_val(
                            match_doc, foreign_field
                        )
                        key = _hashable_group_key(foreign_value)
                        foreign_index.setdefault(key, []).append(
                            match_doc.copy()
                        )
                    all_foreign_docs: list[dict[str, Any]] | None = None
                elif sub_pipeline is not None:
                    foreign_index = {}
                    all_foreign_docs = list(
                        from_collection.find(session=session)
                    )
                else:
                    raise MalformedQueryException(
                        "$lookup requires localField/foreignField or pipeline"
                    )

                lookup_eval = (
                    ExprEvaluator(
                        data_column="data",
                        db_connection=query_engine.collection.db,
                    )
                    if let_spec
                    else None
                )

                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    if local_field is not None and foreign_field is not None:
                        matching_docs: list[dict[str, Any]] = []
                        for lv in _local_candidates(doc):
                            key = _hashable_group_key(lv)
                            matching_docs.extend(
                                deepcopy(matched)
                                for matched in foreign_index.get(key, [])
                            )
                    else:
                        matching_docs = deepcopy(all_foreign_docs or [])

                    if sub_pipeline is not None:
                        eff_pipeline = sub_pipeline
                        if let_spec and lookup_eval is not None:
                            eval_ctx = _bucket_eval_doc(doc, dc["__root__"])
                            let_vars = {
                                f"$${var_name}": lookup_eval._evaluate_operand_python(
                                    var_expr, eval_ctx
                                )
                                for var_name, var_expr in let_spec.items()
                            }
                            eff_pipeline = _substitute_let_vars(
                                sub_pipeline, let_vars
                            )
                        if matching_docs and eff_pipeline:
                            matching_docs = execute_python_aggregation(
                                query_engine,
                                eff_pipeline,
                                session=session,
                                initial_docs=matching_docs,
                            )

                    doc[as_field] = matching_docs
            case "$addFields":
                add_fields_spec = stage["$addFields"]
                # Create expression evaluator for this stage
                evaluator_add = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )

                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    root = dc["__root__"]

                    eval_doc = {
                        **doc,
                        "$$ROOT": root,
                        "$$CURRENT": doc,
                    }
                    for new_field, expr in add_fields_spec.items():
                        if _is_expression(expr):
                            # Full expression - evaluate in Python with current context
                            value = evaluator_add._evaluate_expr_python(
                                expr, eval_doc
                            )
                            query_engine.collection._set_val(
                                doc, new_field, value
                            )
                        elif isinstance(expr, str) and expr.startswith("$"):
                            # Field reference
                            if expr.startswith("$$"):
                                value = evaluator_add._evaluate_operand_python(
                                    expr, eval_doc
                                )
                                if isinstance(value, (dict, list)):
                                    value = deepcopy(value)
                                query_engine.collection._set_val(
                                    doc, new_field, value
                                )
                            else:
                                # Regular field reference - may reference newly added field
                                source_field_name = expr[1:]
                                source_value = query_engine.collection._get_val(
                                    doc, source_field_name
                                )
                                query_engine.collection._set_val(
                                    doc, new_field, source_value
                                )
                        else:
                            # Literal value
                            query_engine.collection._set_val(
                                doc, new_field, expr
                            )
            case "$setWindowFields":
                from ..query_helper.window_operators import (
                    process_set_window_fields,
                )

                window_spec = stage["$setWindowFields"]
                evaluator_window = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_set_window_fields(
                    docs_with_context,
                    window_spec,
                    query_engine.collection,
                    evaluator_window,
                )
            case "$graphLookup":
                from ..query_helper.graph_lookup import process_graph_lookup

                graph_spec = stage["$graphLookup"]
                evaluator_graph = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_graph_lookup(
                    docs_with_context,
                    graph_spec,
                    query_engine.collection,
                    evaluator_graph,
                )
            case "$fill":
                from ..query_helper.fill_stage import process_fill

                fill_spec = stage["$fill"]
                evaluator_fill = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                docs_with_context = process_fill(
                    docs_with_context,
                    fill_spec,
                    query_engine.collection,
                    evaluator_fill,
                )
            case "$sample":
                sample_spec = stage["$sample"]
                sample_size = sample_spec["size"]
                if (
                    not isinstance(sample_size, int)
                    or isinstance(sample_size, bool)
                    or sample_size < 0
                ):
                    raise MalformedQueryException(
                        "$sample size must be a non-negative integer"
                    )
                docs_with_context = random.sample(
                    docs_with_context,
                    min(sample_size, len(docs_with_context)),
                )
            case "$facet":
                from ..query_helper.utils import get_force_fallback

                facet_spec = stage["$facet"]

                # Get input documents
                sub_docs = [dc["__doc__"] for dc in docs_with_context]

                facet_results: dict[str, Any] = {}
                if get_force_fallback():
                    for facet_name, sub_pipeline in facet_spec.items():
                        facet_results[facet_name] = (
                            execute_python_aggregation(
                                query_engine,
                                sub_pipeline,
                                session=session,
                                initial_docs=deepcopy(sub_docs),
                            )
                            if sub_docs
                            else []
                        )
                else:
                    facet_tables: dict[str, str] = {}

                    # Run each sub-pipeline, streaming results to temp tables
                    for facet_name, sub_pipeline in facet_spec.items():
                        result_table = query_engine.helpers._run_subpipeline(
                            sub_pipeline, sub_docs
                        )
                        facet_tables[facet_name] = result_table

                    # Load all results from temp tables and combine
                    for facet_name, table_name in facet_tables.items():
                        cursor = query_engine.collection.db.execute(
                            f"SELECT data FROM {table_name}"
                        )
                        from neosqlite.collection.json_helpers import (
                            neosqlite_json_loads,
                        )

                        facet_results[facet_name] = [
                            neosqlite_json_loads(row[0])
                            for row in cursor.fetchall()
                        ]
                        # Clean up temp table after loading
                        try:
                            query_engine.collection.db.execute(
                                f"DROP TABLE IF EXISTS {quote_table_name(table_name)}"
                            )
                        except Exception as e:
                            logger.debug(
                                f"Failed to drop facet temporary table '{table_name}': {e}"
                            )
                            pass

                docs_with_context = [
                    {"__doc__": facet_results, "__root__": facet_results}
                ]
            case "$count":
                count_field = stage["$count"]
                docs_with_context = [
                    {
                        "__doc__": {count_field: len(docs_with_context)},
                        "__root__": {count_field: len(docs_with_context)},
                    }
                ]
            case "$bucket":
                bucket_spec = stage["$bucket"]
                group_by = bucket_spec.get("groupBy")
                boundaries = bucket_spec.get("boundaries", [])
                default_label = bucket_spec.get("default", "Other")
                output_spec = bucket_spec.get("output", {"count": {"$sum": 1}})

                if group_by is None or group_by == "" or not boundaries:
                    docs_with_context = []
                    break

                sorted_boundaries = sorted(boundaries)
                bucket_eval = ExprEvaluator()

                # Group documents by bucket using lower boundary as _id
                buckets: dict[Any, list[dict[str, Any]]] = {}
                for dc in docs_with_context:
                    eval_doc = _bucket_eval_doc(dc["__doc__"], dc["__root__"])
                    val = bucket_eval._evaluate_operand_python(
                        group_by, eval_doc
                    )

                    bucket_key: Any = default_label
                    if val is not None:
                        try:
                            for lower, upper in pairwise(sorted_boundaries):
                                if isinstance(val, bool) != isinstance(
                                    lower, bool
                                ):
                                    continue
                                if lower <= val < upper:
                                    bucket_key = lower
                                    break
                        except TypeError:
                            bucket_key = default_label

                    if bucket_key not in buckets:
                        buckets[bucket_key] = []
                    buckets[bucket_key].append(dc)

                def _bucket_sort_key(item: tuple[Any, Any]) -> tuple[int, Any]:
                    """Compute a BSON-ordered sort key for bucket entries."""
                    key = item[0]
                    if key is None:
                        return (0, "")
                    if isinstance(key, bool):
                        return (3, key)
                    if isinstance(key, (int, float)):
                        return (1, key)
                    if isinstance(key, str):
                        return (2, key)
                    return (4, str(key))

                new_docs = [
                    _compute_bucket_output(
                        query_engine,
                        bucket_id,
                        [dc["__doc__"] for dc in bucket_dcs],
                        output_spec,
                        evaluator=bucket_eval,
                        doc_contexts=bucket_dcs,
                    )
                    for bucket_id, bucket_dcs in sorted(
                        buckets.items(), key=_bucket_sort_key
                    )
                ]

                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in new_docs
                ]
            case "$bucketAuto":
                bucket_auto_spec = stage["$bucketAuto"]
                group_by = bucket_auto_spec.get("groupBy")
                num_buckets = bucket_auto_spec.get("buckets", 10)
                output_spec = bucket_auto_spec.get(
                    "output", {"count": {"$sum": 1}}
                )

                if not isinstance(num_buckets, int) or isinstance(
                    num_buckets, bool
                ):
                    raise MalformedQueryException(
                        "$bucketAuto buckets must be an integer"
                    )

                if group_by is None or group_by == "" or num_buckets <= 0:
                    docs_with_context = []
                    break

                bucket_eval = ExprEvaluator()
                evaluated_items: list[tuple[dict[str, Any], Any, tuple]] = []
                for dc in docs_with_context:
                    eval_doc = _bucket_eval_doc(dc["__doc__"], dc["__root__"])
                    val = bucket_eval._evaluate_operand_python(
                        group_by, eval_doc
                    )
                    evaluated_items.append((dc, val, bson_sort_key(val)))

                def _bucket_auto_sort_key(
                    item: tuple[dict[str, Any], Any, tuple],
                ) -> tuple:
                    """Sort evaluated $bucketAuto docs using BSON sort key."""
                    return item[2]

                evaluated_items.sort(key=_bucket_auto_sort_key)

                # Group tied sort keys so equal groupBy values share a bucket
                value_groups: list[tuple[Any, tuple, list[dict[str, Any]]]] = []
                for dc, val, sort_k in evaluated_items:
                    if not value_groups or value_groups[-1][1] != sort_k:
                        value_groups.append((val, sort_k, [dc]))
                    else:
                        value_groups[-1][2].append(dc)

                num_groups = len(value_groups)
                auto_buckets: dict[
                    int, list[tuple[Any, list[dict[str, Any]]]]
                ] = {}
                for g_idx, (val, _sort_k, group_dcs) in enumerate(value_groups):
                    b_idx = (g_idx * num_buckets) // num_groups
                    auto_buckets.setdefault(b_idx, []).append((val, group_dcs))

                new_docs = []
                for b_idx in sorted(auto_buckets.keys()):
                    grouped = auto_buckets[b_idx]
                    bmin = grouped[0][0]
                    bmax = grouped[-1][0]
                    bucket_dcs = [dc for _v, g_dcs in grouped for dc in g_dcs]
                    new_docs.append(
                        _compute_bucket_output(
                            query_engine,
                            {"min": bmin, "max": bmax},
                            [dc["__doc__"] for dc in bucket_dcs],
                            output_spec,
                            evaluator=bucket_eval,
                            doc_contexts=bucket_dcs,
                        )
                    )

                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in new_docs
                ]
            case "$unionWith":
                union_spec = stage["$unionWith"]
                coll_name: str | None
                if isinstance(union_spec, str):
                    coll_name = union_spec
                    pipeline = []
                elif isinstance(union_spec, dict):
                    coll_name = union_spec.get("coll")
                    pipeline = union_spec.get("pipeline", [])
                else:
                    raise MalformedQueryException(
                        f"$unionWith requires string or dict, got {type(union_spec).__name__}"
                    )

                if not isinstance(coll_name, str) or not coll_name:
                    break

                # Get documents from the other collection. When a pipeline
                # is present it fully determines the unioned side — fetching
                # the whole collection first was pure waste (#151).
                other_coll = query_engine.collection._database[coll_name]
                if pipeline:
                    other_docs = list(other_coll.aggregate(pipeline))
                else:
                    other_docs = list(other_coll.find())

                # Combine documents
                current_docs = [dc["__doc__"] for dc in docs_with_context]
                combined_docs = current_docs + other_docs

                docs_with_context = [
                    {"__doc__": doc, "__root__": doc} for doc in combined_docs
                ]
            case "$merge":
                # $merge writes results to a collection
                merge_spec = stage["$merge"]

                # Handle different merge spec formats
                if isinstance(merge_spec, str):
                    # Simple format: just collection name
                    target_coll_name = merge_spec
                    merge_options = {}
                elif isinstance(merge_spec, dict):
                    # Full format with options
                    into = merge_spec.get("into", "")
                    if isinstance(into, dict):
                        target_coll_name = (
                            into.get("db", "") + "." + into.get("coll", "")
                        )
                    else:
                        target_coll_name = into
                    merge_options = {
                        "on": merge_spec.get("on", "_id"),
                        "whenMatched": merge_spec.get("whenMatched", "replace"),
                        "whenNotMatched": merge_spec.get(
                            "whenNotMatched", "insert"
                        ),
                    }
                else:
                    target_coll_name = "merged"
                    merge_options = {}

                # Get or create target collection
                if "." in target_coll_name:
                    db_name, coll_name = target_coll_name.split(".", 1)
                    target_coll = _resolve_target_collection(
                        query_engine, coll_name, db_name
                    )
                else:
                    target_coll = _resolve_target_collection(
                        query_engine, target_coll_name
                    )

                # Process each document
                for dc in docs_with_context:
                    doc = dc["__doc__"]

                    # Get the "on" field value for matching
                    on_field = merge_options.get("on", "_id")
                    on_value = query_engine.collection._get_val(doc, on_field)

                    # Try to find existing document
                    existing = None
                    if on_value is not None:
                        existing = target_coll.find_one({on_field: on_value})

                    when_matched = merge_options.get("whenMatched", "replace")
                    when_not_matched = merge_options.get(
                        "whenNotMatched", "insert"
                    )

                    if existing:
                        match when_matched:
                            case "replace":
                                update_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                if update_doc:
                                    target_coll.update_one(
                                        {on_field: on_value},
                                        {"$set": update_doc},
                                    )
                                fields_to_remove = [
                                    k
                                    for k in existing
                                    if k not in update_doc
                                    and k not in (on_field, "_id")
                                ]
                                if fields_to_remove:
                                    target_coll.update_one(
                                        {on_field: on_value},
                                        {
                                            "$unset": {
                                                f: "" for f in fields_to_remove
                                            }
                                        },
                                    )
                            case "merge":
                                update_doc = {
                                    k: v for k, v in doc.items() if k != "_id"
                                }
                                target_coll.update_one(
                                    {on_field: on_value},
                                    {"$set": update_doc},
                                )
                            case "keepExisting":
                                # Keep existing, don't update
                                pass
                            case "fail":
                                raise Exception(
                                    f"$merge failed: document with {on_field}={on_value} already exists"
                                )
                            # Note: "pipeline" mode not implemented
                    else:
                        # Document doesn't exist - handle based on whenNotMatched
                        match when_not_matched:
                            case "insert":
                                target_coll.insert_one(doc)
                            case "fail":
                                raise Exception(
                                    f"$merge failed: no document found with {on_field}={on_value}"
                                )

                # After merge, return empty or pass through based on requirements
                # MongoDB returns the merged documents for further pipeline processing
                pass

            case "$out":
                out_spec = stage["$out"]
                if isinstance(out_spec, str):
                    target_coll = _resolve_target_collection(
                        query_engine, out_spec
                    )
                elif isinstance(out_spec, dict):
                    out_coll_name = out_spec.get("coll")
                    out_db_name = out_spec.get("db")
                    if not isinstance(out_coll_name, str) or not out_coll_name:
                        raise MalformedQueryException(
                            "$out requires non-empty collection name"
                        )
                    target_coll = _resolve_target_collection(
                        query_engine, out_coll_name, out_db_name
                    )
                else:
                    raise MalformedQueryException(
                        "$out requires string or dict specification"
                    )

                if hasattr(target_coll, "_initialize_table"):
                    target_coll._initialize_table()
                out_docs = [dc["__doc__"] for dc in docs_with_context]
                target_coll.delete_many({})
                if out_docs:
                    target_coll.insert_many(out_docs)
                docs_with_context = []

            case "$redact":
                # $redact filters document content based on conditions
                redact_spec = stage["$redact"]

                # Create evaluator for condition evaluation
                evaluator_redact = ExprEvaluator(
                    data_column="data", db_connection=query_engine.collection.db
                )
                _pruned_sentinel = object()

                def evaluate_redact_condition(spec, doc, root):
                    """Evaluate the redact condition for a document level.

                    Binds ``$$ROOT``, ``$$CURRENT``, ``$$KEEP``, ``$$DESCEND``,
                    and ``$$PRUNE`` and evaluates ``spec`` with
                    ``ExprEvaluator``. Fails closed by raising
                    ``MalformedQueryException`` if evaluation raises an error
                    or returns anything other than ``$$KEEP``, ``$$DESCEND``,
                    or ``$$PRUNE``.

                    Args:
                        spec: ``$redact`` expression specification.
                        doc: Current document context (``$$CURRENT``).
                        root: Original root document (``$$ROOT``).

                    Returns:
                        str: One of ``"$$KEEP"``, ``"$$DESCEND"``, or
                        ``"$$PRUNE"``.

                    Raises:
                        MalformedQueryException: If the expression fails to
                            evaluate or produces an invalid action string.
                    """
                    eval_doc = {
                        **doc,
                        "$$ROOT": root,
                        "$$CURRENT": doc,
                        "$$KEEP": "$$KEEP",
                        "$$DESCEND": "$$DESCEND",
                        "$$PRUNE": "$$PRUNE",
                    }
                    try:
                        result = evaluator_redact._evaluate_operand_python(
                            spec, eval_doc
                        )
                    except Exception as e:
                        raise MalformedQueryException(
                            f"$redact condition evaluation failed: {e}"
                        ) from e
                    if result not in ("$$KEEP", "$$DESCEND", "$$PRUNE"):
                        raise MalformedQueryException(
                            f"Invalid $redact result {result!r}; expected "
                            "'$$KEEP', '$$DESCEND', or '$$PRUNE'"
                        )
                    return result

                def _redact_list(
                    items: list[Any], spec: Any, root: Any
                ) -> list[Any]:
                    """Recursively redact elements of a list, omitting pruned items."""
                    out_items: list[Any] = []
                    for item in items:
                        if isinstance(item, dict):
                            redacted_item = apply_redact(item, spec, root)
                            if redacted_item is not _pruned_sentinel:
                                out_items.append(redacted_item)
                        elif isinstance(item, list):
                            out_items.append(_redact_list(item, spec, root))
                        else:
                            out_items.append(item)
                    return out_items

                def apply_redact(doc, spec, root):
                    """Recursively apply redaction once per subdocument level.

                    Evaluates ``spec`` via ``evaluate_redact_condition()`` once
                    at ``doc``'s level: ``$$KEEP`` preserves the subtree,
                    ``$$PRUNE`` returns a private sentinel so parents omit the
                    entry, and ``$$DESCEND`` recurses into nested dicts and
                    lists.

                    Args:
                        doc: Current document or subdocument being redacted.
                        spec: ``$redact`` expression specification.
                        root: Original root document (bound to ``$$ROOT``).

                    Returns:
                        Any: Redacted copy of ``doc``, or ``_pruned_sentinel``
                        when the current level is pruned.
                    """
                    if not isinstance(doc, dict):
                        return doc

                    redact_action = evaluate_redact_condition(spec, doc, root)
                    if redact_action == "$$KEEP":
                        return deepcopy(doc)
                    if redact_action == "$$PRUNE":
                        return _pruned_sentinel

                    # $$DESCEND: retain scalars and recurse into subdocuments/arrays
                    result = {}
                    for key, value in doc.items():
                        if isinstance(value, dict):
                            redacted_val = apply_redact(value, spec, root)
                            if redacted_val is not _pruned_sentinel:
                                result[key] = redacted_val
                        elif isinstance(value, list):
                            result[key] = _redact_list(value, spec, root)
                        else:
                            result[key] = value
                    return result

                # Apply redaction to each document
                new_docs_with_context = []
                for dc in docs_with_context:
                    doc = dc["__doc__"]
                    root = dc["__root__"]

                    redacted_doc = apply_redact(doc, redact_spec, root)
                    if redacted_doc is not _pruned_sentinel:
                        new_docs_with_context.append(
                            {"__doc__": redacted_doc, "__root__": root}
                        )

                docs_with_context = new_docs_with_context

            case "$densify":
                # $densify fills gaps in sequential data
                densify_spec = stage["$densify"]

                field = densify_spec.get("field")
                range_spec = densify_spec.get("range", {})
                raw_partition = (
                    densify_spec.get("partitionByFields")
                    or densify_spec.get("partitionBy")
                    or []
                )
                if isinstance(raw_partition, str):
                    partition_by = [raw_partition.lstrip("$")]
                elif isinstance(raw_partition, list):
                    partition_by = [str(pf).lstrip("$") for pf in raw_partition]
                else:
                    partition_by = []
                output_spec = densify_spec.get("output", {})

                if not field:
                    # No field specified, pass through
                    pass
                else:
                    # Get bounds
                    bounds = range_spec.get("bounds")
                    step = range_spec.get("step", 1)
                    unit = range_spec.get("unit", None)  # For dates

                    if (
                        step is None
                        or not isinstance(step, (int, float))
                        or step <= 0
                    ):
                        raise ValueError(
                            f"$densify step must be greater than 0, got {step}"
                        )

                    # Determine if we're working with dates or numbers
                    is_date = unit is not None

                    # Group documents by partition fields
                    partitions: dict[tuple, list[dict[str, Any]]] = {}
                    for dc in docs_with_context:
                        doc = dc["__doc__"]

                        # Get partition key
                        if partition_by:
                            partition_key = tuple(
                                query_engine.collection._get_val(doc, pf)
                                for pf in partition_by
                            )
                        else:
                            partition_key = ()

                        if partition_key not in partitions:
                            partitions[partition_key] = []

                        field_val = query_engine.collection._get_val(doc, field)
                        partitions[partition_key].append(
                            {"doc": doc, "field_val": field_val, "dc": dc}
                        )

                    # Generate densified output for each partition
                    new_docs_with_context = []
                    for partition_key, items in partitions.items():
                        # Get all field values in this partition
                        field_values = [
                            item["field_val"]
                            for item in items
                            if item["field_val"] is not None
                        ]

                        if not field_values:
                            continue

                        # Determine range
                        if bounds == "full":
                            min_val = min(field_values)
                            max_val = max(field_values)
                        elif isinstance(bounds, list) and len(bounds) >= 2:
                            min_val, max_val = bounds[0], bounds[1]
                        else:
                            min_val = min(field_values)
                            max_val = max(field_values)

                        # Generate all values in range
                        existing_values = set(field_values)
                        all_values = []

                        if is_date:
                            # Handle date ranges
                            current = min_val
                            while current <= max_val:
                                all_values.append(current)
                                match unit:
                                    case "year":
                                        current = current.replace(
                                            year=current.year + step
                                        )
                                    case "month":
                                        new_month = current.month + step
                                        new_year = (
                                            current.year + (new_month - 1) // 12
                                        )
                                        new_month = ((new_month - 1) % 12) + 1
                                        try:
                                            current = current.replace(
                                                year=new_year,
                                                month=new_month,
                                            )
                                        except ValueError:
                                            # Handle month-end edge
                                            break
                                    case "day":
                                        current = current + timedelta(days=step)
                                    case "hour":
                                        current = current + timedelta(
                                            hours=step
                                        )
                                    case "minute":
                                        current = current + timedelta(
                                            minutes=step
                                        )
                                    case "second":
                                        current = current + timedelta(
                                            seconds=step
                                        )
                                    case _:
                                        break
                        else:
                            # Handle numeric ranges
                            current = min_val
                            while current <= max_val:
                                all_values.append(current)
                                current = current + step

                        # Preserve all existing documents in this partition,
                        # and synthesize minimal documents only for missing values
                        # (matching SQL Tier 1/Tier 2 and MongoDB $densify semantics).
                        for item in items:
                            new_docs_with_context.append(item["dc"])

                        for val in all_values:
                            if val not in existing_values:
                                base_doc: dict[str, Any] = {}
                                for pf, pk_val in zip(
                                    partition_by, partition_key
                                ):
                                    target = base_doc
                                    parts = pf.split(".")
                                    for part in parts[:-1]:
                                        target = target.setdefault(part, {})
                                    target[parts[-1]] = pk_val

                                target = base_doc
                                fparts = field.split(".")
                                for part in fparts[:-1]:
                                    target = target.setdefault(part, {})
                                target[fparts[-1]] = val

                                # Apply output spec for additional fields
                                base_doc.update(
                                    {
                                        out_field: out_expr
                                        for out_field, out_expr in output_spec.items()
                                        if out_field != field
                                    }
                                )

                                new_docs_with_context.append(
                                    {
                                        "__doc__": base_doc,
                                        "__root__": base_doc,
                                    }
                                )

                    docs_with_context = new_docs_with_context
            case "$collStats":
                from ..query_helper.utils import get_force_fallback

                coll_stats_spec = (
                    stage.get("$collStats") or stage.get(" $collStats") or {}
                )
                table_name = query_engine.collection.name

                storage_size = 0
                total_index_size = 0
                index_sizes: dict[str, int] = {}

                if get_force_fallback():
                    from neosqlite.collection.json_helpers import (
                        neosqlite_json_dumps,
                    )

                    count = len(docs_with_context)
                    size = sum(
                        len(neosqlite_json_dumps(dc["__doc__"]))
                        for dc in docs_with_context
                    )
                    avg_obj_size = size / count if count > 0 else 0
                else:
                    quoted_table = quote_table_name(table_name)
                    db = query_engine.collection.db

                    count_cursor = db.execute(
                        f"SELECT COUNT(*) FROM {quoted_table}"
                    )
                    count = count_cursor.fetchone()[0] or 0

                    size = 0
                    try:
                        size_cursor = db.execute(
                            f"SELECT SUM(LENGTH(data)) FROM {quoted_table}"
                        )
                        size = size_cursor.fetchone()[0] or 0
                    except Exception as e:
                        logger.debug(
                            f"Failed to calculate collection size for stats: {e}"
                        )
                        pass

                    avg_obj_size = size / count if count > 0 else 0

                    try:
                        db.execute(
                            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat(main)"
                        )

                        storage_cursor = db.execute(
                            "SELECT SUM(pgsize) FROM dbstat WHERE name = ?",
                            (table_name,),
                        )
                        storage_size = storage_cursor.fetchone()[0] or 0

                        index_cursor = db.execute(
                            "SELECT name, SUM(pgsize) as size FROM dbstat "
                            "WHERE tbl_name = ? AND type = 'index' GROUP BY name",
                            (table_name,),
                        )
                        for row in index_cursor.fetchall():
                            idx_name, idx_size = row
                            if idx_name and idx_size:
                                index_sizes[idx_name] = idx_size
                                total_index_size += idx_size
                    except Exception as e:
                        logger.debug(
                            f"Failed to calculate storage/index sizes for stats: {e}"
                        )
                        pass

                db_name = (
                    query_engine.collection._database.name
                    if query_engine.collection._database
                    else "unknown"
                )
                stats_result: dict[str, Any] = {
                    "ns": f"{db_name}.{table_name}",
                    "count": count,
                    "size": size,
                    "avgObjSize": avg_obj_size,
                    "storageSize": storage_size,
                    "totalIndexSize": total_index_size,
                    "indexSizes": index_sizes,
                }

                if coll_stats_spec and "count" in coll_stats_spec:
                    stats_result = {"count": count}
                elif coll_stats_spec and "storageStats" in coll_stats_spec:
                    stats_result = {
                        "ns": f"{db_name}.{table_name}",
                        "storageStats": {
                            "count": count,
                            "size": size,
                            "avgObjSize": avg_obj_size,
                            "storageSize": storage_size,
                            "totalIndexSize": total_index_size,
                            "indexSizes": index_sizes,
                        },
                    }

                docs_with_context = [
                    {"__doc__": stats_result, "__root__": stats_result}
                ]
            case _:
                raise MalformedQueryException(
                    f"Aggregation stage '{stage_name}' not supported"
                )
    if initial_docs is None:
        query_engine._notify_tier_change("tier3", pipeline)
    results = []
    for dc in docs_with_context:
        doc = dc["__doc__"]
        if (
            isinstance(doc, dict)
            and "_textScore" in doc
            and (
                not isinstance(dc.get("__root__"), dict)
                or "_textScore" not in dc["__root__"]
            )
        ):
            doc.pop("_textScore", None)
        results.append(doc)
    return results
