"""Python implementation of MongoDB ``$setWindowFields`` operators."""

from __future__ import annotations

import math
from copy import deepcopy
from itertools import pairwise
from typing import TYPE_CHECKING, Any

from ..type_utils import bson_sort_key

if TYPE_CHECKING:
    from .. import Collection
    from ..expr_evaluator import ExprEvaluator


def process_set_window_fields(
    docs_with_context: list[dict[str, Any]],
    spec: dict[str, Any],
    collection: Collection,
    evaluator: ExprEvaluator,
) -> list[dict[str, Any]]:
    """Evaluate MongoDB ``$setWindowFields`` stage in Python.

    Partitions ``docs_with_context`` according to ``spec["partitionBy"]``,
    sorts each partition by ``spec["sortBy"]`` using BSON ordering, computes
    each requested window output field (``$rank``, ``$denseRank``, or
    frame-based window operators), and mutates a deep copy of each
    document before returning the concatenated partition results.

    Args:
        docs_with_context: List of pipeline wrapper dicts containing
            ``"__doc__"`` keys.
        spec: ``$setWindowFields`` stage specification dict with optional
            ``partitionBy`` and ``sortBy`` keys and required ``output`` map.
        collection: Target ``Collection`` used for field access and setters.
        evaluator: ``ExprEvaluator`` instance for evaluating expressions in
            Python.

    Returns:
        list[dict[str, Any]]: Processed document-with-context dicts ordered
        by sorted partition key and per-partition sort order.
    """
    partition_by = spec.get("partitionBy")
    sort_by: dict[str, int] = spec.get("sortBy", {})
    output: dict[str, dict[str, Any]] = spec.get("output", {})

    # 1. Partitioning
    partitions: dict[Any, list[int]] = {}
    for i, dc in enumerate(docs_with_context):
        if partition_by is None:
            key = None
        else:
            key = evaluator._evaluate_operand_python(
                partition_by, dc["__doc__"]
            )
            if isinstance(key, (dict, list)):
                key = str(key)

        if key not in partitions:
            partitions[key] = []
        partitions[key].append(i)

    # 2. Process each partition
    all_processed_docs = []
    # Sort partitions keys for deterministic output
    for partition_key in sorted(partitions.keys(), key=str):
        indices = partitions[partition_key]
        partition_docs = [docs_with_context[i] for i in indices]

        # Sort the partition if sortBy is provided
        if sort_by:
            for field, direction in reversed(list(sort_by.items())):
                is_desc = direction == -1

                def get_sort_val(dc):
                    """Compute BSON sort key for the current field.

                    Args:
                        dc: Document-with-context dictionary.

                    Returns:
                        tuple: BSON-ordered sort key tuple.
                    """
                    # BSON-ordered key: safe on mixed/missing values (#102)
                    return bson_sort_key(
                        collection._get_val(dc["__doc__"], field)
                    )

                partition_docs.sort(key=get_sort_val, reverse=is_desc)

        # Pre-calculate partition-wide O(N) sequence operators if needed
        ranks: list[int] | None = None
        dense_ranks: list[int] | None = None
        exp_moving_avgs: dict[str, list[Any]] = {}

        # 3. Apply window operators against unmodified partition snapshot
        new_partition_docs: list[dict[str, Any]] = []
        for i, dc in enumerate(partition_docs):
            doc = deepcopy(dc["__doc__"])

            for field_path, op_spec in output.items():
                op_name = next(iter(op_spec.keys()))
                op_val = op_spec[op_name]
                window_spec = op_spec.get("window")

                if op_name == "$rank":
                    if ranks is None:
                        ranks = _calculate_all_ranks(
                            partition_docs, sort_by, collection
                        )
                    result = ranks[i]
                elif op_name == "$denseRank":
                    if dense_ranks is None:
                        dense_ranks = _calculate_all_dense_ranks(
                            partition_docs, sort_by, collection
                        )
                    result = dense_ranks[i]
                elif op_name == "$expMovingAvg":
                    if field_path not in exp_moving_avgs:
                        exp_moving_avgs[field_path] = (
                            _calculate_all_exp_moving_avg(
                                partition_docs, op_val, evaluator
                            )
                        )
                    result = exp_moving_avgs[field_path][i]
                else:
                    frame = _get_window_frame(
                        i,
                        partition_docs,
                        window_spec,
                        sort_by=sort_by,
                        collection=collection,
                    )
                    result = _apply_window_operator(
                        op_name,
                        op_val,
                        i,
                        partition_docs,
                        frame,
                        evaluator,
                        collection,
                        sort_by,
                    )

                collection._set_val(doc, field_path, result)

            new_dc = dict(dc)
            new_dc["__doc__"] = doc
            new_partition_docs.append(new_dc)

        all_processed_docs.extend(new_partition_docs)

    return all_processed_docs


def _get_window_frame(
    current_idx: int,
    partition_docs: list[dict[str, Any]],
    window_spec: dict[str, Any] | None,
    sort_by: dict[str, int] | None = None,
    collection: Any | None = None,
) -> list[int]:
    """Compute partition document indices belonging to a window frame.

    Supports ``documents`` frames (positional offsets with ``"unbounded"``
    or ``"current"`` bounds) and numeric ``range`` frames (using the first
    field in ``sort_by`` when ``unit`` is unset). Returns all partition
    indices when ``window_spec`` is omitted or has no matching frame mode.

    Args:
        current_idx: Zero-based index of the current document in
            ``partition_docs``.
        partition_docs: Ordered list of document-with-context dicts in the
            partition.
        window_spec: Optional ``window`` specification dict from an output
            operator, or ``None`` for the full partition.
        sort_by: Optional ``sortBy`` mapping from ``$setWindowFields``.
        collection: Optional ``Collection`` used for nested field lookup
            in ``range`` frames.

    Returns:
        list[int]: Partition indices included in the frame for
        ``current_idx``.
    """
    if not window_spec:
        return list(range(len(partition_docs)))

    if "documents" in window_spec:
        lower, upper = window_spec["documents"]

        if lower == "unbounded":
            start = 0
        elif lower == "current":
            start = current_idx
        else:
            start = max(0, current_idx + lower)

        if upper == "unbounded":
            end = len(partition_docs)
        elif upper == "current":
            end = current_idx + 1
        else:
            end = min(len(partition_docs), current_idx + upper + 1)

        return list(range(start, end))

    if "range" in window_spec and sort_by and collection is not None:
        lower, upper = window_spec["range"]
        sort_field, direction = next(iter(sort_by.items()))
        cur_val = collection._get_val(
            partition_docs[current_idx]["__doc__"], sort_field
        )
        if not isinstance(cur_val, (int, float)) or isinstance(cur_val, bool):
            return []

        unit = window_spec.get("unit")
        if unit is not None:
            return [current_idx]

        def resolve_offset(bound: Any) -> float | None:
            """Resolve a range window boundary to a numeric offset.

            Args:
                bound: Window bound (``"unbounded"``, ``"current"``, or
                    numeric offset).

            Returns:
                float | None: Numeric offset relative to current value,
                or ``None`` when unbounded.
            """
            if bound == "unbounded":
                return None
            if bound == "current":
                return 0.0
            if isinstance(bound, (int, float)) and not isinstance(bound, bool):
                return float(bound)
            return 0.0

        lo_off = resolve_offset(lower)
        hi_off = resolve_offset(upper)
        indices = []
        for idx, dc in enumerate(partition_docs):
            val = collection._get_val(dc["__doc__"], sort_field)
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                continue
            if direction == -1:
                ok_lo = True if lo_off is None else val <= cur_val - lo_off
                ok_hi = True if hi_off is None else val >= cur_val - hi_off
            else:
                ok_lo = True if lo_off is None else val >= cur_val + lo_off
                ok_hi = True if hi_off is None else val <= cur_val + hi_off
            if ok_lo and ok_hi:
                indices.append(idx)
        return indices

    return list(range(len(partition_docs)))


def _apply_window_operator(
    op_name: str,
    op_val: Any,
    current_idx: int,
    partition_docs: list[dict[str, Any]],
    frame_indices: list[int],
    evaluator: ExprEvaluator,
    collection: Collection,
    sort_by: dict[str, int],
) -> Any:
    """Evaluate a single ``$setWindowFields`` output operator in Python.

    Supports ``$documentNumber``, ``$shift``, ``$covariancePop``,
    ``$covarianceSamp``, ``$expMovingAvg``, ``$derivative``,
    ``$integral``, ``$top``, ``$topN``, ``$bottom``, ``$bottomN``,
    ``$count``, and standard accumulators (``$sum``, ``$avg``, ``$min``,
    ``$max``, ``$push``, ``$addToSet``, ``$first``, ``$last``,
    ``$firstN``, ``$lastN``, ``$minN``, ``$maxN``, ``$stdDevPop``,
    ``$stdDevSamp``).

    Args:
        op_name: MongoDB window operator name (e.g., ``"$sum"``,
            ``"$shift"``).
        op_val: Operator operand or specification dictionary.
        current_idx: Zero-based index of the current document within
            ``partition_docs``.
        partition_docs: Ordered document-with-context dicts for the active
            partition.
        frame_indices: Partition indices included in the current window
            frame.
        evaluator: ``ExprEvaluator`` instance for Python operand evaluation.
        collection: Target ``Collection`` used for field lookup.
        sort_by: Stage-level ``sortBy`` specification mapping.

    Returns:
        Any: Computed operator value for the current document, or ``None``
        when the frame lacks valid inputs or the operator is unsupported.
    """
    # 1. Operators that don't use frames or use documents directly
    if op_name == "$documentNumber":
        return current_idx + 1

    if op_name == "$shift":
        output_expr = op_val.get("output")
        by = op_val.get("by", 0)
        default = op_val.get("default")

        target_idx = current_idx + by
        if 0 <= target_idx < len(partition_docs):
            doc = partition_docs[target_idx]["__doc__"]
            return evaluator._evaluate_operand_python(output_expr, doc)
        return default

    # 2. Operators that use frames
    frame_docs = [partition_docs[idx]["__doc__"] for idx in frame_indices]

    if op_name in ["$covariancePop", "$covarianceSamp"]:
        val1_expr, val2_expr = op_val
        v1_list = []
        v2_list = []
        for doc in frame_docs:
            v1 = evaluator._evaluate_operand_python(val1_expr, doc)
            v2 = evaluator._evaluate_operand_python(val2_expr, doc)
            if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                v1_list.append(v1)
                v2_list.append(v2)

        if not v1_list:
            return None

        mean1 = sum(v1_list) / len(v1_list)
        mean2 = sum(v2_list) / len(v2_list)
        cov_sum = math.sumprod(
            (v1 - mean1 for v1 in v1_list),
            (v2 - mean2 for v2 in v2_list),
        )
        div = len(v1_list) if op_name == "$covariancePop" else len(v1_list) - 1
        return cov_sum / div if div > 0 else None

    if op_name == "$expMovingAvg":
        input_expr = op_val.get("input")
        if "alpha" in op_val:
            alpha = op_val["alpha"]
        elif "n" in op_val:
            alpha = 2 / (op_val["n"] + 1)
        else:
            return None

        # Standard MongoDB $expMovingAvg usually operates on the sequence from start of partition.
        # If the window is [-inf, current] or similar, we calculate it cumulatively.
        # For simplicity in this fallback, we re-calculate up to the current point.
        # In a more optimized version, we could cache the previous EMA value.
        ema = None
        # We search from the START of the partition up to current_idx
        for i in range(current_idx + 1):
            doc = partition_docs[i]["__doc__"]
            val = evaluator._evaluate_operand_python(input_expr, doc)
            if not isinstance(val, (int, float)):
                continue
            if ema is None:
                ema = val
            else:
                ema = val * alpha + ema * (1 - alpha)
        return ema

    if op_name in ["$derivative", "$integral"]:
        if not sort_by:
            return None
        input_expr = op_val.get("input")
        # Find the time/coordinate field (first field in sortBy)
        time_field = next(iter(sort_by.keys()))

        if op_name == "$derivative":
            if len(frame_indices) < 2:
                return None
            # (v_end - v_start) / (t_end - t_start)
            idx_start, idx_end = frame_indices[0], frame_indices[-1]
            doc_start, doc_end = (
                partition_docs[idx_start]["__doc__"],
                partition_docs[idx_end]["__doc__"],
            )
            v_start = evaluator._evaluate_operand_python(input_expr, doc_start)
            v_end = evaluator._evaluate_operand_python(input_expr, doc_end)
            t_start = collection._get_val(doc_start, time_field)
            t_end = collection._get_val(doc_end, time_field)

            if (
                all(
                    isinstance(x, (int, float))
                    for x in [v_start, v_end, t_start, t_end]
                )
                and t_start != t_end
            ):
                return (v_end - v_start) / (t_end - t_start)
            return None

        if op_name == "$integral":
            # Trapezoidal rule: sum of (v_i + v_{i-1})/2 * (t_i - t_{i-1})
            total = 0.0
            for idx_prev, idx_curr in pairwise(frame_indices):
                doc_prev, doc_curr = (
                    partition_docs[idx_prev]["__doc__"],
                    partition_docs[idx_curr]["__doc__"],
                )
                v_prev = evaluator._evaluate_operand_python(
                    input_expr, doc_prev
                )
                v_curr = evaluator._evaluate_operand_python(
                    input_expr, doc_curr
                )
                t_prev = collection._get_val(doc_prev, time_field)
                t_curr = collection._get_val(doc_curr, time_field)

                if all(
                    isinstance(x, (int, float))
                    for x in [v_prev, v_curr, t_prev, t_curr]
                ):
                    total += (v_prev + v_curr) / 2.0 * (t_curr - t_prev)
            return total

    if op_name in ["$top", "$topN", "$bottom", "$bottomN"]:
        # These operators can have their own sortBy
        op_sortBy = op_val.get("sortBy")
        output_expr = op_val.get("output")

        # Determine the set of documents to sort
        # These operators typically use the entire window frame
        docs_to_sort = [partition_docs[idx] for idx in frame_indices]

        effective_sortBy = op_sortBy if op_sortBy is not None else sort_by

        if effective_sortBy:
            # Sort the frame docs based on effective_sortBy
            for field, direction in reversed(list(effective_sortBy.items())):
                is_desc = direction == -1

                def get_sort_val(dc):
                    """Compute BSON sort key for the current field.

                    Args:
                        dc: Document-with-context dictionary.

                    Returns:
                        tuple: BSON-ordered sort key tuple.
                    """
                    # BSON-ordered key: safe on mixed/missing values (#102)
                    return bson_sort_key(
                        collection._get_val(dc["__doc__"], field)
                    )

                docs_to_sort.sort(key=get_sort_val, reverse=is_desc)

        sorted_frame_docs = [dc["__doc__"] for dc in docs_to_sort]

        if op_name == "$top":
            if not sorted_frame_docs:
                return None
            return evaluator._evaluate_operand_python(
                output_expr, sorted_frame_docs[0]
            )

        if op_name == "$bottom":
            if not sorted_frame_docs:
                return None
            return evaluator._evaluate_operand_python(
                output_expr, sorted_frame_docs[-1]
            )

        if op_name in ["$topN", "$bottomN"]:
            n_expr = op_val.get("n", 1)
            n = evaluator._evaluate_operand_python(
                n_expr, partition_docs[current_idx]["__doc__"]
            )
            if not isinstance(n, int) or n < 0:
                return None

            values = [
                evaluator._evaluate_operand_python(output_expr, doc)
                for doc in sorted_frame_docs
            ]
            if op_name == "$topN":
                return values[:n]
            else:
                # bottomN returns values from the end, but preserves order or not?
                # MongoDB $bottomN returns the last N elements.
                # If n=2 and docs are [A, B, C, D], bottomN returns [C, D] or [D, C]?
                # According to MongoDB docs, $bottomN returns the last N elements in the specified sort order.
                # So [C, D].
                return values[-n:] if n > 0 else []

    # 3. Standard accumulators
    if op_name == "$count":
        return len(frame_docs)

    if op_name in [
        "$sum",
        "$avg",
        "$min",
        "$max",
        "$push",
        "$addToSet",
        "$first",
        "$last",
        "$firstN",
        "$lastN",
        "$minN",
        "$maxN",
        "$stdDevPop",
        "$stdDevSamp",
    ]:
        if op_name in ["$firstN", "$lastN", "$minN", "$maxN"]:
            input_expr = op_val.get("input")
            n_expr = op_val.get("n", 1)
            n = evaluator._evaluate_operand_python(
                n_expr, partition_docs[current_idx]["__doc__"]
            )
            if not isinstance(n, int) or n < 0:
                return None
        else:
            input_expr = op_val

        values = []
        for doc in frame_docs:
            val = evaluator._evaluate_operand_python(input_expr, doc)
            if val is not None:
                values.append(val)

        if not values and op_name not in [
            "$push",
            "$addToSet",
            "$firstN",
            "$lastN",
            "$minN",
            "$maxN",
        ]:
            return None

        if op_name == "$sum":
            return sum(v for v in values if isinstance(v, (int, float)))
        if op_name == "$avg":
            num_values = [v for v in values if isinstance(v, (int, float))]
            return sum(num_values) / len(num_values) if num_values else None
        if op_name == "$min":
            return min(values) if values else None
        if op_name == "$max":
            return max(values) if values else None
        if op_name == "$push":
            return values
        if op_name == "$addToSet":
            # Canonical-key dedupe — O(n) and document-safe (#155)
            unique_values: list[Any] = []
            seen: set[Any] = set()
            from .aggregation import _addtoset_key

            for v in values:
                k = _addtoset_key(v)
                if k not in seen:
                    seen.add(k)
                    unique_values.append(v)
            return unique_values
        if op_name == "$first":
            return values[0] if values else None
        if op_name == "$last":
            return values[-1] if values else None
        if op_name == "$firstN":
            return values[:n]
        if op_name == "$lastN":
            return values[-n:] if n > 0 else []
        if op_name == "$minN":
            return sorted(values)[:n]
        if op_name == "$maxN":
            return sorted(values, reverse=True)[:n]
        if op_name in ("$stdDevPop", "$stdDevSamp"):
            nums = [
                v
                for v in values
                if isinstance(v, (int, float)) and not isinstance(v, bool)
            ]
            if not nums or (op_name == "$stdDevSamp" and len(nums) < 2):
                return None
            mean = sum(nums) / len(nums)
            denom = len(nums) if op_name == "$stdDevPop" else len(nums) - 1
            return math.sqrt(sum((v - mean) ** 2 for v in nums) / denom)

    return None


def _get_sort_key(
    doc: dict[str, Any], sort_by: dict[str, int], collection: Collection
) -> tuple:
    """Extract a tuple of field values from ``doc`` for tie comparison.

    Args:
        doc: Document dictionary to read field values from.
        sort_by: Ordered mapping of field paths to sort directions.
        collection: ``Collection`` instance providing ``_get_val``.

    Returns:
        tuple: Tuple of extracted field values ordered by ``sort_by``.
    """
    return tuple(collection._get_val(doc, field) for field in sort_by)


def _calculate_all_ranks(
    partition_docs: list[dict[str, Any]],
    sort_by: dict[str, int],
    collection: Collection,
) -> list[int]:
    """Calculate 1-based standard competition ranks (``$rank``) in a partition.

    Documents sharing identical ``sort_by`` key tuples receive the same
    rank, and the next distinct key tuple advances to its 1-based position
    (leaving gaps after ties).

    Args:
        partition_docs: Sorted partition entries containing ``"__doc__"``.
        sort_by: Mapping of sort fields to directions.
        collection: ``Collection`` instance used for field extraction.

    Returns:
        list[int]: One-based rank integer for each document in
        ``partition_docs``.
    """
    ranks: list[int] = []
    current_rank = 1
    for i in range(len(partition_docs)):
        if i > 0:
            if _get_sort_key(
                partition_docs[i]["__doc__"], sort_by, collection
            ) != _get_sort_key(
                partition_docs[i - 1]["__doc__"], sort_by, collection
            ):
                current_rank = i + 1
        ranks.append(current_rank)
    return ranks


def _calculate_all_dense_ranks(
    partition_docs: list[dict[str, Any]],
    sort_by: dict[str, int],
    collection: Collection,
) -> list[int]:
    """Calculate 1-based dense ranks (``$denseRank``) in a partition.

    Documents sharing identical ``sort_by`` key tuples receive the same
    rank, and each subsequent distinct key tuple increments the rank by 1
    with no gaps.

    Args:
        partition_docs: Sorted partition entries containing ``"__doc__"``.
        sort_by: Mapping of sort fields to directions.
        collection: ``Collection`` instance used for field extraction.

    Returns:
        list[int]: One-based dense rank integer for each document in
        ``partition_docs``.
    """
    ranks: list[int] = []
    current_rank = 1
    for i in range(len(partition_docs)):
        if i > 0:
            if _get_sort_key(
                partition_docs[i]["__doc__"], sort_by, collection
            ) != _get_sort_key(
                partition_docs[i - 1]["__doc__"], sort_by, collection
            ):
                current_rank += 1
        ranks.append(current_rank)
    return ranks


def _calculate_all_exp_moving_avg(
    partition_docs: list[dict[str, Any]],
    op_val: dict[str, Any],
    evaluator: ExprEvaluator,
) -> list[Any]:
    """Calculate cumulative exponential moving averages for a partition in O(N)."""
    input_expr = op_val.get("input")
    if "alpha" in op_val:
        alpha = op_val["alpha"]
    elif "n" in op_val:
        alpha = 2 / (op_val["n"] + 1)
    else:
        return [None] * len(partition_docs)

    results: list[Any] = []
    ema: Any = None
    for dc in partition_docs:
        val = evaluator._evaluate_operand_python(input_expr, dc["__doc__"])
        if isinstance(val, (int, float)):
            if ema is None:
                ema = val
            else:
                ema = val * alpha + ema * (1 - alpha)
        results.append(ema)
    return results
