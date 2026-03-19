"""
Python implementation of MongoDB $setWindowFields operators.
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, TYPE_CHECKING
from copy import deepcopy

if TYPE_CHECKING:
    from .. import Collection
    from ..expr_evaluator import ExprEvaluator


def process_set_window_fields(
    docs_with_context: List[Dict[str, Any]],
    spec: Dict[str, Any],
    collection: Collection,
    evaluator: ExprEvaluator,
) -> List[Dict[str, Any]]:
    """
    Python fallback implementation of $setWindowFields.
    """
    partition_by = spec.get("partitionBy")
    sort_by: Dict[str, int] = spec.get("sortBy", {})
    output: Dict[str, Dict[str, Any]] = spec.get("output", {})

    # 1. Partitioning
    partitions: Dict[Any, List[int]] = {}
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
                    val = collection._get_val(dc["__doc__"], field)
                    if val is None:
                        return (0 if is_desc else 1, None)
                    return (0, val)

                partition_docs.sort(key=get_sort_val, reverse=is_desc)

        # Pre-calculate ranks if needed
        ranks: List[int] | None = None
        dense_ranks: List[int] | None = None

        # 3. Apply window operators
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
                else:
                    frame = _get_window_frame(i, partition_docs, window_spec)
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

            dc["__doc__"] = doc

        all_processed_docs.extend(partition_docs)

    return all_processed_docs


def _get_window_frame(
    current_idx: int,
    partition_docs: List[Dict[str, Any]],
    window_spec: Dict[str, Any] | None,
) -> List[int]:
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

    return list(range(len(partition_docs)))


def _apply_window_operator(
    op_name: str,
    op_val: Any,
    current_idx: int,
    partition_docs: List[Dict[str, Any]],
    frame_indices: List[int],
    evaluator: ExprEvaluator,
    collection: Collection,
    sort_by: Dict[str, int],
) -> Any:
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
        cov_sum = sum(
            (v1 - mean1) * (v2 - mean2) for v1, v2 in zip(v1_list, v2_list)
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
            for i in range(1, len(frame_indices)):
                idx_prev, idx_curr = frame_indices[i - 1], frame_indices[i]
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
                    val = collection._get_val(dc["__doc__"], field)
                    if val is None:
                        return (0 if is_desc else 1, None)
                    return (0, val)

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
            unique_values: List[Any] = []
            for v in values:
                if v not in unique_values:
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

    return None


def _get_sort_key(
    doc: Dict[str, Any], sort_by: Dict[str, int], collection: Collection
) -> Tuple:
    return tuple(collection._get_val(doc, field) for field in sort_by)


def _calculate_all_ranks(
    partition_docs: List[Dict[str, Any]],
    sort_by: Dict[str, int],
    collection: Collection,
) -> List[int]:
    ranks: List[int] = []
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
    partition_docs: List[Dict[str, Any]],
    sort_by: Dict[str, int],
    collection: Collection,
) -> List[int]:
    ranks: List[int] = []
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
