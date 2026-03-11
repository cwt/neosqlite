"""
Python implementation of MongoDB $setWindowFields operators.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
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
    for partition_key in sorted(partitions.keys(), key=lambda x: str(x)):
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
        ranks: Optional[List[int]] = None
        dense_ranks: Optional[List[int]] = None

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
                    )

                collection._set_val(doc, field_path, result)

            dc["__doc__"] = doc

        all_processed_docs.extend(partition_docs)

    return all_processed_docs


def _get_window_frame(
    current_idx: int,
    partition_docs: List[Dict[str, Any]],
    window_spec: Optional[Dict[str, Any]],
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
) -> Any:
    frame_docs = [partition_docs[idx]["__doc__"] for idx in frame_indices]

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
            # n can be an expression
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
