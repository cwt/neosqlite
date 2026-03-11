"""
Python implementation of MongoDB $fill aggregation stage.
"""

from typing import Any, Dict, List


def process_fill(
    docs_with_context: List[Dict[str, Any]],
    spec: Dict[str, Any],
    collection: Any,
    evaluator: Any,
) -> List[Dict[str, Any]]:
    """
    Python fallback implementation of $fill.
    """
    partition_by = spec.get("partitionBy")
    sort_by = spec.get("sortBy", {})
    output = spec.get("output", {})

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
    for partition_key in sorted(partitions.keys(), key=lambda x: str(x)):
        indices = partitions[partition_key]
        partition_docs = [docs_with_context[i] for i in indices]

        # Sort partition
        if sort_by:
            for field, direction in reversed(list(sort_by.items())):
                is_desc = direction == -1

                def get_sort_val(dc):
                    val = collection._get_val(dc["__doc__"], field)
                    if val is None:
                        return (0 if is_desc else 1, None)
                    return (0, val)

                partition_docs.sort(key=get_sort_val, reverse=is_desc)

        # 3. Apply fill for each output field
        for field_path, fill_spec in output.items():
            if "value" in fill_spec:
                _fill_constant(
                    partition_docs, field_path, fill_spec["value"], collection
                )
            elif fill_spec.get("method") == "locf":
                _fill_locf(partition_docs, field_path, collection)
            elif fill_spec.get("method") == "linear":
                _fill_linear(partition_docs, field_path, sort_by, collection)

        all_processed_docs.extend(partition_docs)

    return all_processed_docs


def _fill_constant(partition_docs, field_path, value, collection):
    for dc in partition_docs:
        doc = dc["__doc__"]
        if collection._get_val(doc, field_path) is None:
            collection._set_val(doc, field_path, value)


def _fill_locf(partition_docs, field_path, collection):
    last_val = None
    for dc in partition_docs:
        doc = dc["__doc__"]
        val = collection._get_val(doc, field_path)
        if val is not None:
            last_val = val
        elif last_val is not None:
            collection._set_val(doc, field_path, last_val)


def _fill_linear(partition_docs, field_path, sort_by, collection):
    """Linear interpolation."""
    if not sort_by:
        return  # Linear requires sorting

    sort_field = next(iter(sort_by.keys()))

    i = 0
    while i < len(partition_docs):
        val = collection._get_val(partition_docs[i]["__doc__"], field_path)
        if val is not None:
            i += 1
            continue

        # Found a None, look for next non-None
        start_idx = i - 1
        if start_idx < 0:
            # Leading Nones cannot be linearly interpolated
            i += 1
            continue

        end_idx = i
        while (
            end_idx < len(partition_docs)
            and collection._get_val(
                partition_docs[end_idx]["__doc__"], field_path
            )
            is None
        ):
            end_idx += 1

        if end_idx >= len(partition_docs):
            # Trailing Nones cannot be linearly interpolated
            break

        # We have a range to interpolate: (start_idx, end_idx)
        v_start = collection._get_val(
            partition_docs[start_idx]["__doc__"], field_path
        )
        v_end = collection._get_val(
            partition_docs[end_idx]["__doc__"], field_path
        )
        t_start = collection._get_val(
            partition_docs[start_idx]["__doc__"], sort_field
        )
        t_end = collection._get_val(
            partition_docs[end_idx]["__doc__"], sort_field
        )

        if (
            not isinstance(v_start, (int, float))
            or not isinstance(v_end, (int, float))
            or not isinstance(t_start, (int, float))
            or not isinstance(t_end, (int, float))
            or t_start == t_end
        ):
            i = end_idx
            continue

        for k in range(start_idx + 1, end_idx):
            t_k = collection._get_val(partition_docs[k]["__doc__"], sort_field)
            if isinstance(t_k, (int, float)):
                v_k = v_start + (v_end - v_start) * (t_k - t_start) / (
                    t_end - t_start
                )
                collection._set_val(
                    partition_docs[k]["__doc__"], field_path, v_k
                )

        i = end_idx
