"""Python implementation of MongoDB ``$fill`` aggregation stage.

Supports partitioning via ``partitionBy`` or ``partitionByFields``, BSON-safe
partition sorting via ``sortBy``, and constant-value, last-observation-carried-
forward (``locf``), and linear interpolation (``linear``) fill strategies.
"""

from copy import deepcopy
from typing import Any

from ..type_utils import bson_sort_key


def process_fill(
    docs_with_context: list[dict[str, Any]],
    spec: dict[str, Any],
    collection: Any,
    evaluator: Any,
) -> list[dict[str, Any]]:
    """Apply the MongoDB ``$fill`` aggregation stage in Python.

    Groups input document contexts by partition key, sorts each partition
    according to ``sortBy`` using BSON ordering, fills missing (``None``)
    output fields using constant values, ``locf``, or ``linear`` interpolation,
    and returns the combined list of processed document-context dictionaries.

    Args:
        docs_with_context: List of document-context dictionaries, each holding
            the document under ``"__doc__"``.
        spec: The ``$fill`` stage specification dictionary containing optional
            ``partitionBy``, ``partitionByFields``, ``sortBy``, and ``output``.
        collection: Collection instance providing ``_get_val`` and ``_set_val``
            field access helpers.
        evaluator: Expression evaluator providing
            ``_evaluate_operand_python`` for computing ``partitionBy`` keys.

    Returns:
        list[dict[str, Any]]: Processed document-context dictionaries ordered
            by partition key (string-sorted) and per-partition ``sortBy``.
    """
    partition_by = spec.get("partitionBy")
    partition_by_fields = spec.get("partitionByFields")
    sort_by = spec.get("sortBy", {})
    output = spec.get("output", {})

    # 1. Partitioning
    partitions: dict[Any, list[int]] = {}
    for i, dc in enumerate(docs_with_context):
        doc = dc["__doc__"]
        key: Any
        if partition_by is None and partition_by_fields is not None:
            key = tuple(
                (
                    str(v)
                    if isinstance(
                        v := collection._get_val(doc, f.lstrip("$")),
                        (dict, list),
                    )
                    else v
                )
                for f in partition_by_fields
            )
        elif partition_by is None:
            key = None
        elif isinstance(partition_by, dict) and not any(
            k.startswith("$") for k in partition_by
        ):
            parts = []
            for v_expr in partition_by.values():
                pv = evaluator._evaluate_operand_python(v_expr, doc)
                if isinstance(pv, (dict, list)):
                    pv = str(pv)
                parts.append(pv)
            key = tuple(parts)
        else:
            key = evaluator._evaluate_operand_python(partition_by, doc)
            if isinstance(key, (dict, list)):
                key = str(key)

        if key not in partitions:
            partitions[key] = []
        partitions[key].append(i)

    # 2. Process each partition
    all_processed_docs = []
    for partition_key in sorted(partitions.keys(), key=str):
        indices = partitions[partition_key]
        partition_docs = [docs_with_context[i] for i in indices]

        # Sort partition
        if sort_by:
            for field, direction in reversed(list(sort_by.items())):
                is_desc = direction == -1

                def get_sort_val(dc):
                    """Return the BSON-ordered sort key for a document context.

                    Args:
                        dc: Document-context dictionary containing
                            ``"__doc__"``.

                    Returns:
                        tuple: BSON-ordered sort key for ``field``.
                    """
                    # BSON-ordered key: safe on mixed/missing values (#102)
                    return bson_sort_key(
                        collection._get_val(dc["__doc__"], field)
                    )

                partition_docs.sort(key=get_sort_val, reverse=is_desc)

        # 3. Apply fill for each output field
        for field_path, fill_spec in output.items():
            if "value" in fill_spec:
                _fill_constant(
                    partition_docs,
                    field_path,
                    fill_spec["value"],
                    collection,
                    evaluator,
                )
            elif fill_spec.get("method") == "locf":
                _fill_locf(partition_docs, field_path, collection)
            elif fill_spec.get("method") == "linear":
                _fill_linear(partition_docs, field_path, sort_by, collection)

        all_processed_docs.extend(partition_docs)

    return all_processed_docs


def _fill_constant(
    partition_docs, field_path, value, collection, evaluator=None
):
    """Fill missing (``None``) values of a field using a value or expression.

    Args:
        partition_docs: Ordered list of document-context dicts in a partition.
        field_path: Target field path to populate when its value is ``None``.
        value: Constant value, field reference, or expression dictionary.
        collection: Collection instance providing ``_get_val`` and
            ``_set_val``.
        evaluator: Optional ``ExprEvaluator`` for evaluating expressions.
    """
    for dc in partition_docs:
        doc = dc["__doc__"]
        if collection._get_val(doc, field_path) is None:
            if evaluator is not None:
                eval_doc = dict(doc)
                eval_doc["$$CURRENT"] = doc
                eval_doc["$$ROOT"] = dc.get("__root__", doc)
                resolved = evaluator._evaluate_operand_python(value, eval_doc)
                if isinstance(resolved, (dict, list)):
                    resolved = deepcopy(resolved)
            else:
                resolved = (
                    deepcopy(value)
                    if isinstance(value, (dict, list))
                    else value
                )
            collection._set_val(doc, field_path, resolved)


def _fill_locf(partition_docs, field_path, collection):
    """Fill missing field values using last observation carried forward.

    Tracks the most recent non-``None`` value seen for ``field_path`` within
    ``partition_docs`` and copies it into subsequent documents whose value is
    ``None``. Leading ``None`` values before any observation remain ``None``.

    Args:
        partition_docs: Ordered list of document-context dicts in a partition.
        field_path: Target field path to fill.
        collection: Collection instance providing ``_get_val`` and
            ``_set_val``.
    """
    last_val = None
    for dc in partition_docs:
        doc = dc["__doc__"]
        val = collection._get_val(doc, field_path)
        if val is not None:
            last_val = val
        elif last_val is not None:
            collection._set_val(doc, field_path, last_val)


def _fill_linear(partition_docs, field_path, sort_by, collection):
    """Fill missing numeric field values using linear interpolation.

    Interpolates interior gaps of ``None`` values based on the first field in
    ``sort_by`` when both surrounding endpoints and intermediate sort values
    are numeric (``int`` or ``float``) with distinct endpoint sort keys.
    Returns early when ``sort_by`` is empty, and leaves leading or trailing
    ``None`` runs untouched.

    Args:
        partition_docs: Ordered list of document-context dicts in a partition.
        field_path: Target numeric field path to interpolate.
        sort_by: Non-empty mapping whose first key supplies the sort coordinate
            for interpolation.
        collection: Collection instance providing ``_get_val`` and
            ``_set_val``.
    """
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
