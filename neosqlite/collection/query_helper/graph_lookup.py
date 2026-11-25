"""Python implementation of MongoDB $graphLookup aggregation stage."""

from collections import deque
from copy import deepcopy
from typing import Any

from ..type_utils import _hashable_group_key

_GRAPH_LOOKUP_IN_BATCH_SIZE = 500


def _enqueue_lookup_values(
    queue: deque[tuple[Any, int]],
    raw_value: Any,
    depth: int,
) -> None:
    """Append scalar or list frontier values to the BFS queue."""
    if isinstance(raw_value, list):
        for item in raw_value:
            queue.append((item, depth))
    else:
        queue.append((raw_value, depth))


def process_graph_lookup(
    docs_with_context: list[dict[str, Any]],
    spec: dict[str, Any],
    collection: Any,
    evaluator: Any,
) -> list[dict[str, Any]]:
    """Perform a Python-tier ``$graphLookup`` breadth-first traversal.

    Resolves ``startWith`` against each input document and performs a
    breadth-first search across ``spec["from"]``, matching
    ``connectToField`` values against ``connectFromField`` values while
    tracking visited ``_id`` values to avoid cycles. Supports optional
    ``maxDepth``, ``depthField``, and ``restrictSearchWithMatch`` filters,
    returning ``docs_with_context`` unchanged if any required stage key is
    missing or falsy.

    Args:
        docs_with_context: List of pipeline wrapper dicts containing
            ``__doc__`` and ``__root__`` keys; modified in place.
        spec: MongoDB ``$graphLookup`` stage specification dictionary.
        collection: Source collection providing database access and field
            getters/setters.
        evaluator: Expression evaluator used to compute ``startWith`` per
            document.

    Returns:
        list[dict[str, Any]]: The updated ``docs_with_context`` list with the
            collected match array written to ``spec["as"]``.
    """
    from_collection_name = spec.get("from")
    start_with_expr = spec.get("startWith")
    connect_from_field = spec.get("connectFromField")
    connect_to_field = spec.get("connectToField")
    as_field = spec.get("as")
    max_depth = spec.get("maxDepth")
    depth_field = spec.get("depthField")
    restrict_search = spec.get("restrictSearchWithMatch")

    if (
        not all(
            [
                from_collection_name,
                connect_from_field,
                connect_to_field,
                as_field,
            ]
        )
        or start_with_expr is None
    ):
        return docs_with_context

    # Get the target collection
    target_collection = collection.database.get_collection(from_collection_name)

    for dc in docs_with_context:
        doc = dc["__doc__"]

        # 1. Evaluate startWith
        start_val = evaluator._evaluate_operand_python(start_with_expr, doc)

        search_queue: deque[tuple[Any, int]] = deque()
        _enqueue_lookup_values(search_queue, start_val, 0)

        visited_ids: set[Any] = set()
        queried_keys: set[tuple] = set()
        results: list[dict[str, Any]] = []

        # 2. Level-batched breadth-first traversal
        while search_queue:
            depth = search_queue[0][1]
            frontier_vals: list[Any] = []
            while search_queue and search_queue[0][1] == depth:
                current_val, _ = search_queue.popleft()
                if current_val is None:
                    continue
                val_key = _hashable_group_key(current_val)
                if val_key in queried_keys:
                    continue
                queried_keys.add(val_key)
                frontier_vals.append(current_val)

            if not frontier_vals:
                continue

            found_docs: list[dict[str, Any]] = []
            for start_idx in range(
                0, len(frontier_vals), _GRAPH_LOOKUP_IN_BATCH_SIZE
            ):
                chunk = frontier_vals[
                    start_idx : start_idx + _GRAPH_LOOKUP_IN_BATCH_SIZE
                ]
                query: dict[str, Any] = {str(connect_to_field): {"$in": chunk}}
                if restrict_search:
                    query = {"$and": [query, restrict_search]}
                found_docs.extend(target_collection.find(query))

            for found_doc in found_docs:
                doc_id = found_doc.get("_id")
                if doc_id in visited_ids:
                    continue

                visited_ids.add(doc_id)

                # Add depth field if requested
                result_doc = deepcopy(found_doc)
                if depth_field:
                    result_doc[depth_field] = depth

                results.append(result_doc)

                # Check depth limit
                if max_depth is not None and depth >= max_depth:
                    continue

                # Get next values to search
                next_val = target_collection._get_val(
                    found_doc, connect_from_field
                )
                _enqueue_lookup_values(search_queue, next_val, depth + 1)

        # 3. Add results to the document
        collection._set_val(doc, as_field, results)
        dc["__doc__"] = doc

    return docs_with_context
