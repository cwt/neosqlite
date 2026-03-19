"""
Python implementation of MongoDB $graphLookup aggregation stage.
"""

from copy import deepcopy
from typing import Any, Dict, List, Set


def process_graph_lookup(
    docs_with_context: List[Dict[str, Any]],
    spec: Dict[str, Any],
    collection: Any,
    evaluator: Any,
) -> List[Dict[str, Any]]:
    """
    Python fallback implementation of $graphLookup.

    Args:
        docs_with_context: List of documents with context (__doc__, __root__)
        spec: $graphLookup specification
        collection: Current collection instance
        evaluator: ExprEvaluator for expression evaluation

    Returns:
        Updated docs_with_context
    """
    from_collection_name = spec.get("from")
    start_with_expr = spec.get("startWith")
    connect_from_field = spec.get("connectFromField")
    connect_to_field = spec.get("connectToField")
    as_field = spec.get("as")
    max_depth = spec.get("maxDepth")
    depth_field = spec.get("depthField")
    restrict_search = spec.get("restrictSearchWithMatch")

    if not all(
        [
            from_collection_name,
            start_with_expr,
            connect_from_field,
            connect_to_field,
            as_field,
        ]
    ):
        return docs_with_context

    # Get the target collection
    target_collection = collection.database.get_collection(from_collection_name)

    for dc in docs_with_context:
        doc = dc["__doc__"]

        # 1. Evaluate startWith
        start_val = evaluator._evaluate_operand_python(start_with_expr, doc)

        # start_val can be a single value or an array
        if not isinstance(start_val, list):
            search_queue = [(start_val, 0)]
        else:
            search_queue = [(v, 0) for v in start_val]

        visited_ids: Set[Any] = set()
        results: List[Dict[str, Any]] = []

        # 2. Recursive search
        while search_queue:
            current_val, depth = search_queue.pop(0)

            if current_val is None:
                continue

            # Find documents in target collection where connectToField == current_val
            query = {connect_to_field: current_val}
            if restrict_search:
                # Merge with restrict_search
                query = {"$and": [query, restrict_search]}

            found_docs = list(target_collection.find(query))

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
                if isinstance(next_val, list):
                    for v in next_val:
                        search_queue.append((v, depth + 1))
                else:
                    search_queue.append((next_val, depth + 1))

        # 3. Add results to the document
        collection._set_val(doc, as_field, results)
        dc["__doc__"] = doc

    return docs_with_context
