from .. import query_operators
from ..binary import Binary
from ..exceptions import MalformedDocument, MalformedQueryException
from .cursor import DESCENDING
from copy import deepcopy
from neosqlite.collection.json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_dumps_for_sql,
)
from neosqlite.collection.text_search import unified_text_search
from typing import Any, Dict, List

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore

# Global flag to force fallback - for benchmarking and debugging
_FORCE_FALLBACK = False


def _convert_bytes_to_binary(obj: Any) -> Any:
    """
    Recursively convert bytes objects to Binary objects in a document.

    This function traverses a document structure (dict, list, etc.) and converts
    any bytes objects to Binary objects, which can be properly serialized to JSON.
    Existing Binary objects are left unchanged to preserve their subtype information.

    Args:
        obj: The object to process (can be dict, list, bytes, Binary, or other types)

    Returns:
        The processed object with bytes converted to Binary objects
    """
    # Check for Binary first, since Binary inherits from bytes
    if isinstance(obj, Binary):
        # Leave Binary objects unchanged to preserve subtype information
        return obj
    elif isinstance(obj, bytes):
        return Binary(obj)
    elif isinstance(obj, dict):
        return {
            key: _convert_bytes_to_binary(value) for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [_convert_bytes_to_binary(item) for item in obj]
    else:
        return obj


def set_force_fallback(force=True):
    """Set global flag to force all aggregation queries to use Python fallback.

    This function is useful for benchmarking and debugging to compare performance
    between the optimized SQL path and the Python fallback path.

    Args:
        force (bool): If True, forces all aggregation queries to use Python fallback.
                     If False, allows normal optimization behavior.
    """
    global _FORCE_FALLBACK
    _FORCE_FALLBACK = force


def get_force_fallback():
    """Get the current state of the force fallback flag.

    Returns:
        bool: True if fallback is forced, False otherwise.
    """
    global _FORCE_FALLBACK
    return _FORCE_FALLBACK


class QueryHelper:
    """
    A helper class for the QueryEngine that provides methods for building queries,
    performing updates, and processing aggregation pipelines.

    This class contains the core logic for translating MongoDB-like queries and
    operations into SQL statements that can be executed against the SQLite database.
    It handles both simple operations that can be done directly with SQL JSON
    functions and complex operations that require Python-based processing.
    """

    def __init__(self, collection):
        """
        Initialize the QueryHelper with a collection.

        Args:
            collection: The collection instance this QueryHelper will operate on.
        """
        self.collection = collection

    def _internal_insert(self, document: Dict[str, Any]) -> int:
        """
        Inserts a document into the collection and returns the inserted document's _id.

        Args:
            document (dict): The document to insert. Must be a dictionary.

        Returns:
            int: The _id of the inserted document.
        """
        if not isinstance(document, dict):
            raise MalformedDocument(
                f"document must be a dictionary, not a {type(document)}"
            )

        doc_to_insert = deepcopy(document)
        doc_to_insert.pop("_id", None)

        # Convert any bytes objects to Binary objects for proper JSON serialization
        doc_to_insert = _convert_bytes_to_binary(doc_to_insert)

        cursor = self.collection.db.execute(
            f"INSERT INTO {self.collection.name}(data) VALUES (?)",
            (neosqlite_json_dumps(doc_to_insert),),
        )
        inserted_id = cursor.lastrowid

        if inserted_id is None:
            raise sqlite3.Error("Failed to get last row id.")

        document["_id"] = inserted_id
        return inserted_id

    # --- Helper Methods ---
    def _internal_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
    ):
        """
        Helper method for updating documents.

        Attempts to use SQL-based updates for simple operations, falling back to
        Python-based updates for complex operations.

        Args:
            doc_id (int): The ID of the document to update.
            update_spec (Dict[str, Any]): The update specification.
            original_doc (Dict[str, Any]): The original document before the update.

        Returns:
            Dict[str, Any]: The updated document.
        """
        # Try to use SQL-based updates for simple operations
        if self._can_use_sql_updates(update_spec, doc_id):
            return self._perform_sql_update(doc_id, update_spec)
        else:
            # Fall back to Python-based updates for complex operations
            return self._perform_python_update(
                doc_id, update_spec, original_doc
            )

    def _can_use_sql_updates(
        self,
        update_spec: Dict[str, Any],
        doc_id: int,
    ) -> bool:
        """
        Check if all operations in the update spec can be handled with SQL.

        This method determines whether the update operations can be efficiently
        executed using SQL directly, which allows for better performance compared
        to iterating over each document and applying updates in Python.

        Args:
            update_spec (Dict[str, Any]): The update operations to be checked.
            doc_id (int): The document ID, which is used to determine if the update
                          is an upsert.

        Returns:
            bool: True if all operations can be handled with SQL, False otherwise.
        """
        # Only handle operations that can be done purely with SQL
        supported_ops = {"$set", "$unset", "$inc", "$mul", "$min", "$max"}
        # Also check that doc_id is not 0 (which indicates an upsert)
        # Disable SQL updates for documents containing Binary objects
        has_binary_values = any(
            isinstance(val, bytes) and hasattr(val, "encode_for_storage")
            for op in update_spec.values()
            if isinstance(op, dict)
            for val in op.values()
        )

        return (
            doc_id != 0
            and not has_binary_values
            and all(op in supported_ops for op in update_spec.keys())
        )

    def _perform_sql_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform update operations using SQL JSON functions.

        This method builds SQL clauses for updating document fields based on the
        provided update specification. It supports both `$set` and `$unset` operations
        using SQLite's `json_set` and `json_remove` functions, respectively. The
        method then executes the SQL commands to apply the updates and fetches
        the updated document from the database.

        Args:
            doc_id (int): The ID of the document to be updated.
            update_spec (Dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.

        Returns:
            Dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the
                          update process.
        """
        set_clauses = []
        set_params = []
        unset_clauses = []
        unset_params = []

        # Build SQL update clauses for each operation
        for op, value in update_spec.items():
            clauses, params = self._build_sql_update_clause(op, value)
            if clauses:
                if op == "$unset":
                    unset_clauses.extend(clauses)
                    unset_params.extend(params)
                else:
                    set_clauses.extend(clauses)
                    set_params.extend(params)

        # Execute the SQL updates
        sql_params = []
        if unset_clauses:
            # Handle $unset operations with json_remove
            cmd = (
                f"UPDATE {self.collection.name} "
                f"SET data = json_remove(data, {', '.join(unset_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = unset_params + [doc_id]
            self.collection.db.execute(cmd, sql_params)

        if set_clauses:
            # Handle other operations with json_set
            cmd = (
                f"UPDATE {self.collection.name} "
                f"SET data = json_set(data, {', '.join(set_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = set_params + [doc_id]
            cursor = self.collection.db.execute(cmd, sql_params)

            # Check if any rows were updated
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        elif not unset_clauses:
            # No operations to perform
            raise RuntimeError("No valid operations to perform")

        # Fetch and return the updated document
        if row := self.collection.db.execute(
            f"SELECT data FROM {self.collection.name} WHERE id = ?", (doc_id,)
        ).fetchone():
            return self.collection._load(doc_id, row[0])

        # This shouldn't happen, but just in case
        raise RuntimeError("Failed to fetch updated document")

    def _build_update_clause(
        self,
        update: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Build the SQL update clause based on the provided update operations.

        Args:
            update (Dict[str, Any]): A dictionary containing update operations.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL update clause
                                          and parameters, or None if no update
                                          clauses are generated.
        """
        set_clauses = []
        params = []

        for op, value in update.items():
            match op:
                case "$set":
                    for field, field_val in value.items():
                        set_clauses.append(f"'$.{field}', ?")
                        params.append(field_val)
                case "$inc":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, json_extract(data, {path}) + ?"
                        )
                        params.append(field_val)
                case "$mul":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, json_extract(data, {path}) * ?"
                        )
                        params.append(field_val)
                case "$min":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, min(json_extract(data, {path}), ?)"
                        )
                        params.append(field_val)
                case "$max":
                    for field, field_val in value.items():
                        path = f"'$.{field}'"
                        set_clauses.append(
                            f"{path}, max(json_extract(data, {path}), ?)"
                        )
                        params.append(field_val)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        path = f"'$.{field}'"
                        set_clauses.append(path)
                    # json_remove has a different syntax
                    if set_clauses:
                        return (
                            f"data = json_remove(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        # No fields to unset
                        return None
                case "$rename":
                    # $rename is complex to do in SQL,
                    # so we'll fall back to the Python implementation
                    return None
                case _:
                    return None  # Fallback for unsupported operators

        if not set_clauses:
            return None

        # For $unset, we already returned above
        if "$unset" not in update:
            return f"data = json_set(data, {', '.join(set_clauses)})", params
        else:
            # This case should have been handled above
            return None

    def _build_sql_update_clause(
        self,
        op: str,
        value: Any,
    ) -> tuple[List[str], List[Any]]:
        """
        Build SQL update clause for a single operation.

        Args:
            op (str): The update operation, such as "$set", "$inc", "$mul", etc.
            value (Any): The value associated with the update operation.

        Returns:
            tuple[List[str], List[Any]]: A tuple containing the SQL update clauses
                                         and parameters.
        """
        clauses = []
        params = []

        match op:
            case "$set":
                for field, field_val in value.items():
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(f"'$.{field}', json(?)")
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(f"'$.{field}', ?")
                        params.append(converted_val)
            case "$inc":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{path}, json_extract(data, {path}) + json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{path}, json_extract(data, {path}) + ?"
                        )
                        params.append(converted_val)
            case "$mul":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{path}, json_extract(data, {path}) * json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{path}, json_extract(data, {path}) * ?"
                        )
                        params.append(converted_val)
            case "$min":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(
                        f"{path}, min(json_extract(data, {path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{path}, min(json_extract(data, {path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$max":
                for field, field_val in value.items():
                    path = f"'$.{field}'"
                    clauses.append(
                        f"{path}, max(json_extract(data, {path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{path}, max(json_extract(data, {path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$unset":
                # For $unset, we use json_remove
                for field in value:
                    path = f"'$.{field}'"
                    clauses.append(path)

        return clauses, params

    def _perform_python_update(
        self,
        doc_id: int,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform update operations using Python-based logic.

        Args:
            doc_id (int): The document ID of the document to update.
            update_spec (Dict[str, Any]): A dictionary specifying the update
                                          operations to perform.
            original_doc (Dict[str, Any]): The original document before applying
                                           the updates.

        Returns:
            Dict[str, Any]: The updated document.
        """
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            match op:
                case "$set":
                    doc_to_update.update(value)
                case "$unset":
                    for k in value:
                        doc_to_update.pop(k, None)
                case "$inc":
                    for k, v in value.items():
                        doc_to_update[k] = doc_to_update.get(k, 0) + v
                case "$push":
                    for k, v in value.items():
                        doc_to_update.setdefault(k, []).append(v)
                case "$pull":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[k] = [
                                item for item in doc_to_update[k] if item != v
                            ]
                case "$pop":
                    for k, v in value.items():
                        if v == 1:
                            doc_to_update.get(k, []).pop()
                        elif v == -1:
                            doc_to_update.get(k, []).pop(0)
                case "$rename":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[v] = doc_to_update.pop(k)
                case "$mul":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[k] *= v
                case "$min":
                    for k, v in value.items():
                        if k not in doc_to_update or doc_to_update[k] > v:
                            doc_to_update[k] = v
                case "$max":
                    for k, v in value.items():
                        if k not in doc_to_update or doc_to_update[k] < v:
                            doc_to_update[k] = v
                case _:
                    raise MalformedQueryException(
                        f"Update operator '{op}' not supported"
                    )

        # If this is an upsert (doc_id == 0), we don't update the database
        # We just return the updated document for insertion by the caller
        if doc_id != 0:
            self.collection.db.execute(
                f"UPDATE {self.collection.name} SET data = ? WHERE id = ?",
                (neosqlite_json_dumps(doc_to_update), doc_id),
            )

        return doc_to_update

    def _internal_replace(self, doc_id: int, replacement: Dict[str, Any]):
        """
        Replace the document with the specified ID with a new document.

        Args:
            doc_id (int): The ID of the document to replace.
            replacement (Dict[str, Any]): The new document to replace the existing one.
        """
        self.collection.db.execute(
            f"UPDATE {self.collection.name} SET data = ? WHERE id = ?",
            (neosqlite_json_dumps(replacement), doc_id),
        )

    def _internal_delete(self, doc_id: int):
        """
        Deletes a document from the collection based on the document ID.

        Args:
            doc_id (int): The ID of the document to delete.
        """
        self.collection.db.execute(
            f"DELETE FROM {self.collection.name} WHERE id = ?", (doc_id,)
        )

    def _is_text_search_query(self, query: Dict[str, Any]) -> bool:
        """
        Check if the query is a text search query (contains $text operator).

        Args:
            query: The query to check.

        Returns:
            True if the query is a text search query, False otherwise.
        """
        return "$text" in query

    def _build_text_search_query(
        self, query: Dict[str, Any]
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL query for text search using FTS5.

        Args:
            query: A dictionary representing the text search query with $text operator.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause
                                          and a list of parameters, or None if the
                                          query is invalid or FTS index doesn't exist.
        """
        if "$text" not in query:
            return None

        text_query = query["$text"]
        if not isinstance(text_query, dict) or "$search" not in text_query:
            return None

        search_term = text_query["$search"]
        if not isinstance(search_term, str):
            return None

        # Find FTS tables for this collection
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
            (f"{self.collection.name}_%_fts",),
        )
        fts_tables = cursor.fetchall()

        if not fts_tables:
            return None

        # Build UNION query to search across ALL FTS tables
        subqueries = []
        params = []

        for (fts_table_name,) in fts_tables:
            # Extract field name from FTS table name (collection_field_fts -> field)
            index_name = fts_table_name[
                len(f"{self.collection.name}_") : -4
            ]  # Remove collection_ prefix and _fts suffix

            # Add subquery for this FTS table
            subqueries.append(
                f"SELECT rowid FROM {fts_table_name} WHERE {index_name} MATCH ?"
            )
            params.append(search_term.lower())

        # Combine all subqueries with UNION to get documents matching in ANY FTS index
        union_query = " UNION ".join(subqueries)

        # Build the FTS query
        where_clause = f"""
        WHERE id IN ({union_query})
        """
        return where_clause, params

    def _get_indexed_fields(self) -> List[str]:
        """
        Get a list of indexed fields for this collection.

        Returns:
            List[str]: A list of field names that have indexes.
        """
        # Get indexes that match our naming convention
        cmd = (
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE ?"
        )
        like_pattern = f"idx_{self.collection.name}_%"
        indexes = self.collection.db.execute(cmd, (like_pattern,)).fetchall()

        indexed_fields = []
        for idx in indexes:
            # Extract key name from index name (idx_collection_key -> key)
            key_name = idx[0][len(f"idx_{self.collection.name}_") :]
            # Convert underscores back to dots for nested keys
            key_name = key_name.replace("_", ".")
            indexed_fields.append(key_name)

        return indexed_fields

    def _estimate_result_size(self, pipeline: List[Dict[str, Any]]) -> int:
        """
        Estimate the size of the aggregation result in bytes.

        This method analyzes the pipeline to estimate the size of the result set.

        Args:
            pipeline: The aggregation pipeline to analyze

        Returns:
            Estimated size in bytes
        """
        # Get the base collection size
        base_count = self.collection.estimated_document_count()

        # Apply pipeline stages to estimate result size
        estimated_count = base_count
        estimated_avg_doc_size = 1024  # Default estimate of 1KB per document

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            match stage_name:
                case "$match":
                    # Matches typically reduce the result set
                    # For now, we'll use a rough estimate
                    estimated_count = max(1, int(estimated_count * 0.5))
                case "$limit":
                    limit_count = stage["$limit"]
                    estimated_count = min(estimated_count, limit_count)
                case "$skip":
                    skip_count = stage["$skip"]
                    estimated_count = max(0, estimated_count - skip_count)
                case "$unwind":
                    # Unwind operations can multiply the result set
                    # This is a very rough estimate
                    estimated_count = (
                        estimated_count * 3
                    )  # Assume 3 elements per array on average
                case "$group":
                    # Group operations typically reduce the result set
                    # This is a very rough estimate
                    estimated_count = max(1, int(estimated_count * 0.1))
                case _:
                    # For other operations, we'll assume they don't significantly change the size
                    pass

        # Apply some limits to prevent extreme estimates
        estimated_count = min(
            estimated_count, base_count * 10
        )  # Cap at 10x the base count
        estimated_count = max(estimated_count, 0)  # Ensure non-negative

        return estimated_count * estimated_avg_doc_size

    def _estimate_query_cost(self, query: Dict[str, Any]) -> float:
        """
        Estimate the cost of executing a query based on index availability.

        Lower cost values indicate more efficient queries.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.

        Returns:
            float: Estimated cost of the query (lower is better).
        """
        # Get indexed fields
        indexed_fields = self._get_indexed_fields()

        # Base cost
        cost = 1.0

        # Check if we can use indexes for any fields in the query
        for field, value in query.items():
            if field in ("$and", "$or", "$nor", "$not"):
                # Handle logical operators recursively
                if isinstance(value, list):
                    for subquery in value:
                        if isinstance(subquery, dict):
                            cost *= self._estimate_query_cost(subquery)
                elif isinstance(value, dict):
                    cost *= self._estimate_query_cost(value)
            elif field == "_id":
                # _id field is always indexed (it's a column)
                cost *= 0.1  # Very low cost for _id queries
            elif field in indexed_fields:
                # Field is indexed, reduce cost
                cost *= 0.3  # Lower cost when using an index
            else:
                # Field is not indexed, increase cost
                cost *= 1.0  # No change for non-indexed fields

        return cost

    def _estimate_pipeline_cost(self, pipeline: List[Dict[str, Any]]) -> float:
        """
        Estimate the total cost of executing an aggregation pipeline.

        Lower cost values indicate more efficient pipelines.
        This method considers data flow - earlier stages affect more documents.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages.

        Returns:
            float: Estimated cost of the pipeline (lower is better).
        """
        total_cost = 0.0
        cumulative_multiplier = (
            1.0  # Represents how much data flows through each stage
        )

        for i, stage in enumerate(pipeline):
            stage_name = next(iter(stage.keys()))
            stage_cost = 0.0

            match stage_name:
                case "$match":
                    # Estimate cost of match stage
                    query = stage["$match"]
                    stage_cost = self._estimate_query_cost(query)

                    # Matches early in the pipeline are more beneficial because they reduce
                    # the amount of data flowing to later stages
                    stage_cost *= cumulative_multiplier

                    # Update data flow multiplier based on selectivity
                    # Assume matches reduce data by 50% on average
                    cumulative_multiplier *= 0.5

                case "$sort":
                    # Sort operations have moderate cost, weighted by data volume
                    stage_cost = 1.0 * cumulative_multiplier
                case "$skip":
                    # Skip operations have low cost
                    stage_cost = 0.1 * cumulative_multiplier
                case "$limit":
                    # Limit operations have low cost but dramatically reduce data flow
                    stage_cost = 0.1 * cumulative_multiplier

                    # Limits significantly reduce data flow to subsequent stages
                    cumulative_multiplier *= (
                        0.1  # Assume limits reduce data by 90%
                    )

                case "$group":
                    # Group operations have high cost (require processing all data)
                    stage_cost = 5.0 * cumulative_multiplier

                    # Groups typically reduce data significantly
                    cumulative_multiplier *= (
                        0.2  # Assume groups reduce data by 80%
                    )

                case "$unwind":
                    # Unwind operations multiply the data size, increasing cost and data flow
                    stage_cost = 2.0 * cumulative_multiplier

                    # Unwinds increase data volume (assume 5x increase on average)
                    cumulative_multiplier *= 5.0

                case "$lookup":
                    # Lookup operations have high cost (joins)
                    stage_cost = 3.0 * cumulative_multiplier

                    # Lookups may increase data slightly
                    cumulative_multiplier *= 1.2

                case _:
                    # Unknown operations have moderate cost
                    stage_cost = 1.5 * cumulative_multiplier

                    # Assume unknown operations don't significantly change data volume
                    # cumulative_multiplier stays the same

            total_cost += stage_cost

        return total_cost

    def _optimize_match_pushdown(
        self, pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Optimize pipeline by pushing $match stages down to earlier positions when beneficial.

        This optimization moves $match stages earlier in the pipeline when they can
        filter data before expensive operations like $unwind or $group.

        Args:
            pipeline (List[Dict[str, Any]]): The pipeline stages to optimize.

        Returns:
            List[Dict[str, Any]]: The optimized pipeline.
        """
        if len(pipeline) < 2:
            return pipeline

        # Look for patterns where we can push matches down
        optimized = pipeline.copy()

        # Find all $match stages
        match_stages = []
        other_stages = []

        for i, stage in enumerate(optimized):
            stage_name = next(iter(stage.keys()))
            if stage_name == "$match":
                match_stages.append((i, stage))
            else:
                other_stages.append((i, stage))

        # If we have matches and expensive operations, consider reordering
        expensive_ops = {"$unwind", "$group", "$lookup"}
        has_expensive_ops = any(
            next(iter(stage.keys())) in expensive_ops
            for _, stage in other_stages
        )

        if match_stages and has_expensive_ops:
            # Move matches to the front to filter early
            match_stage_items = [stage for _, stage in match_stages]
            other_stage_items = [stage for _, stage in other_stages]
            return match_stage_items + other_stage_items

        return optimized

    def _build_simple_where_clause(
        self,
        query: Dict[str, Any],
    ) -> tuple[str, List[Any]] | None:
        """
        Builds a SQL WHERE clause for simple queries that can be handled with json_extract.

        This method constructs a SQL WHERE clause based on the query provided.
        It handles simple equality checks and query operators like $eq, $gt, $lt,
        etc. for fields stored in JSON data. For more complex queries, it returns
        None, indicating that a Python-based method should be used instead.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL WHERE clause
                                          and a list of parameters, or None if the
                                          query contains unsupported operators.
        """
        # Handle text search queries separately
        if self._is_text_search_query(query):
            return self._build_text_search_query(query)

        clauses = []
        params = []

        for field, value in query.items():
            # Handle logical operators by falling back to Python processing
            # This is more robust than trying to build complex SQL queries
            if field in ("$and", "$or", "$nor", "$not"):
                return (
                    None  # Fall back to Python processing for logical operators
                )

            elif field == "_id":
                # Handle _id field specially since it's stored as a column,
                # not in the JSON data
                clauses.append(f"{self.collection.name}.id = ?")
                params.append(value)
                continue

            else:
                # For all fields (including nested ones), use json_extract to get
                # values from the JSON data.

                # Convert dot notation to JSON path notation.
                # (e.g., "profile.age" -> "$.profile.age")
                json_path = f"'$.{field}'"

                if isinstance(value, dict):
                    # Handle query operators like $eq, $gt, $lt, etc.
                    clause, clause_params = self._build_operator_clause(
                        json_path, value
                    )
                    if clause is None:
                        return None  # Unsupported operator, fallback to Python
                    clauses.append(clause)
                    params.extend(clause_params)
                else:
                    # Simple equality check
                    clauses.append(f"json_extract(data, {json_path}) = ?")
                    # Serialize Binary objects for SQL comparisons using compact format
                    if isinstance(value, bytes) and hasattr(
                        value, "encode_for_storage"
                    ):
                        params.append(neosqlite_json_dumps_for_sql(value))
                    else:
                        params.append(value)

        if not clauses:
            return "", []
        return "WHERE " + " AND ".join(clauses), params

    def _build_operator_clause(
        self,
        json_path: str,
        operators: Dict[str, Any],
    ) -> tuple[str | None, List[Any]]:
        """
        Builds a SQL clause for query operators.

        This method constructs a SQL clause based on the provided operators for
        a specific JSON path. It handles various operators like $eq, $gt, $lt, etc.,
        and returns a tuple containing the SQL clause and a list of parameters.
        If an unsupported operator is encountered, it returns None, indicating
        that a fallback to Python processing is needed.

        Args:
            json_path (str): The JSON path to extract the value from.
            operators (Dict[str, Any]): A dictionary of operators and their values.

        Returns:
            tuple[str | None, List[Any]]: A tuple containing the SQL clause and
                                          parameters. If the operator is unsupported,
                                          returns (None, []).
        """
        for op, op_val in operators.items():
            # Serialize Binary objects for SQL comparisons using compact format
            if isinstance(op_val, bytes) and hasattr(
                op_val, "encode_for_storage"
            ):
                op_val = neosqlite_json_dumps_for_sql(op_val)

            match op:
                case "$eq":
                    return f"json_extract(data, {json_path}) = ?", [op_val]
                case "$gt":
                    return f"json_extract(data, {json_path}) > ?", [op_val]
                case "$lt":
                    return f"json_extract(data, {json_path}) < ?", [op_val]
                case "$gte":
                    return f"json_extract(data, {json_path}) >= ?", [op_val]
                case "$lte":
                    return f"json_extract(data, {json_path}) <= ?", [op_val]
                case "$ne":
                    return f"json_extract(data, {json_path}) != ?", [op_val]
                case "$in":
                    placeholders = ", ".join("?" for _ in op_val)
                    return (
                        f"json_extract(data, {json_path}) IN ({placeholders})",
                        op_val,
                    )
                case "$nin":
                    placeholders = ", ".join("?" for _ in op_val)
                    return (
                        f"json_extract(data, {json_path}) NOT IN ({placeholders})",
                        op_val,
                    )
                case "$exists":
                    # Handle boolean value for $exists
                    if op_val is True:
                        return (
                            f"json_extract(data, {json_path}) IS NOT NULL",
                            [],
                        )
                    elif op_val is False:
                        return f"json_extract(data, {json_path}) IS NULL", []
                    else:
                        # Invalid value for $exists, fallback to Python
                        return None, []
                case "$mod":
                    # Handle [divisor, remainder] array
                    if isinstance(op_val, (list, tuple)) and len(op_val) == 2:
                        divisor, remainder = op_val
                        return f"json_extract(data, {json_path}) % ? = ?", [
                            divisor,
                            remainder,
                        ]
                    else:
                        # Invalid format for $mod, fallback to Python
                        return None, []
                case "$size":
                    # Handle array size comparison
                    if isinstance(op_val, int):
                        return (
                            f"json_array_length(json_extract(data, {json_path})) = ?",
                            [op_val],
                        )
                    else:
                        # Invalid value for $size, fallback to Python
                        return None, []
                case "$contains":
                    # Handle case-insensitive substring search
                    if isinstance(op_val, str):
                        return (
                            f"lower(json_extract(data, {json_path})) LIKE ?",
                            [f"%{op_val.lower()}%"],
                        )
                    else:
                        # Invalid value for $contains, fallback to Python
                        return None, []
                case _:
                    # Unsupported operator, fallback to Python
                    return None, []

        # This shouldn't happen, but just in case
        return None, []

    def _apply_query(
        self,
        query: Dict[str, Any],
        document: Dict[str, Any],
    ) -> bool:
        """
        Applies a query to a document to determine if it matches the query criteria.

        Handles logical operators ($and, $or, $nor, $not) and nested field paths.
        Processes both simple equality checks and complex query operators.

        Args:
            query (Dict[str, Any]): A dictionary representing the query criteria.
            document (Dict[str, Any]): The document to apply the query to.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        if document is None:
            return False
        matches: List[bool] = []

        def reapply(q: Dict[str, Any]) -> bool:
            """
            Recursively apply the query to the document to determine if it matches
            the query criteria.

            Args:
                q (Dict[str, Any]): The query to apply.
                document (Dict[str, Any]): The document to apply the query to.

            Returns:
                bool: True if the document matches the query, False otherwise.
            """
            return self._apply_query(q, document)

        for field, value in query.items():
            match field:
                case "$text":
                    # Handle $text operator in Python fallback
                    # This is a simplified implementation that just does basic string matching
                    if isinstance(value, dict) and "$search" in value:
                        search_term = value["$search"]
                        if isinstance(search_term, str):
                            # Find FTS tables for this collection to determine which fields are indexed
                            cursor = self.collection.db.execute(
                                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
                                (f"{self.collection.name}_%_fts",),
                            )
                            fts_tables = cursor.fetchall()

                            # Check each FTS-indexed field for matches
                            for fts_table in fts_tables:
                                fts_table_name = fts_table[0]
                                # Extract field name from FTS table name
                                # (collection_field_fts -> field)
                                index_name = fts_table_name[
                                    len(f"{self.collection.name}_") : -4
                                ]  # Remove collection_ prefix and _fts suffix
                                # Convert underscores back to dots for nested keys
                                field_name = index_name.replace("_", ".")
                                # Check if this field has content that matches the search term
                                field_value = self.collection._get_val(
                                    document, field_name
                                )
                                if field_value and isinstance(field_value, str):
                                    # Simple case-insensitive substring search
                                    if (
                                        search_term.lower()
                                        in field_value.lower()
                                    ):
                                        matches.append(True)
                                        break
                            else:
                                # If no FTS indexes exist, use enhanced text search on all fields
                                # This provides better international character support and diacritic-insensitive matching
                                if unified_text_search(document, search_term):
                                    matches.append(True)
                                else:
                                    matches.append(False)
                        else:
                            matches.append(False)
                    else:
                        matches.append(False)
                case "$and":
                    matches.append(all(map(reapply, value)))
                case "$or":
                    matches.append(any(map(reapply, value)))
                case "$nor":
                    matches.append(not any(map(reapply, value)))
                case "$not":
                    matches.append(not self._apply_query(value, document))
                case _:
                    if isinstance(value, dict):
                        for operator, arg in value.items():
                            if not self._get_operator_fn(operator)(
                                field, arg, document
                            ):
                                matches.append(False)
                                break
                        else:
                            matches.append(True)
                    else:
                        doc_value: Dict[str, Any] | None = document
                        if doc_value and field in doc_value:
                            doc_value = doc_value.get(field, None)
                        else:
                            for path in field.split("."):
                                if not isinstance(doc_value, dict):
                                    break
                                doc_value = doc_value.get(path, None)
                        if value != doc_value:
                            matches.append(False)
        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """
        Retrieve the function associated with the given operator from the
        query_operators module.

        Args:
            op (str): The operator string, which should start with a '$' prefix.

        Returns:
            Any: The function corresponding to the operator.

        Raises:
            MalformedQueryException: If the operator does not start with '$'.
            MalformedQueryException: If the operator is not currently implemented.
        """
        if not op.startswith("$"):
            raise MalformedQueryException(
                f"Operator '{op}' is not a valid query operation"
            )
        try:
            return getattr(query_operators, op.replace("$", "_"))
        except AttributeError:
            raise MalformedQueryException(
                f"Operator '{op}' is not currently implemented"
            )

    def _reorder_pipeline_for_indexes(
        self, pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Reorder pipeline stages to optimize performance based on index availability.

        Moves $match stages with indexed fields to the beginning of the pipeline
        to take advantage of index-based filtering.

        Args:
            pipeline (List[Dict[str, Any]]): The original pipeline stages.

        Returns:
            List[Dict[str, Any]]: The reordered pipeline stages.
        """
        if not pipeline:
            return pipeline

        # Get indexed fields
        indexed_fields = set(self._get_indexed_fields())

        # Separate match stages with indexed fields from others
        indexed_matches = []
        other_stages = []

        for stage in pipeline:
            stage_name = next(iter(stage.keys()))
            if stage_name == "$match":
                # Check if this match uses indexed fields
                match_query = stage["$match"]
                has_indexed_field = False

                # Simple check for direct field references
                for field in match_query.keys():
                    if field in indexed_fields or field == "_id":
                        has_indexed_field = True
                        break

                # For logical operators, check nested fields
                if not has_indexed_field:
                    for field, value in match_query.items():
                        if field in ("$and", "$or") and isinstance(value, list):
                            for condition in value:
                                if isinstance(condition, dict):
                                    for subfield in condition.keys():
                                        if (
                                            subfield in indexed_fields
                                            or subfield == "_id"
                                        ):
                                            has_indexed_field = True
                                            break
                                    if has_indexed_field:
                                        break
                        elif field == "_id":
                            has_indexed_field = True

                if has_indexed_field:
                    indexed_matches.append(stage)
                else:
                    other_stages.append(stage)
            else:
                other_stages.append(stage)

        # Return reordered pipeline: indexed matches first, then other stages
        return indexed_matches + other_stages

    def _build_aggregation_query(
        self,
        pipeline: List[Dict[str, Any]],
    ) -> tuple[str, List[Any], List[str] | None] | None:
        """
        Builds a SQL query for the given MongoDB-like aggregation pipeline.

        This method constructs a SQL query based on the stages provided in the
        aggregation pipeline. It currently handles $match, $sort, $skip,
        and $limit stages, while $group stages are handled in Python. The method
        returns a tuple containing the SQL command and a list of parameters.

        Args:
            pipeline (List[Dict[str, Any]]): A list of aggregation pipeline stages.

        Returns:
            tuple[str, List[Any]] | None: A tuple containing the SQL command and
                                          a list of parameters, or None if the
                                          pipeline contains unsupported stages
                                          or complex queries.
        """
        # Check if we should force fallback for benchmarking/debugging
        global _FORCE_FALLBACK
        if _FORCE_FALLBACK:
            return None  # Force fallback to Python implementation

        # Try to optimize the pipeline by reordering for better index usage
        optimized_pipeline = self._reorder_pipeline_for_indexes(pipeline)

        # Estimate costs for both original and optimized pipelines
        original_cost = self._estimate_pipeline_cost(pipeline)
        optimized_cost = self._estimate_pipeline_cost(optimized_pipeline)

        # Use the better pipeline based on cost estimation
        if optimized_cost < original_cost:
            # Use optimized pipeline
            effective_pipeline = optimized_pipeline
        else:
            # Use original pipeline
            effective_pipeline = pipeline

        # Additional optimization: Check if we can push match filters down into SQL operations
        effective_pipeline = self._optimize_match_pushdown(effective_pipeline)

        where_clause = ""
        params: List[Any] = []
        order_by = ""
        limit = ""
        offset = ""
        group_by = ""
        select_clause = "SELECT id, data"
        output_fields: List[str] | None = None

        for i, stage in enumerate(effective_pipeline):
            stage_name = next(iter(stage.keys()))
            match stage_name:
                case "$match":
                    query = stage["$match"]
                    where_result = self._build_simple_where_clause(query)
                    if where_result is None:
                        return None  # Fallback for complex queries
                    where_clause, params = where_result
                case "$sort":
                    sort_spec = stage["$sort"]
                    sort_clauses = []
                    for key, direction in sort_spec.items():
                        # When sorting after a group stage, we sort by the output field name
                        if group_by:
                            sort_clauses.append(
                                f"{key} {'DESC' if direction == DESCENDING else 'ASC'}"
                            )
                        else:
                            sort_clauses.append(
                                f"json_extract(data, '$.{key}') "
                                f"{'DESC' if direction == DESCENDING else 'ASC'}"
                            )
                    order_by = "ORDER BY " + ", ".join(sort_clauses)
                case "$skip":
                    count = stage["$skip"]
                    offset = f"OFFSET {count}"
                case "$limit":
                    count = stage["$limit"]
                    limit = f"LIMIT {count}"
                case "$group":
                    # Check if this is a $unwind + $group pattern we can optimize
                    optimization_result = self._optimize_unwind_group_pattern(
                        i, pipeline
                    )
                    if optimization_result is not None:
                        return optimization_result

                    # A group stage must be the first stage or after a match stage
                    if i > 1 or (i == 1 and "$match" not in pipeline[0]):
                        return None
                    group_spec = stage["$group"]
                    group_result = self._build_group_query(group_spec)
                    if group_result is None:
                        return None
                    select_clause, group_by, output_fields = group_result
                case "$unwind":
                    # Check if this is part of an $unwind + $group pattern we can optimize
                    # Case 1: $unwind is first stage followed by $group
                    if i == 0 and len(pipeline) > 1 and "$group" in pipeline[1]:
                        # $unwind followed by $group - try to optimize with SQL
                        group_stage = pipeline[1]["$group"]
                        unwind_field = stage["$unwind"]

                        if (
                            isinstance(unwind_field, str)
                            and unwind_field.startswith("$")
                            and isinstance(group_stage.get("_id"), str)
                            and group_stage.get("_id").startswith("$")
                        ):

                            unwind_field_name = unwind_field[
                                1:
                            ]  # Remove leading $
                            group_id_field = group_stage["_id"][
                                1:
                            ]  # Remove leading $

                            # Check if we can handle this specific group operation
                            can_optimize = True
                            select_expressions = []
                            output_fields = ["_id"]

                            # Handle _id field
                            if group_id_field == unwind_field_name:
                                # Grouping by the unwound field
                                select_expressions.append("je.value AS _id")
                                group_by_clause = "GROUP BY je.value"
                            else:
                                # Grouping by another field
                                select_expressions.append(
                                    f"json_extract({self.collection.name}.data, '$.{group_id_field}') AS _id"
                                )
                                group_by_clause = f"GROUP BY json_extract({self.collection.name}.data, '$.{group_id_field}')"

                            # Try to build the group query using the general method
                            # This supports all accumulator operations including $avg, $min, $max
                            group_result = self._build_group_query(group_stage)
                            if group_result is not None:
                                (
                                    select_clause,
                                    group_by_clause,
                                    group_output_fields,
                                ) = group_result

                                # Modify the SELECT clause to work with the unwound data
                                # Replace json_extract(data, '$.field') with appropriate expressions

                                # For the _id field, if it matches the unwind field, use je.value
                                group_id_field = group_stage["_id"][
                                    1:
                                ]  # Remove leading $
                                if group_id_field == unwind_field_name:
                                    # Replace the _id extraction with je.value
                                    modified_select = select_clause.replace(
                                        f"json_extract(data, '$.{unwind_field_name}') AS _id",
                                        "je.value AS _id",
                                    )
                                else:
                                    modified_select = select_clause

                                # For accumulator expressions that reference the unwound field,
                                # replace json_extract(data, '$.unwind_field_name') with je.value
                                # This handles cases like $push: "$tags" or $addToSet: "$tags" where tags is the unwound field
                                modified_select = modified_select.replace(
                                    f"json_extract(data, '$.{unwind_field_name}')",
                                    "je.value",
                                )

                                # For GROUP BY clause, if grouping by the unwind field, use je.value
                                if group_id_field == unwind_field_name:
                                    modified_group_by = "GROUP BY je.value"
                                else:
                                    # Keep the original GROUP BY but ensure it references the correct table
                                    modified_group_by = group_by_clause.replace(
                                        "json_extract(data,",
                                        f"json_extract({self.collection.name}.data,",
                                    )

                                # Build the FROM clause with json_each for unwinding
                                from_clause = f"FROM {self.collection.name}, json_each(json_extract({self.collection.name}.data, '$.{unwind_field_name}')) as je"

                                # Add ordering by _id for consistent results
                                order_by_clause = "ORDER BY _id"

                                # Construct the full SQL command
                                cmd = f"{modified_select} {from_clause} {modified_group_by} {order_by_clause}"
                                return cmd, [], group_output_fields
                            else:
                                # If we can't build the group query, fall back to Python
                                return None

                            if can_optimize:
                                # Build the optimized SQL query
                                select_clause = "SELECT " + ", ".join(
                                    select_expressions
                                )
                                from_clause = f"FROM {self.collection.name}, json_each(json_extract({self.collection.name}.data, '$.{unwind_field_name}')) as je"

                                # Add ordering by _id for consistent results
                                order_by_clause = "ORDER BY _id"

                                cmd = f"{select_clause} {from_clause} {group_by_clause} {order_by_clause}"
                                # Skip both the $unwind and $group stages
                                i = 1  # Will be incremented to 2 by the loop
                                return cmd, [], output_fields
                            else:
                                # If we can't optimize the $unwind + $group combination,
                                # fall back to Python processing for the entire pipeline
                                return None

                    # Case 2: $match followed by $unwind + $group
                    elif (
                        i == 1
                        and len(pipeline) > 2
                        and "$match" in pipeline[0]
                        and "$group" in pipeline[2]
                    ):
                        # $match followed by $unwind followed by $group - try to optimize with SQL
                        match_stage = pipeline[0]["$match"]
                        group_stage = pipeline[2]["$group"]
                        unwind_field = stage["$unwind"]

                        if (
                            isinstance(unwind_field, str)
                            and unwind_field.startswith("$")
                            and isinstance(group_stage.get("_id"), str)
                            and group_stage.get("_id").startswith("$")
                        ):

                            unwind_field_name = unwind_field[
                                1:
                            ]  # Remove leading $
                            group_id_field = group_stage["_id"][
                                1:
                            ]  # Remove leading $

                            # Check if we can handle this specific group operation
                            can_optimize = True
                            select_expressions = []
                            output_fields = ["_id"]

                            # Handle _id field
                            if group_id_field == unwind_field_name:
                                # Grouping by the unwound field
                                select_expressions.append("je.value AS _id")
                                group_by_clause = "GROUP BY je.value"
                            else:
                                # Grouping by another field
                                select_expressions.append(
                                    f"json_extract({self.collection.name}.data, '$.{group_id_field}') AS _id"
                                )
                                group_by_clause = f"GROUP BY json_extract({self.collection.name}.data, '$.{group_id_field}')"

                            # Handle accumulator operations
                            for field, accumulator in group_stage.items():
                                if field == "_id":
                                    continue

                                if (
                                    not isinstance(accumulator, dict)
                                    or len(accumulator) != 1
                                ):
                                    can_optimize = False
                                    break

                                op, expr = next(iter(accumulator.items()))

                                match op:
                                    case "$sum" if (
                                        isinstance(expr, int) and expr == 1
                                    ):
                                        # Count operation
                                        select_expressions.append(
                                            f"COUNT(*) AS {field}"
                                        )
                                        output_fields.append(field)
                                    case "$count":
                                        # Count operation
                                        select_expressions.append(
                                            f"COUNT(*) AS {field}"
                                        )
                                        output_fields.append(field)
                                    case "$push":
                                        # $push accumulator - collect all values including duplicates
                                        if isinstance(
                                            expr, str
                                        ) and expr.startswith("$"):
                                            push_field = expr[
                                                1:
                                            ]  # Remove leading $
                                            if push_field == unwind_field_name:
                                                # Collect the unwound values
                                                select_expressions.append(
                                                    f'json_group_array(je.value) AS "{field}"'
                                                )
                                            else:
                                                # Collect values from another field
                                                select_expressions.append(
                                                    f"json_group_array(json_extract({self.collection.name}.data, '$.{push_field}')) AS \"{field}\""
                                                )
                                            output_fields.append(field)
                                        else:
                                            # Unsupported expression, fallback to Python
                                            can_optimize = False
                                            break
                                    case "$addToSet":
                                        # $addToSet accumulator - collect unique values only
                                        if isinstance(
                                            expr, str
                                        ) and expr.startswith("$"):
                                            add_to_set_field = expr[
                                                1:
                                            ]  # Remove leading $
                                            if (
                                                add_to_set_field
                                                == unwind_field_name
                                            ):
                                                # Collect unique unwound values
                                                select_expressions.append(
                                                    f'json_group_array(DISTINCT je.value) AS "{field}"'
                                                )
                                            else:
                                                # Collect unique values from another field
                                                select_expressions.append(
                                                    f"json_group_array(DISTINCT json_extract({self.collection.name}.data, '$.{add_to_set_field}')) AS \"{field}\""
                                                )
                                            output_fields.append(field)
                                        else:
                                            # Unsupported expression, fallback to Python
                                            can_optimize = False
                                            break
                                    case _:
                                        # Unsupported operation, fallback to Python
                                        can_optimize = False
                                        break

                            if can_optimize:
                                # Build the optimized SQL query with WHERE clause from $match
                                select_clause = "SELECT " + ", ".join(
                                    select_expressions
                                )
                                from_clause = f"FROM {self.collection.name}, json_each(json_extract({self.collection.name}.data, '$.{unwind_field_name}')) as je"

                                # Add WHERE clause from $match
                                where_result = self._build_simple_where_clause(
                                    match_stage
                                )
                                if (
                                    where_result and where_result[0]
                                ):  # Has WHERE clause
                                    where_clause = where_result[0]
                                    params = where_result[1]
                                else:
                                    where_clause = ""
                                    params = []

                                # Add ordering by _id for consistent results
                                order_by_clause = "ORDER BY _id"

                                cmd = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"
                                # Skip the $match, $unwind, and $group stages
                                i = 2  # Will be incremented to 3 by the loop
                                return cmd, params, output_fields
                            else:
                                # If we can't optimize the $unwind + $group combination,
                                # fall back to Python processing for the entire pipeline
                                return None

                    # Handle $unwind stages (original logic)
                    # Check if this is the first stage or follows a $match stage
                    valid_position = (i == 0) or (
                        i == 1 and "$match" in pipeline[0]
                    )

                    # Special case: Check for $unwind followed by $match with $text (text search integration enhancement)
                    if (
                        i == 0
                        and len(pipeline) > 1
                        and "$unwind" in pipeline[i]
                        and "$match" in pipeline[i + 1]
                    ):
                        unwind_spec = pipeline[i]["$unwind"]
                        match_spec = pipeline[i + 1]["$match"]

                        # Check if this is a simple string unwind with text search
                        if (
                            isinstance(unwind_spec, str)
                            and unwind_spec.startswith("$")
                            and "$text" in match_spec
                        ):
                            text_query = match_spec["$text"]
                            if (
                                isinstance(text_query, dict)
                                and "$search" in text_query
                                and isinstance(text_query["$search"], str)
                            ):
                                # This is the pattern we want to optimize
                                unwind_field = unwind_spec[
                                    1:
                                ]  # Remove leading $
                                search_term = text_query["$search"]

                                # Check if there are more stages after $match
                                has_additional_stages = len(pipeline) > 2

                                # Build SQL query for text search on unwound elements
                                # For object arrays, we need to handle nested field access
                                if "." in unwind_field:
                                    # This is a nested field like "posts.content"
                                    # For now, fall back to Python for complex nested object cases
                                    # A more advanced implementation would handle this
                                    pass
                                else:
                                    # Simple array of strings or objects
                                    select_clause = f"SELECT {self.collection.name}.id, je.value as data"
                                    from_clause = f"FROM {self.collection.name}, json_each(json_extract({self.collection.name}.data, '$.{unwind_field}')) as je"
                                    where_clause = (
                                        "WHERE lower(je.value) LIKE ?"
                                    )
                                    params = [f"%{search_term.lower()}%"]

                                    if has_additional_stages:
                                        # For now, fall back to Python for complex pipelines
                                        # A more advanced implementation could handle additional stages
                                        pass
                                    else:
                                        cmd = f"{select_clause} {from_clause} {where_clause}"
                                        # Skip both the $unwind and $match stages
                                        i = 1  # Will be incremented to 2 by the loop
                                        return cmd, params, None

                    # Check if there are multiple consecutive $unwind stages
                    unwind_stages = []
                    unwind_specs = []
                    j = i
                    has_advanced_options = False
                    while j < len(pipeline) and "$unwind" in pipeline[j]:
                        unwind_spec = pipeline[j]["$unwind"]
                        unwind_specs.append(unwind_spec)
                        # Check if this unwind stage has advanced options
                        if isinstance(unwind_spec, dict) and (
                            "includeArrayIndex" in unwind_spec
                            or "preserveNullAndEmptyArrays" in unwind_spec
                        ):
                            has_advanced_options = True
                        # Extract the path for backward compatibility
                        if isinstance(unwind_spec, str):
                            unwind_stages.append(unwind_spec)
                        elif isinstance(unwind_spec, dict):
                            unwind_stages.append(unwind_spec["path"])
                        else:
                            return None  # Invalid unwind specification
                        j += 1

                    # If we have valid positioning and at least one $unwind stage
                    # Note: Advanced options require fallback to Python implementation for now
                    if (
                        valid_position
                        and unwind_stages
                        and not has_advanced_options
                    ):
                        result = self._build_unwind_query(
                            i, pipeline, unwind_stages
                        )
                        if result:
                            cmd, params, output_fields = result
                            # Skip all processed stages
                            i = j - 1  # Set to the last processed stage index
                            return cmd, params, output_fields
                        else:
                            return None
                    elif unwind_stages:
                        # $unwind not in valid position, has advanced options, or complex case - fallback to Python
                        return None
                case "$lookup":
                    # Check if this is the last stage in the pipeline
                    # If not, we can't optimize with SQL because we need to handle subsequent stages
                    if i < len(pipeline) - 1:
                        return None  # Fallback to Python to handle subsequent stages

                    # Check if this is a valid position for $lookup (first stage or after $match)
                    if i > 1 or (i == 1 and "$match" not in pipeline[0]):
                        return None  # Fallback for complex positions

                    lookup_spec = stage["$lookup"]
                    from_collection = lookup_spec["from"]
                    local_field = lookup_spec["localField"]
                    foreign_field = lookup_spec["foreignField"]
                    as_field = lookup_spec["as"]

                    # Check if we can handle this lookup operation
                    if (
                        not isinstance(from_collection, str)
                        or not isinstance(local_field, str)
                        or not isinstance(foreign_field, str)
                        or not isinstance(as_field, str)
                    ):
                        return None  # Fallback for complex specifications

                    # Build the optimized SQL query for $lookup
                    # Handle special case for _id field (stored as column, not in JSON)
                    if foreign_field == "_id":
                        foreign_extract = "related.id"
                    else:
                        foreign_extract = (
                            f"json_extract(related.data, '$.{foreign_field}')"
                        )

                    if local_field == "_id":
                        local_extract = f"{self.collection.name}.id"
                    else:
                        local_extract = f"json_extract({self.collection.name}.data, '$.{local_field}')"

                    select_clause = (
                        f"SELECT {self.collection.name}.id, "
                        f"json_set({self.collection.name}.data, '$.\"{as_field}\"', "
                        f"coalesce(( "
                        f"  SELECT json_group_array(json(related.data)) "
                        f"  FROM {from_collection} as related "
                        f"  WHERE {foreign_extract} = "
                        f"        {local_extract} "
                        f"), '[]')) as data"
                    )

                    from_clause = f"FROM {self.collection.name}"

                    # If there's a $match stage before this, incorporate its WHERE clause
                    if i == 1 and "$match" in pipeline[0]:
                        match_query = pipeline[0]["$match"]
                        where_result = self._build_simple_where_clause(
                            match_query
                        )
                        if where_result and where_result[0]:  # Has WHERE clause
                            where_clause = where_result[0]
                            params = where_result[1]
                        else:
                            where_clause = ""
                            params = []
                    else:
                        where_clause = ""
                        params = []

                    cmd = f"{select_clause} {from_clause} {where_clause}"
                    return cmd, params, None
                case _:
                    return None  # Fallback for unsupported stages

        cmd = f"{select_clause} FROM {self.collection.name} {where_clause} {group_by} {order_by} {limit} {offset}"
        return cmd, params, output_fields

    def _optimize_unwind_group_pattern(
        self, group_stage_index: int, pipeline: List[Dict[str, Any]]
    ) -> tuple[str, List[Any], List[str]] | None:
        """
        Optimize $unwind + $group pattern with SQL-based processing.

        This method handles the specific optimization pattern where a $unwind stage
        is immediately followed by a $group stage. It supports all accumulator
        operations by leveraging the general _build_group_query method while
        handling the $unwind optimization.

        Args:
            group_stage_index: Index of the $group stage in the pipeline
            pipeline: The complete aggregation pipeline

        Returns:
            tuple[str, List[Any], List[str]] | None: SQL command, params, and output fields
            if optimization is possible, None otherwise
        """
        # Check if this is a $unwind + $group pattern we can optimize
        if group_stage_index == 1 and "$unwind" in pipeline[0]:
            # $unwind followed by $group - try to optimize with SQL
            unwind_stage = pipeline[0]["$unwind"]
            group_spec = pipeline[group_stage_index]["$group"]

            if (
                isinstance(unwind_stage, str)
                and unwind_stage.startswith("$")
                and isinstance(group_spec.get("_id"), str)
                and group_spec.get("_id").startswith("$")
            ):

                unwind_field = unwind_stage[1:]  # Remove leading $

                # Try to build the group query using the general method
                group_result = self._build_group_query(group_spec)
                if group_result is not None:
                    select_clause, group_by_clause, output_fields = group_result

                    # Modify the SELECT clause to work with the unwound data
                    # Replace json_extract(data, '$.field') with appropriate expressions

                    # For the _id field, if it matches the unwind field, use je.value
                    group_id_field = group_spec["_id"][1:]  # Remove leading $
                    if group_id_field == unwind_field:
                        # Replace the _id extraction with je.value
                        modified_select = select_clause.replace(
                            f"json_extract(data, '$.{unwind_field}') AS _id",
                            "je.value AS _id",
                        )
                    else:
                        modified_select = select_clause

                    # For GROUP BY clause, if grouping by the unwind field, use je.value
                    if group_id_field == unwind_field:
                        modified_group_by = "GROUP BY je.value"
                    else:
                        # Keep the original GROUP BY but ensure it references the correct table
                        modified_group_by = group_by_clause.replace(
                            "json_extract(data,",
                            f"json_extract({self.collection.name}.data,",
                        )

                    # Build the FROM clause with json_each for unwinding
                    from_clause = f"FROM {self.collection.name}, json_each(json_extract({self.collection.name}.data, '$.{unwind_field}')) as je"

                    # Add ordering by _id for consistent results
                    order_by_clause = "ORDER BY _id"

                    # Construct the full SQL command
                    cmd = f"{modified_select} {from_clause} {modified_group_by} {order_by_clause}"
                    return cmd, [], output_fields
                else:
                    # If we can't build the group query, fall back to Python
                    return None

        return None

    def _build_unwind_query(
        self,
        pipeline_index: int,
        pipeline: List[Dict[str, Any]],
        unwind_stages: List[str],
    ) -> tuple[str, List[Any], List[str] | None] | None:
        """
        Builds a SQL query for a sequence of $unwind stages.

        This method constructs a SQL query to handle one or more consecutive $unwind
        stages in an aggregation pipeline. It processes array fields by joining
        with SQLite's `json_each` function to "unwind" the arrays into separate rows.
        The method also handles necessary array type checks and integrates with
        other pipeline stages like $match, $sort, $skip, and $limit.

        Args:
            pipeline_index (int): The index of the first $unwind stage in the pipeline.
            pipeline (List[Dict[str, Any]]): The full aggregation pipeline.
            unwind_stages (List[str]): A list of field paths to unwind,
                                       each prefixed with '$'.

        Returns:
            tuple[str, List[Any], List[str] | None] | None: A tuple containing:
                - The constructed SQL command string.
                - A list of parameters for the SQL query.
                - A list of output field names (None if not applicable).
            Returns None if the unwind stages cannot be processed with SQL and a
            fallback to Python is required.
        """
        field_names = []
        for field in unwind_stages:
            if (
                not isinstance(field, str)
                or not field.startswith("$")
                or len(field) == 1
            ):
                return None  # Fallback to Python implementation
            field_names.append(field[1:])

        # Build SELECT clause with nested json_set calls
        select_parts = [f"{self.collection.name}.data"]
        for i, field_name in enumerate(field_names):
            select_parts.insert(0, "json_set(")
            select_parts.append(f", '$.\"{field_name}\"', je{i + 1}.value)")
        select_expr = "".join(select_parts)
        select_clause = (
            f"SELECT {self.collection.name}.id, {select_expr} as data"
        )

        # Build FROM clause with multiple json_each calls
        from_clause, unwound_fields = self._build_unwind_from_clause(
            field_names
        )

        # Handle $match stage and array type checks
        all_where_clauses = []
        params: List[Any] = []
        if pipeline_index == 1 and "$match" in pipeline[0]:
            match_query = pipeline[0]["$match"]
            where_result = self._build_simple_where_clause(match_query)
            if where_result and where_result[0]:
                all_where_clauses.append(
                    where_result[0].replace("WHERE ", "", 1)
                )
                params.extend(where_result[1])

        for field_name in field_names:
            parent_field, parent_alias = self._find_parent_unwind(
                field_name, unwound_fields
            )
            if parent_field and parent_alias:
                nested_path = field_name[len(parent_field) + 1 :]
                all_where_clauses.append(
                    f"json_type(json_extract({parent_alias}.value, '$.{nested_path}')) = 'array'"
                )
            else:
                all_where_clauses.append(
                    f"json_type(json_extract({self.collection.name}.data, '$.{field_name}')) = 'array'"
                )

        where_clause = ""
        if all_where_clauses:
            where_clause = "WHERE " + " AND ".join(all_where_clauses)

        # Handle sort, skip, and limit operations
        start_index = pipeline_index + len(unwind_stages)
        end_index = len(pipeline)
        order_by, limit, offset = self._build_sort_skip_limit_clauses(
            pipeline, start_index, end_index, unwound_fields
        )

        cmd = f"{select_clause} {from_clause} {where_clause} {order_by} {limit} {offset}"
        return cmd, params, None

    def _build_unwind_from_clause(
        self, field_names: List[str]
    ) -> tuple[str, Dict[str, str]]:
        """
        Builds the FROM clause for a SQL query with one or more $unwind stages.

        This method constructs the FROM clause needed to handle multiple $unwind
        operations in an aggregation pipeline. It creates joins with SQLite's
        `json_each` function for each field to be unwound, allowing array elements
        to be processed as separate rows. It also manages nested unwinds by
        identifying parent-child relationships between fields.

        Args:
            field_names (List[str]): A list of field paths to unwind. Each path
                                     should be a string without the leading '$'.

        Returns:
            tuple[str, Dict[str, str]]: A tuple containing:
                - The constructed FROM clause as a string.
                - A dictionary mapping each unwound field path to its corresponding
                  alias (e.g., 'je1', 'je2').
        """
        from_parts = [f"FROM {self.collection.name}"]
        unwound_fields: Dict[str, str] = {}

        for i, field_name in enumerate(field_names):
            je_alias = f"je{i + 1}"
            parent_field, parent_alias = self._find_parent_unwind(
                field_name, unwound_fields
            )

            if parent_field and parent_alias:
                nested_path = field_name[len(parent_field) + 1 :]
                from_parts.append(
                    f", json_each(json_extract({parent_alias}.value, '$.{nested_path}')) as {je_alias}"
                )
            else:
                from_parts.append(
                    f", json_each(json_extract({self.collection.name}.data, '$.{field_name}')) as {je_alias}"
                )
            unwound_fields[field_name] = je_alias

        return " ".join(from_parts), unwound_fields

    def _find_parent_unwind(
        self, field_name: str, unwound_fields: Dict[str, str]
    ) -> tuple[str | None, str | None]:
        """
        Find the parent unwind field for a nested unwind.

        This method searches through already processed unwind fields to find a
        parent field that the current field is nested within. This is used to
        properly construct SQL joins for nested array unwinding operations.

        Args:
            field_name (str): The field name to find the parent for.
            unwound_fields (Dict[str, str]): A dictionary mapping field paths to
                                             their aliases.

        Returns:
            tuple[str | None, str | None]: A tuple containing the parent field
                                           name and its alias, or (None, None)
                                           if no parent is found.
        """
        parent_field = None
        parent_alias = None
        longest_match_len = -1

        for p_field, p_alias in unwound_fields.items():
            prefix = p_field + "."
            if field_name.startswith(prefix):
                if len(p_field) > longest_match_len:
                    longest_match_len = len(p_field)
                    parent_field = p_field
                    parent_alias = p_alias
        return parent_field, parent_alias

    def _build_sort_skip_limit_clauses(
        self,
        pipeline: List[Dict[str, Any]],
        start_index: int,
        end_index: int,
        unwound_fields: Dict[str, str],
    ) -> tuple[str, str, str]:
        """
        Build ORDER BY, LIMIT, and OFFSET clauses for aggregation queries.

        This method constructs the SQL clauses for sorting, skipping, and limiting
        results in an aggregation pipeline. It handles both regular fields and
        fields that have been unwound from arrays, ensuring proper SQL generation
        for nested array elements.

        Args:
            pipeline (List[Dict[str, Any]]): The aggregation pipeline stages.
            start_index (int): The starting index in the pipeline to process stages from.
            end_index (int): The ending index in the pipeline to process stages to.
            unwound_fields (Dict[str, str]): A mapping of field names to their aliases
                                             for unwound fields.

        Returns:
            tuple[str, str, str]: A tuple containing:
                - The ORDER BY clause (empty string if no sorting)
                - The LIMIT clause (empty string if no limit)
                - The OFFSET clause (empty string if no offset)
        """
        local_order_by = ""
        local_limit = ""
        local_offset = ""

        sort_stages = []
        skip_value = 0
        limit_value = None

        for stage_idx in range(start_index, end_index):
            stage = pipeline[stage_idx]
            if "$sort" in stage:
                sort_stages.append(stage["$sort"])
            elif "$skip" in stage:
                skip_value = stage["$skip"]
            elif "$limit" in stage:
                limit_value = stage["$limit"]

        if sort_stages:
            sort_clauses = []
            for sort_spec in sort_stages:
                for key, direction in sort_spec.items():
                    parent_field, parent_alias = self._find_parent_unwind(
                        key, unwound_fields
                    )
                    if parent_field and parent_alias:
                        nested_path = key[len(parent_field) + 1 :]
                        sort_clauses.append(
                            f"json_extract({parent_alias}.value, '$.{nested_path}') "
                            f"{'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    elif key in unwound_fields:
                        unwound_alias = unwound_fields[key]
                        sort_clauses.append(
                            f"{unwound_alias}.value {'DESC' if direction == DESCENDING else 'ASC'}"
                        )
                    else:
                        sort_clauses.append(
                            f"json_extract({self.collection.name}.data, '$.{key}') "
                            f"{'DESC' if direction == DESCENDING else 'ASC'}"
                        )
            if sort_clauses:
                local_order_by = "ORDER BY " + ", ".join(sort_clauses)

        if limit_value is not None:
            local_limit = f"LIMIT {limit_value}"
            if skip_value > 0:
                local_offset = f"OFFSET {skip_value}"
        elif skip_value > 0:
            # SQLite requires LIMIT when using OFFSET
            local_limit = "LIMIT -1"
            local_offset = f"OFFSET {skip_value}"

        return local_order_by, local_limit, local_offset

    def _build_group_query(
        self, group_spec: Dict[str, Any]
    ) -> tuple[str, str, List[str]] | None:
        """
        Builds the SELECT and GROUP BY clauses for a $group stage.

        This method constructs SQL SELECT and GROUP BY clauses for MongoDB-like
        $group aggregation stages that can be handled directly with SQL. It supports
        grouping by a single field and various accumulator operations like $sum,
        $avg, $min, $max, $count, $push, and $addToSet.

        Args:
            group_spec (Dict[str, Any]): A dictionary representing the $group stage
                                         specification. It should contain an "_id"
                                         field for grouping and accumulator operations
                                         for other fields.

        Returns:
            tuple[str, str, List[str]] | None: A tuple containing:
                - The SELECT clause string with all required expressions
                - The GROUP BY clause string
                - A list of output field names
            Returns None if the group specification contains unsupported operations
            that require Python-based processing.
        """
        group_id_expr = group_spec.get("_id")
        if group_id_expr is None:
            group_by_clause = ""
            select_expressions = ["NULL AS _id"]
            output_fields = ["_id"]
        elif isinstance(group_id_expr, str) and group_id_expr.startswith("$"):
            group_by_field = group_id_expr[1:]
            group_by_clause = (
                f"GROUP BY json_extract(data, '$.{group_by_field}')"
            )
            select_expressions = [
                f"json_extract(data, '$.{group_by_field}') AS _id"
            ]
            output_fields = ["_id"]
        else:
            return None  # Fallback for complex _id expressions

        for field, accumulator in group_spec.items():
            if field == "_id":
                continue

            if not isinstance(accumulator, dict) or len(accumulator) != 1:
                return None

            op, expr = next(iter(accumulator.items()))

            if op == "$count":
                select_expressions.append(f"COUNT(*) AS {field}")
                output_fields.append(field)
                continue

            if op == "$push":
                # Handle $push accumulator
                if not isinstance(expr, str) or not expr.startswith("$"):
                    return None  # Fallback for complex accumulator expressions
                field_name = expr[1:]
                select_expressions.append(
                    f"json_group_array(json_extract(data, '$.{field_name}')) AS \"{field}\""
                )
                output_fields.append(field)
                continue

            if op == "$addToSet":
                # Handle $addToSet accumulator
                if not isinstance(expr, str) or not expr.startswith("$"):
                    return None  # Fallback for complex accumulator expressions
                field_name = expr[1:]
                select_expressions.append(
                    f"json_group_array(DISTINCT json_extract(data, '$.{field_name}')) AS \"{field}\""
                )
                output_fields.append(field)
                continue

            # Handle special case for $sum with integer literal 1 (count operation)
            if op == "$sum" and isinstance(expr, int) and expr == 1:
                select_expressions.append(f"COUNT(*) AS {field}")
                output_fields.append(field)
                continue

            # Handle field-based operations
            if not isinstance(expr, str) or not expr.startswith("$"):
                return None  # Fallback for complex accumulator expressions

            field_name = expr[1:]
            sql_func = {
                "$sum": "SUM",
                "$avg": "AVG",
                "$min": "MIN",
                "$max": "MAX",
            }.get(op)

            if not sql_func:
                return None  # Unsupported accumulator

            select_expressions.append(
                f"{sql_func}(json_extract(data, '$.{field_name}')) AS {field}"
            )
            output_fields.append(field)

        select_clause = "SELECT " + ", ".join(select_expressions)
        return select_clause, group_by_clause, output_fields

    def _process_group_stage(
        self,
        group_query: Dict[str, Any],
        docs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Process the $group stage of an aggregation pipeline.

        This method groups documents by a specified field and performs specified
        accumulator operations on other fields.

        Args:
            group_query (Dict[str, Any]): A dictionary representing the $group
                                          stage of the aggregation pipeline.
            docs (List[Dict[str, Any]]): A list of documents to be grouped.

        Returns:
            List[Dict[str, Any]]: A list of grouped documents with applied
                                  accumulator operations.
        """
        grouped_docs: Dict[Any, Dict[str, Any]] = {}
        group_id_key = group_query.get("_id")

        # Create a copy of group_query without _id for processing accumulator operations
        accumulators = {k: v for k, v in group_query.items() if k != "_id"}

        for doc in docs:
            if group_id_key is None:
                group_id = None
            else:
                group_id = self.collection._get_val(doc, group_id_key)
            group = grouped_docs.setdefault(group_id, {"_id": group_id})

            for field, accumulator in accumulators.items():
                # Check if accumulator is a valid dictionary format
                if not isinstance(accumulator, dict) or len(accumulator) != 1:
                    # Invalid accumulator format, skip this field
                    continue

                op, key = next(iter(accumulator.items()))

                if op == "$count":
                    group[field] = group.get(field, 0) + 1
                    continue

                # Handle literal values (e.g., $sum: 1 for counting)
                if isinstance(key, (int, float)):
                    value = key
                elif isinstance(key, dict):
                    # Check if this is one of our new N-value operators
                    if op in {"$firstN", "$lastN", "$minN", "$maxN"}:
                        # These operators use dict format, so we'll process them normally
                        value = None  # Value will be extracted inside the operator case
                    else:
                        # Complex expression like {"$multiply": [...]}, not supported in Python fallback
                        continue
                else:
                    value = self.collection._get_val(doc, key)

                match op:
                    case "$sum":
                        group[field] = (group.get(field, 0) or 0) + (value or 0)
                    case "$avg":
                        avg_info = group.get(field, {"sum": 0, "count": 0})
                        avg_info["sum"] += value or 0
                        avg_info["count"] += 1
                        group[field] = avg_info
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
                        # Initialize the list if it doesn't exist
                        if field not in group:
                            group[field] = []
                        # Only add the value if it's not already in the list
                        if value not in group[field]:
                            group[field].append(value)
                    case "$first":
                        # Only set the value if it hasn't been set yet (first document in group)
                        if field not in group:
                            group[field] = value
                    case "$last":
                        # Always update with the latest value (last document in group)
                        group[field] = value
                    case "$stdDevPop":
                        # Track sum, sum of squares, and count for population standard deviation
                        if field not in group:
                            group[field] = {
                                "sum": 0,
                                "sum_squares": 0,
                                "count": 0,
                                "type": "stdDevPop",
                            }
                        if value is not None:
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
                        if value is not None:
                            group[field]["sum"] += value
                            group[field]["sum_squares"] += value * value
                            group[field]["count"] += 1
                    case "$mergeObjects":
                        # Merge objects, with later values overwriting earlier ones
                        if field not in group:
                            group[field] = {}
                        if isinstance(value, dict):
                            group[field].update(value)
                    case "$firstN":
                        # Get first N values
                        input_value = (
                            self.collection._get_val(
                                doc, key["input"].lstrip("$")
                            )
                            if isinstance(key["input"], str)
                            and key["input"].startswith("$")
                            else key["input"]
                        )
                        n = key["n"]
                        if field not in group:
                            group[field] = {
                                "values": [],
                                "n": n,
                                "type": "firstN",
                            }
                        if len(group[field]["values"]) < n:
                            group[field]["values"].append(input_value)
                    case "$lastN":
                        # Get last N values
                        if (
                            isinstance(key, dict)
                            and "input" in key
                            and "n" in key
                        ):
                            input_value = (
                                self.collection._get_val(
                                    doc, key["input"].lstrip("$")
                                )
                                if isinstance(key["input"], str)
                                and key["input"].startswith("$")
                                else key["input"]
                            )
                            n = key["n"]
                            if field not in group:
                                group[field] = {
                                    "values": [],
                                    "n": n,
                                    "type": "lastN",
                                }
                            if len(group[field]["values"]) < n:
                                group[field]["values"].append(input_value)
                            else:
                                # Remove first element and add new one at the end (sliding window)
                                group[field]["values"] = group[field]["values"][
                                    1:
                                ] + [input_value]
                    case "$minN":
                        # Get N minimum values
                        if (
                            isinstance(key, dict)
                            and "input" in key
                            and "n" in key
                        ):
                            input_value = (
                                self.collection._get_val(
                                    doc, key["input"].lstrip("$")
                                )
                                if isinstance(key["input"], str)
                                and key["input"].startswith("$")
                                else key["input"]
                            )
                            n = key["n"]
                            if field not in group:
                                group[field] = {
                                    "values": [],
                                    "n": n,
                                    "type": "minN",
                                }
                            group[field]["values"].append(input_value)
                    case "$maxN":
                        # Get N maximum values
                        if (
                            isinstance(key, dict)
                            and "input" in key
                            and "n" in key
                        ):
                            input_value = (
                                self.collection._get_val(
                                    doc, key["input"].lstrip("$")
                                )
                                if isinstance(key["input"], str)
                                and key["input"].startswith("$")
                                else key["input"]
                            )
                            n = key["n"]
                            if field not in group:
                                group[field] = {
                                    "values": [],
                                    "n": n,
                                    "type": "maxN",
                                }
                            group[field]["values"].append(input_value)

        # Finalize results (e.g., calculate average and standard deviation)
        for group in grouped_docs.values():
            for field, value in group.items():
                if isinstance(value, dict):
                    # Check if this is for average or standard deviation calculation
                    if "sum" in value and "count" in value:
                        # Check if this is for average calculation
                        if "sum_squares" not in value and "values" not in value:
                            # Average calculation
                            if value["count"] > 0:
                                group[field] = value["sum"] / value["count"]
                            else:
                                group[field] = 0  # or None?
                        elif "sum_squares" in value:
                            # Standard deviation calculation
                            count = value["count"]
                            if count == 0:
                                group[field] = 0
                            elif count == 1:
                                group[field] = 0
                            else:
                                variance = (
                                    value["sum_squares"]
                                    - (value["sum"] ** 2) / count
                                ) / (
                                    count
                                    if value["type"] == "stdDevPop"
                                    else (count - 1)
                                )
                                group[field] = (
                                    variance**0.5 if variance >= 0 else 0
                                )
                    elif "values" in value:
                        # Handle N-value accumulators
                        if value["type"] == "minN":
                            # Sort and take first N values
                            sorted_values = sorted(value["values"])
                            group[field] = sorted_values[: value["n"]]
                        elif value["type"] == "maxN":
                            # Sort in descending order and take first N values
                            sorted_values = sorted(
                                value["values"], reverse=True
                            )
                            group[field] = sorted_values[: value["n"]]
                        else:
                            # For firstN and lastN, values are already in correct order
                            group[field] = value["values"]

        return list(grouped_docs.values())

    def _apply_projection(
        self,
        projection: Dict[str, Any],
        document: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Applies the projection to the document, selecting or excluding fields
        based on the projection criteria.

        Args:
            projection (Dict[str, Any]): A dictionary specifying which fields to
                                         include or exclude.
            document (Dict[str, Any]): The document to apply the projection to.

        Returns:
            Dict[str, Any]: The document with fields applied based on the projection.
        """
        if not projection:
            return document

        doc = deepcopy(document)
        projected_doc: Dict[str, Any] = {}
        include_id = projection.get("_id", 1) == 1

        # Inclusion mode
        if any(v == 1 for v in projection.values()):
            for key, value in projection.items():
                if value == 1 and key in doc:
                    projected_doc[key] = doc[key]
            if include_id and "_id" in doc:
                projected_doc["_id"] = doc["_id"]
            return projected_doc

        # Exclusion mode
        for key, value in projection.items():
            if value == 0 and key in doc:
                doc.pop(key, None)
        if not include_id and "_id" in doc:
            doc.pop("_id", None)
        return doc
