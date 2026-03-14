"""
Update operations mixin for QueryHelper.

This module contains the UpdateOperationsMixin class which provides
SQL-based and Python-based update operations for NeoSQLite collections.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple

from ...sql_utils import quote_table_name
from ...binary import Binary
from ...exceptions import MalformedQueryException
from copy import deepcopy
from datetime import datetime
from ..json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)
from ..json_path_utils import (
    parse_json_path,
)

# Import utility functions
from .utils import (
    _FORCE_FALLBACK,
    _convert_bytes_to_binary,
    _get_json_function,
    _validate_inc_mul_field_value,
    _supports_relative_json_indexing,
)

# Import positional update functions
from .positional_update import (
    _apply_positional_update,
    _set_nested_field,
)

# Import helper functions

if TYPE_CHECKING:
    from .. import Collection


class UpdateOperationsMixin:
    """
    A mixin class providing update operations for QueryHelper.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self._jsonb_supported
    - self._json_function_prefix
    - self._build_simple_where_clause method
    """

    collection: "Collection"
    _jsonb_supported: bool
    _json_function_prefix: str
    _get_integer_id_for_oid: Any

    def _internal_update(
        self,
        doc_id: Any,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
        array_filters: List[Dict[str, Any]] | None = None,
        query_filter: Dict[str, Any] | None = None,
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Helper method for updating documents.

        Attempts to use SQL-based updates for simple operations, falling back to
        Python-based updates for complex operations.

        Args:
            doc_id (Any): The ID of the document to update (can be ObjectId, int, etc.).
            update_spec (Dict[str, Any]): The update specification.
            original_doc (Dict[str, Any]): The original document before the update.
            array_filters (List[Dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (Dict[str, Any], optional): The query filter for $ operator.

        Returns:
            Tuple[Dict[str, Any], bool]: The updated document and whether it was modified.
        """
        # Validate $inc and $mul operations before choosing implementation
        # This ensures consistent behavior between SQL and Python implementations
        for op, value in update_spec.items():
            if op in {"$inc", "$mul"}:
                for field_name in value.keys():
                    # Get the current value of the field
                    if field_name in original_doc:
                        field_value = original_doc[field_name]
                        # Validate the field value
                        _validate_inc_mul_field_value(
                            field_name, field_value, op
                        )
                    # If field doesn't exist, it will be treated as 0, which is valid
                    # (no validation needed for missing fields)

        # Respect the kill switch - force Python fallback if enabled
        if _FORCE_FALLBACK:
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

        # Try to use SQL-based updates for simple operations
        # Note: SQL updates don't support array_filters or positional operators, so fall back to Python if provided
        if array_filters:
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

        if self._can_use_sql_updates(update_spec, doc_id):
            # Use enhanced SQL update with json_insert/json_replace when possible
            try:
                updated_doc = self._perform_enhanced_sql_update(
                    doc_id, update_spec
                )
                # For SQL updates, assume modified if we got a result
                return updated_doc, updated_doc != original_doc
            except Exception:
                # If enhanced update fails, fall back to standard SQL update
                try:
                    updated_doc = self._perform_sql_update(doc_id, update_spec)
                    return updated_doc, updated_doc != original_doc
                except Exception:
                    return self._perform_python_update(
                        doc_id,
                        update_spec,
                        original_doc,
                        array_filters,
                        query_filter,
                    )
        else:
            # Fall back to Python-based updates for complex operations
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
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
        # Respect the kill switch - force fallback if enabled
        if _FORCE_FALLBACK:
            return False

        # Tier 1: Simple operations that can use json_set/json_remove
        # Tier 2: More complex operations that can use SQL with some limitations
        supported_ops = {"$set", "$unset", "$inc", "$mul", "$min", "$max"}

        # Also check that doc_id is not 0 (which indicates an upsert)
        # Disable SQL updates for documents containing Binary objects
        has_binary_values = any(
            isinstance(val, bytes) and hasattr(val, "encode_for_storage")
            for op in update_spec.values()
            if isinstance(op, dict)
            for val in op.values()
        )

        # Check for positional operators in field paths (requires Python fallback)
        has_positional_operators = False
        for op, value in update_spec.items():
            if isinstance(value, dict):
                for field_path in value.keys():
                    if "$" in field_path:
                        has_positional_operators = True
                        break
            if has_positional_operators:
                break

        # Positional operators require Python fallback
        if has_positional_operators:
            return False

        # Check for complex $push modifiers
        has_complex_push = False
        if "$push" in update_spec:
            push_spec = update_spec["$push"]
            if isinstance(push_spec, dict):
                for value in push_spec.values():
                    if isinstance(value, dict) and (
                        "$each" in value
                        or "$position" in value
                        or "$slice" in value
                    ):
                        # SQL optimization supports $each, but not $position or $slice
                        if "$position" in value or "$slice" in value:
                            has_complex_push = True
                            break
                        # $each is supported in our enhanced SQL path

        # Check for $pop operator
        has_pop = "$pop" in update_spec
        # SQL $pop requires relative indexing [#-1] support
        if has_pop and not _supports_relative_json_indexing():
            return False

        # Check for $addToSet operator
        has_add_to_set = "$addToSet" in update_spec
        if has_add_to_set:
            add_to_set_spec = update_spec["$addToSet"]
            if isinstance(add_to_set_spec, dict):
                for value in add_to_set_spec.values():
                    # $each in $addToSet is complex in SQL
                    if isinstance(value, dict) and "$each" in value:
                        return False

        # Complex $push with $position/$slice requires Python fallback
        if has_complex_push:
            return False

        return (
            doc_id != 0
            and not has_binary_values
            and all(
                op in supported_ops
                or op in {"$push", "$bit", "$pop", "$addToSet"}
                for op in update_spec.keys()
            )
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

        # Get integer ID for the document
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # Execute the SQL updates
        sql_params = []
        if unset_clauses:
            # Handle $unset operations with json_remove
            func_name = _get_json_function("remove", self._jsonb_supported)
            cmd = (
                f"UPDATE {quote_table_name(self.collection.name)} "
                f"SET data = {func_name}(data, {', '.join(unset_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = unset_params + [int_doc_id]
            self.collection.db.execute(cmd, sql_params)

        if set_clauses:
            # Handle other operations with json_set
            func_name = _get_json_function("set", self._jsonb_supported)
            cmd = (
                f"UPDATE {quote_table_name(self.collection.name)} "
                f"SET data = {func_name}(data, {', '.join(set_clauses)}) "
                "WHERE id = ?"
            )
            sql_params = set_params + [int_doc_id]
            cursor = self.collection.db.execute(cmd, sql_params)

            # Check if any rows were updated
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        elif not unset_clauses:
            # No operations to perform
            raise RuntimeError("No valid operations to perform")

        # Fetch and return the updated document
        # Use the instance's JSONB support flag to determine how to select data
        if self._jsonb_supported:
            cmd = f"SELECT id, json(data) as data FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        else:
            cmd = f"SELECT id, data FROM {quote_table_name(self.collection.name)} WHERE id = ?"

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])

        # This shouldn't happen, but just in case
        raise RuntimeError("Failed to fetch updated document")

    def _perform_enhanced_sql_update(
        self,
        doc_id: Any,
        update_spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform update operations using SQL JSON functions with field-level granularity.

        This method optimizes update operations by using specialized JSON functions
        (json_insert, json_replace, etc.) based on whether fields already exist.
        It provides field-level updates rather than whole-document rewrites.

        Args:
            doc_id (Any): The ID of the document to be updated.
            update_spec (Dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.

        Returns:
            Dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the
                          update process.
        """
        # Get integer ID for the document immediately to avoid UnboundLocalError
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # First, we need to determine which fields exist in the document
        # and which are new to decide between json_insert and json_replace
        existing_fields = self._get_document_fields(doc_id)

        insert_clauses = []
        insert_params = []
        replace_clauses = []
        replace_params = []
        set_clauses = []  # For backward compatibility with json_set
        set_params = []
        unset_clauses = []
        unset_params: List[Any] = []

        # Build SQL update clauses for each operation
        for op, value in update_spec.items():
            match op:
                case "$set":
                    # For $set, we need to determine whether to use json_insert or json_replace
                    for field, field_val in value.items():
                        # Convert bytes to Binary for proper JSON serialization
                        converted_val = _convert_bytes_to_binary(field_val)
                        # If it's a Binary object, serialize it to JSON and use json() function
                        if isinstance(converted_val, Binary):
                            param_value = neosqlite_json_dumps(converted_val)
                            use_json_func = True
                        # For complex objects (dict, list), serialize them to JSON
                        elif isinstance(converted_val, (dict, list)):
                            param_value = neosqlite_json_dumps(converted_val)
                            use_json_func = True
                        else:
                            param_value = converted_val
                            use_json_func = False

                        # For dotted field names, we should use json_set
                        if "." in field and field not in existing_fields:
                            json_path = f"'{parse_json_path(field)}'"
                            if use_json_func:
                                set_clauses.append(f"{json_path}, json(?)")
                            else:
                                set_clauses.append(f"{json_path}, ?")
                            set_params.append(param_value)
                        else:
                            # Check if field exists in the document
                            json_path = f"'{parse_json_path(field)}'"
                            if field in existing_fields:
                                # Use json_replace for existing fields
                                if use_json_func:
                                    replace_clauses.append(
                                        f"{json_path}, json(?)"
                                    )
                                else:
                                    replace_clauses.append(f"{json_path}, ?")
                                replace_params.append(param_value)
                            else:
                                # Use json_insert for new fields
                                if use_json_func:
                                    insert_clauses.append(
                                        f"{json_path}, json(?)"
                                    )
                                else:
                                    insert_clauses.append(f"{json_path}, ?")
                                insert_params.append(param_value)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        json_path = f"'{parse_json_path(field)}'"
                        unset_clauses.append(json_path)
                case "$push":
                    # Tier 2: $push (with $each) can use SQL json_set with [#]
                    for field, push_value in value.items():
                        # Extract values to push (handle $each)
                        values_to_push = []
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Append each value using [#]
                        json_path = f"'{parse_json_path(field)}[#]'"
                        for val in values_to_push:
                            converted_val = _convert_bytes_to_binary(val)
                            if isinstance(converted_val, (dict, list, Binary)):
                                param_value = neosqlite_json_dumps(
                                    converted_val
                                )
                                set_clauses.append(f"{json_path}, json(?)")
                            else:
                                param_value = converted_val
                                set_clauses.append(f"{json_path}, ?")
                            set_params.append(param_value)
                case "$pop":
                    # Tier 2: $pop uses json_remove with [0] or [#-1]
                    for field, pop_direction in value.items():
                        index_path = (
                            "[0]" if int(pop_direction) < 0 else "[#-1]"
                        )
                        json_path = f"'{parse_json_path(field)}{index_path}'"
                        unset_clauses.append(json_path)
                case "$addToSet":
                    # Tier 2: $addToSet (without $each) can use conditional SQL
                    for field, val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        converted_val = _convert_bytes_to_binary(val)
                        if isinstance(converted_val, (Binary, dict, list)):
                            param_value = neosqlite_json_dumps(converted_val)
                            use_json = True
                        else:
                            param_value = converted_val
                            use_json = False

                        if not use_json:
                            insert_func = _get_json_function(
                                "insert", self._jsonb_supported
                            )
                            array_path = json_path
                            append_path = f"'{parse_json_path(field)}[#]'"
                            cmd = (
                                f"UPDATE {quote_table_name(self.collection.name)} "
                                f"SET data = {insert_func}(data, {append_path}, ?) "
                                f"WHERE id = ? AND NOT EXISTS ("
                                f"  SELECT 1 FROM json_each(data, {array_path}) "
                                f"  WHERE value = ?"
                                f")"
                            )
                            self.collection.db.execute(
                                cmd, (param_value, int_doc_id, param_value)
                            )
                        else:
                            raise NotImplementedError(
                                "$addToSet with complex values requires Python fallback"
                            )
                case "$bit":
                    # Tier 2: $bit using bitwise operators
                    for field, bit_spec in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        extract_func = _get_json_function(
                            "extract", self._jsonb_supported
                        )
                        current_expr = (
                            f"COALESCE({extract_func}(data, {json_path}), 0)"
                        )
                        bit_expr = current_expr
                        if "and" in bit_spec:
                            bit_expr = f"({bit_expr} & {int(bit_spec['and'])})"
                        if "or" in bit_spec:
                            bit_expr = f"({bit_expr} | {int(bit_spec['or'])})"
                        if "xor" in bit_spec:
                            xor_val = int(bit_spec["xor"])
                            bit_expr = f"(({bit_expr} | {xor_val}) & ~(({bit_expr}) & {xor_val}))"
                        set_clauses.append(f"{json_path}, {bit_expr}")
                case _:
                    # For other operations, use the standard approach
                    clauses, params = self._build_sql_update_clause(op, value)
                    if clauses:
                        set_clauses.extend(clauses)
                        set_params.extend(params)

        # Execute updates in order
        if unset_clauses:
            func_name = _get_json_function("remove", self._jsonb_supported)
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {func_name}(data, {', '.join(unset_clauses)}) WHERE id = ?"
            self.collection.db.execute(cmd, unset_params + [int_doc_id])

        if insert_clauses:
            func_name = _get_json_function("insert", self._jsonb_supported)
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {func_name}(data, {', '.join(insert_clauses)}) WHERE id = ?"
            self.collection.db.execute(cmd, insert_params + [int_doc_id])

        if replace_clauses:
            func_name = _get_json_function("replace", self._jsonb_supported)
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {func_name}(data, {', '.join(replace_clauses)}) WHERE id = ?"
            self.collection.db.execute(cmd, replace_params + [int_doc_id])

        if set_clauses:
            func_name = _get_json_function("set", self._jsonb_supported)
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {func_name}(data, {', '.join(set_clauses)}) WHERE id = ?"
            cursor = self.collection.db.execute(cmd, set_params + [int_doc_id])
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")

        # Fetch updated document
        if self._jsonb_supported:
            cmd = f"SELECT id, json(data) as data FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        else:
            cmd = f"SELECT id, data FROM {quote_table_name(self.collection.name)} WHERE id = ?"

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])
        raise RuntimeError("Failed to fetch updated document")

    def _get_document_fields(self, doc_id: Any) -> Set[str]:
        """
        Get the set of field names in a document.

        This method extracts the field names from a document to determine which fields
        already exist and which are new. This is used to decide between json_insert
        and json_replace operations.

        Args:
            doc_id (Any): The ID of the document to analyze.

        Returns:
            set: A set of field names in the document.
        """
        # Get the integer ID for the document
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # Fetch the document data
        if self._jsonb_supported:
            cmd = f"SELECT json(data) as data FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        else:
            cmd = f"SELECT data FROM {quote_table_name(self.collection.name)} WHERE id = ?"

        row = self.collection.db.execute(cmd, (int_doc_id,)).fetchone()
        if not row:
            return set()

        # Parse the JSON to get field names
        try:
            doc_data = neosqlite_json_loads(
                row[0] if self._jsonb_supported else row[0]
            )
            if isinstance(doc_data, dict):
                return set(doc_data.keys())
            else:
                return set()
        except Exception:
            # If we can't parse the document, return empty set
            return set()

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
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(f"{json_path}, ?")
                        params.append(field_val)
                case "$inc":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) + ?"
                        )
                        params.append(field_val)
                case "$mul":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(field_val)
                case "$min":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, min({self._json_function_prefix}_extract(data, {json_path}), ?)"
                        )
                        params.append(field_val)
                case "$max":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, max({self._json_function_prefix}_extract(data, {json_path}), ?)"
                        )
                        params.append(field_val)
                case "$unset":
                    # For $unset, we use json_remove
                    for field in value:
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(json_path)
                    # json_remove has a different syntax
                    if set_clauses:
                        func_name = _get_json_function(
                            "remove", self._jsonb_supported
                        )
                        return (
                            f"data = {func_name}(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        # No fields to unset
                        return None
                case "$currentDate":
                    for field, type_spec in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        # Set to current datetime ISO string
                        set_clauses.append(
                            f"{json_path}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                        )
                case "$pop":
                    # For $pop, we use json_remove
                    if not _supports_relative_json_indexing():
                        return None
                    for field, pop_direction in value.items():
                        # 1: remove last, -1: remove first
                        index_path = (
                            "[0]" if int(pop_direction) < 0 else "[#-1]"
                        )
                        json_path = f"'{parse_json_path(field)}{index_path}'"
                        set_clauses.append(json_path)
                    # json_remove has a different syntax
                    if set_clauses:
                        func_name = _get_json_function(
                            "remove", self._jsonb_supported
                        )
                        return (
                            f"data = {func_name}(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        return None
                case "$push":
                    # Optimized $push (with $each) using [#]
                    for field, push_value in value.items():
                        # Handle $each
                        values_to_push = []
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            # SQL optimization supports $each, but not $position or $slice
                            if (
                                "$position" in push_value
                                or "$slice" in push_value
                            ):
                                return None
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Append each value
                        json_path = f"'{parse_json_path(field)}[#]'"
                        for val in values_to_push:
                            set_clauses.append(f"{json_path}, ?")
                            params.append(val)
                case "$setOnInsert":
                    # $setOnInsert is conditional on upsert, force Python fallback
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
            func_name = _get_json_function("set", self._jsonb_supported)
            return f"data = {func_name}(data, {', '.join(set_clauses)})", params
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
                    json_path = f"'{parse_json_path(field)}'"
                    if isinstance(converted_val, Binary):
                        clauses.append(f"{json_path}, json(?)")
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(f"{json_path}, ?")
                        params.append(converted_val)
            case "$inc":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) + json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) + ?"
                        )
                        params.append(converted_val)
            case "$mul":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) * json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self._json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(converted_val)
            case "$min":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(
                        f"{json_path}, min({self._json_function_prefix}_extract(data, {json_path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, min({self._json_function_prefix}_extract(data, {json_path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$max":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(
                        f"{json_path}, max({self._json_function_prefix}_extract(data, {json_path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, max({self._json_function_prefix}_extract(data, {json_path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$push":
                # Optimized $push (with $each) using [#]
                for field, push_value in value.items():
                    # Handle $each
                    values_to_push = []
                    if isinstance(push_value, dict) and "$each" in push_value:
                        # SQL optimization supports $each, but not $position or $slice
                        if "$position" in push_value or "$slice" in push_value:
                            # This should have been caught by _can_use_sql_updates
                            # but as a safeguard we return empty to avoid SQL error
                            return [], []
                        values_to_push = push_value["$each"]
                        if not isinstance(values_to_push, list):
                            values_to_push = [values_to_push]
                    else:
                        values_to_push = [push_value]

                    # Append each value
                    json_path = f"'{parse_json_path(field)}[#]'"
                    for val in values_to_push:
                        # Convert bytes to Binary for proper JSON serialization
                        converted_val = _convert_bytes_to_binary(val)
                        if isinstance(converted_val, Binary):
                            clauses.append(f"{json_path}, json(?)")
                            params.append(neosqlite_json_dumps(converted_val))
                        elif isinstance(converted_val, (dict, list)):
                            clauses.append(f"{json_path}, json(?)")
                            params.append(neosqlite_json_dumps(converted_val))
                        else:
                            clauses.append(f"{json_path}, ?")
                            params.append(converted_val)
            case "$pop":
                # Tier 2: $pop uses json_remove with [0] or [#-1]
                if not _supports_relative_json_indexing():
                    return [], []
                for field, pop_direction in value.items():
                    # 1: remove last, -1: remove first
                    index_path = "[0]" if int(pop_direction) < 0 else "[#-1]"
                    json_path = f"'{parse_json_path(field)}{index_path}'"
                    clauses.append(json_path)
            case "$addToSet":
                # Tier 2: $addToSet (without $each) can use conditional SQL
                for field, val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Convert value for proper parameter handling
                    converted_val = _convert_bytes_to_binary(val)
                    if isinstance(converted_val, Binary):
                        param_value = neosqlite_json_dumps(converted_val)
                        use_json = True
                    elif isinstance(converted_val, (dict, list)):
                        param_value = neosqlite_json_dumps(converted_val)
                        use_json = True
                    else:
                        param_value = converted_val
                        use_json = False

                    if not use_json:
                        # Build SQL that only inserts if value not in array
                        insert_func = _get_json_function(
                            "insert", self._jsonb_supported
                        )
                        array_path = json_path
                        append_path = f"'{parse_json_path(field)}[#]'"

                        # Use a CASE expression to conditionally call json_insert
                        exists_subquery = f"EXISTS (SELECT 1 FROM json_each(data, {array_path}) WHERE value = ?)"

                        clauses.append(
                            f"{array_path}, CASE WHEN {exists_subquery} THEN data ELSE {insert_func}(data, {append_path}, ?) END"
                        )
                        params.extend([param_value, param_value])
                    else:
                        # Complex values - fall back to Python
                        return [], []
            case "$unset":
                # For $unset, we use json_remove
                for field in value:
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(json_path)

        return clauses, params

    def _perform_python_update(
        self,
        doc_id: Any,
        update_spec: Dict[str, Any],
        original_doc: Dict[str, Any],
        array_filters: List[Dict[str, Any]] | None = None,
        query_filter: Dict[str, Any] | None = None,
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Perform update operations using Python-based logic.

        Args:
            doc_id (Any): The document ID of the document to update (can be ObjectId, int, etc.).
            update_spec (Dict[str, Any]): A dictionary specifying the update
                                          operations to perform.
            original_doc (Dict[str, Any]): The original document before applying
                                           the updates.
            array_filters (List[Dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (Dict[str, Any], optional): The query filter for $ operator.

        Returns:
            Tuple[Dict[str, Any], bool]: The updated document and whether it was modified.
        """
        doc_to_update = deepcopy(original_doc)

        for op, value in update_spec.items():
            match op:
                case "$set":
                    # Handle positional operators in field paths
                    for k, v in value.items():
                        if "$" in k:
                            # Use positional update
                            _apply_positional_update(
                                doc_to_update, k, v, array_filters, query_filter
                            )
                        else:
                            _set_nested_field(doc_to_update, k, v)
                case "$unset":
                    for k in value:
                        doc_to_update.pop(k, None)
                case "$inc":
                    for k, v in value.items():
                        # Validate that the field value is numeric before performing operation
                        current_value = doc_to_update.get(k)
                        _validate_inc_mul_field_value(k, current_value, "$inc")
                        doc_to_update[k] = doc_to_update.get(k, 0) + v
                case "$push":
                    for k, v in value.items():
                        # Check if v is a dict with modifiers ($each, $position, $slice)
                        if isinstance(v, dict) and "$each" in v:
                            # Get the array to push to
                            current_list = doc_to_update.setdefault(k, [])

                            # Get values to add
                            values_to_add = v["$each"]
                            if not isinstance(values_to_add, list):
                                values_to_add = [values_to_add]

                            # Handle $position modifier
                            position = v.get("$position")
                            if position is not None:
                                # Insert at specific position
                                for i, val in enumerate(values_to_add):
                                    current_list.insert(position + i, val)
                            else:
                                # Append to end
                                current_list.extend(values_to_add)

                            # Handle $slice modifier (after adding values)
                            slice_val = v.get("$slice")
                            if slice_val is not None:
                                if slice_val == 0:
                                    doc_to_update[k] = []
                                elif slice_val > 0:
                                    # Keep first N elements
                                    doc_to_update[k] = current_list[:slice_val]
                                else:
                                    # Keep last N elements (negative slice)
                                    doc_to_update[k] = current_list[slice_val:]
                        else:
                            # Simple push (no modifiers)
                            doc_to_update.setdefault(k, []).append(v)
                case "$addToSet":
                    for k, v in value.items():
                        current_list = doc_to_update.setdefault(k, [])
                        if v not in current_list:
                            current_list.append(v)
                case "$pull":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[k] = [
                                item for item in doc_to_update[k] if item != v
                            ]
                case "$pullAll":
                    for k, v in value.items():
                        if k in doc_to_update and isinstance(v, (list, tuple)):
                            # Only process if the field is a list
                            if isinstance(doc_to_update[k], list):
                                # Remove all instances of values in the array
                                # Use list instead of set to handle unhashable types
                                values_to_remove = list(v)
                                new_list = [
                                    item
                                    for item in doc_to_update[k]
                                    if item not in values_to_remove
                                ]
                                # Only update if the list actually changed
                                if new_list != doc_to_update[k]:
                                    doc_to_update[k] = new_list
                case "$pop":
                    for k, v in value.items():
                        if v == 1:
                            doc_to_update.get(k, []).pop()
                        elif v == -1:
                            doc_to_update.get(k, []).pop(0)
                case "$bit":
                    for k, bit_op in value.items():
                        if not isinstance(bit_op, dict):
                            raise MalformedQueryException(
                                "$bit operator requires a dict with 'and', 'or', or 'xor'"
                            )

                        # Get current value (default to 0)
                        current_val = doc_to_update.get(k, 0)

                        # Apply bitwise operations
                        if "and" in bit_op:
                            current_val &= bit_op["and"]
                        if "or" in bit_op:
                            current_val |= bit_op["or"]
                        if "xor" in bit_op:
                            current_val ^= bit_op["xor"]

                        doc_to_update[k] = current_val
                case "$rename":
                    for k, v in value.items():
                        if k in doc_to_update:
                            doc_to_update[v] = doc_to_update.pop(k)
                case "$mul":
                    for k, v in value.items():
                        # Validate that the field value is numeric before performing operation
                        if k in doc_to_update:
                            _validate_inc_mul_field_value(
                                k, doc_to_update[k], "$mul"
                            )
                            doc_to_update[k] *= v
                case "$min":
                    for k, v in value.items():
                        if k not in doc_to_update or doc_to_update[k] > v:
                            doc_to_update[k] = v
                case "$max":
                    for k, v in value.items():
                        if k not in doc_to_update or doc_to_update[k] < v:
                            doc_to_update[k] = v
                case "$currentDate":
                    for k, type_spec in value.items():
                        doc_to_update[k] = datetime.now().isoformat()
                case "$setOnInsert":
                    # Only apply on upsert (doc_id == 0)
                    if doc_id == 0:
                        for k, v in value.items():
                            doc_to_update[k] = v
                case _:
                    raise MalformedQueryException(
                        f"Update operator '{op}' not supported"
                    )

        # If this is an upsert (doc_id == 0), we don't update the database
        # We just return the updated document for insertion by the caller
        if doc_id != 0:
            # Convert the doc_id to integer ID for internal operations
            int_doc_id = self._get_integer_id_for_oid(doc_id)
            self.collection.db.execute(
                f"UPDATE {quote_table_name(self.collection.name)} SET data = ? WHERE id = ?",
                (neosqlite_json_dumps(doc_to_update), int_doc_id),
            )

        # Check if document was actually modified
        was_modified = doc_to_update != original_doc

        return doc_to_update, was_modified
