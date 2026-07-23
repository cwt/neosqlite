"""SQL-based update operations mixin for QueryHelper.

Extracted from update_operations.py.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ...binary import Binary
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)
from ..json_path_utils import parse_json_path
from ..jsonb_support import json_data_column
from .utils import (
    _convert_bytes_to_binary,
    _get_json_function,
    _supports_relative_json_indexing,
    get_force_fallback,
)

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


class SqlUpdatesMixin:
    """Mixin providing SQL-based update operations.

    Designed to be composed into UpdateOperationsMixin.
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _get_integer_id_for_oid: Any

    def _can_use_sql_updates(
        self,
        update_spec: dict[str, Any],
        doc_id: int,
        original_doc: dict[str, Any] | None = None,
    ) -> bool:
        """
        Check if all operations in the update spec can be handled with SQL.

        This method determines whether the update operations can be efficiently
        executed using SQL directly, which allows for better performance compared
        to iterating over each document and applying updates in Python.

        Args:
            update_spec (dict[str, Any]): The update operations to be checked.
            doc_id (int): The document ID, which is used to determine if the update
                          is an upsert.

        Returns:
            bool: True if all operations can be handled with SQL, False otherwise.
        """
        # Respect the kill switch - force fallback if enabled
        if get_force_fallback():
            return False

        # Tier 1: Simple operations that can use json_set/json_remove
        # Tier 2: More complex operations that can use SQL with some limitations
        supported_ops = {
            "$set",
            "$unset",
            "$inc",
            "$mul",
            "$min",
            "$max",
            "$pull",
            "$pullAll",
            "$currentDate",
            "$rename",
            "$setOnInsert",
        }

        # Also check that doc_id is not 0 (which indicates an upsert)
        # Disable SQL updates for documents containing Binary objects
        has_binary_values = any(
            isinstance(val, bytes) and hasattr(val, "encode_for_storage")
            for op in update_spec.values()
            if isinstance(op, dict)
            for val in op.values()
        )

        # Check for positional operators in field paths (not supported in SQL tier)
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
        # SQL optimization supports $each, $position, and $slice - all handled in SQL tier
        # No need to check for Python fallback anymore

        # Check for $pop operator
        has_pop = "$pop" in update_spec
        # SQL $pop requires relative indexing [#-1] support
        if has_pop and not _supports_relative_json_indexing():
            return False

        # Check for $addToSet operator - now supports $each in SQL
        # Note: We no longer return False for $each since we have SQL implementation

        # Check for $pull and $pullAll - require field to exist and be a list in original_doc
        if original_doc is not None:
            for op in update_spec:
                if op in {"$pull", "$pullAll"}:
                    op_spec = update_spec[op]
                    if isinstance(op_spec, dict):
                        for field in op_spec.keys():
                            if field not in original_doc or not isinstance(
                                original_doc.get(field), list
                            ):
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
        update_spec: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Perform update operations using SQL JSON functions.

        This method builds SQL clauses for updating document fields based on the
        provided update specification. It supports both `$set` and `$unset` operations
        using SQLite's `json_set` and `json_remove` functions, respectively. The
        method then executes the SQL commands to apply the updates and fetches
        the updated document from the database.

        Args:
            doc_id (int): The ID of the document to be updated.
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.

        Returns:
            dict[str, Any]: The updated document.

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

        # Execute the SQL updates using a single consolidated UPDATE statement if possible
        # This significantly reduces disk I/O and transaction overhead
        current_data = "data"
        all_params = []

        if unset_clauses:
            # Handle $unset operations with json_remove
            func_name = _get_json_function("remove", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(unset_clauses)})"
            )
            all_params.extend(unset_params)

        if set_clauses:
            # Handle other operations with json_set
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(set_clauses)})"
            )
            all_params.extend(set_params)

        if current_data != "data":
            cmd = (
                f"UPDATE {quote_table_name(self.collection.name)} "
                f"SET data = {current_data} "
                "WHERE id = ?"
            )
            cursor = self.collection.db.execute(cmd, all_params + [int_doc_id])

            # Check if any rows were updated
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        else:
            # No operations to perform
            raise RuntimeError("No valid operations to perform")

        # Fetch and return the updated document
        # Use the instance's JSONB support flag to determine how to select data
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT id, {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])

        # This shouldn't happen, but just in case
        raise RuntimeError("Failed to fetch updated document")

    def _perform_enhanced_sql_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Perform update operations using SQL JSON functions with field-level granularity.

        This method optimizes update operations by using specialized JSON functions
        (json_insert, json_replace, etc.) based on whether fields already exist.
        It provides field-level updates rather than whole-document rewrites.

        Args:
            doc_id (Any): The ID of the document to be updated.
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to be performed.
            original_doc (dict[str, Any], optional): The original document before the update.
                                                     If provided, used to determine existing fields
                                                     instead of fetching again.

        Returns:
            dict[str, Any]: The updated document.

        Raises:
            RuntimeError: If no rows are updated or if an error occurs during the
                          update process.
        """
        # Get integer ID for the document immediately to avoid UnboundLocalError
        int_doc_id = self._get_integer_id_for_oid(doc_id)

        # First, we need to determine which fields exist in the document
        # and which are new to decide between json_insert and json_replace
        # Use original_doc if provided to avoid extra fetch
        if original_doc is not None:
            existing_fields = (
                set(original_doc.keys())
                if isinstance(original_doc, dict)
                else set()
            )
        else:
            existing_fields = self._get_document_fields(doc_id)

        insert_clauses = []
        insert_params = []
        replace_clauses = []
        replace_params = []
        set_clauses = []  # For backward compatibility with json_set
        set_params = []
        unset_clauses = []
        unset_params: list[Any] = []

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
                    # Tier 2: $push (with $each, optionally with $position and/or $slice) can use SQL
                    for field, push_value in value.items():
                        # Extract values to push (handle $each)
                        values_to_push = []
                        slice_value = None
                        position_value = None
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            # Check for $slice and $position
                            if "$slice" in push_value:
                                slice_value = push_value["$slice"]
                            if "$position" in push_value:
                                position_value = push_value["$position"]
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Check for $slice without $each
                        if (
                            isinstance(push_value, dict)
                            and "$slice" in push_value
                            and "$each" not in push_value
                        ):
                            slice_value = push_value["$slice"]

                        # Check for $position without $each
                        if (
                            isinstance(push_value, dict)
                            and "$position" in push_value
                            and "$each" not in push_value
                        ):
                            position_value = push_value["$position"]

                        json_path = f"'{parse_json_path(field)}'"

                        # Convert values to add
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in values_to_push
                        ]
                        # Build JSON array of new values
                        new_values_json = neosqlite_json_dumps(converted_values)

                        # Handle $position: need to reconstruct array with insertion at position
                        if position_value is not None:
                            # Position 0 = insert at beginning
                            # Position N = insert after N elements
                            # Clamp position to valid range
                            position = int(position_value)
                            if position < 0:
                                position = 0

                            if slice_value is not None and slice_value == 0:
                                # Slice 0 means empty array
                                set_clauses.append(f"{json_path}, json('[]')")
                            elif slice_value is not None:
                                # Both $position and $slice - apply slice after insertion
                                # Get elements before position, new values, then slice from position
                                slice_limit = (
                                    slice_value if slice_value > 0 else 1000000
                                )
                                set_clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {slice_limit} OFFSET {position}))"
                                )
                            else:
                                # Only $position, no $slice - insert at position
                                set_clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT -1 OFFSET {position}))"
                                )
                        elif slice_value is not None:
                            # Handle $slice only (existing logic)
                            if slice_value == 0:
                                # Slice 0 means empty array
                                set_clauses.append(f"{json_path}, json('[]')")
                            else:
                                # Get existing array, concatenate with new values, then slice
                                set_clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) UNION ALL SELECT value FROM json_each({new_values_json}) LIMIT {slice_value if slice_value > 0 else 1000000}))"
                                )
                        else:
                            # No $position or $slice - just append values using [#]
                            append_path = f"'{parse_json_path(field)}[#]'"
                            for val in values_to_push:
                                converted_val = _convert_bytes_to_binary(val)
                                if isinstance(
                                    converted_val, (dict, list, Binary)
                                ):
                                    param_value = neosqlite_json_dumps(
                                        converted_val
                                    )
                                    set_clauses.append(
                                        f"{append_path}, json(?)"
                                    )
                                else:
                                    param_value = converted_val
                                    set_clauses.append(f"{append_path}, ?")
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
                    # Tier 2: $addToSet (with or without $each) can use conditional SQL
                    insert_func = _get_json_function(
                        "insert", self.jsonb.jsonb_supported
                    )
                    for field, val in value.items():
                        json_path = f"'{parse_json_path(field)}'"

                        # Handle $each modifier
                        values_to_add = []
                        if isinstance(val, dict) and "$each" in val:
                            each_values = val["$each"]
                            if not isinstance(each_values, list):
                                each_values = [each_values]
                            values_to_add = each_values
                        else:
                            values_to_add = [val]

                        # Process each value
                        for each_val in values_to_add:
                            converted_val = _convert_bytes_to_binary(each_val)
                            if isinstance(converted_val, (Binary, dict, list)):
                                param_value = neosqlite_json_dumps(
                                    converted_val
                                )
                                use_json = True
                            else:
                                param_value = converted_val
                                use_json = False

                            array_path = json_path
                            append_path = f"'{parse_json_path(field)}[#]'"
                            if not use_json:
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
                                # For complex values (dict, list, Binary), use a more complex SQL
                                # We need to check if the JSON value already exists
                                cmd = (
                                    f"UPDATE {quote_table_name(self.collection.name)} "
                                    f"SET data = {insert_func}(data, {append_path}, json(?)) "
                                    f"WHERE id = ? AND NOT EXISTS ("
                                    f"  SELECT 1 FROM json_each(data, {array_path}) "
                                    f"  WHERE json(value) = json(?)"
                                    f")"
                                )
                                self.collection.db.execute(
                                    cmd, (param_value, int_doc_id, param_value)
                                )
                case "$bit":
                    # Tier 2: $bit using bitwise operators
                    for field, bit_spec in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        extract_func = _get_json_function(
                            "extract", self.jsonb.jsonb_supported
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
                case "$currentDate":
                    # SQL implementation for $currentDate
                    for field, type_spec in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        # Determine type: true defaults to date, { $type: "timestamp" } or { $type: "date" }
                        if (
                            isinstance(type_spec, dict)
                            and type_spec.get("$type") == "timestamp"
                        ):
                            type_value = "timestamp"
                        else:
                            type_value = "date"
                        # Set to current datetime ISO string
                        if type_value == "timestamp":
                            set_clauses.append(
                                f"{json_path}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                            )
                        else:
                            # For date type, match Python's datetime.now().isoformat() format
                            set_clauses.append(
                                f"{json_path}, strftime('%Y-%m-%dT%H:%M:%f', 'now')"
                            )
                case "$rename":
                    # SQL implementation for $rename using single UPDATE
                    # Combine json_set and json_remove in a single operation
                    for old_field, new_field in value.items():
                        old_json_path = f"'{parse_json_path(old_field)}'"
                        new_json_path = f"'{parse_json_path(new_field)}'"
                        extract_func = _get_json_function(
                            "extract", self.jsonb.jsonb_supported
                        )
                        # First set the new field, then remove the old field
                        # We need to nest the operations: json_remove(json_set(data, new_path, value), old_path)
                        # This requires special handling - we'll use a combined approach
                        set_clauses.append(
                            f"{new_json_path}, {extract_func}(data, {old_json_path})"
                        )
                        unset_clauses.append(old_json_path)
                case _:
                    # For other operations, use the standard approach
                    clauses, params = self._build_sql_update_clause(op, value)
                    if clauses:
                        set_clauses.extend(clauses)
                        set_params.extend(params)

        # Execute updates using a single consolidated UPDATE statement if possible
        # This significantly reduces disk I/O and transaction overhead

        # Combine all operations into a single nested data update
        # We start with 'data' and wrap it with functions in order
        current_data = "data"
        all_params = []

        # Special handling for $rename: combine json_set and json_remove logic
        has_rename = any(op == "$rename" for op in update_spec.keys())
        if has_rename and set_clauses and unset_clauses:
            set_func = _get_json_function("set", self.jsonb.jsonb_supported)
            remove_func = _get_json_function(
                "remove", self.jsonb.jsonb_supported
            )
            current_data = f"{remove_func}({set_func}({current_data}, {', '.join(set_clauses)}), {', '.join(unset_clauses)})"
            all_params.extend(set_params)
            # Clear them so we don't process them again below
            set_clauses = []
            unset_clauses = []

        # Process remaining clauses in order to build a single nested expression
        if unset_clauses:
            func_name = _get_json_function("remove", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(unset_clauses)})"
            )
            all_params.extend(unset_params)

        if insert_clauses:
            func_name = _get_json_function("insert", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(insert_clauses)})"
            )
            all_params.extend(insert_params)

        if replace_clauses:
            func_name = _get_json_function(
                "replace", self.jsonb.jsonb_supported
            )
            current_data = (
                f"{func_name}({current_data}, {', '.join(replace_clauses)})"
            )
            all_params.extend(replace_params)

        if set_clauses:
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            current_data = (
                f"{func_name}({current_data}, {', '.join(set_clauses)})"
            )
            all_params.extend(set_params)

        if current_data != "data":
            cmd = f"UPDATE {quote_table_name(self.collection.name)} SET data = {current_data} WHERE id = ?"
            cursor = self.collection.db.execute(cmd, all_params + [int_doc_id])
            if cursor.rowcount == 0:
                raise RuntimeError(f"No rows updated for doc_id {doc_id}")
        elif (
            not has_rename
        ):  # If has_rename was True, it was already handled or clauses were cleared
            # This check might be redundant if we ensured at least one op, but safe to keep
            pass

        # Fetch updated document
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT id, {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        if row := self.collection.db.execute(cmd, (int_doc_id,)).fetchone():
            return self.collection._load(row[0], row[1])
        raise RuntimeError("Failed to fetch updated document")

    def _get_document_fields(self, doc_id: Any) -> set[str]:
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
        jsonb = self.jsonb.jsonb_supported
        cmd = (
            f"SELECT {json_data_column(jsonb)} as data "
            f"FROM {quote_table_name(self.collection.name)} WHERE id = ?"
        )

        row = self.collection.db.execute(cmd, (int_doc_id,)).fetchone()
        if not row:
            return set()

        # Parse the JSON to get field names
        try:
            doc_data = neosqlite_json_loads(
                row[0] if self.jsonb.jsonb_supported else row[0]
            )
            if isinstance(doc_data, dict):
                return set(doc_data.keys())
            else:
                return set()
        except Exception as e:
            # If we can't parse the document, return empty set
            logger.debug(
                f"Failed to parse document for indexed fields extraction: {e}"
            )
            return set()

    def _build_update_clause(
        self,
        update: dict[str, Any],
    ) -> tuple[str, list[Any]] | None:
        """
        Build the SQL update clause based on the provided update operations.

        Args:
            update (dict[str, Any]): A dictionary containing update operations.

        Returns:
            tuple[str, list[Any]] | None: A tuple containing the SQL update clause
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
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + ?"
                        )
                        params.append(field_val)
                case "$mul":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(field_val)
                case "$min":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, min({self.jsonb.json_function_prefix}_extract(data, {json_path}), ?)"
                        )
                        params.append(field_val)
                case "$max":
                    for field, field_val in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        set_clauses.append(
                            f"{json_path}, max({self.jsonb.json_function_prefix}_extract(data, {json_path}), ?)"
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
                            "remove", self.jsonb.jsonb_supported
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
                        # Determine type: true defaults to date, { $type: "timestamp" } or { $type: "date" }
                        if (
                            isinstance(type_spec, dict)
                            and type_spec.get("$type") == "timestamp"
                        ):
                            type_value = "timestamp"
                        else:
                            type_value = "date"
                        # Set to current datetime ISO string to match Python implementation
                        # Use strftime for consistent ISO format (Python uses isoformat() like '2026-03-15T12:34:56.789012')
                        if type_value == "timestamp":
                            set_clauses.append(
                                f"{json_path}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                            )
                        else:
                            # For date type, match Python's datetime.now().isoformat() format
                            set_clauses.append(
                                f"{json_path}, strftime('%Y-%m-%dT%H:%M:%f', 'now')"
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
                            "remove", self.jsonb.jsonb_supported
                        )
                        return (
                            f"data = {func_name}(data, {', '.join(set_clauses)})",
                            params,
                        )
                    else:
                        return None
                case "$push":
                    # Optimized $push (with $each, optionally with $slice) using [#]
                    for field, push_value in value.items():
                        # Handle $each, $position, and $slice
                        values_to_push = []
                        slice_value = None
                        position_value = None
                        if (
                            isinstance(push_value, dict)
                            and "$each" in push_value
                        ):
                            # Check for $slice and $position
                            if "$slice" in push_value:
                                slice_value = push_value["$slice"]
                            if "$position" in push_value:
                                position_value = push_value["$position"]
                            # SQL optimization supports $each, $position, and $slice
                            values_to_push = push_value["$each"]
                            if not isinstance(values_to_push, list):
                                values_to_push = [values_to_push]
                        else:
                            values_to_push = [push_value]

                        # Check for $slice without $each
                        if (
                            isinstance(push_value, dict)
                            and "$slice" in push_value
                            and "$each" not in push_value
                        ):
                            slice_value = push_value["$slice"]

                        # Check for $position without $each
                        if (
                            isinstance(push_value, dict)
                            and "$position" in push_value
                            and "$each" not in push_value
                        ):
                            position_value = push_value["$position"]

                        json_path = f"'{parse_json_path(field)}'"

                        # Handle $position or $slice: need to reconstruct array
                        if (
                            position_value is not None
                            or slice_value is not None
                        ):
                            # Convert values to add
                            converted_values = [
                                _convert_bytes_to_binary(v)
                                for v in values_to_push
                            ]
                            # Check if any values are complex
                            has_complex = any(
                                isinstance(v, (dict, list, Binary))
                                for v in converted_values
                            )

                            # Build JSON array of new values
                            new_values_json = neosqlite_json_dumps(
                                converted_values
                            )

                            # Handle $position: insert at specific position
                            if position_value is not None:
                                position = int(position_value)
                                if position < 0:
                                    position = 0

                                if slice_value is not None and slice_value == 0:
                                    # Slice 0 means empty array
                                    set_clauses.append(
                                        f"{json_path}, json('[]')"
                                    )
                                elif slice_value is not None:
                                    # Both $position and $slice
                                    slice_limit = (
                                        slice_value
                                        if slice_value > 0
                                        else 1000000
                                    )
                                    set_clauses.append(
                                        f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {slice_limit} OFFSET {position}))"
                                    )
                                else:
                                    # Only $position, no $slice
                                    set_clauses.append(
                                        f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT -1 OFFSET {position}))"
                                    )
                            elif slice_value is not None:
                                # Handle $slice only
                                if slice_value == 0:
                                    # Slice 0 means empty array
                                    set_clauses.append(
                                        f"{json_path}, json('[]')"
                                    )
                                else:
                                    # Get existing array, concatenate with new values, then slice
                                    set_clauses.append(
                                        f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) UNION ALL SELECT value FROM json_each({new_values_json}) LIMIT {slice_value if slice_value > 0 else 1000000}))"
                                    )
                        else:
                            # No $position or $slice - just append values
                            append_path = f"'{parse_json_path(field)}[#]'"
                            for val in values_to_push:
                                converted_val = _convert_bytes_to_binary(val)
                                set_clauses.append(f"{append_path}, ?")
                                params.append(converted_val)
                case "$setOnInsert":
                    # $setOnInsert only applies on upsert (doc_id == 0)
                    # For existing documents (doc_id != 0), this is a no-op
                    # We can safely skip it in the SQL path
                    pass
                case "$rename":
                    # $rename is handled in _perform_enhanced_sql_update for proper
                    # set + unset ordering. Return None here to use enhanced path.
                    return None
                case "$pull":
                    # SQL optimization for $pull: filter array elements using json_each
                    for field, pull_value in value.items():
                        json_path = f"'{parse_json_path(field)}'"
                        converted_val = _convert_bytes_to_binary(pull_value)
                        if isinstance(converted_val, (dict, list, Binary)):
                            pull_value_json = neosqlite_json_dumps(
                                converted_val
                            )
                            set_clauses.append(
                                f"{json_path}, (SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE json(value) != {pull_value_json} OR json(value) IS NULL)"
                            )
                        else:
                            set_clauses.append(
                                f"{json_path}, (SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value != ?)"
                            )
                            params.append(converted_val)
                case "$pullAll":
                    # SQL optimization for $pullAll: filter multiple values from array
                    for field, pull_values in value.items():
                        if not isinstance(pull_values, list):
                            pull_values = [pull_values]
                        json_path = f"'{parse_json_path(field)}'"

                        has_complex = any(
                            isinstance(
                                _convert_bytes_to_binary(v),
                                (dict, list, Binary),
                            )
                            for v in pull_values
                        )

                        if has_complex:
                            pull_values_json = [
                                neosqlite_json_dumps(
                                    _convert_bytes_to_binary(v)
                                )
                                for v in pull_values
                            ]
                            conditions = " OR ".join(
                                [
                                    f"json(value) != {v}"
                                    for v in pull_values_json
                                ]
                            )
                            set_clauses.append(
                                f"{json_path}, (SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE {conditions} OR json(value) IS NULL)"
                            )
                        else:
                            placeholders = ", ".join(["?" for _ in pull_values])
                            converted_values = [
                                _convert_bytes_to_binary(v) for v in pull_values
                            ]
                            set_clauses.append(
                                f"{json_path}, (SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value NOT IN ({placeholders}))"
                            )
                            params.extend(converted_values)
                case _:
                    return None  # Fallback for unsupported operators

        if not set_clauses:
            return None

        # For $unset, we already returned above
        if "$unset" not in update:
            func_name = _get_json_function("set", self.jsonb.jsonb_supported)
            return f"data = {func_name}(data, {', '.join(set_clauses)})", params
        else:
            # This case should have been handled above
            return None

    def _build_sql_update_clause(
        self,
        op: str,
        value: Any,
    ) -> tuple[list[str], list[Any]]:
        """
        Build SQL update clause for a single operation.

        Args:
            op (str): The update operation, such as "$set", "$inc", "$mul", etc.
            value (Any): The value associated with the update operation.

        Returns:
            tuple[list[str], list[Any]]: A tuple containing the SQL update clauses
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
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) + ?"
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
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * json(?)"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        clauses.append(
                            f"{json_path}, COALESCE({self.jsonb.json_function_prefix}_extract(data, {json_path}), 0) * ?"
                        )
                        params.append(converted_val)
            case "$min":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(
                        f"{json_path}, min({self.jsonb.json_function_prefix}_extract(data, {json_path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, min({self.jsonb.json_function_prefix}_extract(data, {json_path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$max":
                for field, field_val in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    clauses.append(
                        f"{json_path}, max({self.jsonb.json_function_prefix}_extract(data, {json_path}), ?)"
                    )
                    # Convert bytes to Binary for proper JSON serialization
                    converted_val = _convert_bytes_to_binary(field_val)
                    # If it's a Binary object, serialize it to JSON and use json() function
                    if isinstance(converted_val, Binary):
                        clauses[-1] = (
                            f"{json_path}, max({self.jsonb.json_function_prefix}_extract(data, {json_path}), json(?))"
                        )
                        params.append(neosqlite_json_dumps(converted_val))
                    else:
                        params.append(converted_val)
            case "$push":
                # Optimized $push (with $each, optionally with $position and/or $slice) using [#]
                for field, push_value in value.items():
                    # Handle $each, $position, and $slice
                    values_to_push = []
                    slice_value = None
                    position_value = None
                    if isinstance(push_value, dict) and "$each" in push_value:
                        # Check for $slice and $position
                        if "$slice" in push_value:
                            slice_value = push_value["$slice"]
                        if "$position" in push_value:
                            position_value = push_value["$position"]
                        # SQL optimization supports $each, $position, and $slice
                        values_to_push = push_value["$each"]
                        if not isinstance(values_to_push, list):
                            values_to_push = [values_to_push]
                    else:
                        values_to_push = [push_value]

                    # Check for $slice without $each
                    if (
                        isinstance(push_value, dict)
                        and "$slice" in push_value
                        and "$each" not in push_value
                    ):
                        slice_value = push_value["$slice"]

                    # Check for $position without $each
                    if (
                        isinstance(push_value, dict)
                        and "$position" in push_value
                        and "$each" not in push_value
                    ):
                        position_value = push_value["$position"]

                    json_path = f"'{parse_json_path(field)}'"

                    # Handle $position or $slice: need to reconstruct array
                    if position_value is not None or slice_value is not None:
                        # Convert values to add
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in values_to_push
                        ]
                        # Build JSON array of new values
                        new_values_json = neosqlite_json_dumps(converted_values)

                        # Handle $position: insert at specific position
                        if position_value is not None:
                            position = int(position_value)
                            if position < 0:
                                position = 0

                            if slice_value is not None and slice_value == 0:
                                # Slice 0 means empty array
                                clauses.append(f"{json_path}, json('[]')")
                            elif slice_value is not None:
                                # Both $position and $slice
                                slice_limit = (
                                    slice_value if slice_value > 0 else 1000000
                                )
                                clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {slice_limit} OFFSET {position}))"
                                )
                            else:
                                # Only $position, no $slice
                                clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT {position} UNION ALL SELECT value FROM json_each({new_values_json}) UNION ALL SELECT value FROM json_each(json_extract(data, {json_path})) LIMIT -1 OFFSET {position}))"
                                )
                        elif slice_value is not None:
                            # Handle $slice only
                            if slice_value == 0:
                                # Slice 0 means empty array
                                clauses.append(f"{json_path}, json('[]')")
                            else:
                                # Get existing array, concatenate with new values, then slice
                                clauses.append(
                                    f"{json_path}, (SELECT json_group_array(value) FROM (SELECT value FROM json_each(json_extract(data, {json_path})) UNION ALL SELECT value FROM json_each({new_values_json}) LIMIT {slice_value if slice_value > 0 else 1000000}))"
                                )
                    else:
                        # No $position or $slice - just append values
                        append_path = f"'{parse_json_path(field)}[#]'"
                        for val in values_to_push:
                            # Convert bytes to Binary for proper JSON serialization
                            converted_val = _convert_bytes_to_binary(val)
                            if isinstance(converted_val, Binary):
                                clauses.append(f"{append_path}, json(?)")
                                params.append(
                                    neosqlite_json_dumps(converted_val)
                                )
                            elif isinstance(converted_val, (dict, list)):
                                clauses.append(f"{append_path}, json(?)")
                                params.append(
                                    neosqlite_json_dumps(converted_val)
                                )
                            else:
                                clauses.append(f"{append_path}, ?")
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
                            "insert", self.jsonb.jsonb_supported
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
            case "$pull":
                # SQL optimization for $pull: filter array elements using json_each
                for field, pull_value in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    converted_val = _convert_bytes_to_binary(pull_value)
                    if isinstance(converted_val, (dict, list, Binary)):
                        # For complex values, serialize to JSON for comparison
                        pull_value_json = neosqlite_json_dumps(converted_val)
                        clauses.append(
                            f"{json_path}, (SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE json(value) != {pull_value_json} OR json(value) IS NULL)"
                        )
                    else:
                        # For simple values, compare directly
                        clauses.append(
                            f"{json_path}, (SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value != ?)"
                        )
                        params.append(converted_val)
            case "$pullAll":
                # SQL optimization for $pullAll: filter multiple values from array
                for field, pull_values in value.items():
                    if not isinstance(pull_values, list):
                        pull_values = [pull_values]
                    json_path = f"'{parse_json_path(field)}'"

                    # Check if any values are complex (dict, list, Binary)
                    has_complex = any(
                        isinstance(
                            _convert_bytes_to_binary(v), (dict, list, Binary)
                        )
                        for v in pull_values
                    )

                    if has_complex:
                        # For complex values, serialize each and build JSON comparison
                        pull_values_json = [
                            neosqlite_json_dumps(_convert_bytes_to_binary(v))
                            for v in pull_values
                        ]
                        conditions = " OR ".join(
                            [f"json(value) != {v}" for v in pull_values_json]
                        )
                        clauses.append(
                            f"{json_path}, (SELECT json_group_array(json(value)) FROM json_each(json_extract(data, {json_path})) WHERE {conditions} OR json(value) IS NULL)"
                        )
                    else:
                        # For simple values, use IN clause
                        placeholders = ", ".join(["?" for _ in pull_values])
                        converted_values = [
                            _convert_bytes_to_binary(v) for v in pull_values
                        ]
                        clauses.append(
                            f"{json_path}, (SELECT json_group_array(value) FROM json_each(json_extract(data, {json_path})) WHERE value NOT IN ({placeholders}))"
                        )
                        params.extend(converted_values)
            case "$currentDate":
                # SQL implementation for $currentDate
                for field, type_spec in value.items():
                    json_path = f"'{parse_json_path(field)}'"
                    # Determine type: true defaults to date, { $type: "timestamp" } or { $type: "date" }
                    if (
                        isinstance(type_spec, dict)
                        and type_spec.get("$type") == "timestamp"
                    ):
                        type_value = "timestamp"
                    else:
                        type_value = "date"
                    # Set to current datetime ISO string to match Python implementation
                    if type_value == "timestamp":
                        clauses.append(
                            f"{json_path}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                        )
                    else:
                        # For date type, match Python's datetime.now().isoformat() format
                        clauses.append(
                            f"{json_path}, strftime('%Y-%m-%dT%H:%M:%f', 'now')"
                        )
            case "$setOnInsert":
                # $setOnInsert only applies on upsert (doc_id == 0)
                # For existing documents (doc_id != 0), this is a no-op
                # We can safely skip it - return empty clauses
                pass
            case "$rename":
                # SQL implementation for $rename
                # $rename requires: get value, set at new path, remove old path
                # This is handled in _perform_enhanced_sql_update specially
                # Return empty to trigger fallback for complex cases
                return [], []

        return clauses, params

    def _validate_inc_mul_types_sql(
        db: Any,
        collection_name: str,
        where_clause: str | None,
        where_params: list[Any],
        update: dict[str, Any],
        jsonb_supported: bool,
    ) -> bool:
        """
        Validate that fields in $inc/$mul operations are numeric.

        This checks the JSON type of each field being incremented/multiplied
        to ensure they're numeric types.

        Args:
            db: Database connection
            collection_name: Name of the collection
            where_clause: The translated WHERE clause
            where_params: Parameters for the WHERE clause
            update: The update operations
            jsonb_supported: Whether JSONB is supported

        Returns:
            True if all fields are numeric or don't exist, False if any field is non-numeric
        """
        from .utils import _get_json_function

        fields_to_check = []
        for op in ("$inc", "$mul"):
            if op in update:
                fields_to_check.extend(update[op].keys())

        if not fields_to_check:
            return True

        json_func = _get_json_function("type", jsonb_supported)

        # Build a single query to check all fields
        select_expressions = []
        for field in fields_to_check:
            # json_type(data, '$.field') or jsonb_type(data, '$.field')
            json_path = f"$.{field}"
            select_expressions.append(f"{json_func}(data, '{json_path}')")

        if not where_clause:
            where_clause = "WHERE 1=1"

        cmd = f"SELECT {', '.join(select_expressions)} FROM {quote_table_name(collection_name)} {where_clause} LIMIT 1"
        try:
            cursor = db.execute(cmd, where_params)
            row = cursor.fetchone()
            if row:
                for field_type in row:
                    if field_type is not None:
                        if jsonb_supported:
                            # JSONB type returns 'number' for both int and float
                            if field_type not in (
                                "number",
                                "null",
                                "integer",
                                "real",
                            ):
                                return False
                        else:
                            # Standard JSON type returns 'integer' or 'real'
                            if field_type not in ("null", "integer", "real"):
                                return False
            # If no row matches, the update will be a no-op anyway, so it's safe to use fast path
            return True
        except Exception as e:
            # Fallback to slow path on any SQL error
            logger.debug(f"Fast path check failed due to SQL error: {e}")
            return False
