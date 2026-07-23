"""
Update operations for QueryHelper.

Dispatches between SQL-based and Python-based update paths.
The SQL implementation lives in _sql_updates.py (SqlUpdatesMixin).
"""

import logging
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..json_helpers import (
    neosqlite_json_dumps,
)

logger = logging.getLogger(__name__)

from ._sql_updates import SqlUpdatesMixin
from .positional_update import (
    _apply_positional_update,
    _set_nested_field,
)
from .utils import (
    _validate_inc_mul_field_value,
    get_force_fallback,
)

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext


class UpdateOperationsMixin(SqlUpdatesMixin):
    """
    A mixin class providing update operations for QueryHelper.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self.jsonb.jsonb_supported
    - self.jsonb.json_function_prefix
    - self._build_simple_where_clause method
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _get_integer_id_for_oid: Any

    def _internal_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """
        Helper method for updating documents.

        Attempts to use SQL-based updates for simple operations, falling back to
        Python-based updates for complex operations.

        Args:
            doc_id (Any): The ID of the document to update (can be ObjectId, int, etc.).
            update_spec (dict[str, Any]): The update specification.
            original_doc (dict[str, Any]): The original document before the update.
            array_filters (list[dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (dict[str, Any], optional): The query filter for $ operator.

        Returns:
            tuple[dict[str, Any], bool]: The updated document and whether it was modified.
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
        if get_force_fallback():
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

        # Try to use SQL-based updates for simple operations
        # Note: SQL updates don't support array_filters or positional operators, so fall back to Python if provided
        if array_filters:
            return self._perform_python_update(
                doc_id, update_spec, original_doc, array_filters, query_filter
            )

        if self._can_use_sql_updates(update_spec, doc_id, original_doc):
            # Use enhanced SQL update with json_insert/json_replace when possible
            try:
                updated_doc = self._perform_enhanced_sql_update(
                    doc_id, update_spec, original_doc
                )
                # For SQL updates, assume modified if we got a result
                return updated_doc, updated_doc != original_doc
            except Exception as e:
                # If enhanced update fails, fall back to standard SQL update
                logger.debug(
                    f"Enhanced update failed: {e}. Falling back to standard SQL update."
                )
                try:
                    updated_doc = self._perform_sql_update(doc_id, update_spec)
                    return updated_doc, updated_doc != original_doc
                except Exception as e2:
                    logger.debug(
                        f"Standard SQL update failed: {e2}. Falling back to Python update."
                    )
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

    def _perform_python_update(
        self,
        doc_id: Any,
        update_spec: dict[str, Any],
        original_doc: dict[str, Any],
        array_filters: list[dict[str, Any]] | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """
        Perform update operations using Python-based logic.

        Args:
            doc_id (Any): The document ID of the document to update (can be ObjectId, int, etc.).
            update_spec (dict[str, Any]): A dictionary specifying the update
                                          operations to perform.
            original_doc (dict[str, Any]): The original document before applying
                                           the updates.
            array_filters (list[dict[str, Any]], optional): Filter documents for array positional operators.
            query_filter (dict[str, Any], optional): The query filter for $ operator.

        Returns:
            tuple[dict[str, Any], bool]: The updated document and whether it was modified.
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
                        # Handle $each modifier
                        values_to_add = []
                        if isinstance(v, dict) and "$each" in v:
                            each_values = v["$each"]
                            if not isinstance(each_values, list):
                                each_values = [each_values]
                            values_to_add = each_values
                        else:
                            values_to_add = [v]
                        # Add each value if not already present
                        for val in values_to_add:
                            if val not in current_list:
                                current_list.append(val)
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
