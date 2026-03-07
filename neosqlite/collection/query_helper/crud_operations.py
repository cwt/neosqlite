"""CRUD operations for QueryHelper."""

from typing import TYPE_CHECKING, Any, Dict

from ..._sqlite import sqlite3
from ...sql_utils import quote_table_name
from ...objectid import ObjectId
from ..json_helpers import neosqlite_json_dumps

if TYPE_CHECKING:
    from .. import Collection


class CRUDOperationsMixin:
    """Mixin providing CRUD operations for QueryHelper."""

    collection: "Collection"
    _get_integer_id_for_oid: Any
    _validate_json_document: Any
    _get_json_error_position: Any

    def _internal_insert(self, document: Dict[str, Any]) -> Any:
        """
        Inserts a document into the collection and returns the inserted document's _id.

        This method inserts a document into the collection after converting any bytes
        objects to Binary objects for proper JSON serialization and validating the
        resulting JSON string. It handles both databases with JSON1 support and those
        without by providing appropriate fallbacks.

        Args:
            document (dict): The document to insert. Must be a dictionary.

        Returns:
            int: The auto-increment id of the inserted document.

        Raises:
            MalformedDocument: If the document is not a dictionary
            ValueError: If the document contains invalid JSON
            sqlite3.Error: If database operations fail
        """
        from ...exceptions import MalformedDocument
        from ..json_helpers import neosqlite_json_dumps
        from .utils import _convert_bytes_to_binary
        from copy import deepcopy

        if not isinstance(document, dict):
            raise MalformedDocument(
                f"document must be a dictionary, not a {type(document)}"
            )

        doc_to_insert = deepcopy(document)
        original_has_id = "_id" in doc_to_insert
        doc_to_insert.pop(
            "_id", None
        )  # Remove _id from doc_to_insert to avoid duplication

        # Convert any bytes objects to Binary objects for proper JSON serialization
        doc_to_insert = _convert_bytes_to_binary(doc_to_insert)

        # Serialize to JSON string
        json_str = neosqlite_json_dumps(doc_to_insert)

        # Validate JSON
        if not self._validate_json_document(json_str):
            # Try to get error position for better error reporting
            error_pos = self._get_json_error_position(json_str)
            if error_pos >= 0:
                raise ValueError(
                    f"Invalid JSON document at position {error_pos}"
                )
            else:
                raise ValueError("Invalid JSON document")

        # Handle _id generation if not provided in the document
        if not original_has_id:
            # Generate a new ObjectId for the _id field
            generated_id: ObjectId | Any = ObjectId()
        else:
            # If _id was provided in the original document, use that value in the _id column
            provided_id = document["_id"]

            if provided_id is None:
                # If _id was explicitly set to None, generate a new ObjectId
                generated_id = ObjectId()
            elif isinstance(provided_id, str) and len(provided_id) == 24:
                try:
                    generated_id = ObjectId(provided_id)
                except ValueError:
                    # If it's not a valid ObjectId string, keep the original
                    generated_id = provided_id
            elif isinstance(provided_id, ObjectId):
                generated_id = provided_id
            else:
                # For other types, keep the original value
                generated_id = provided_id

        # Insert with the _id value in the dedicated column
        cursor = self.collection.db.execute(
            f"INSERT INTO {quote_table_name(self.collection.name)}(data, _id) VALUES (?, ?)",
            (
                json_str,
                (
                    str(generated_id)
                    if hasattr(generated_id, "__str__")
                    else generated_id
                ),
            ),
        )
        inserted_id = cursor.lastrowid

        if inserted_id is None:
            raise sqlite3.Error("Failed to get last row id.")

        # Only add the _id field to the original document if it wasn't originally provided
        # This preserves the user-provided _id value if one was given
        if not original_has_id:
            document["_id"] = generated_id

        return generated_id

    def _internal_replace(self, doc_id: Any, replacement: Dict[str, Any]):
        """
        Replaces an entire document in the collection.

        Args:
            doc_id (Any): The ID of the document to replace (can be ObjectId, int, etc.).
            replacement (Dict[str, Any]): The new document to replace the existing one.
        """
        # Convert the doc_id to integer ID for internal operations
        int_doc_id = self._get_integer_id_for_oid(doc_id)
        self.collection.db.execute(
            f"UPDATE {quote_table_name(self.collection.name)} SET data = ? WHERE id = ?",
            (neosqlite_json_dumps(replacement), int_doc_id),
        )

    def _internal_delete(self, doc_id: Any):
        """
        Deletes a document from the collection based on the document ID.

        Args:
            doc_id (Any): The ID of the document to delete (can be ObjectId, int, etc.).
        """
        # Convert the doc_id to integer ID for internal operations
        int_doc_id = self._get_integer_id_for_oid(doc_id)
        self.collection.db.execute(
            f"DELETE FROM {quote_table_name(self.collection.name)} WHERE id = ?",
            (int_doc_id,),
        )
