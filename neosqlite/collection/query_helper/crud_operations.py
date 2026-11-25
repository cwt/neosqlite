"""CRUD operations for QueryHelper."""

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from ..._sqlite import sqlite3
from ...objectid import ObjectId
from ...sql_utils import quote_table_name
from ..json_helpers import neosqlite_json_dumps

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .. import Collection


class CRUDOperationsMixin:
    """Mixin providing low-level CRUD operations for QueryHelper.

    Implements internal insert, replace, and delete helpers against the
    underlying SQLite table, including ``_id`` generation/preservation,
    ``bytes``-to-``Binary`` conversion, and JSON validation.

    Expected attributes from host class:
        - collection: Target ``Collection`` instance.
        - _get_integer_id_for_oid: Resolver mapping document IDs to row IDs.
        - _validate_json_document: Validator for serialized JSON documents.
        - _get_json_error_position: Diagnostic helper for invalid JSON.
    """

    collection: "Collection"
    _get_integer_id_for_oid: Any
    _validate_json_document: Any
    _get_json_error_position: Any

    def _internal_insert(self, document: dict[str, Any]) -> Any:
        """Insert a document into the collection and return its _id.

        Generates a new ``ObjectId`` when ``_id`` is omitted or ``None``,
        converts ``bytes`` values to ``Binary`` for JSON serialization, and
        validates the resulting JSON string before inserting into SQLite. If
        ``_id`` was not present in ``document``, it is populated in place.

        Args:
            document: The document dictionary to insert.

        Returns:
            Any: The generated ``ObjectId`` or user-provided ``_id`` value.

        Raises:
            MalformedDocument: If ``document`` is not a dictionary.
            ValueError: If serialized JSON validation fails.
            sqlite3.Error: If the SQLite insert fails to yield a row ID.
        """
        from ...exceptions import MalformedDocument

        # Imported locally so tests and embedders can monkeypatch the
        # serialization hook on the json_helpers module.
        from ..json_helpers import neosqlite_json_dumps
        from .utils import _convert_bytes_to_binary

        if not isinstance(document, dict):
            raise MalformedDocument(
                f"document must be a dictionary, not a {type(document)}"
            )

        doc_to_insert = deepcopy(document)
        original_has_id = "_id" in doc_to_insert

        # Handle _id generation if not provided in the document
        if not original_has_id or document["_id"] is None:
            # Generate a new ObjectId for the _id field
            generated_id: ObjectId | Any = ObjectId()
        else:
            # If _id was provided in the original document, keep the original value as-is
            generated_id = document["_id"]

        doc_to_insert["_id"] = generated_id

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

        # Insert with the _id value in the dedicated column
        cursor = self.collection.db.execute(
            f"INSERT INTO {quote_table_name(self.collection.name)}(data, _id) VALUES (?, ?)",
            (
                json_str,
                (
                    generated_id
                    if isinstance(generated_id, (str, int, float, bytes))
                    and not isinstance(generated_id, bool)
                    else str(generated_id)
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

    def _internal_replace(self, doc_id: Any, replacement: dict[str, Any]):
        """Replace an entire document in the collection.

        Preserves the existing ``_id`` from the stored JSON document or the
        dedicated ``_id`` column when ``replacement`` omits ``"_id"``.

        Args:
            doc_id: ID of the document to replace (ObjectId, int, etc.).
            replacement: Replacement document mapping.
        """
        from ...exceptions import MalformedDocument
        from ...query_operators import _bson_values_equal
        from ..json_helpers import neosqlite_json_loads
        from .utils import _convert_bytes_to_binary

        if not isinstance(replacement, dict):
            raise MalformedDocument(
                f"replacement must be a dictionary, not a {type(replacement)}"
            )

        # Convert the doc_id to integer ID for internal operations
        int_doc_id = self._get_integer_id_for_oid(doc_id)
        replacement_copy = dict(replacement)

        existing_id: Any = None
        has_existing_id = False
        cursor = self.collection.db.execute(
            f"SELECT data, _id FROM {quote_table_name(self.collection.name)} WHERE id = ?",
            (int_doc_id,),
        )
        if row := cursor.fetchone():
            if row[0]:
                try:
                    orig_doc = neosqlite_json_loads(row[0])
                    if "_id" in orig_doc:
                        existing_id = orig_doc["_id"]
                        has_existing_id = True
                except Exception:
                    pass
            if not has_existing_id and row[1] is not None:
                existing_id = self.collection._parse_stored_id(row[1])
                has_existing_id = True

        if "_id" in replacement_copy:
            if has_existing_id and not _bson_values_equal(
                replacement_copy["_id"], existing_id
            ):
                raise MalformedDocument(
                    "Performing an update on the path '_id' would modify the immutable field '_id'"
                )
        elif has_existing_id:
            replacement_copy["_id"] = existing_id

        replacement_copy = _convert_bytes_to_binary(replacement_copy)

        self.collection.db.execute(
            f"UPDATE {quote_table_name(self.collection.name)} SET data = ? WHERE id = ?",
            (neosqlite_json_dumps(replacement_copy), int_doc_id),
        )

    def _internal_delete(self, doc_id: Any):
        """Delete a document from the collection by document ID.

        Resolves ``doc_id`` to the internal SQLite integer row ID and deletes
        the matching row from the collection table.

        Args:
            doc_id: ID of the document to delete (ObjectId, int, etc.).
        """
        # Convert the doc_id to integer ID for internal operations
        int_doc_id = self._get_integer_id_for_oid(doc_id)
        self.collection.db.execute(
            f"DELETE FROM {quote_table_name(self.collection.name)} WHERE id = ?",
            (int_doc_id,),
        )
