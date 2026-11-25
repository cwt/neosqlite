from typing import Any


class _BaseResult:
    """Base mixin providing PyMongo ``acknowledged`` compatibility."""

    @property
    def acknowledged(self) -> bool:
        """Return whether the write operation was acknowledged."""
        return True


class InsertOneResult(_BaseResult):
    """Represents the result of a single-document insert operation.

    Exposes the identifier assigned to or supplied for the inserted document
    via the ``inserted_id`` property.
    """

    def __init__(self, inserted_id: Any):
        """Initialize an ``InsertOneResult`` instance.

        Args:
            inserted_id: The ``_id`` of the inserted document.
        """
        self._inserted_id = inserted_id

    @property
    def inserted_id(self) -> Any:
        """Return the ``_id`` of the inserted document.

        Returns:
            Any: The identifier of the inserted document.
        """
        return self._inserted_id


class InsertManyResult(_BaseResult):
    """Represents the result of a multi-document insert operation.

    Exposes the ordered list of identifiers for all inserted documents via the
    ``inserted_ids`` property.
    """

    def __init__(self, inserted_ids: list[Any]):
        """Initialize an ``InsertManyResult`` instance.

        Args:
            inserted_ids: Ordered list of ``_id`` values for the inserted
                documents.
        """
        self._inserted_ids = inserted_ids

    @property
    def inserted_ids(self) -> list[Any]:
        """Return the ``_id`` values of the inserted documents.

        Returns:
            list[Any]: Ordered list of inserted document identifiers.
        """
        return self._inserted_ids


class UpdateResult(_BaseResult):
    """Represents the result of an update or replace operation.

    Encapsulates matched, modified, and optional upserted identifier details
    returned by ``update_one``, ``update_many``, and ``replace_one``.
    """

    def __init__(
        self,
        matched_count: int,
        modified_count: int,
        upserted_id: Any | None,
    ):
        """Initialize an ``UpdateResult`` instance.

        Args:
            matched_count: Number of documents matching the filter criteria.
            modified_count: Number of documents actually modified.
            upserted_id: ``_id`` of the newly inserted document when an upsert
                occurred, or ``None`` otherwise.
        """
        self._matched_count = matched_count
        self._modified_count = modified_count
        self._upserted_id = upserted_id

    @property
    def matched_count(self) -> int:
        """Return the number of documents matching the filter criteria.

        Returns:
            int: Count of matched documents.
        """
        return self._matched_count

    @property
    def modified_count(self) -> int:
        """Return the number of documents modified by the operation.

        Returns:
            int: Count of modified documents.
        """
        return self._modified_count

    @property
    def upserted_id(self) -> Any | None:
        """Return the ``_id`` of an upserted document, if any.

        Returns:
            Any | None: Identifier of the inserted document when an upsert
                occurred, or ``None`` otherwise.
        """
        return self._upserted_id


class DeleteResult(_BaseResult):
    """Represents the result of a single or multi-document delete operation.

    Exposes the total number of removed documents via ``deleted_count``.
    """

    def __init__(self, deleted_count: int):
        """Initialize a ``DeleteResult`` with the deleted document count.

        Args:
            deleted_count: Number of documents removed by the delete
                operation.
        """
        self._deleted_count = deleted_count

    @property
    def deleted_count(self) -> int:
        """Return the number of documents deleted by the operation.

        Returns:
            int: Count of deleted documents.
        """
        return self._deleted_count


class BulkWriteResult(_BaseResult):
    """Represents the aggregated result of a bulk write operation.

    Stores operation counts across inserts, updates, deletes, and upserts,
    along with a mapping from bulk operation indices to upserted identifiers.

    Attributes:
        inserted_count: Number of documents inserted.
        matched_count: Number of documents matched by update/replace
            operations.
        modified_count: Number of documents modified by update/replace
            operations.
        deleted_count: Number of documents deleted.
        upserted_count: Number of documents inserted via upserts.
        upserted_ids: Mapping from operation index to upserted ``_id``.
    """

    def __init__(
        self,
        inserted_count: int,
        matched_count: int,
        modified_count: int,
        deleted_count: int,
        upserted_count: int,
        upserted_ids: dict[int, Any] | None = None,
    ):
        """Initialize a ``BulkWriteResult`` with bulk operation counts.

        Args:
            inserted_count: Number of documents inserted.
            matched_count: Number of documents that matched filter criteria.
            modified_count: Number of documents modified.
            deleted_count: Number of documents deleted.
            upserted_count: Number of documents upserted.
            upserted_ids: Optional mapping from operation index to upserted
                ``_id``; defaults to an empty dictionary when ``None``.
        """
        self.inserted_count = inserted_count
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count
        self.upserted_count = upserted_count
        self.upserted_ids = upserted_ids if upserted_ids is not None else {}
