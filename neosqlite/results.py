from typing import List


class InsertOneResult:
    """
    Represents the result of a single insert operation.
    """

    def __init__(self, inserted_id: int):
        """
        Initialize an InsertOneResult object.

        Args:
            inserted_id (int): The ID of the inserted document.
        """
        self._inserted_id = inserted_id

    @property
    def inserted_id(self) -> int:
        """
        The ID of the inserted document.

        Returns:
            int: The ID of the inserted document.
        """
        return self._inserted_id


class InsertManyResult:
    """
    Represents the result of a multiple insert operation.
    """

    def __init__(self, inserted_ids: List[int]):
        """
        Initialize an InsertManyResult object.

        Args:
            inserted_ids (List[int]): The IDs of the inserted documents.
        """
        self._inserted_ids = inserted_ids

    @property
    def inserted_ids(self) -> List[int]:
        """
        The IDs of the inserted documents.

        Returns:
            List[int]: The IDs of the inserted documents.
        """
        return self._inserted_ids


class UpdateResult:
    """
    Represents the result of a single update operation.
    """

    def __init__(
        self,
        matched_count: int,
        modified_count: int,
        upserted_id: int | None,
    ):
        """
        Initialize an UpdateResult object.

        Args:
            matched_count (int): The number of documents that matched the filter criteria.
            modified_count (int): The number of documents that were modified.
            upserted_id (int | None): The ID of the inserted document if an upsert
                                      operation was performed; None otherwise.
        """
        self._matched_count = matched_count
        self._modified_count = modified_count
        self._upserted_id = upserted_id

    @property
    def matched_count(self) -> int:
        """
        The number of documents that matched the filter criteria.

        Returns:
            int: The number of documents that matched the filter criteria.
        """
        return self._matched_count

    @property
    def modified_count(self) -> int:
        """
        The number of documents that were modified.

        Returns:
            int: The number of documents that were modified.
        """
        return self._modified_count

    @property
    def upserted_id(self) -> int | None:
        """
        The ID of the inserted document.

        Returns:
            int | None: The ID of the inserted document if an upsert operation was
                        performed; None otherwise.
        """
        return self._upserted_id


class DeleteResult:
    """
    Represents the result of a single delete operation.
    """

    def __init__(self, deleted_count: int):
        """
        Initialize a DeleteResult object with the count of deleted documents.

        Args:
            deleted_count (int): The number of documents that were deleted.
        """
        self._deleted_count = deleted_count

    @property
    def deleted_count(self) -> int:
        """
        The number of documents that were deleted.

        Returns:
            int: The number of documents that were deleted.
        """
        return self._deleted_count


class BulkWriteResult:
    """
    Represents the result of a bulk write operation.
    """

    def __init__(
        self,
        inserted_count: int,
        matched_count: int,
        modified_count: int,
        deleted_count: int,
        upserted_count: int,
    ):
        """
        Initialize a BulkWriteResult object with counts of various operations.

        Args:
            inserted_count (int): The number of documents that were inserted.
            matched_count (int): The number of documents that matched the filter criteria.
            modified_count (int): The number of documents that were modified.
            deleted_count (int): The number of documents that were deleted.
            upserted_count (int): The number of documents that were upserted.
        """
        self.inserted_count = inserted_count
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count
        self.upserted_count = upserted_count
