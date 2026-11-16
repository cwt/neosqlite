from typing import Any


class MalformedQueryException(Exception):
    """Exception raised when a query is malformed.

    Raised during query validation, update specification parsing, or
    aggregation pipeline evaluation when syntax or operand types are invalid.
    """

    pass


class MalformedDocument(Exception):
    """Exception raised when a document is malformed.

    Raised when an operation receives a non-dictionary value or an invalid
    document structure where a valid document is required.
    """

    pass


class CollectionInvalid(Exception):
    """Exception raised when a collection is invalid.

    Raised for invalid collection management operations, such as creating a
    collection that already exists or renaming to or from invalid names.
    """

    pass


class InvalidOperation(Exception):
    """Exception raised when an operation is not valid in the current state.

    Raised when an API call cannot proceed in the object's current lifecycle
    or transaction state, such as committing without an active transaction.
    """

    pass


class BulkWriteError(Exception):
    """Exception raised when a bulk write operation has errors.

    Stores the error details and partial execution counts in ``self.details``
    so callers can inspect ``writeErrors`` and operation totals.
    """

    def __init__(self, results: dict[str, Any]):
        """Initialize a BulkWriteError with bulk operation results.

        Stores ``results`` on ``self.details`` and initializes the base
        ``Exception`` with ``str(results)``.

        Args:
            results: Dictionary with ``writeErrors``, ``writeConcernErrors``,
                operation counts, and ``upserted`` entries.
        """
        self.details = results
        super().__init__(str(results))
