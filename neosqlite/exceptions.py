from typing import Any


class MalformedQueryException(Exception):
    """
    Exception raised when a query is malformed.
    """

    pass


class MalformedDocument(Exception):
    """
    Exception raised when a document is malformed.
    """

    pass


class CollectionInvalid(Exception):
    """
    Exception raised when a collection is invalid.
    """

    pass


class InvalidOperation(Exception):
    """
    Exception raised when an operation is not valid in the current state.
    """

    pass


class BulkWriteError(Exception):
    """
    Exception raised when a bulk write operation has errors.
    """

    def __init__(self, results: dict[str, Any]):
        self.details = results
        super().__init__(str(results))
