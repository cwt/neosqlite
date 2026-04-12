from typing import Any


class InsertOne:
    """
    Represents an insert operation for a single document.
    """

    def __init__(self, document: dict[str, Any]):
        """
        Initialize an InsertOne object.

        Args:
            document (dict[str, Any]): The document to be inserted.
        """
        self.document = document


class UpdateOne:
    """
    Represents an update operation for a single document.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ):
        """
        Initialize an UpdateOne object.

        Args:
            filter (dict[str, Any]): The filter criteria for selecting the document to update.
            update (dict[str, Any]): The update operations to apply to the selected document.
            upsert (bool, optional): If True, insert the document if no document matches the filter criteria. Defaults to False.
        """
        self.filter = filter
        self.update = update
        self.upsert = upsert


class DeleteOne:
    """
    Represents a delete operation for a single document.
    """

    def __init__(self, filter: dict[str, Any]):
        """
        Initialize a DeleteOne object.

        Args:
            filter (dict[str, Any]): The filter criteria for selecting the document to delete.
        """
        self.filter = filter
