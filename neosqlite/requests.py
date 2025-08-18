from typing import Any, Dict


class InsertOne:
    """
    Represents an insert operation for a single document.
    """

    def __init__(self, document: Dict[str, Any]):
        """
        Initialize an InsertOne object.

        Args:
            document (Dict[str, Any]): The document to be inserted.
        """
        self.document = document


class UpdateOne:
    """
    Represents an update operation for a single document.
    """

    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ):
        """
        Initialize an UpdateOne object.

        Args:
            filter (Dict[str, Any]): The filter criteria for selecting the document to update.
            update (Dict[str, Any]): The update operations to apply to the selected document.
            upsert (bool, optional): If True, insert the document if no document matches the filter criteria. Defaults to False.
        """
        self.filter = filter
        self.update = update
        self.upsert = upsert


class DeleteOne:
    """
    Represents a delete operation for a single document.
    """

    def __init__(self, filter: Dict[str, Any]):
        """
        Initialize a DeleteOne object.

        Args:
            filter (Dict[str, Any]): The filter criteria for selecting the document to delete.
        """
        self.filter = filter
