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


class UpdateMany:
    """
    Represents an update operation for multiple documents.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
    ):
        """
        Initialize an UpdateMany object.

        Args:
            filter (dict[str, Any]): The filter criteria for selecting documents to update.
            update (dict[str, Any]): The update operations to apply to selected documents.
            upsert (bool, optional): If True, insert a document if none matches. Defaults to False.
            array_filters (list[dict[str, Any]], optional): Filter documents for array positional operators.
        """
        self.filter = filter
        self.update = update
        self.upsert = upsert
        self.array_filters = array_filters


class ReplaceOne:
    """
    Represents a replace operation for a single document.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        upsert: bool = False,
    ):
        """
        Initialize a ReplaceOne object.

        Args:
            filter (dict[str, Any]): The filter criteria for selecting the document to replace.
            replacement (dict[str, Any]): The replacement document.
            upsert (bool, optional): If True, insert a document if none matches. Defaults to False.
        """
        self.filter = filter
        self.replacement = replacement
        self.upsert = upsert


class DeleteMany:
    """
    Represents a delete operation for multiple documents.
    """

    def __init__(self, filter: dict[str, Any]):
        """
        Initialize a DeleteMany object.

        Args:
            filter (dict[str, Any]): The filter criteria for selecting documents to delete.
        """
        self.filter = filter


def _parse_bulk_request(req: Any) -> tuple[str, dict[str, Any]]:
    """
    Parse a bulk write request into operation type and kwargs.

    Supports neosqlite.requests, pymongo.operations, and duck-typed objects.
    """
    cls_name = type(req).__name__
    if cls_name == "InsertOne" or (
        (hasattr(req, "document") or hasattr(req, "_doc"))
        and not hasattr(req, "filter")
        and not hasattr(req, "_filter")
    ):
        doc = getattr(req, "document", getattr(req, "_doc", None))
        return "insert_one", {"document": doc}

    if cls_name == "ReplaceOne" or hasattr(req, "replacement"):
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        replacement = getattr(req, "replacement", getattr(req, "_doc", None))
        upsert = bool(
            getattr(req, "upsert", False) or getattr(req, "_upsert", False)
        )
        return "replace_one", {
            "filter": flt,
            "replacement": replacement,
            "upsert": upsert,
        }

    if cls_name == "UpdateOne":
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        update = getattr(req, "update", getattr(req, "_doc", None))
        upsert = bool(
            getattr(req, "upsert", False) or getattr(req, "_upsert", False)
        )
        array_filters = getattr(
            req, "array_filters", getattr(req, "_array_filters", None)
        )
        return "update_one", {
            "filter": flt,
            "update": update,
            "upsert": upsert,
            "array_filters": array_filters,
        }

    if cls_name == "UpdateMany":
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        update = getattr(req, "update", getattr(req, "_doc", None))
        upsert = bool(
            getattr(req, "upsert", False) or getattr(req, "_upsert", False)
        )
        array_filters = getattr(
            req, "array_filters", getattr(req, "_array_filters", None)
        )
        return "update_many", {
            "filter": flt,
            "update": update,
            "upsert": upsert,
            "array_filters": array_filters,
        }

    if cls_name == "DeleteOne":
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        return "delete_one", {"filter": flt}

    if cls_name == "DeleteMany":
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        return "delete_many", {"filter": flt}

    # Duck-typing fallback for generic custom objects
    if hasattr(req, "filter") and hasattr(req, "update"):
        flt = getattr(req, "filter")
        update = getattr(req, "update")
        upsert = bool(getattr(req, "upsert", False))
        array_filters = getattr(req, "array_filters", None)
        multi = bool(getattr(req, "multi", False))
        op = "update_many" if multi else "update_one"
        return op, {
            "filter": flt,
            "update": update,
            "upsert": upsert,
            "array_filters": array_filters,
        }

    if hasattr(req, "filter") or hasattr(req, "_filter"):
        flt = getattr(req, "filter", getattr(req, "_filter", None))
        multi = bool(getattr(req, "multi", False))
        op = "delete_many" if multi else "delete_one"
        return op, {"filter": flt}

    raise ValueError(f"Unknown bulk write operation: {req!r}")
