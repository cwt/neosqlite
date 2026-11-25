from typing import Any


class InsertOne:
    """Represents an insert operation for a single document.

    Used with ``bulk_write()`` to enqueue inserting ``self.document`` into
    a collection.
    """

    def __init__(self, document: dict[str, Any]):
        """Initialize an InsertOne request.

        Args:
            document: The document dictionary to insert.
        """
        self.document = document


class UpdateOne:
    """Represents an update operation for a single document.

    Used with ``bulk_write()`` to update at most one document matching
    ``self.filter`` with ``self.update``, optionally upserting.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
    ):
        """Initialize an UpdateOne request.

        Args:
            filter: Query filter selecting the document to update.
            update: Update operations to apply to the matched document.
            upsert: If True, insert a new document when no document matches
                ``filter``. Defaults to False.
            array_filters: Optional list of filter dictionaries for positional
                filtered array updates. Defaults to None.
        """
        self.filter = filter
        self.update = update
        self.upsert = upsert
        self.array_filters = array_filters


class DeleteOne:
    """Represents a delete operation for a single document.

    Used with ``bulk_write()`` to delete at most one document matching
    ``self.filter``.
    """

    def __init__(self, filter: dict[str, Any]):
        """Initialize a DeleteOne request.

        Args:
            filter: Query filter selecting the document to delete.
        """
        self.filter = filter


class UpdateMany:
    """Represents an update operation for multiple documents.

    Used with ``bulk_write()`` to update all documents matching
    ``self.filter`` with ``self.update``, optionally supporting upserts
    and ``array_filters``.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
    ):
        """Initialize an UpdateMany request.

        Args:
            filter: Query filter selecting documents to update.
            update: Update operations to apply to matched documents.
            upsert: If True, insert a new document when no documents match
                ``filter``. Defaults to False.
            array_filters: Optional list of filter dictionaries for positional
                filtered array updates. Defaults to None.
        """
        self.filter = filter
        self.update = update
        self.upsert = upsert
        self.array_filters = array_filters


class ReplaceOne:
    """Represents a replace operation for a single document.

    Used with ``bulk_write()`` to replace at most one document matching
    ``self.filter`` with ``self.replacement``, optionally upserting.
    """

    def __init__(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        upsert: bool = False,
    ):
        """Initialize a ReplaceOne request.

        Args:
            filter: Query filter selecting the document to replace.
            replacement: The replacement document dictionary.
            upsert: If True, insert the replacement when no document matches
                ``filter``. Defaults to False.
        """
        self.filter = filter
        self.replacement = replacement
        self.upsert = upsert


class DeleteMany:
    """Represents a delete operation for multiple documents.

    Used with ``bulk_write()`` to delete all documents matching
    ``self.filter``.
    """

    def __init__(self, filter: dict[str, Any]):
        """Initialize a DeleteMany request.

        Args:
            filter: Query filter selecting documents to delete.
        """
        self.filter = filter


def _parse_bulk_request(req: Any) -> tuple[str, dict[str, Any]]:
    """Parse a bulk write request into an operation name and keyword args.

    Supports ``neosqlite.requests`` classes, ``pymongo.operations`` objects
    (via ``_doc``, ``_filter``, ``_upsert``, ``_array_filters``), and
    duck-typed objects with ``filter``, ``update``, or ``multi`` attributes.

    Args:
        req: Bulk write request object to inspect.

    Returns:
        tuple[str, dict[str, Any]]: Pair of ``(operation_name, kwargs)``
            where ``operation_name`` is one of ``"insert_one"``,
            ``"replace_one"``, ``"update_one"``, ``"update_many"``,
            ``"delete_one"``, or ``"delete_many"``.

    Raises:
        ValueError: If ``req`` does not match any supported bulk operation.
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
