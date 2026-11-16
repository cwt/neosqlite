"""IndexModel class for PyMongo-compatible index creation."""

from __future__ import annotations

from typing import Any

from .collection.cursor import ASCENDING


class IndexModel:
    """Specification for a collection index passed to ``create_indexes()``.

    Wraps an index key specification (a single field name, a list of field
    names, or a list of ``(field, direction)`` tuples) together with optional
    index creation flags such as ``unique`` or ``sparse``. When no direction
    is supplied for a key, ``ASCENDING`` is assumed.
    """

    def __init__(
        self,
        keys: str | list[str] | list[tuple[str, int]],
        **kwargs: Any,
    ):
        """Initialize an ``IndexModel`` with keys and index options.

        Args:
            keys: Single field name, list of field names, or list of
                ``(key, direction)`` tuples specifying indexed fields.
            **kwargs: Additional index options (e.g., ``unique``, ``sparse``,
                ``name``) stored verbatim for the index document.
        """
        self._keys = keys
        self._kwargs = kwargs

    @property
    def document(self) -> dict[str, Any]:
        """Build an index specification document for ``createIndexes``.

        Converts a string ``keys`` value into ``{keys: ASCENDING}`` and a
        list of strings or ``(key, direction)`` tuples into a mapping under
        ``"key"`` (defaulting plain string entries to ``ASCENDING``), then
        merges any keyword options passed at initialization.

        Returns:
            dict[str, Any]: Dictionary containing the ``"key"`` mapping and
                any additional index creation options.
        """
        doc: dict[str, Any] = {}

        # Build the key specification
        if isinstance(self._keys, str):
            doc["key"] = {self._keys: ASCENDING}
        elif isinstance(self._keys, list):
            key_spec = {}
            for item in self._keys:
                if isinstance(item, tuple):
                    key_spec[item[0]] = item[1]
                else:
                    key_spec[item] = ASCENDING
            doc["key"] = key_spec

        # Add options
        doc.update(self._kwargs)

        return doc
