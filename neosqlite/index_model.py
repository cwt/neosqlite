"""IndexModel class for PyMongo-compatible index creation."""

from __future__ import annotations

from typing import Any, List, Tuple, Union

from .collection.cursor import ASCENDING


class IndexModel:
    """Create an Index instance.

    For use with create_indexes().

    Takes either a single key or a list containing (key, direction) pairs
    or keys. If no direction is given, ASCENDING will be assumed.

    Parameters:
        keys: A single key or a list of (key, direction) pairs or keys.
        **kwargs: Additional index creation options (unique, sparse, etc.).
    """

    def __init__(
        self,
        keys: Union[str, List[str], List[Tuple[str, int]]],
        **kwargs: Any,
    ):
        self._keys = keys
        self._kwargs = kwargs

    @property
    def document(self) -> dict[str, Any]:
        """An index document suitable for passing to the createIndexes command."""
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
        for option, value in self._kwargs.items():
            doc[option] = value

        return doc
