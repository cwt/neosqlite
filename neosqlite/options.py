from __future__ import annotations
from typing import Any, Dict, Optional, Union


class WriteConcern:
    """
    Represents a write concern for MongoDB compatibility.
    Maps to SQLite synchronous PRAGMAs.
    """

    def __init__(
        self,
        w: Optional[Union[int, str]] = None,
        wtimeout: Optional[int] = None,
        j: Optional[bool] = None,
        fsync: Optional[bool] = None,
    ):
        self._document = {}
        if w is not None:
            self._document["w"] = w
        if wtimeout is not None:
            self._document["wtimeout"] = wtimeout
        if j is not None:
            self._document["j"] = j
        if fsync is not None:
            self._document["fsync"] = fsync

    @property
    def document(self) -> Dict[str, Any]:
        return self._document

    @property
    def acknowledged(self) -> bool:
        return self._document.get("w") != 0

    def __repr__(self) -> str:
        return f"WriteConcern({', '.join(f'{k}={v}' for k, v in self._document.items())})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, WriteConcern):
            return self._document == other._document
        return self._document == other


class ReadPreference:
    """
    Represents a read preference for MongoDB compatibility.
    (Mostly a placeholder in NeoSQLite as SQLite is single-node).
    """

    PRIMARY = 0
    PRIMARY_PREFERRED = 1
    SECONDARY = 2
    SECONDARY_PREFERRED = 3
    NEAREST = 4

    def __init__(
        self,
        mode: int,
        tag_sets: Optional[list] = None,
        max_staleness_ms: Optional[int] = None,
        hedge: Optional[dict] = None,
    ):
        self._mode = mode
        self._tag_sets = tag_sets
        self._max_staleness_ms = max_staleness_ms
        self._hedge = hedge

    @property
    def mode(self) -> int:
        return self._mode

    @property
    def document(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {"mode": self._mode}
        if self._tag_sets:
            doc["tag_sets"] = self._tag_sets
        return doc

    def __repr__(self) -> str:
        return f"ReadPreference(mode={self._mode})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ReadPreference):
            return self._mode == other._mode
        return False


class ReadConcern:
    """
    Represents a read concern for MongoDB compatibility.
    """

    def __init__(self, level: Optional[str] = None):
        self._level = level

    @property
    def level(self) -> Optional[str]:
        return self._level

    @property
    def ok_for_legacy(self) -> bool:
        return self._level is None or self._level == "local"

    @property
    def document(self) -> Dict[str, Any]:
        if self._level:
            return {"level": self._level}
        return {}

    def __repr__(self) -> str:
        return f"ReadConcern(level={self._level})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ReadConcern):
            return self._level == other._level
        return False


class CodecOptions:
    """
    Represents codec options for MongoDB compatibility.
    """

    def __init__(
        self,
        document_class: type = dict,
        tz_aware: bool = False,
        uuid_representation: int = 0,
    ):
        self.document_class = document_class
        self.tz_aware = tz_aware
        self.uuid_representation = uuid_representation

    def __repr__(self) -> str:
        return f"CodecOptions(document_class={self.document_class.__name__}, tz_aware={self.tz_aware})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CodecOptions):
            return (
                self.document_class == other.document_class
                and self.tz_aware == other.tz_aware
                and self.uuid_representation == other.uuid_representation
            )
        return False
