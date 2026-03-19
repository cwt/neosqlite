from __future__ import annotations

from typing import Any, Dict


class WriteConcern:
    """
    Represents a write concern for MongoDB compatibility.
    Maps to SQLite synchronous PRAGMAs.
    """

    def __init__(
        self,
        w: int | str | None = None,
        wtimeout: int | None = None,
        j: bool | None = None,
        fsync: bool | None = None,
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
        tag_sets: list | None = None,
        max_staleness_ms: int | None = None,
        hedge: dict | None = None,
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

    def __init__(self, level: str | None = None):
        self._level = level

    @property
    def level(self) -> str | None:
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


class JournalMode:
    """
    Represents valid SQLite journal modes.
    Used to validate the journal_mode parameter in Connection.
    """

    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    PERSIST = "PERSIST"
    MEMORY = "MEMORY"
    WAL = "WAL"
    OFF = "OFF"

    @classmethod
    def validate(cls, mode: str) -> str:
        """
        Validate and normalize a journal mode string.

        Args:
            mode (str): The journal mode string to validate.

        Returns:
            str: The normalized (uppercase) journal mode string.

        Raises:
            ValueError: If the journal mode is not valid.
        """
        if not isinstance(mode, str):
            raise ValueError(f"journal_mode must be a string, not {type(mode)}")

        normalized = mode.upper()
        valid_modes = [
            cls.DELETE,
            cls.TRUNCATE,
            cls.PERSIST,
            cls.MEMORY,
            cls.WAL,
            cls.OFF,
        ]

        if normalized not in valid_modes:
            raise ValueError(
                f"Invalid journal_mode: '{mode}'. Must be one of: {', '.join(valid_modes)}"
            )

        return normalized


class AutoVacuumMode:
    """
    Represents SQLite auto_vacuum modes.
    Controls how SQLite reclaims space after deleting data.
    """

    NONE = 0
    FULL = 1
    INCREMENTAL = 2

    @classmethod
    def validate(cls, mode: int | str) -> int:
        """
        Validate and normalize an auto_vacuum mode.

        Args:
            mode: The auto_vacuum mode (0, 1, 2) or string ("NONE", "FULL", "INCREMENTAL").

        Returns:
            int: The normalized auto_vacuum mode value.

        Raises:
            ValueError: If the auto_vacuum mode is not valid.
        """
        if isinstance(mode, str):
            normalized = mode.upper()
            if normalized == "NONE":
                return cls.NONE
            elif normalized == "FULL":
                return cls.FULL
            elif normalized == "INCREMENTAL":
                return cls.INCREMENTAL
            else:
                raise ValueError(
                    f"Invalid auto_vacuum mode: '{mode}'. Must be one of: NONE, FULL, INCREMENTAL"
                )

        if mode not in (cls.NONE, cls.FULL, cls.INCREMENTAL):
            raise ValueError(
                f"Invalid auto_vacuum mode: {mode}. Must be 0 (NONE), 1 (FULL), or 2 (INCREMENTAL)"
            )
        return mode

    @classmethod
    def to_string(cls, mode: int) -> str:
        """Convert numeric mode to string representation."""
        return {
            cls.NONE: "NONE",
            cls.FULL: "FULL",
            cls.INCREMENTAL: "INCREMENTAL",
        }.get(mode, "NONE")
