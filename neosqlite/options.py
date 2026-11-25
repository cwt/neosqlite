"""MongoDB-compatible and SQLite configuration option classes."""

from __future__ import annotations

from typing import Any


class WriteConcern:
    """Represents a write concern for MongoDB compatibility.

    Maps write concern settings to SQLite synchronous PRAGMAs and stores
    non-None options in an internal document dictionary.
    """

    def __init__(
        self,
        w: int | str | None = None,
        wtimeout: int | None = None,
        j: bool | None = None,
        fsync: bool | None = None,
    ):
        """Initialize a WriteConcern instance.

        Args:
            w: Write acknowledgment level (e.g., 0, 1, or "majority").
            wtimeout: Write concern timeout in milliseconds.
            j: Whether writes wait for journal sync.
            fsync: Whether writes wait for fsync.
        """
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
    def document(self) -> dict[str, Any]:
        """Return the write concern options as a dictionary.

        Returns:
            dict[str, Any]: Dictionary of configured write concern options.
        """
        return self._document

    @property
    def acknowledged(self) -> bool:
        """Return whether writes are acknowledged.

        Returns:
            bool: True unless ``w`` is explicitly set to 0.
        """
        return self._document.get("w") != 0

    def __repr__(self) -> str:
        """Return a developer-friendly string representation.

        Returns:
            str: Formatted string showing configured options.
        """
        return f"WriteConcern({', '.join(f'{k}={v}' for k, v in self._document.items())})"

    def __eq__(self, other: Any) -> bool:
        """Check equality against another WriteConcern or dictionary.

        Args:
            other: Object to compare with.

        Returns:
            bool: True if the underlying write concern documents match.
        """
        if isinstance(other, WriteConcern):
            return self._document == other._document
        return self._document == other


class ReadPreference:
    """Represents a read preference for MongoDB compatibility.

    Mostly a placeholder in NeoSQLite because SQLite runs on a single node.
    Defines constants for standard MongoDB read preference modes.
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
        """Initialize a ReadPreference instance.

        Args:
            mode: Read preference mode integer constant.
            tag_sets: Optional list of replica set tag sets.
            max_staleness_ms: Optional maximum replication lag in ms.
            hedge: Optional hedged read options dictionary.
        """
        self._mode = mode
        self._tag_sets = tag_sets
        self._max_staleness_ms = max_staleness_ms
        self._hedge = hedge

    @property
    def mode(self) -> int:
        """Return the configured read preference mode.

        Returns:
            int: Read preference mode integer value.
        """
        return self._mode

    @property
    def document(self) -> dict[str, Any]:
        """Return the read preference options as a dictionary.

        Returns:
            dict[str, Any]: Dictionary containing ``mode`` and non-empty
                ``tag_sets``.
        """
        doc: dict[str, Any] = {"mode": self._mode}
        if self._tag_sets:
            doc["tag_sets"] = self._tag_sets
        return doc

    def __repr__(self) -> str:
        """Return a developer-friendly string representation.

        Returns:
            str: String representation with the read preference mode.
        """
        return f"ReadPreference(mode={self._mode})"

    def __eq__(self, other: Any) -> bool:
        """Check equality against another ReadPreference.

        Args:
            other: Object to compare with.

        Returns:
            bool: True if ``other`` is a ReadPreference with the same mode.
        """
        if isinstance(other, ReadPreference):
            return self._mode == other._mode
        return False


class ReadConcern:
    """Represents a read concern for MongoDB compatibility.

    Stores an optional read concern level string (such as ``"local"`` or
    ``"majority"``) for PyMongo API compatibility.
    """

    def __init__(self, level: str | None = None):
        """Initialize a ReadConcern instance.

        Args:
            level: Optional read concern level name.
        """
        self._level = level

    @property
    def level(self) -> str | None:
        """Return the configured read concern level.

        Returns:
            str | None: The read concern level string, or None if unset.
        """
        return self._level

    @property
    def ok_for_legacy(self) -> bool:
        """Return whether this read concern is valid for legacy reads.

        Returns:
            bool: True when ``level`` is None or ``"local"``.
        """
        return self._level is None or self._level == "local"

    @property
    def document(self) -> dict[str, Any]:
        """Return the read concern options as a dictionary.

        Returns:
            dict[str, Any]: Dictionary with ``level`` if set, else empty dict.
        """
        if self._level:
            return {"level": self._level}
        return {}

    def __repr__(self) -> str:
        """Return a developer-friendly string representation.

        Returns:
            str: String representation showing the read concern level.
        """
        return f"ReadConcern(level={self._level})"

    def __eq__(self, other: Any) -> bool:
        """Check equality against another ReadConcern.

        Args:
            other: Object to compare with.

        Returns:
            bool: True if ``other`` is a ReadConcern with the same level.
        """
        if isinstance(other, ReadConcern):
            return self._level == other._level
        return False


class CodecOptions:
    """Represents codec options for MongoDB compatibility.

    Stores document class, timezone awareness, and UUID representation
    settings matching PyMongo's ``CodecOptions`` interface.
    """

    def __init__(
        self,
        document_class: type = dict,
        tz_aware: bool = False,
        uuid_representation: int = 0,
    ):
        """Initialize a CodecOptions instance.

        Args:
            document_class: Class used to represent decoded documents.
            tz_aware: Whether decoded datetimes are timezone-aware.
            uuid_representation: Integer code for UUID binary representation.
        """
        self.document_class = document_class
        self.tz_aware = tz_aware
        self.uuid_representation = uuid_representation

    def __repr__(self) -> str:
        """Return a developer-friendly string representation.

        Returns:
            str: Formatted string showing document class and tz_aware flag.
        """
        return f"CodecOptions(document_class={self.document_class.__name__}, tz_aware={self.tz_aware})"

    def __eq__(self, other: Any) -> bool:
        """Check equality against another CodecOptions instance.

        Args:
            other: Object to compare with.

        Returns:
            bool: True if ``other`` is a CodecOptions with identical fields.
        """
        if isinstance(other, CodecOptions):
            return (
                self.document_class == other.document_class
                and self.tz_aware == other.tz_aware
                and self.uuid_representation == other.uuid_representation
            )
        return False


class JournalMode:
    """Represents valid SQLite journal modes.

    Used to validate the ``journal_mode`` parameter in ``Connection``.
    Exposes constants for ``DELETE``, ``TRUNCATE``, ``PERSIST``, ``MEMORY``,
    ``WAL``, and ``OFF``.
    """

    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    PERSIST = "PERSIST"
    MEMORY = "MEMORY"
    WAL = "WAL"
    OFF = "OFF"

    @classmethod
    def validate(cls, mode: str) -> str:
        """Validate and normalize a journal mode string.

        Args:
            mode: The journal mode string to validate.

        Returns:
            str: The normalized (uppercase) journal mode string.

        Raises:
            ValueError: If ``mode`` is not a string or not a valid journal
                mode.
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
    """Represents SQLite auto_vacuum modes.

    Controls how SQLite reclaims space after deleting data via numeric
    modes ``NONE`` (0), ``FULL`` (1), and ``INCREMENTAL`` (2).
    """

    NONE = 0
    FULL = 1
    INCREMENTAL = 2

    @classmethod
    def validate(cls, mode: int | str) -> int:
        """Validate and normalize an auto_vacuum mode.

        Args:
            mode: The auto_vacuum mode (0, 1, 2) or string (``"NONE"``,
                ``"FULL"``, ``"INCREMENTAL"``).

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

        if isinstance(mode, bool) or mode not in (
            cls.NONE,
            cls.FULL,
            cls.INCREMENTAL,
        ):
            raise ValueError(
                f"Invalid auto_vacuum mode: {mode}. Must be 0 (NONE), 1 (FULL), or 2 (INCREMENTAL)"
            )
        return mode

    @classmethod
    def to_string(cls, mode: int) -> str:
        """Convert numeric auto_vacuum mode to its string representation.

        Args:
            mode: Numeric auto_vacuum mode (0, 1, or 2).

        Returns:
            str: Mode name (``"NONE"``, ``"FULL"``, or ``"INCREMENTAL"``),
                defaulting to ``"NONE"`` for unrecognized values.
        """
        return {
            cls.NONE: "NONE",
            cls.FULL: "FULL",
            cls.INCREMENTAL: "INCREMENTAL",
        }.get(mode, "NONE")
