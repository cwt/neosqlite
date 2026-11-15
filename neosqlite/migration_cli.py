"""CLI migration for legacy NeoSQLite ``_id TEXT`` collection tables.

NeoSQLite originally stored every logical ``_id`` in a dedicated ``TEXT``
column. SQLite's TEXT affinity converts integer and real IDs to text, which
loses the distinction between MongoDB types such as ``1`` and ``"1"``.

This tool copies a database to a new file and rebuilds only NeoSQLite
collection tables whose ``_id`` column is still declared ``TEXT``, changing
the declaration to ``BLOB`` and recovering the original ``_id`` value from
the document JSON where possible. The source database is never modified.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from ._sqlite import sqlite3
from .connection import _is_internal_collection_name
from .sql_utils import quote_identifier

_ID_TYPE_RE = re.compile(r"\b_id\s+TEXT\b", re.IGNORECASE)

# Recover the logical _id from the document JSON. ObjectIds are encoded as
# {"__neosqlite_objectid__": true, "id": "<hex>"} and must stay as their hex
# string in the _id column. Other objects/arrays and legacy documents without
# a JSON _id fall back to the old stored value; their original type is not
# recoverable from a TEXT column alone.
_MIGRATE_ID_EXPR = """
CASE
    WHEN json_valid(data) THEN
        CASE
            WHEN json_type(data, '$._id') IS NULL THEN _id
            WHEN json_type(data, '$._id') = 'object'
                 AND json_extract(data, '$._id.__neosqlite_objectid__') = 1
                THEN json_extract(data, '$._id.id')
            WHEN json_type(data, '$._id') IN ('array', 'object') THEN _id
            WHEN json_type(data, '$._id') = 'null' THEN _id
            ELSE json_extract(data, '$._id')
        END
    ELSE _id
END
"""


class MigrationError(RuntimeError):
    """Raised when a database cannot be migrated safely."""


@dataclass(frozen=True, slots=True)
class LegacyTable:
    """A collection table whose ``_id`` column still uses TEXT affinity."""

    name: str
    create_sql: str
    indexes: tuple[str, ...]
    triggers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Summary returned by :func:`migrate_database`."""

    tables: tuple[str, ...]
    row_counts: dict[str, int]
    destination: str | None


def _table_info(conn: sqlite3.Connection, table_name: str) -> list[tuple]:
    quoted = quote_identifier(table_name)
    return conn.execute(f"PRAGMA table_info({quoted})").fetchall()


def _all_table_names(conn: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    ]


def _has_incoming_foreign_key(
    conn: sqlite3.Connection, table_name: str, all_tables: list[str]
) -> bool:
    for other in all_tables:
        if other == table_name:
            continue
        quoted = quote_identifier(other)
        for row in conn.execute(f"PRAGMA foreign_key_list({quoted})"):
            # row[2] is the referenced table name.
            if row[2] == table_name:
                return True
    return False


def _is_collection_table(conn: sqlite3.Connection, table_name: str) -> bool:
    columns = {
        row[1].lower(): (row[2] or "").upper()
        for row in _table_info(conn, table_name)
    }
    if set(columns) != {"id", "_id", "data"}:
        return False
    return columns["_id"] == "TEXT" and columns["data"] == "TEXT"


def find_legacy_tables(conn: sqlite3.Connection) -> list[LegacyTable]:
    """Find collection tables that still use TEXT affinity for ``_id``."""
    all_tables = _all_table_names(conn)
    legacy: list[LegacyTable] = []

    for table_name, create_sql in conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ):
        if not create_sql or _is_internal_collection_name(table_name):
            continue
        if create_sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE"):
            continue
        if not _is_collection_table(conn, table_name):
            continue
        if conn.execute(
            f"PRAGMA foreign_key_list({quote_identifier(table_name)})"
        ).fetchall():
            continue
        if _has_incoming_foreign_key(conn, table_name, all_tables):
            continue

        new_sql = _ID_TYPE_RE.sub("_id BLOB", create_sql, count=1)
        if new_sql == create_sql:
            continue

        indexes = tuple(
            row[0]
            for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' "
                "AND tbl_name=? AND sql IS NOT NULL ORDER BY name",
                (table_name,),
            )
        )
        triggers = tuple(
            row[0]
            for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' "
                "AND tbl_name=? AND sql IS NOT NULL ORDER BY name",
                (table_name,),
            )
        )
        legacy.append(
            LegacyTable(
                name=table_name,
                create_sql=new_sql,
                indexes=indexes,
                triggers=triggers,
            )
        )

    return legacy


def _rebuild_table(conn: sqlite3.Connection, table: LegacyTable) -> int:
    quoted_name = quote_identifier(table.name)
    temp_name = f"__neosqlite_migrate_{uuid.uuid4().hex}"
    quoted_temp = quote_identifier(temp_name)

    conn.execute(f"ALTER TABLE {quoted_name} RENAME TO {quoted_temp}")
    conn.execute(table.create_sql)
    conn.execute(
        f"INSERT INTO {quoted_name} (id, _id, data) "
        f"SELECT id, {_MIGRATE_ID_EXPR}, data FROM {quoted_temp}"
    )

    old_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_temp}").fetchone()[
        0
    ]
    new_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_name}").fetchone()[
        0
    ]
    if old_count != new_count:
        raise MigrationError(
            f"Row count changed for {table.name!r}: "
            f"{old_count} -> {new_count}"
        )

    # Drops the old table together with its old indexes and triggers.
    conn.execute(f"DROP TABLE {quoted_temp}")

    for index_sql in table.indexes:
        conn.execute(index_sql)
    for trigger_sql in table.triggers:
        conn.execute(trigger_sql)

    return new_count


def _backup_database(source: str, destination: str) -> None:
    source_conn = sqlite3.connect(source)
    destination_conn = sqlite3.connect(destination)
    try:
        source_conn.backup(destination_conn)
    finally:
        destination_conn.close()
        source_conn.close()


def _checkpoint_and_close(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    conn.close()


def _remove_db_files(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = path + suffix
        try:
            os.remove(candidate)
        except FileNotFoundError:
            pass


def _remove_sidecars(path: str) -> None:
    for suffix in ("-wal", "-shm"):
        candidate = path + suffix
        try:
            os.remove(candidate)
        except FileNotFoundError:
            pass


def migrate_database(
    source: str,
    destination: str,
    *,
    overwrite: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> MigrationResult:
    """Copy *source* to *destination* and rebuild legacy ``_id TEXT`` tables.

    The source file is only read. The destination is written through a
    temporary file and atomically replaced on success.
    """
    source = os.path.abspath(source)
    destination = os.path.abspath(destination)

    if not os.path.isfile(source):
        raise MigrationError(f"Source database does not exist: {source}")
    if source == destination:
        raise MigrationError(
            "Source and destination must be different files; "
            "the source is never modified in place"
        )
    if os.path.exists(destination) and not overwrite and not dry_run:
        raise MigrationError(
            f"Destination already exists: {destination} "
            "(pass --overwrite to replace it)"
        )
    if not os.path.isdir(os.path.dirname(destination) or "."):
        raise MigrationError(
            f"Destination directory does not exist: {os.path.dirname(destination)}"
        )

    source_conn = sqlite3.connect(source)
    try:
        tables = find_legacy_tables(source_conn)
    finally:
        source_conn.close()

    if dry_run:
        return MigrationResult(
            tables=tuple(table.name for table in tables),
            row_counts={},
            destination=None,
        )

    temp_fd, temp_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(destination)}.migrate-",
        suffix=".db",
        dir=os.path.dirname(destination) or ".",
    )
    os.close(temp_fd)

    legacy_tables: list[LegacyTable] = []
    row_counts: dict[str, int] = {}
    try:
        _backup_database(source, temp_path)

        conn = sqlite3.connect(temp_path)
        conn.isolation_level = None
        try:
            legacy_tables = find_legacy_tables(conn)

            if legacy_tables:
                # Keep view/trigger SQL stable while the table is temporarily
                # renamed; triggers are saved and recreated explicitly.
                conn.execute("PRAGMA legacy_alter_table=ON")
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for table in legacy_tables:
                        if verbose:
                            print(f"Migrating table {table.name!r}...")
                        row_counts[table.name] = _rebuild_table(conn, table)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        finally:
            _checkpoint_and_close(conn)

        try:
            os.chmod(temp_path, os.stat(source).st_mode & 0o777)
        except OSError:
            pass
        _remove_sidecars(temp_path)
        _remove_sidecars(destination)
        os.replace(temp_path, destination)
        temp_path = ""
    except Exception:
        if temp_path:
            _remove_db_files(temp_path)
        raise

    return MigrationResult(
        tables=tuple(table.name for table in legacy_tables),
        row_counts=row_counts,
        destination=destination,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="neosqlite-migration",
        description=(
            "Copy a NeoSQLite database and rebuild legacy _id TEXT "
            "collection tables with BLOB affinity."
        ),
    )
    parser.add_argument("source", help="Existing NeoSQLite database file")
    parser.add_argument(
        "destination",
        nargs="?",
        help="New database file (must not exist unless --overwrite is used)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing destination file atomically",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list legacy collection tables; write nothing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print per-table progress and tracebacks on failure",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Console entry point for ``neosqlite-migration``."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.dry_run and not args.destination:
        parser.error("destination is required unless --dry-run is used")

    try:
        result = migrate_database(
            args.source,
            args.destination or "",
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1

    if args.dry_run:
        if result.tables:
            print("Legacy _id TEXT collection tables:")
            for table in result.tables:
                print(f"  {table}")
        else:
            print("No legacy _id TEXT collection tables found.")
        return 0

    if not result.tables:
        print("No legacy _id TEXT collection tables found; copied unchanged.")
    else:
        for table in result.tables:
            print(f"Migrated {table}: {result.row_counts[table]} rows")
    print(f"Destination: {result.destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
