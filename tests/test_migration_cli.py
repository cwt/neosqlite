"""Tests for the legacy _id TEXT migration CLI."""

from __future__ import annotations

import json

from neosqlite._sqlite import sqlite3
from neosqlite.collection.json_helpers import neosqlite_json_dumps
from neosqlite.migration_cli import main
from neosqlite.objectid import ObjectId


def _make_legacy_database(path, *, include_legacy_table=True):
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "_id TEXT, "
            "data TEXT NOT NULL"
            ")"
        )
        conn.execute("CREATE UNIQUE INDEX idx_items_id ON items(_id)")
        conn.execute("CREATE INDEX idx_items_data ON items(data)")
        conn.execute("CREATE TABLE audit (msg TEXT)")
        conn.execute(
            "CREATE TRIGGER items_update AFTER UPDATE ON items "
            "BEGIN INSERT INTO audit(msg) VALUES ('updated'); END"
        )

        oid = ObjectId()
        rows = [
            ("1", {"_id": 1, "kind": "int"}),
            ("one", {"_id": "one", "kind": "string"}),
            ("2.5", {"_id": 2.5, "kind": "float"}),
            (str(oid), {"_id": oid, "kind": "objectid"}),
            ("legacy", {"kind": "legacy"}),
        ]
        conn.executemany(
            "INSERT INTO items(_id, data) VALUES (?, ?)",
            [(stored_id, neosqlite_json_dumps(doc)) for stored_id, doc in rows],
        )
        conn.commit()
    finally:
        conn.close()


def _read_rows(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT _id, typeof(_id), json_extract(data, '$.kind') "
            "FROM items ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def _table_id_type(path):
    conn = sqlite3.connect(path)
    try:
        for row in conn.execute("PRAGMA table_info(items)"):
            if row[1] == "_id":
                return row[2]
        return None
    finally:
        conn.close()


def test_migrate_database_preserves_types_indexes_and_triggers(tmp_path):
    source = tmp_path / "legacy.db"
    destination = tmp_path / "migrated.db"
    _make_legacy_database(str(source))

    assert main([str(source), str(destination)]) == 0
    assert source.exists() and destination.exists()
    assert _table_id_type(str(source)) == "TEXT"
    assert _table_id_type(str(destination)) == "BLOB"

    rows = _read_rows(str(destination))
    by_kind = {kind: (value, sql_type) for value, sql_type, kind in rows}
    assert by_kind["int"] == (1, "integer")
    assert by_kind["string"] == ("one", "text")
    assert by_kind["float"] == (2.5, "real")
    assert by_kind["objectid"][1] == "text"
    assert by_kind["objectid"][0] == rows[3][0]
    assert by_kind["legacy"] == ("legacy", "text")

    conn = sqlite3.connect(str(destination))
    try:
        names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('index', 'trigger')"
            )
        }
        assert "idx_items_id" in names
        assert "idx_items_data" in names
        assert "items_update" in names

        conn.execute("UPDATE items SET data = data WHERE _id = 1")
        assert conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 1
    finally:
        conn.close()


def test_dry_run_writes_nothing(tmp_path, capsys):
    source = tmp_path / "legacy.db"
    destination = tmp_path / "migrated.db"
    _make_legacy_database(str(source))

    assert main(["--dry-run", str(source)]) == 0
    assert not destination.exists()
    output = capsys.readouterr().out
    assert "items" in output


def test_existing_destination_requires_overwrite(tmp_path):
    source = tmp_path / "legacy.db"
    destination = tmp_path / "migrated.db"
    _make_legacy_database(str(source))
    destination.write_text("occupied")

    assert main([str(source), str(destination)]) == 1
    assert destination.read_text() == "occupied"

    assert main(["--overwrite", str(source), str(destination)]) == 0
    assert _table_id_type(str(destination)) == "BLOB"


def test_non_legacy_database_is_copied_unchanged(tmp_path):
    source = tmp_path / "modern.db"
    destination = tmp_path / "copy.db"
    conn = sqlite3.connect(str(source))
    try:
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "_id BLOB, "
            "data TEXT NOT NULL"
            ")"
        )
        conn.execute(
            "INSERT INTO items(_id, data) VALUES (?, ?)",
            (1, json.dumps({"_id": 1, "kind": "int"})),
        )
        conn.commit()
    finally:
        conn.close()

    assert main([str(source), str(destination)]) == 0
    assert _table_id_type(str(destination)) == "BLOB"
    assert _read_rows(str(destination)) == [(1, "integer", "int")]


def test_migrate_id_jsonb_table_to_blob(tmp_path):
    """Migrate tables where _id was created with JSONB (NUMERIC) affinity."""
    from neosqlite.collection.jsonb_support import supports_jsonb

    source = tmp_path / "jsonb_legacy.db"
    destination = tmp_path / "jsonb_migrated.db"
    conn = sqlite3.connect(str(source))
    try:
        use_jsonb = supports_jsonb(conn)
        data_col_type = "JSONB" if use_jsonb else "TEXT"
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "_id JSONB, "
            f"data {data_col_type} NOT NULL"
            ")"
        )
        rows = [
            ("1", {"_id": "1", "kind": "str"}),
            (2.5, {"_id": 2.5, "kind": "float"}),
        ]
        if use_jsonb:
            conn.executemany(
                "INSERT INTO items(_id, data) VALUES (?, jsonb(?))",
                [(sid, neosqlite_json_dumps(doc)) for sid, doc in rows],
            )
        else:
            conn.executemany(
                "INSERT INTO items(_id, data) VALUES (?, ?)",
                [(sid, neosqlite_json_dumps(doc)) for sid, doc in rows],
            )
        conn.commit()
    finally:
        conn.close()

    assert main([str(source), str(destination)]) == 0
    assert _table_id_type(str(destination)) == "BLOB"

    dest_conn = sqlite3.connect(str(destination))
    try:
        migrated = dest_conn.execute(
            "SELECT _id, typeof(_id), json_extract(data, '$.kind') "
            "FROM items ORDER BY id"
        ).fetchall()
        assert migrated == [
            ("1", "text", "str"),
            (2.5, "real", "float"),
        ]
    finally:
        dest_conn.close()
