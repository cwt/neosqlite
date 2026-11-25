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


def test_migrate_preserves_boolean_and_integer_id_distinction(tmp_path):
    """Booleans True/False stay 'True'/'False' text distinct from int 1/0."""
    import neosqlite
    from neosqlite.collection.jsonb_support import supports_jsonb
    from neosqlite.migration_cli import migrate_database

    source = tmp_path / "bool_legacy.db"
    destination = tmp_path / "bool_migrated.db"
    conn = sqlite3.connect(str(source))
    try:
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "_id TEXT UNIQUE, "
            "data TEXT NOT NULL"
            ")"
        )
        conn.executemany(
            "INSERT INTO items(_id, data) VALUES (?, ?)",
            [
                ("1", neosqlite_json_dumps({"_id": 1, "kind": "int_1"})),
                ("0", neosqlite_json_dumps({"_id": 0, "kind": "int_0"})),
                (
                    "True",
                    neosqlite_json_dumps({"_id": True, "kind": "bool_true"}),
                ),
                (
                    "False",
                    neosqlite_json_dumps({"_id": False, "kind": "bool_false"}),
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    migrate_database(str(source), str(destination))

    check_conn = sqlite3.connect(str(destination))
    try:
        raw_rows = check_conn.execute(
            "SELECT _id, typeof(_id), json_extract(data, '$.kind') "
            "FROM items ORDER BY id"
        ).fetchall()
        assert raw_rows == [
            (1, "integer", "int_1"),
            (0, "integer", "int_0"),
            ("True", "text", "bool_true"),
            ("False", "text", "bool_false"),
        ]
    finally:
        check_conn.close()

    with neosqlite.Connection(str(destination)) as nconn:
        items = nconn["items"]
        doc_int_1 = items.find_one({"_id": 1})
        doc_int_0 = items.find_one({"_id": 0})
        doc_bool_t = items.find_one({"kind": "bool_true"})
        doc_bool_f = items.find_one({"kind": "bool_false"})
        assert doc_int_1 == {"_id": 1, "kind": "int_1"}
        assert doc_int_0 == {"_id": 0, "kind": "int_0"}
        assert doc_bool_t is not None and doc_bool_t["_id"] is True
        assert doc_bool_f is not None and doc_bool_f["_id"] is False

    # When only boolean _id rows are present, find_one({'_id': 1/0}) must not
    # accidentally return the boolean documents.
    bool_only_src = tmp_path / "bool_only_src.db"
    bool_only_dst = tmp_path / "bool_only_dst.db"
    bconn = sqlite3.connect(str(bool_only_src))
    try:
        use_jsonb = supports_jsonb(bconn)
        data_col = "JSONB" if use_jsonb else "TEXT"
        insert_expr = "jsonb(?)" if use_jsonb else "?"
        bconn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            f"_id TEXT UNIQUE, data {data_col} NOT NULL)"
        )
        bconn.executemany(
            f"INSERT INTO items(_id, data) VALUES (?, {insert_expr})",
            [
                (
                    "True",
                    neosqlite_json_dumps({"_id": True, "kind": "bool_true"}),
                ),
                (
                    "False",
                    neosqlite_json_dumps({"_id": False, "kind": "bool_false"}),
                ),
            ],
        )
        bconn.commit()
    finally:
        bconn.close()

    migrate_database(str(bool_only_src), str(bool_only_dst))
    with neosqlite.Connection(str(bool_only_dst)) as nconn:
        items = nconn["items"]
        assert items.find_one({"_id": 1}) is None
        assert items.find_one({"_id": 0}) is None
        assert items.find_one({"kind": "bool_true"})["_id"] is True
        assert items.find_one({"kind": "bool_false"})["_id"] is False


def test_migrate_ignores_corrupt_non_jsonb_blob_data(tmp_path):
    """Non-JSONB binary blobs in data do not crash migration."""
    from neosqlite.migration_cli import migrate_database

    source = tmp_path / "blob_legacy.db"
    destination = tmp_path / "blob_migrated.db"
    conn = sqlite3.connect(str(source))
    try:
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "_id TEXT, "
            "data TEXT NOT NULL"
            ")"
        )
        conn.execute(
            "INSERT INTO items(_id, data) VALUES (?, ?)",
            ("corrupt", b"\xff\x00\x01not-jsonb"),
        )
        conn.execute(
            "INSERT INTO items(_id, data) VALUES (?, ?)",
            ("42", neosqlite_json_dumps({"_id": 42, "kind": "ok"})),
        )
        conn.commit()
    finally:
        conn.close()

    migrate_database(str(source), str(destination))

    dest_conn = sqlite3.connect(str(destination))
    try:
        rows = dest_conn.execute(
            "SELECT _id, typeof(_id), typeof(data) FROM items ORDER BY id"
        ).fetchall()
        assert rows == [
            ("corrupt", "text", "blob"),
            (42, "integer", "text"),
        ]
        kind = dest_conn.execute(
            "SELECT json_extract(data, '$.kind') FROM items WHERE id = 2"
        ).fetchone()[0]
        assert kind == "ok"
    finally:
        dest_conn.close()


def test_migrate_quoted_id_column_definitions(tmp_path):
    """Quoted column definitions like \"_id\" or [_id] or `_id` are migrated."""
    from neosqlite.migration_cli import migrate_database

    for col_def in ('"_id" TEXT', "[_id] TEXT", "`_id` JSONB"):
        sub = tmp_path / col_def.replace('"', "d").replace(" ", "_")
        sub.mkdir()
        source = sub / "legacy.db"
        destination = sub / "migrated.db"
        conn = sqlite3.connect(str(source))
        try:
            conn.execute(
                "CREATE TABLE items ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"{col_def} UNIQUE, "
                "data TEXT NOT NULL"
                ")"
            )
            conn.execute(
                "INSERT INTO items(_id, data) VALUES (?, ?)",
                ("1", neosqlite_json_dumps({"_id": 1, "kind": "int"})),
            )
            conn.execute(
                "INSERT INTO items(_id, data) VALUES (?, ?)",
                ("True", neosqlite_json_dumps({"_id": True, "kind": "bool"})),
            )
            conn.commit()
        finally:
            conn.close()

        migrate_database(str(source), str(destination))
        assert _table_id_type(str(destination)) == "BLOB"
        assert _read_rows(str(destination)) == [
            (1, "integer", "int"),
            ("True", "text", "bool"),
        ]
