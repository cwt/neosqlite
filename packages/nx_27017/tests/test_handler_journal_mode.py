"""Tests for journal mode configuration."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestJournalMode:
    """Test journal mode configuration for NeoSQLiteHandler."""

    def test_handler_default_journal_mode_is_wal(self, tmp_path):
        """Test that default journal mode is WAL."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path)
        assert handler.journal_mode == "WAL"
        assert handler.conn.journal_mode == "WAL"

    def test_handler_with_explicit_wal_mode(self, tmp_path):
        """Test handler with explicit WAL mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="WAL")
        assert handler.journal_mode == "WAL"
        assert handler.conn.journal_mode == "WAL"

    def test_handler_with_delete_mode(self, tmp_path):
        """Test handler with DELETE journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")
        assert handler.journal_mode == "DELETE"
        assert handler.conn.journal_mode == "DELETE"

    def test_handler_with_memory_mode(self, tmp_path):
        """Test handler with MEMORY journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="MEMORY")
        assert handler.journal_mode == "MEMORY"
        assert handler.conn.journal_mode == "MEMORY"

    def test_handler_with_truncate_mode(self, tmp_path):
        """Test handler with TRUNCATE journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="TRUNCATE")
        assert handler.journal_mode == "TRUNCATE"
        assert handler.conn.journal_mode == "TRUNCATE"

    def test_handler_with_persist_mode(self, tmp_path):
        """Test handler with PERSIST journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="PERSIST")
        assert handler.journal_mode == "PERSIST"
        assert handler.conn.journal_mode == "PERSIST"

    def test_handler_with_off_mode(self, tmp_path):
        """Test handler with OFF journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="OFF")
        assert handler.journal_mode == "OFF"
        assert handler.conn.journal_mode == "OFF"

    def test_handler_with_memory_path_and_journal_mode(self):
        """Test handler with :memory: path and journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:", journal_mode="DELETE")
        assert handler.journal_mode == "DELETE"
        assert handler.conn.journal_mode == "DELETE"

    def test_get_database_inherits_journal_mode(self, tmp_path):
        """Test that get_database() uses the same journal mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")

        db2 = handler.get_database("other_db")
        assert db2.journal_mode == "DELETE"

    def test_cli_journal_mode_argument_parsing(self):
        """Test that CLI argument parsing works correctly."""
        import argparse

        from nx_27017.nx_27017 import JournalMode

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-j",
            "--journal-mode",
            dest="journal_mode",
            default="WAL",
            choices=["WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"],
        )

        args = parser.parse_args([])
        assert args.journal_mode == "WAL"
        assert JournalMode.validate(args.journal_mode) == "WAL"

        args = parser.parse_args(["-j", "WAL"])
        assert args.journal_mode == "WAL"

        args = parser.parse_args(["--journal-mode", "DELETE"])
        assert args.journal_mode == "DELETE"

        assert JournalMode.validate("wal") == "WAL"
        assert JournalMode.validate("delete") == "DELETE"

    def test_handler_crud_operations_with_delete_mode(self, tmp_path):
        """Test that CRUD operations work correctly with non-WAL mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        db_path = str(tmp_path / "test.db")
        handler = NeoSQLiteHandler(db_path, journal_mode="DELETE")
        assert handler.conn.journal_mode == "DELETE"

        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Alice", "age": 30}]),
            ],
        }
        _, response = handler.handle_insert(insert_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {"name": "Alice"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["name"] == "Alice"

    def test_handler_crud_operations_with_memory_mode(self):
        """Test that CRUD operations work correctly with MEMORY mode."""
        from nx_27017.nx_27017 import NeoSQLiteHandler

        handler = NeoSQLiteHandler(":memory:", journal_mode="MEMORY")
        assert handler.conn.journal_mode == "MEMORY"

        insert_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"insert": "users", "$db": "test"}),
                ("payload_docs", [{"name": "Bob", "age": 25}]),
            ],
        }
        _, response = handler.handle_insert(insert_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        find_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "users",
                        "filter": {"name": "Bob"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["name"] == "Bob"
