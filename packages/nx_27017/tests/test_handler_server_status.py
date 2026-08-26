"""Tests for the serverStatus command memory accounting."""

import sys

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    db_path = str(tmp_path / "test.db")
    h = NeoSQLiteHandler(db_path)
    yield h
    h.conn.close()


class TestServerStatusMemory:
    def test_mem_fields_present_and_integers(self, handler):
        """mem.resident and mem.virtual must be integers >= 0."""
        msg = {
            "request_id": 1,
            "sections": [("body", {"serverStatus": 1, "$db": "test"})],
        }
        _, response = handler.handle_command(msg)
        assert response["ok"] == 1
        mem = response["mem"]
        assert isinstance(mem["resident"], int)
        assert isinstance(mem["virtual"], int)
        assert mem["resident"] >= 0
        assert mem["virtual"] >= 0

    def test_resident_and_virtual_differ(self, handler):
        """On Linux resident and virtual RSS reports must not be
        identical (they report different quantities) - regression for
        both being derived from the same ru_maxrss * 1024."""
        msg = {
            "request_id": 2,
            "sections": [("body", {"serverStatus": 1, "$db": "test"})],
        }
        _, response = handler.handle_command(msg)
        resident = response["mem"]["resident"]
        virtual = response["mem"]["virtual"]
        if sys.platform == "linux":
            assert resident != virtual
        else:
            assert resident >= 0
            assert virtual >= 0
