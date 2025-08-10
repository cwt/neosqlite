from unittest.mock import patch
import pynosqlite as nosqlite


def test_connect():
    conn = nosqlite.Connection(":memory:")
    assert conn.db.isolation_level is None


@patch("pynosqlite.nosqlite.sqlite3")
def test_context_manager_closes_connection(sqlite):
    with nosqlite.Connection() as conn:
        pass
    assert conn.db.close.called


@patch("pynosqlite.nosqlite.sqlite3")
@patch("pynosqlite.nosqlite.Collection")
def test_getitem_returns_collection(mock_collection, sqlite):
    sqlite.connect.return_value = sqlite
    mock_collection.return_value = mock_collection
    conn = nosqlite.Connection()
    assert "foo" not in conn._collections
    assert conn["foo"] == mock_collection


@patch("pynosqlite.nosqlite.sqlite3")
def test_getitem_returns_cached_collection(sqlite):
    conn = nosqlite.Connection()
    conn._collections["foo"] = "bar"
    assert conn["foo"] == "bar"


@patch("pynosqlite.nosqlite.sqlite3")
def test_drop_collection(sqlite):
    conn = nosqlite.Connection()
    conn.drop_collection("foo")
    conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")


@patch("pynosqlite.nosqlite.sqlite3")
def test_getattr_returns_attribute(sqlite):
    conn = nosqlite.Connection()
    assert conn.__getattr__("db") is not None


@patch("pynosqlite.nosqlite.sqlite3")
def test_getattr_returns_collection(sqlite):
    conn = nosqlite.Connection()
    foo = conn.__getattr__("foo")
    assert isinstance(foo, nosqlite.Collection)
