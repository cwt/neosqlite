from unittest.mock import patch
import neosqlite


def test_connect():
    conn = neosqlite.Connection(":memory:")
    assert conn.db.isolation_level is None


@patch("neosqlite.neosqlite.sqlite3")
def test_context_manager_closes_connection(sqlite):
    with neosqlite.Connection() as conn:
        pass
    assert conn.db.close.called


@patch("neosqlite.neosqlite.sqlite3")
@patch("neosqlite.neosqlite.Collection")
def test_getitem_returns_collection(mock_collection, sqlite):
    sqlite.connect.return_value = sqlite
    mock_collection.return_value = mock_collection
    conn = neosqlite.Connection()
    assert "foo" not in conn._collections
    assert conn["foo"] == mock_collection


@patch("neosqlite.neosqlite.sqlite3")
def test_getitem_returns_cached_collection(sqlite):
    conn = neosqlite.Connection()
    conn._collections["foo"] = "bar"
    assert conn["foo"] == "bar"


@patch("neosqlite.neosqlite.sqlite3")
def test_drop_collection(sqlite):
    conn = neosqlite.Connection()
    conn.drop_collection("foo")
    conn.db.execute.assert_called_with("DROP TABLE IF EXISTS foo")


@patch("neosqlite.neosqlite.sqlite3")
def test_getattr_returns_attribute(sqlite):
    conn = neosqlite.Connection()
    assert conn.__getattr__("db") is not None


@patch("neosqlite.neosqlite.sqlite3")
def test_getattr_returns_collection(sqlite):
    conn = neosqlite.Connection()
    foo = conn.__getattr__("foo")
    assert isinstance(foo, neosqlite.Collection)
