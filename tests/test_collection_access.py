"""
Test that both dict-style (conn["name"]) and attribute-style (conn.name)
collection access work correctly on the Connection object.

Covers issue #84: confusion about conn.collection_name vs conn["collection_name"].
"""

import pytest

import neosqlite
from neosqlite.collection import Collection


@pytest.fixture
def conn():
    """Create an in-memory connection, insert a document for query tests."""
    db = neosqlite.Connection(":memory:")
    collection = db["stars"]
    collection.insert_one({"abbreviation": "Ori", "name": "Orion"})
    collection.insert_one({"abbreviation": "UMa", "name": "Ursa Major"})
    return db


# ── Basic access: both styles return a Collection ────────────────────────────


def test_dict_style_returns_collection(conn):
    """conn['name'] returns a Collection."""
    c = conn["stars"]
    assert isinstance(c, Collection)
    assert c.name == "stars"


def test_attr_style_returns_collection(conn):
    """conn.name returns a Collection."""
    c = conn.stars
    assert isinstance(c, Collection)
    assert c.name == "stars"


def test_both_styles_return_same_object(conn):
    """conn['stars'] is the same cached object as conn.stars."""
    a = conn["stars"]
    b = conn.stars
    assert a is b


# ── Collection operations work through both styles ───────────────────────────


def test_find_one_dict_style(conn):
    """find_one works via dict-style access."""
    doc = conn["stars"].find_one({"abbreviation": "Ori"})
    assert doc is not None
    assert doc["name"] == "Orion"


def test_find_one_attr_style(conn):
    """find_one works via attribute-style access."""
    doc = conn.stars.find_one({"abbreviation": "UMa"})
    assert doc is not None
    assert doc["name"] == "Ursa Major"


def test_insert_one_dict_style(conn):
    """insert_one works via dict-style access."""
    result = conn["stars"].insert_one({"abbreviation": "Cyg", "name": "Cygnus"})
    assert result.inserted_id is not None
    doc = conn.stars.find_one({"abbreviation": "Cyg"})
    assert doc["name"] == "Cygnus"


def test_insert_one_attr_style(conn):
    """insert_one works via attribute-style access."""
    result = conn.stars.insert_one({"abbreviation": "Lyr", "name": "Lyra"})
    assert result.inserted_id is not None
    doc = conn.stars.find_one({"abbreviation": "Lyr"})
    assert doc["name"] == "Lyra"


def test_find_returns_list(conn):
    """find() works through both access styles and returns matching docs."""
    via_dict = list(conn["stars"].find())
    via_attr = list(conn.stars.find())
    assert len(via_dict) >= 2
    assert via_dict == via_attr


# ── Real attributes are NOT shadowed ─────────────────────────────────────────


def test_db_is_sqlite_connection_not_collection(conn):
    """conn.db returns the underlying sqlite3.Connection, NOT a Collection."""
    import sqlite3

    assert isinstance(conn.db, sqlite3.Connection)
    assert not isinstance(conn.db, Collection)


def test_real_attribute_present():
    """Known real attributes on Connection are accessible."""
    conn = neosqlite.Connection(":memory:")
    assert isinstance(conn.name, str)
    assert isinstance(conn.debug, bool)


def test_collection_named_like_real_attribute(conn):
    """A collection named 'db' must be accessed via dict-style only."""
    # Create a collection called 'db'
    conn["db"].insert_one({"x": 1})
    import sqlite3

    # Attribute style returns the real sqlite3.Connection, not the collection
    assert isinstance(conn.db, sqlite3.Connection)

    # Dict style returns the collection correctly
    coll = conn["db"]
    assert isinstance(coll, Collection)
    assert coll.find_one({"x": 1}) is not None


# ── create_collection / get_collection work both styles ──────────────────────


def test_create_collection_creates_accessible_via_both(conn):
    """A collection created via create_collection() is accessible via both styles."""
    conn.create_collection("planets")
    assert isinstance(conn["planets"], Collection)
    assert isinstance(conn.planets, Collection)
    assert conn["planets"] is conn.planets


def test_get_collection_works(conn):
    """get_collection returns existing collection."""
    c = conn.get_collection("stars")
    assert isinstance(c, Collection)
    assert c.name == "stars"


# ── Edge cases ───────────────────────────────────────────────────────────────


def test_nonexistent_collection_returns_collection():
    """Accessing a nonexistent collection returns a Collection object."""
    conn = neosqlite.Connection(":memory:")
    via_dict = conn["nonexistent"]
    via_attr = conn.nonexistent
    assert isinstance(via_dict, Collection)
    assert isinstance(via_attr, Collection)


def test_collection_with_underscores(conn):
    """Collection names with underscores work with both styles."""
    conn.create_collection("my_collection_v2")
    conn["my_collection_v2"].insert_one({"hello": "world"})
    doc = conn.my_collection_v2.find_one({"hello": "world"})
    assert doc is not None


def test_collection_with_digits_fails_gracefully():
    """Collection names with leading digits are rejected (SQL identifier rule)."""
    conn = neosqlite.Connection(":memory:")
    # Python attribute-access doesn't support leading digits anyway,
    # and the SQL backend enforces valid identifier naming.
    with pytest.raises(ValueError, match="must not start with a digit"):
        conn["2026_logs"]
