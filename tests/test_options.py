"""
Unit tests for NeoSQLite options classes (WriteConcern, ReadPreference, ReadConcern, CodecOptions).
"""

import neosqlite
from neosqlite import WriteConcern, ReadPreference, ReadConcern, CodecOptions


def test_write_concern_init():
    """Test WriteConcern initialization and properties."""
    wc = WriteConcern(w=1, j=True, wtimeout=1000)
    assert wc.document == {"w": 1, "j": True, "wtimeout": 1000}
    assert wc.acknowledged is True

    wc_unack = WriteConcern(w=0)
    assert wc_unack.acknowledged is False
    assert wc_unack.document == {"w": 0}


def test_write_concern_repr():
    """Test WriteConcern string representation."""
    wc = WriteConcern(w=1, j=True)
    # The order of keys in dict repr might vary, but let's check for content
    r = repr(wc)
    assert "WriteConcern" in r
    assert "w=1" in r
    assert "j=True" in r


def test_read_preference_init():
    """Test ReadPreference initialization and properties."""
    rp = ReadPreference(ReadPreference.SECONDARY_PREFERRED)
    assert rp.mode == 3
    assert rp.document == {"mode": 3}


def test_read_concern_init():
    """Test ReadConcern initialization and properties."""
    rc = ReadConcern(level="majority")
    assert rc.level == "majority"
    assert rc.document == {"level": "majority"}

    rc_default = ReadConcern()
    assert rc_default.level is None
    assert rc_default.document == {}


def test_codec_options_init():
    """Test CodecOptions initialization."""
    co = CodecOptions(tz_aware=True, document_class=dict)
    assert co.tz_aware is True
    assert co.document_class is dict


def test_options_equality():
    """Test equality operators for options classes."""
    assert WriteConcern(w=1) == WriteConcern(w=1)
    assert WriteConcern(w=1) != WriteConcern(w=0)

    assert ReadPreference(ReadPreference.PRIMARY) == ReadPreference(
        ReadPreference.PRIMARY
    )
    assert ReadPreference(ReadPreference.PRIMARY) != ReadPreference(
        ReadPreference.SECONDARY
    )

    assert ReadConcern(level="local") == ReadConcern(level="local")
    assert ReadConcern(level="local") != ReadConcern(level="majority")

    assert CodecOptions(tz_aware=True) == CodecOptions(tz_aware=True)
    assert CodecOptions(tz_aware=True) != CodecOptions(tz_aware=False)


def test_connection_with_options_objects():
    """Test that Connection accepts options objects in constructor and with_options."""
    wc = WriteConcern(w=1, j=True)
    rp = ReadPreference(ReadPreference.PRIMARY)
    rc = ReadConcern(level="local")
    co = CodecOptions(tz_aware=True)

    # Constructor
    with neosqlite.Connection(
        ":memory:",
        write_concern=wc,
        read_preference=rp,
        read_concern=rc,
        codec_options=co,
    ) as conn:
        assert conn.write_concern == wc
        assert conn.read_preference == rp
        assert conn.read_concern == rc
        assert conn.codec_options == co

    # with_options
    with neosqlite.Connection(":memory:") as conn:
        conn2 = conn.with_options(write_concern=wc, read_preference=rp)
        assert conn2.write_concern == wc
        assert conn2.read_preference == rp
        # Original should be unchanged
        assert conn.write_concern is None


def test_collection_with_options_objects(collection):
    """Test that Collection accepts options objects in with_options."""
    wc = WriteConcern(w=1, j=True)

    coll2 = collection.with_options(write_concern=wc)
    assert coll2.write_concern == wc
    # Verify it's a different instance
    assert coll2 is not collection


def test_write_concern_pragma_mapping():
    """Test that WriteConcern objects correctly map to SQLite PRAGMAs."""
    # j=True -> synchronous=FULL (2)
    wc_j = WriteConcern(j=True)
    with neosqlite.Connection(":memory:", write_concern=wc_j) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 2

    # w=0 -> synchronous=OFF (0)
    wc_w0 = WriteConcern(w=0)
    with neosqlite.Connection(":memory:", write_concern=wc_w0) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 0

    # w=1 -> synchronous=NORMAL (1)
    wc_w1 = WriteConcern(w=1)
    with neosqlite.Connection(":memory:", write_concern=wc_w1) as conn:
        cursor = conn.db.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 1
