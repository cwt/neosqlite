"""Tests for ObjectId ordering and _id range queries."""

from operator import attrgetter

import pytest

import neosqlite
from neosqlite.collection.query_helper import set_force_fallback
from neosqlite.objectid import ObjectId


@pytest.fixture(params=[False, True], ids=["sql_tier", "python_tier"])
def tier(request):
    """Run each test on both the SQL tier and the Python fallback tier."""
    set_force_fallback(request.param)
    yield request.param
    set_force_fallback(False)


@pytest.fixture
def ordered_collection():
    """Collection with five time-ordered ObjectIds inserted in order."""
    conn = neosqlite.Connection(":memory:")
    try:
        coll = conn["ordered"]
        ids = [ObjectId(1700000000 + i) for i in range(5)]
        for pos, oid in enumerate(ids):
            coll.insert_one({"_id": oid, "n": pos})
        yield coll, ids
    finally:
        conn.close()


def _positions(cursor):
    return sorted(doc["n"] for doc in cursor)


def test_ordering_compares_twelve_byte_value():
    earlier = ObjectId(100)
    later = ObjectId(200)
    assert earlier < later
    assert earlier <= later
    assert later > earlier
    assert later >= earlier
    assert not later < earlier
    assert earlier <= ObjectId(100)


def test_ordering_matches_byte_order():
    ids = [ObjectId() for _ in range(50)]
    assert sorted(ids) == sorted(ids, key=attrgetter("binary"))


def test_ordering_rejects_other_types():
    oid = ObjectId()
    for other in ("6aa6a9a0d12ed95647ec57b5", b"x" * 12, 42, None):
        with pytest.raises(TypeError):
            _ = oid < other
        with pytest.raises(TypeError):
            _ = oid > other


def test_equality_with_string_still_works():
    oid = ObjectId()
    assert oid == oid.hex
    assert not (oid != oid.hex)


def test_find_id_range_operators(tier, ordered_collection):
    coll, ids = ordered_collection
    mid = ids[2]
    assert _positions(coll.find({"_id": {"$lt": mid}})) == [0, 1]
    assert _positions(coll.find({"_id": {"$gt": mid}})) == [3, 4]
    assert _positions(coll.find({"_id": {"$lte": mid}})) == [0, 1, 2]
    assert _positions(coll.find({"_id": {"$gte": mid}})) == [2, 3, 4]


def test_count_documents_id_range(tier, ordered_collection):
    coll, ids = ordered_collection
    assert coll.count_documents({"_id": {"$gt": ids[2]}}) == 2
    assert coll.count_documents({"_id": {"$lt": ids[2]}}) == 2
    assert coll.count_documents({"_id": {"$gte": ids[2]}}) == 3


def test_id_range_matches_only_objectids(tier):
    conn = neosqlite.Connection(":memory:")
    try:
        coll = conn["mixed"]
        anchor = ObjectId(1700000000)
        coll.insert_one({"_id": 1, "kind": "int"})
        coll.insert_one({"_id": "abc", "kind": "str"})
        coll.insert_one({"_id": anchor, "kind": "oid"})
        matched = [doc["kind"] for doc in coll.find({"_id": {"$gte": anchor}})]
        assert matched == ["oid"]
    finally:
        conn.close()


def test_cursor_pagination_newer_than(tier, ordered_collection):
    coll, ids = ordered_collection
    first_page = list(coll.find({}).sort([("_id", 1)]).limit(2))
    assert [doc["n"] for doc in first_page] == [0, 1]
    cursor = first_page[-1]["_id"]
    second_page = list(
        coll.find({"_id": {"$gt": cursor}}).sort([("_id", 1)]).limit(2)
    )
    assert [doc["n"] for doc in second_page] == [2, 3]


def test_id_equality_unaffected(tier, ordered_collection):
    coll, ids = ordered_collection
    assert [doc["n"] for doc in coll.find({"_id": ids[2]})] == [2]
    assert _positions(coll.find({"_id": {"$in": [ids[0], ids[4]]}})) == [0, 4]
    assert coll.count_documents({"_id": ids[2]}) == 1
