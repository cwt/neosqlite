"""Tests for PyMongo modern API parity (find kwargs, index opts, db helpers)."""

import pytest

import neosqlite
from neosqlite import IndexModel


@pytest.fixture
def connection():
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def seeded(connection):
    coll = connection["parity"]
    coll.insert_many([{"n": i} for i in range(10)])
    return coll


def test_find_limit_kwarg(seeded):
    docs = list(seeded.find({}, limit=3))
    assert len(docs) == 3


def test_find_limit_zero_means_no_limit(seeded):
    docs = list(seeded.find({}, limit=0))
    assert len(docs) == 10


def test_find_skip_kwarg(seeded):
    docs = list(seeded.find({}, skip=8))
    assert len(docs) == 2


def test_find_skip_and_limit_kwargs(seeded):
    docs = list(seeded.find({}, skip=2, limit=3))
    assert len(docs) == 3
    assert sorted([d["n"] for d in docs]) == [2, 3, 4]


def test_find_sort_kwarg_list(seeded):
    docs = list(seeded.find({}, sort=[("n", -1)], limit=3))
    assert [d["n"] for d in docs] == [9, 8, 7]


def test_find_sort_kwarg_dict(seeded):
    docs = list(seeded.find({}, sort={"n": -1}, limit=2))
    assert [d["n"] for d in docs] == [9, 8]


def test_find_sort_kwarg_str(seeded):
    docs = list(seeded.find({}, sort="n", limit=2))
    assert [d["n"] for d in docs] == [0, 1]


def test_find_sort_kwarg_tuple(seeded):
    docs = list(seeded.find({}, sort=("n", -1), limit=2))
    assert [d["n"] for d in docs] == [9, 8]


def test_find_kwargs_combine_with_chain(seeded):
    docs = list(seeded.find({}, limit=5).skip(3))
    assert len(docs) == 5
    assert [d["n"] for d in docs] == [3, 4, 5, 6, 7]


def test_find_unknown_kwarg_ignored(seeded):
    docs = list(seeded.find({}, limit=2, maxTimeMS=100))
    assert len(docs) == 2


def test_create_index_accepts_expire_after_seconds(connection):
    coll = connection["ttl_decl"]
    name = coll.create_index("date", expireAfterSeconds=43200)
    assert name == "idx_ttl_decl_date"
    info = coll.index_information()
    assert info[name]["expireAfterSeconds"] == 43200


def test_create_index_rejects_bad_expire_type(connection):
    coll = connection["ttl_bad"]
    with pytest.raises(TypeError):
        coll.create_index("date", expireAfterSeconds="3600")


def test_create_index_rejects_negative_expire(connection):
    coll = connection["ttl_neg"]
    with pytest.raises(ValueError):
        coll.create_index("date", expireAfterSeconds=-1)


def test_create_index_rejects_compound_ttl(connection):
    coll = connection["ttl_compound"]
    with pytest.raises(ValueError):
        coll.create_index([("a", 1), ("b", 1)], expireAfterSeconds=60)


def test_create_indexes_with_index_model_ttl(connection):
    coll = connection["ttl_model"]
    names = coll.create_indexes([IndexModel("date", expireAfterSeconds=3600)])
    assert len(names) == 1
    info = coll.index_information()
    assert info[names[0]]["expireAfterSeconds"] == 3600


def test_create_index_accepts_name_and_background(connection):
    coll = connection["idx_opts"]
    name = coll.create_index("age", name="custom_name", background=True)
    assert name == "idx_idx_opts_age"
    assert name in coll.list_indexes()


def test_get_database_returns_self(connection):
    assert connection.get_database() is connection
    assert connection.get_database("anything") is connection


def test_drop_database_is_method_not_collection(connection):
    assert callable(connection.drop_database)
    assert callable(connection.get_database)


def test_drop_database_clears_collections(connection):
    connection["a"].insert_one({"x": 1})
    connection["b"].insert_one({"y": 2})
    assert "a" in connection.list_collection_names()
    connection.drop_database()
    assert connection.list_collection_names() == []


def test_drop_database_with_name_arg(connection):
    connection["a"].insert_one({"x": 1})
    connection.drop_database("whatever_name")
    assert connection.list_collection_names() == []
