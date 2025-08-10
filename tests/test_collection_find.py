# coding: utf-8
import neosqlite


def test_find_returns_cursor(collection):
    cursor = collection.find()
    assert isinstance(cursor, neosqlite.Cursor)


def test_find_with_sort(collection):
    collection.insert_many(
        [
            {"a": 1, "b": "c"},
            {"a": 1, "b": "a"},
            {"a": 5, "b": "x"},
            {"a": 3, "b": "x"},
            {"a": 4, "b": "z"},
        ]
    )

    docs = list(collection.find().sort("a", neosqlite.ASCENDING))
    assert [d["a"] for d in docs] == [1, 1, 3, 4, 5]

    docs = list(collection.find().sort("a", neosqlite.DESCENDING))
    assert [d["a"] for d in docs] == [5, 4, 3, 1, 1]

    docs = list(
        collection.find().sort(
            [("a", neosqlite.ASCENDING), ("b", neosqlite.DESCENDING)]
        )
    )
    a_vals = [d["a"] for d in docs]
    b_vals = [d["b"] for d in docs]
    assert a_vals == [1, 1, 3, 4, 5]
    assert b_vals == ["c", "a", "x", "z", "x"]


def test_find_with_skip_and_limit(collection):
    collection.insert_many([{"i": i} for i in range(10)])

    docs = list(collection.find().skip(5))
    assert len(docs) == 5
    assert docs[0]["i"] == 5

    docs = list(collection.find().limit(5))
    assert len(docs) == 5
    assert docs[0]["i"] == 0

    docs = list(collection.find().skip(2).limit(3))
    assert len(docs) == 3
    assert [d["i"] for d in docs] == [2, 3, 4]


def test_find_with_sort_on_nested_key(collection):
    collection.insert_many(
        [
            {"a": {"b": 5}, "c": "B"},
            {"a": {"b": 9}, "c": "A"},
            {"a": {"b": 7}, "c": "C"},
        ]
    )
    docs = list(collection.find().sort("a.b", neosqlite.ASCENDING))
    assert [d["a"]["b"] for d in docs] == [5, 7, 9]

    docs = list(collection.find().sort("a.b", neosqlite.DESCENDING))
    assert [d["a"]["b"] for d in docs] == [9, 7, 5]


def test_find_one(collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"
    assert collection.find_one({"foo": "baz"}) is None


def test_find_one_with_projection_inclusion(collection):
    collection.insert_one({"foo": "bar", "baz": 42, "qux": [1, 2]})
    doc = collection.find_one({"foo": "bar"}, {"foo": 1, "baz": 1})
    assert doc is not None
    assert "foo" in doc
    assert "baz" in doc
    assert "qux" not in doc
    assert "_id" in doc


def test_find_one_with_projection_exclusion(collection):
    collection.insert_one({"foo": "bar", "baz": 42, "qux": [1, 2]})
    doc = collection.find_one({"foo": "bar"}, {"qux": 0, "_id": 0})
    assert doc is not None
    assert "foo" in doc
    assert "baz" in doc
    assert "qux" not in doc
    assert "_id" not in doc


def test_find_one_with_projection_id_only(collection):
    collection.insert_one({"foo": "bar", "baz": 42})
    doc = collection.find_one({"foo": "bar"}, {"_id": 1})
    assert doc is not None
    assert doc.keys() == {"_id"}


def test_find_with_projection(collection):
    collection.insert_many(
        [
            {"a": 1, "b": "c", "d": True},
            {"a": 1, "b": "a", "d": False},
        ]
    )
    docs = list(collection.find(projection={"a": 1, "_id": 0}))
    assert len(docs) == 2
    for doc in docs:
        assert doc.keys() == {"a"}


def test_find_with_in_operator(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    docs = list(collection.find({"a": {"$in": [1, 3]}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 3}


def test_find_with_nin_operator(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    docs = list(collection.find({"a": {"$nin": [1, 3]}}))
    assert len(docs) == 1
    assert docs[0]["a"] == 2


def test_find_with_comparison_operators(collection):
    collection.insert_many([{"a": 1}, {"a": 5}, {"a": 10}])
    docs = list(collection.find({"a": {"$gt": 3}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {5, 10}

    docs = list(collection.find({"a": {"$gte": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {5, 10}

    docs = list(collection.find({"a": {"$lt": 7}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 5}

    docs = list(collection.find({"a": {"$lte": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 5}

    docs = list(collection.find({"a": {"$ne": 5}}))
    assert len(docs) == 2
    assert {doc["a"] for doc in docs} == {1, 10}
