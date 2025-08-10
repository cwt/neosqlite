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
