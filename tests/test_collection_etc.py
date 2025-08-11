# coding: utf-8
import neosqlite


def test_find_one_and_update(collection):
    collection.insert_one({"foo": "bar", "count": 1})
    doc = collection.find_one_and_update(
        {"foo": "bar"}, {"$set": {"foo": "baz"}, "$inc": {"count": 1}}
    )
    assert doc is not None

    updated_doc = collection.find_one({"_id": doc["_id"]})
    assert updated_doc["foo"] == "baz"
    assert updated_doc["count"] == 2


def test_find_one_and_replace(collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_replace({"foo": "bar"}, {"foo": "baz"})
    assert doc is not None
    assert collection.find_one({"foo": "bar"}) is None
    assert collection.find_one({"foo": "baz"}) is not None


def test_find_one_and_delete(collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one_and_delete({"foo": "bar"})
    assert doc is not None
    assert collection.count_documents({}) == 0


def test_count_documents(collection):
    collection.insert_many([{}, {}, {}])
    assert collection.count_documents({}) == 3
    assert collection.count_documents({"foo": "bar"}) == 0


def test_estimated_document_count(collection):
    collection.insert_many([{}, {}, {}])
    assert collection.estimated_document_count() == 3


def test_distinct(collection):
    collection.insert_many(
        [{"foo": "bar"}, {"foo": "baz"}, {"foo": 10}, {"bar": "foo"}]
    )
    assert set(("bar", "baz", 10)) == collection.distinct("foo")


def test_distinct_nested(collection):
    collection.insert_many(
        [
            {"a": {"b": 1}},
            {"a": {"b": 2}},
            {"a": {"b": 1}},
            {"a": {"c": 1}},
        ]
    )
    assert {1, 2} == collection.distinct("a.b")


def test_distinct_no_match(collection):
    collection.insert_many([{"foo": "bar"}])
    assert set() == collection.distinct("nonexistent")


def test_distinct_with_null(collection):
    collection.insert_many([{"foo": "bar"}, {"foo": None}])
    assert {"bar"} == collection.distinct("foo")


def test_distinct_complex_types(collection):
    collection.insert_many(
        [
            {"foo": [1, 2]},
            {"foo": [1, 2]},
            {"foo": [2, 3]},
            {"foo": {"a": 1}},
            {"foo": {"a": 1}},
        ]
    )
    results = collection.distinct("foo")
    assert len(results) == 3
    results = collection.distinct("foo")
    assert len(results) == 3
    assert (1, 2) in results
    assert (2, 3) in results
    assert '{"a": 1}' in results


def test_distinct_with_filter(collection):
    collection.insert_many(
        [
            {"category": "A", "value": 1},
            {"category": "A", "value": 2},
            {"category": "B", "value": 1},
            {"category": "A", "value": 1},
        ]
    )
    assert {1, 2} == collection.distinct("value", filter={"category": "A"})


def test_distinct_with_filter_no_match(collection):
    collection.insert_many(
        [
            {"category": "A", "value": 1},
            {"category": "B", "value": 2},
        ]
    )
    assert set() == collection.distinct("value", filter={"category": "C"})


def test_distinct_with_filter_and_nested_key(collection):
    collection.insert_many(
        [
            {"group": "X", "data": {"value": 10}},
            {"group": "Y", "data": {"value": 20}},
            {"group": "X", "data": {"value": 10}},
            {"group": "X", "data": {"value": 30}},
        ]
    )
    assert {10, 30} == collection.distinct("data.value", filter={"group": "X"})


def test_rename(collection):
    collection.insert_one({"foo": "bar"})
    collection.rename("new_collection")
    assert collection.name == "new_collection"
    assert collection.find_one({"foo": "bar"}) is not None


def test_rename_to_existing_collection(collection):
    collection.insert_one({"foo": "bar"})
    collection.database.new_collection.insert_one({"baz": "qux"})
    try:
        collection.rename("new_collection")
        assert False, "Should have raised an exception"
    except Exception:
        pass


def test_options(collection):
    options = collection.options()
    assert options["name"] == collection.name
    assert "columns" in options
    assert "indexes" in options
    assert "count" in options


def test_database_property(collection):
    assert isinstance(collection.database, neosqlite.Connection)
    assert (
        collection.database["some_other_collection"].name
        == "some_other_collection"
    )
