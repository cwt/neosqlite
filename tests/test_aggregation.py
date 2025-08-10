# coding: utf-8
import neosqlite


def test_aggregate_match(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    pipeline = [{"$match": {"a": {"$gt": 1}}}]
    result = collection.aggregate(pipeline)
    assert len(result) == 2
    assert {doc["a"] for doc in result} == {2, 3}


def test_aggregate_sort(collection):
    collection.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
    pipeline = [{"$sort": {"a": neosqlite.ASCENDING}}]
    result = collection.aggregate(pipeline)
    assert [doc["a"] for doc in result] == [1, 2, 3]


def test_aggregate_skip_limit(collection):
    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$sort": {"a": 1}}, {"$skip": 2}, {"$limit": 3}]
    result = collection.aggregate(pipeline)
    assert len(result) == 3
    assert [doc["a"] for doc in result] == [2, 3, 4]


def test_aggregate_project(collection):
    collection.insert_many([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    pipeline = [{"$project": {"a": 1, "_id": 0}}]
    result = collection.aggregate(pipeline)
    assert all("b" not in doc for doc in result)
    assert all("_id" not in doc for doc in result)


def test_aggregate_group(collection):
    collection.insert_many(
        [
            {"store": "A", "price": 10},
            {"store": "B", "price": 20},
            {"store": "A", "price": 30},
        ]
    )
    pipeline = [
        {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
        {"$sort": {"_id": 1}},
    ]
    result = collection.aggregate(pipeline)
    assert len(result) == 2
    assert result[0] == {"_id": "A", "total": 40}
    assert result[1] == {"_id": "B", "total": 20}
