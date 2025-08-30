# coding: utf-8
import neosqlite


def test_aggregate_match(collection):
    collection.insert_many([{"a": 1}, {"a": 2}, {"a": 3}])
    pipeline = [{"$match": {"a": {"$gt": 1}}}]
    result = collection.aggregate(pipeline)
    # Verify that result is a cursor-like object (new behavior)
    assert hasattr(result, "__iter__")
    assert hasattr(result, "__next__")
    # Convert to list to check contents
    result_list = list(result)
    assert len(result_list) == 2
    assert {doc["a"] for doc in result_list} == {2, 3}


def test_aggregate_sort(collection):
    collection.insert_many([{"a": 3}, {"a": 1}, {"a": 2}])
    pipeline = [{"$sort": {"a": neosqlite.ASCENDING}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert [doc["a"] for doc in result_list] == [1, 2, 3]


def test_aggregate_skip_limit(collection):
    collection.insert_many([{"a": i} for i in range(10)])
    pipeline = [{"$sort": {"a": 1}}, {"$skip": 2}, {"$limit": 3}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 3
    assert [doc["a"] for doc in result_list] == [2, 3, 4]


def test_aggregate_project(collection):
    collection.insert_many([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    pipeline = [{"$project": {"a": 1, "_id": 0}}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert all("b" not in doc for doc in result_list)
    assert all("_id" not in doc for doc in result_list)


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
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0] == {"_id": "A", "total": 40}
    assert result_list[1] == {"_id": "B", "total": 20}


def test_aggregate_group_accumulators(collection):
    collection.insert_many(
        [
            {"item": "A", "price": 10, "quantity": 2},
            {"item": "B", "price": 20, "quantity": 1},
            {"item": "A", "price": 30, "quantity": 5},
            {"item": "B", "price": 10, "quantity": 2},
        ]
    )
    pipeline = [
        {
            "$group": {
                "_id": "$item",
                "total_quantity": {"$sum": "$quantity"},
                "avg_price": {"$avg": "$price"},
                "min_price": {"$min": "$price"},
                "max_price": {"$max": "$price"},
                "prices": {"$push": "$price"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 2
    assert result_list[0] == {
        "_id": "A",
        "total_quantity": 7,
        "avg_price": 20.0,
        "min_price": 10,
        "max_price": 30,
        "prices": [10, 30],
    }
    assert result_list[1] == {
        "_id": "B",
        "total_quantity": 3,
        "avg_price": 15.0,
        "min_price": 10,
        "max_price": 20,
        "prices": [20, 10],
    }


def test_aggregate_unwind(collection):
    collection.insert_one({"_id": 1, "item": "A", "sizes": ["S", "M", "L"]})
    pipeline = [{"$unwind": "$sizes"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 3
    assert {doc["sizes"] for doc in result_list} == {"S", "M", "L"}
    assert all(doc["item"] == "A" for doc in result_list)


def test_aggregate_fast_path(collection):
    collection.insert_many(
        [
            {"a": 1, "b": 10},
            {"a": 2, "b": 20},
            {"a": 3, "b": 30},
            {"a": 4, "b": 40},
        ]
    )
    pipeline = [
        {"$match": {"a": {"$gt": 1}}},
        {"$sort": {"b": neosqlite.DESCENDING}},
        {"$skip": 1},
        {"$limit": 1},
    ]
    result = collection.aggregate(pipeline)
    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0]["a"] == 3
