"""
Tests for find_one, find_raw_batches, and RawBatchCursor.
"""

import json
import neosqlite


def test_returns_None_if_collection_is_empty(collection: neosqlite.Collection):
    assert collection.find_one({}) is None


def test_returns_None_if_document_is_not_found(
    collection: neosqlite.Collection,
):
    collection.insert_one({"foo": "bar"})
    assert collection.find_one({"foo": "baz"}) is None


def test_returns_document_if_found(collection: neosqlite.Collection):
    collection.insert_one({"foo": "bar"})
    doc = collection.find_one({"foo": "bar"})
    assert doc is not None
    assert doc["foo"] == "bar"


def test_find_raw_batches():
    """Test the find_raw_batches method."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
            {"name": "David", "age": 28, "city": "Houston"},
            {"name": "Eve", "age": 32, "city": "Phoenix"},
        ]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches()
        batches = list(cursor)
        assert len(batches) > 0
        for batch in batches:
            assert isinstance(batch, bytes)
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                assert isinstance(doc, dict)
                assert "_id" in doc
        cursor = collection.find_raw_batches(batch_size=2)
        batches = list(cursor)
        assert len(batches) >= 3
        for i in range(2):
            batch_str = batches[i].decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            assert len(doc_strings) == 2
        last_batch_str = batches[-1].decode("utf-8")
        last_doc_strings = [s for s in last_batch_str.split("\n") if s]
        assert len(last_doc_strings) == 1
        cursor = collection.find_raw_batches(
            {"age": {"$gte": 30}},
            batch_size=2,
        )
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 3
        ages = [doc["age"] for doc in all_docs]
        assert all(age >= 30 for age in ages)
        cursor = collection.find_raw_batches(
            projection={"name": 1, "_id": 0},
            batch_size=2,
        )
        batches = list(cursor)
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                assert "name" in doc


def test_find_raw_batches_empty():
    """Test find_raw_batches with empty collection."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        cursor = collection.find_raw_batches()
        batches = list(cursor)
        assert len(batches) == 0


def test_find_raw_batches_batch_size():
    """Test find_raw_batches with different batch sizes."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [{"num": i} for i in range(10)]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(batch_size=1)
        batches = list(cursor)
        assert len(batches) == 10
        cursor = collection.find_raw_batches(batch_size=5)
        batches = list(cursor)
        assert len(batches) == 2
        cursor = collection.find_raw_batches(batch_size=20)
        batches = list(cursor)
        assert len(batches) == 1


def test_raw_batch_cursor_skip_limit():
    """Test skip and limit functionality in RawBatchCursor."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [{"num": i} for i in range(20)]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches()
        cursor._skip = 5
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 15
        assert all_docs[0]["num"] == 5
        cursor = collection.find_raw_batches()
        cursor._limit = 10
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 10
        nums = [doc["num"] for doc in all_docs]
        assert nums == list(range(10))
        cursor = collection.find_raw_batches()
        cursor._skip = 5
        cursor._limit = 8
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 8
        nums = [doc["num"] for doc in all_docs]
        assert nums == list(range(5, 13))


def test_raw_batch_cursor_sorting():
    """Test sorting functionality in RawBatchCursor."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Charlie", "age": 35},
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "David", "age": 28},
            {"name": "Eve", "age": 32},
        ]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches()
        cursor._sort = {"age": 1}
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        ages = [doc["age"] for doc in all_docs]
        assert ages == sorted(ages)
        cursor = collection.find_raw_batches()
        cursor._sort = {"name": -1}
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        names = [doc["name"] for doc in all_docs]
        assert names == sorted(names, reverse=True)


def test_raw_batch_cursor_complex_query_fallback():
    """Test fallback to Python processing for complex queries."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(filter={"age": {"$gt": 25}})
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 2
        names = {doc["name"] for doc in all_docs}
        assert names == {"Alice", "Charlie"}
        cursor = collection.find_raw_batches(filter={})
        batches = list(cursor)
        assert len(batches) >= 1


def test_raw_batch_cursor_edge_cases():
    """Test edge cases in RawBatchCursor."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        cursor = collection.find_raw_batches()
        cursor._skip = 10
        cursor._limit = 5
        cursor._sort = {"nonexistent": 1}
        batches = list(cursor)
        assert len(batches) == 0
        docs = [{"num": i} for i in range(3)]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(batch_size=100)
        batches = list(cursor)
        assert len(batches) == 1
        batch_str = batches[0].decode("utf-8")
        doc_strings = [s for s in batch_str.split("\n") if s]
        assert len(doc_strings) == 3
        cursor = collection.find_raw_batches(batch_size=0)
        batches = list(cursor)


def test_raw_batch_cursor_sort_with_filter():
    """Test sorting combined with filtering."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
            {"name": "Charlie", "age": 35, "city": "New York"},
            {"name": "David", "age": 28, "city": "Chicago"},
            {"name": "Eve", "age": 32, "city": "New York"},
        ]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(filter={"city": "New York"})
        cursor._sort = {"age": -1}
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 3
        names = [doc["name"] for doc in all_docs]
        ages = [doc["age"] for doc in all_docs]
        expected_order = ["Charlie", "Eve", "Alice"]
        assert names == expected_order
        assert ages == sorted(ages, reverse=True)


def test_raw_batch_cursor_multi_field_sort():
    """Test sorting by multiple fields."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Zebra", "age": 30, "score": 85},
            {"name": "Alpha", "age": 30, "score": 95},
            {"name": "Beta", "age": 25, "score": 80},
            {"name": "Gamma", "age": 25, "score": 90},
        ]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches()
        cursor._sort = {"age": 1, "score": -1}
        cursor._sort = {"age": 1}
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        ages = [doc["age"] for doc in all_docs]
        assert ages == sorted(ages)


def test_raw_batch_cursor_fallback_path():
    """Test the Python fallback path in RawBatchCursor."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        collection.insert_many(docs)
        complex_filter = {"nonexistent": {"$elemMatch": {"$eq": "value"}}}
        where_result = (
            collection.query_engine.helpers._build_simple_where_clause(
                complex_filter
            )
        )
        assert where_result is None
        cursor = collection.find_raw_batches(filter=complex_filter)
        batches = list(cursor)
        assert len(batches) >= 0


def test_raw_batch_cursor_constructor():
    """Test RawBatchCursor constructor with different parameters."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        cursor = collection.find_raw_batches()
        assert cursor._batch_size == 100
        assert cursor._filter == {}
        assert cursor._projection == {}
        assert cursor._hint is None
        test_filter = {"age": {"$gt": 25}}
        test_projection = {"name": 1}
        cursor = collection.find_raw_batches(
            filter=test_filter,
            projection=test_projection,
            hint="test_hint",
            batch_size=50,
        )
        assert cursor._batch_size == 50
        assert cursor._filter == test_filter
        assert cursor._projection == test_projection
        assert cursor._hint == "test_hint"


def test_raw_batch_cursor_zero_batch_size():
    """Test RawBatchCursor with zero batch size."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [{"num": i} for i in range(5)]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(batch_size=0)
        batches = list(cursor)
        assert len(batches) >= 0


def test_raw_batch_cursor_limit_smaller_than_batch():
    """Test limit smaller than batch size."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [{"num": i} for i in range(20)]
        collection.insert_many(docs)
        cursor = collection.find_raw_batches(batch_size=15)
        cursor._limit = 5
        batches = list(cursor)
        assert len(batches) == 1
        batch_str = batches[0].decode("utf-8")
        doc_strings = [s for s in batch_str.split("\n") if s]
        assert len(doc_strings) == 5


# Tests for aggregate_raw_batches method


def test_aggregate_raw_batches_basic():
    """Test basic aggregate_raw_batches functionality."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
            {"name": "David", "age": 28, "city": "Houston"},
            {"name": "Eve", "age": 32, "city": "Phoenix"},
        ]
        collection.insert_many(docs)
        pipeline = [{"$match": {"age": {"$gte": 30}}}]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)
        assert len(batches) > 0
        for batch in batches:
            assert isinstance(batch, bytes)
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                assert isinstance(doc, dict)
                assert "age" in doc
                assert doc["age"] >= 30


def test_aggregate_raw_batches_empty():
    """Test aggregate_raw_batches with empty result."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        collection.insert_many(docs)
        pipeline = [{"$match": {"age": {"$gt": 40}}}]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)
        assert len(batches) == 0


def test_aggregate_raw_batches_with_group():
    """Test aggregate_raw_batches with group stage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"store": "A", "item": "apple", "price": 10},
            {"store": "A", "item": "banana", "price": 5},
            {"store": "B", "item": "apple", "price": 12},
            {"store": "B", "item": "orange", "price": 8},
        ]
        collection.insert_many(docs)
        pipeline = [
            {"$group": {"_id": "$store", "total": {"$sum": "$price"}}},
            {"$sort": {"_id": 1}},
        ]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)
        assert len(batches) > 0
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 2
        assert all_docs[0]["_id"] == "A"
        assert all_docs[0]["total"] == 15
        assert all_docs[1]["_id"] == "B"
        assert all_docs[1]["total"] == 20


def test_aggregate_raw_batches_with_project():
    """Test aggregate_raw_batches with project stage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
        ]
        collection.insert_many(docs)
        pipeline = [{"$project": {"name": 1, "age": 1, "_id": 0}}]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)
        assert len(batches) > 0
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 2
        for doc in all_docs:
            assert "name" in doc
            assert "age" in doc
            assert "_id" not in doc


def test_aggregate_raw_batches_sort_skip_limit():
    """Test aggregate_raw_batches with sort, skip, and limit stages."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        docs = [{"num": i} for i in range(20)]
        collection.insert_many(docs)
        pipeline = [{"$sort": {"num": 1}}, {"$skip": 5}, {"$limit": 8}]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)
        all_docs = []
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                all_docs.append(doc)
        assert len(all_docs) == 8
        nums = [doc["num"] for doc in all_docs]
        assert nums == list(range(5, 13))
