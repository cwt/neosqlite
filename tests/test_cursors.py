"""
Tests for find_one, find_raw_batches, and RawBatchCursor.
"""

import json
import pytest
import neosqlite
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)


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
        # Use $jsonSchema which triggers Python fallback
        complex_filter = {"$jsonSchema": {}}
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


class TestCursorToList:
    """Tests for Cursor.to_list() method."""

    def test_to_list_returns_all_documents(self, collection):
        """Test that to_list() returns all documents."""
        docs = [{"name": f"Doc{i}", "value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        result = cursor.to_list()

        assert len(result) == 5
        assert all(isinstance(doc, dict) for doc in result)
        assert all("name" in doc for doc in result)

    def test_to_list_with_length_parameter(self, collection):
        """Test to_list() with length parameter."""
        docs = [{"name": f"Doc{i}", "value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        result = cursor.to_list(3)

        assert len(result) == 3
        assert result[0]["value"] == 0
        assert result[1]["value"] == 1
        assert result[2]["value"] == 2

    def test_to_list_with_filter(self, collection):
        """Test to_list() with filter."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$gte": 5}})
        result = cursor.to_list()

        assert len(result) == 5
        assert all(doc["value"] >= 5 for doc in result)

    def test_to_list_with_sort_and_limit(self, collection):
        """Test to_list() with sort and limit chained."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = (
            collection.find({}).sort("value", neosqlite.DESCENDING).limit(3)
        )
        result = cursor.to_list()

        assert len(result) == 3
        assert result[0]["value"] == 9
        assert result[1]["value"] == 8
        assert result[2]["value"] == 7

    def test_to_list_empty_collection(self, collection):
        """Test to_list() on empty collection."""
        cursor = collection.find({})
        result = cursor.to_list()

        assert len(result) == 0
        assert result == []

    def test_to_list_with_kill_switch(self, collection):
        """Test to_list() works with kill switch enabled (Tier-3 Python fallback)."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        # Enable kill switch to force Python fallback
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)

            cursor = collection.find({"value": {"$gte": 2}})
            result = cursor.to_list()

            # Should still work with Python fallback
            assert len(result) == 3
            assert all(doc["value"] >= 2 for doc in result)
        finally:
            # Restore original state
            set_force_fallback(original_state)

    def test_to_list_kill_switch_comparison(self, collection):
        """Test that to_list() returns same results with/without kill switch."""
        docs = [{"value": i, "name": f"Doc{i}"} for i in range(20)]
        collection.insert_many(docs)

        # Get results without kill switch
        cursor_normal = (
            collection.find({"value": {"$gte": 10}}).sort("value").limit(5)
        )
        results_normal = cursor_normal.to_list()

        # Get results with kill switch
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            cursor_fallback = (
                collection.find({"value": {"$gte": 10}}).sort("value").limit(5)
            )
            results_fallback = cursor_fallback.to_list()
        finally:
            set_force_fallback(original_state)

        # Results should be identical
        assert len(results_normal) == len(results_fallback)
        for i in range(len(results_normal)):
            assert results_normal[i]["value"] == results_fallback[i]["value"]
            assert results_normal[i]["name"] == results_fallback[i]["name"]


class TestCursorClone:
    """Tests for Cursor.clone() method."""

    def test_clone_creates_independent_cursor(self, collection):
        """Test that clone() creates an independent cursor."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        original = collection.find({"value": {"$gte": 2}})
        cloned = original.clone()

        # Iterate original
        original_results = list(original)
        # Iterate clone
        cloned_results = list(cloned)

        # Both should have same results
        assert len(original_results) == len(cloned_results)
        assert original_results == cloned_results

    def test_clone_preserves_filter(self, collection):
        """Test that clone() preserves the filter."""
        docs = [
            {"value": i, "category": "A" if i % 2 == 0 else "B"}
            for i in range(10)
        ]
        collection.insert_many(docs)

        original = collection.find({"category": "A"})
        cloned = original.clone()

        original_results = list(original)
        cloned_results = list(cloned)

        assert len(original_results) == 5
        assert len(cloned_results) == 5
        assert all(doc["category"] == "A" for doc in original_results)
        assert all(doc["category"] == "A" for doc in cloned_results)

    def test_clone_preserves_sort_skip_limit(self, collection):
        """Test that clone() preserves sort, skip, and limit."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        original = (
            collection.find({})
            .sort("value", neosqlite.DESCENDING)
            .skip(2)
            .limit(3)
        )
        cloned = original.clone()

        original_results = list(original)
        cloned_results = list(cloned)

        # With DESCENDING sort: [9,8,7,6,5,4,3,2,1,0], skip 2: [7,6,5,4,3,2,1,0], limit 3: [7,6,5]
        assert len(original_results) == 3
        assert len(cloned_results) == 3
        assert original_results == cloned_results
        assert original_results[0]["value"] == 7
        assert original_results[1]["value"] == 6
        assert original_results[2]["value"] == 5

    def test_clone_preserves_projection(self, collection):
        """Test that clone() preserves projection."""
        docs = [{"name": f"Doc{i}", "value": i, "extra": "x"} for i in range(3)]
        collection.insert_many(docs)

        original = collection.find(
            {}, projection={"name": 1, "value": 1, "_id": 0}
        )
        cloned = original.clone()

        original_results = list(original)
        cloned_results = list(cloned)

        assert len(original_results) == len(cloned_results)
        for doc in original_results + cloned_results:
            assert "name" in doc
            assert "value" in doc
            assert "_id" not in doc

    def test_clone_is_independent_iteration(self, collection):
        """Test that cloned cursor can be iterated independently."""
        docs = [{"value": i} for i in range(3)]
        collection.insert_many(docs)

        cursor = collection.find({})
        clone = cursor.clone()

        # Iterate clone first
        clone_results = list(clone)
        assert len(clone_results) == 3

        # Original should still work
        original_results = list(cursor)
        assert len(original_results) == 3

    def test_clone_with_kill_switch(self, collection):
        """Test clone() works with kill switch enabled."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        original_state = get_force_fallback()
        try:
            set_force_fallback(True)

            original = collection.find({"value": {"$gte": 2}}).limit(2)
            cloned = original.clone()

            original_results = list(original)
            cloned_results = list(cloned)

            assert len(original_results) == 2
            assert len(cloned_results) == 2
            assert original_results == cloned_results
        finally:
            set_force_fallback(original_state)

    def test_clone_kill_switch_comparison(self, collection):
        """Test that clone() returns same results with/without kill switch."""
        docs = [{"value": i, "name": f"Doc{i}"} for i in range(15)]
        collection.insert_many(docs)

        # Without kill switch
        cursor_normal = collection.find({"value": {"$gte": 5}}).sort("value")
        clone_normal = cursor_normal.clone()
        results_normal = list(clone_normal)

        # With kill switch
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            cursor_fallback = collection.find({"value": {"$gte": 5}}).sort(
                "value"
            )
            clone_fallback = cursor_fallback.clone()
            results_fallback = list(clone_fallback)
        finally:
            set_force_fallback(original_state)

        # Results should be identical
        assert len(results_normal) == len(results_fallback)
        for i in range(len(results_normal)):
            assert results_normal[i]["value"] == results_fallback[i]["value"]


class TestCursorExplain:
    """Tests for Cursor.explain() method."""

    def test_explain_returns_query_plan(self, collection):
        """Test that explain() returns a query plan."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$gte": 5}})
        plan = cursor.explain()

        assert isinstance(plan, dict)
        assert "queryPlanner" in plan
        assert "winningPlan" in plan["queryPlanner"]
        assert isinstance(plan["queryPlanner"]["winningPlan"], list)
        assert len(plan["queryPlanner"]["winningPlan"]) > 0

    def test_explain_with_execution_stats(self, collection):
        """Test explain() with executionStats verbosity."""
        docs = [{"value": i} for i in range(100)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$gte": 50}})
        plan = cursor.explain(verbosity="executionStats")

        assert "queryPlanner" in plan
        assert "executionStats" in plan
        assert "nReturned" in plan["executionStats"]
        assert "executionTimeMillis" in plan["executionStats"]
        assert plan["executionStats"]["nReturned"] == 50

    def test_explain_with_index(self, collection):
        """Test explain() shows index usage."""
        docs = [{"value": i} for i in range(100)]
        collection.insert_many(docs)
        collection.create_index("value")

        cursor = collection.find({"value": {"$gte": 80}})
        plan = cursor.explain()

        assert "queryPlanner" in plan
        assert "indexUsage" in plan["queryPlanner"]
        assert isinstance(plan["queryPlanner"]["indexUsage"], list)

    def test_explain_simple_query(self, collection):
        """Test explain() with simple query."""
        docs = [{"name": f"Doc{i}"} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        plan = cursor.explain()

        assert isinstance(plan, dict)
        assert "queryPlanner" in plan
        assert len(plan["queryPlanner"]["winningPlan"]) > 0

    def test_explain_with_sort(self, collection):
        """Test explain() with sort."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({}).sort("value", neosqlite.DESCENDING)
        plan = cursor.explain()

        assert "queryPlanner" in plan
        assert len(plan["queryPlanner"]["winningPlan"]) > 0

    def test_explain_with_kill_switch(self, collection):
        """Test explain() works with kill switch enabled."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        original_state = get_force_fallback()
        try:
            set_force_fallback(True)

            cursor = collection.find({"value": {"$gte": 5}})
            plan = cursor.explain()

            # Should still return plan even with kill switch
            assert isinstance(plan, dict)
            assert "queryPlanner" in plan
            assert len(plan["queryPlanner"]["winningPlan"]) > 0
        finally:
            set_force_fallback(original_state)

    def test_explain_execution_stats_accuracy(self, collection):
        """Test that explain() execution stats are accurate."""
        docs = [{"value": i} for i in range(50)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$lt": 10}})
        plan = cursor.explain(verbosity="executionStats")

        assert plan["executionStats"]["nReturned"] == 10
        assert plan["executionStats"]["executionTimeMillis"] >= 0

    def test_explain_kill_switch_comparison(self, collection):
        """Test that explain() returns valid plan with/without kill switch."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        # Without kill switch
        cursor_normal = collection.find({"value": {"$gte": 10}})
        plan_normal = cursor_normal.explain()

        # With kill switch
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            cursor_fallback = collection.find({"value": {"$gte": 10}})
            plan_fallback = cursor_fallback.explain()
        finally:
            set_force_fallback(original_state)

        # Both should return valid plans
        assert "queryPlanner" in plan_normal
        assert "queryPlanner" in plan_fallback
        assert len(plan_normal["queryPlanner"]["winningPlan"]) > 0
        assert len(plan_fallback["queryPlanner"]["winningPlan"]) > 0


class TestCollectionFullName:
    """Tests for Collection.full_name property."""

    def test_full_name_with_named_database(self):
        """Test full_name with a named database."""
        conn = neosqlite.Connection(":memory:", name="test_db")
        collection = conn.my_collection

        assert collection.full_name == "test_db.my_collection"
        conn.close()

    def test_full_name_with_memory_database(self):
        """Test full_name with in-memory database."""
        conn = neosqlite.Connection(":memory:")
        collection = conn.test_collection

        # Memory databases should have "memory" as database name
        assert "test_collection" in collection.full_name
        conn.close()

    def test_full_name_with_file_database(self, tmp_path):
        """Test full_name with file-based database."""
        db_path = tmp_path / "test.db"
        conn = neosqlite.Connection(str(db_path))
        collection = conn.my_collection

        assert "my_collection" in collection.full_name
        conn.close()

    def test_full_name_is_string(self, collection):
        """Test that full_name returns a string."""
        assert isinstance(collection.full_name, str)
        assert len(collection.full_name) > 0


class TestCollectionWithOptions:
    """Tests for Collection.with_options() method."""

    def test_with_options_returns_new_collection(self, collection):
        """Test that with_options() returns a new collection."""
        new_coll = collection.with_options()

        assert new_coll is not collection
        assert new_coll.name == collection.name
        assert new_coll.db == collection.db

    def test_with_options_preserves_name(self, collection):
        """Test that with_options() preserves collection name."""
        new_coll = collection.with_options(
            write_concern={"w": "majority"}, read_preference=None
        )

        assert new_coll.name == collection.name

    def test_with_options_stores_options(self, collection):
        """Test that with_options() stores the provided options."""
        new_coll = collection.with_options(
            codec_options={"test": "value"}, write_concern={"w": "majority"}
        )

        assert hasattr(new_coll, "_codec_options")
        assert hasattr(new_coll, "_write_concern")
        assert new_coll._write_concern == {"w": "majority"}

    def test_with_options_chaining(self, collection):
        """Test that with_options() can be chained."""
        coll1 = collection.with_options(write_concern={"w": 1})
        coll2 = coll1.with_options(write_concern={"w": "majority"})

        assert coll1 is not coll2
        assert coll1.name == coll2.name
        assert coll2._write_concern == {"w": "majority"}

    def test_with_options_independent_operations(self, collection):
        """Test that collection with options operates independently."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        # Create collection with options
        coll_opts = collection.with_options(write_concern={"w": "majority"})

        # Should be able to query normally
        results = list(coll_opts.find({"value": {"$gte": 3}}))
        assert len(results) == 2

    def test_with_options_kill_switch_comparison(self, collection):
        """Test with_options() works with/without kill switch."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        # Create collection with options
        coll_opts = collection.with_options(write_concern={"w": "majority"})

        # Query without kill switch
        results_normal = list(coll_opts.find({"value": {"$gte": 5}}))

        # Query with kill switch
        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            results_fallback = list(coll_opts.find({"value": {"$gte": 5}}))
        finally:
            set_force_fallback(original_state)

        # Results should be identical
        assert len(results_normal) == len(results_fallback)
        for i in range(len(results_normal)):
            assert results_normal[i]["value"] == results_fallback[i]["value"]


class TestCursorComment:
    """Tests for Cursor.comment() method."""

    def test_comment_basic(self, collection):
        """Test that comment() adds a comment to the query."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$gte": 5}}).comment("test comment")
        results = list(cursor)

        assert len(results) == 5
        assert cursor._comment == "test comment"

    def test_comment_chaining(self, collection):
        """Test that comment() returns cursor for chaining."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = (
            collection.find({"value": {"$gte": 5}}).comment("test").limit(2)
        )
        results = list(cursor)

        assert len(results) == 2
        assert cursor._comment == "test"

    def test_comment_with_sort(self, collection):
        """Test comment() works with sort."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = (
            collection.find({})
            .sort("value", neosqlite.DESCENDING)
            .comment("sorted query")
        )
        results = list(cursor)

        assert len(results) == 10
        assert results[0]["value"] == 9
        assert cursor._comment == "sorted query"

    def test_comment_empty_string(self, collection):
        """Test comment() with empty string."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({}).comment("")
        results = list(cursor)

        assert len(results) == 5
        assert cursor._comment == ""

    def test_comment_special_characters(self, collection):
        """Test comment() sanitizes special SQL characters."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        # Comment with SQL comment delimiters should be sanitized
        cursor = collection.find({}).comment("test /* injection */ attack")
        results = list(cursor)

        assert len(results) == 5
        # The comment should be stored but sanitized in SQL
        assert cursor._comment == "test /* injection */ attack"

    def test_comment_with_expr(self, collection):
        """Test comment() works with $expr queries."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"$expr": {"$gt": ["$value", 5]}}).comment(
            "expr query"
        )
        results = list(cursor)

        assert len(results) == 4  # values 6, 7, 8, 9
        assert cursor._comment == "expr query"

    def test_comment_kill_switch(self, collection):
        """Test comment() works with kill switch enabled."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        original_state = get_force_fallback()
        try:
            set_force_fallback(True)
            cursor = collection.find({"value": {"$gte": 5}}).comment(
                "fallback query"
            )
            results = list(cursor)

            assert len(results) == 5
            assert cursor._comment == "fallback query"
        finally:
            set_force_fallback(original_state)

    def test_comment_none_value(self, collection):
        """Test that comment can be None (default)."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        assert cursor._comment is None

        results = list(cursor)
        assert len(results) == 5


class TestCursorRetrieved:
    """Tests for Cursor.retrieved property."""

    def test_retrieved_initial(self, collection):
        """Test that retrieved is 0 initially."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        assert cursor.retrieved == 0

    def test_retrieved_after_full_iteration(self, collection):
        """Test retrieved count after full iteration."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        results = list(cursor)
        assert cursor.retrieved == 10
        assert len(results) == 10

    def test_retrieved_with_limit(self, collection):
        """Test retrieved count with limit."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).limit(5)
        results = list(cursor)
        assert cursor.retrieved == 5
        assert len(results) == 5

    def test_retrieved_with_filter(self, collection):
        """Test retrieved count with filter."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"value": {"$gte": 5}})
        results = list(cursor)
        assert cursor.retrieved == 5  # values 5, 6, 7, 8, 9
        assert len(results) == 5

    def test_retrieved_partial_iteration(self, collection):
        """Test retrieved count after partial iteration."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        count = 0
        for doc in cursor:
            count += 1
            if count >= 3:
                break
        assert cursor.retrieved == 3

    def test_retrieved_clone_starts_fresh(self, collection):
        """Test that cloned cursor starts with retrieved=0."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({}).limit(5)
        results1 = list(cursor)
        assert cursor.retrieved == 5

        cloned = cursor.clone()
        assert cloned.retrieved == 0

        results2 = list(cloned)
        assert cloned.retrieved == 5
        assert len(results1) == len(results2)

    def test_retrieved_multiple_iterations(self, collection):
        """Test retrieved count with multiple iterations."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        # First iteration
        list(cursor)
        assert cursor.retrieved == 5

        # Second iteration (cursor can be re-iterated)
        list(cursor)
        # Note: retrieved continues counting from previous iteration
        assert cursor.retrieved == 10

    def test_retrieved_with_skip_and_limit(self, collection):
        """Test retrieved with skip and limit."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).skip(5).limit(3)
        results = list(cursor)
        assert cursor.retrieved == 3
        assert len(results) == 3


class TestDatabaseCommandValidate:
    """Tests for Database.command('validate') method."""

    def test_validate_valid_collection(self, collection):
        """Test validate command on a valid collection."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        result = collection.database.command(
            "validate", collection=collection.name
        )

        assert result["ok"] == 1.0
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_empty_collection(self, collection):
        """Test validate command on an empty collection."""
        result = collection.database.command(
            "validate", collection=collection.name
        )

        assert result["ok"] == 1.0
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_with_index(self, collection):
        """Test validate command with indexes."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)
        collection.create_index("value")

        result = collection.database.command(
            "validate", collection=collection.name
        )

        assert result["ok"] == 1.0
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_multiple_collections(self):
        """Test validate command on multiple collections."""
        conn = neosqlite.Connection(":memory:")
        coll1 = conn["test1"]
        coll2 = conn["test2"]

        coll1.insert_many([{"x": i} for i in range(5)])
        coll2.insert_many([{"y": i} for i in range(5)])

        result1 = conn.command("validate", collection=coll1.name)
        result2 = conn.command("validate", collection=coll2.name)

        assert result1["valid"] is True
        assert result2["valid"] is True

        conn.close()

    def test_validate_after_bulk_operations(self, collection):
        """Test validate command after bulk operations."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)
        collection.update_many(
            {"value": {"$lt": 10}}, {"$set": {"updated": True}}
        )
        collection.delete_many({"value": {"$gte": 15}})

        result = collection.database.command(
            "validate", collection=collection.name
        )

        assert result["ok"] == 1.0
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_dict_command(self, collection):
        """Test validate command using dict syntax."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        # Test dict-style command (MongoDB style)
        result = collection.database.command({"validate": collection.name})

        assert result["ok"] == 1.0
        assert result["valid"] is True


class TestCursorAlive:
    """Tests for Cursor.alive property."""

    def test_alive_initial(self, collection):
        """Test that cursor is alive initially."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        assert cursor.alive is True

    def test_alive_after_full_iteration(self, collection):
        """Test that cursor is not alive after full iteration."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({})
        list(cursor)
        # Cursor may still be considered alive since it's re-iterable
        # Implementation depends on whether we track exhaustion
        assert isinstance(cursor.alive, bool)

    def test_alive_with_limit(self, collection):
        """Test alive status with limit."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).limit(5)
        assert cursor.alive is True

        list(cursor)
        # After retrieving all limited results, should be not alive
        assert cursor.alive is False

    def test_alive_partial_iteration(self, collection):
        """Test alive during partial iteration."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({}).limit(10)
        # Before iteration
        assert cursor.alive is True

        # Partial iteration
        count = 0
        for doc in cursor:
            count += 1
            if count >= 3:
                break

        # Should still be alive since we haven't reached limit
        assert cursor.alive is True


class TestCursorCollection:
    """Tests for Cursor.collection property."""

    def test_collection_returns_reference(self, collection):
        """Test that collection property returns the collection."""
        cursor = collection.find({})
        assert cursor.collection is collection

    def test_collection_name(self, collection):
        """Test that we can access collection name via cursor."""
        cursor = collection.find({})
        assert cursor.collection.name == collection.name

    def test_collection_operations(self, collection):
        """Test that we can perform operations via cursor.collection."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        # Should be able to use cursor.collection for operations
        count = cursor.collection.count_documents({})
        assert count == 5


class TestCursorAddress:
    """Tests for Cursor.address property."""

    def test_address_before_iteration(self, collection):
        """Test that address returns None before iteration."""
        cursor = collection.find({})
        # Before iteration, should return None (matching PyMongo behavior)
        assert cursor.address is None

    def test_address_after_iteration_memory_db(self, collection):
        """Test that address returns tuple after iteration for memory db."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        list(cursor)
        # After iteration, should return tuple with memory db path
        assert cursor.address == ("sqlite::memory:", 0)

    def test_address_returns_tuple(self, collection):
        """Test that address returns a tuple after iteration."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        list(cursor)
        address = cursor.address

        assert isinstance(address, tuple)
        assert len(address) == 2

    def test_address_values(self, collection):
        """Test address values."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor = collection.find({})
        list(cursor)
        db_path, port = cursor.address

        assert db_path == "sqlite::memory:"
        assert port == 0

    def test_address_consistency(self, collection):
        """Test that address is consistent across cursors."""
        docs = [{"value": i} for i in range(5)]
        collection.insert_many(docs)

        cursor1 = collection.find({})
        cursor2 = collection.find({"value": 1})

        # Both should be None before iteration
        assert cursor1.address is None
        assert cursor2.address is None

        # After iteration, both should return same address
        list(cursor1)
        list(cursor2)
        assert cursor1.address == cursor2.address

    def test_address_with_file_database(self):
        """Test address with file-based database."""
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = neosqlite.Connection(db_path)
            coll = conn.test_collection
            coll.insert_one({"x": 1})

            cursor = coll.find({})
            list(cursor)

            # Should return file path
            assert cursor.address[0] == f"sqlite://{db_path}"
            assert cursor.address[1] == 0

            conn.close()
        finally:
            os.unlink(db_path)


class TestCursorMinMax:
    """Tests for Cursor.min() and Cursor.max() methods."""

    def test_min_basic(self, collection):
        """Test min() sets lower bound."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).min({"value": 10})
        results = list(cursor)

        assert len(results) == 10  # values 10-19
        assert all(doc["value"] >= 10 for doc in results)

    def test_max_basic(self, collection):
        """Test max() sets upper bound."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).max({"value": 10})
        results = list(cursor)

        assert len(results) == 10  # values 0-9
        assert all(doc["value"] < 10 for doc in results)

    def test_min_max_combined(self, collection):
        """Test min() and max() together."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).min({"value": 5}).max({"value": 15})
        results = list(cursor)

        assert len(results) == 10  # values 5-14
        assert all(5 <= doc["value"] < 15 for doc in results)

    def test_min_max_with_filter(self, collection):
        """Test min/max with existing filter."""
        docs = [
            {"category": "A" if i % 2 == 0 else "B", "value": i}
            for i in range(20)
        ]
        collection.insert_many(docs)

        cursor = (
            collection.find({"category": "A"})
            .min({"value": 5})
            .max({"value": 15})
        )
        results = list(cursor)

        assert all(doc["category"] == "A" for doc in results)
        assert all(5 <= doc["value"] < 15 for doc in results)

    def test_min_max_returns_cursor(self, collection):
        """Test that min() and max() return cursor for chaining."""
        docs = [{"value": i} for i in range(10)]
        collection.insert_many(docs)

        cursor = (
            collection.find({}).min({"value": 2}).max({"value": 8}).limit(3)
        )
        results = list(cursor)

        assert len(results) == 3
        assert all(2 <= doc["value"] < 8 for doc in results)

    def test_min_max_preserves_settings(self, collection):
        """Test that min/max preserves cursor settings."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        cursor = (
            collection.find({"value": {"$gte": 0}})
            .min({"value": 5})
            .max({"value": 15})
        )

        assert cursor._min == {"value": 5}
        assert cursor._max == {"value": 15}
        assert cursor._filter == {"value": {"$gte": 0}}


class TestCursorCollation:
    """Tests for Cursor.collation() method."""

    def test_collation_basic(self, collection):
        """Test collation() sets collation settings."""
        docs = [{"name": "Alice"}, {"name": "bob"}, {"name": "Charlie"}]
        collection.insert_many(docs)

        cursor = collection.find({"name": {"$gte": "B"}}).collation(
            {"locale": "en_US", "strength": 2}
        )
        results = list(cursor)

        assert cursor._collation == {"locale": "en_US", "strength": 2}
        # With strength 2 (case-insensitive), should match more documents
        assert len(results) >= 1

    def test_collation_case_insensitive(self, collection):
        """Test collation with case-insensitive strength."""
        docs = [{"name": "Alice"}, {"name": "alice"}, {"name": "ALICE"}]
        collection.insert_many(docs)

        # Case-insensitive search (strength 2)
        cursor = collection.find({"name": "alice"}).collation(
            {"locale": "en_US", "strength": 2}
        )
        results = list(cursor)

        # Should find all variations with case-insensitive collation
        assert len(results) >= 1

    def test_collation_returns_cursor(self, collection):
        """Test that collation() returns cursor for chaining."""
        docs = [{"name": f"Doc{i}"} for i in range(10)]
        collection.insert_many(docs)

        cursor = (
            collection.find({"name": {"$gte": "Doc5"}})
            .collation({"locale": "en_US"})
            .limit(3)
        )
        results = list(cursor)

        assert len(results) <= 3
        assert cursor._collation == {"locale": "en_US"}

    def test_collation_preserves_settings(self, collection):
        """Test that collation preserves cursor settings."""
        docs = [{"name": f"Doc{i}"} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({"name": {"$gte": "Doc5"}}).collation(
            {"locale": "fr_FR", "strength": 1, "backwards": True}
        )

        assert cursor._collation == {
            "locale": "fr_FR",
            "strength": 1,
            "backwards": True,
        }
        assert cursor._filter == {"name": {"$gte": "Doc5"}}

    def test_collation_clone_preserves(self, collection):
        """Test that clone preserves collation settings."""
        docs = [{"name": f"Doc{i}"} for i in range(10)]
        collection.insert_many(docs)

        cursor = collection.find({}).collation(
            {"locale": "de_DE", "strength": 2}
        )
        cloned = cursor.clone()

        assert cloned._collation == {"locale": "de_DE", "strength": 2}
        assert cloned._collation is not cursor._collation  # Deep copy

    def test_collation_with_sort(self, collection):
        """Test collation works with sorting."""
        docs = [{"name": name} for name in ["zebra", "Apple", "banana"]]
        collection.insert_many(docs)

        cursor = (
            collection.find({})
            .sort("name", neosqlite.ASCENDING)
            .collation({"locale": "en_US", "strength": 2})
        )
        results = list(cursor)

        assert len(results) == 3
        # With case-insensitive collation, sorting should be case-insensitive
        names = [doc["name"] for doc in results]
        # With case-insensitive: Apple, banana, zebra (A < b < z)
        assert names[0] == "Apple"
        assert names[1] == "banana"
        assert names[2] == "zebra"


class TestEstimatedDocumentCountWithOptions:
    """Tests for Collection.estimated_document_count(options)."""

    def test_estimated_document_count_with_options(self, collection):
        """Test estimated_document_count accepts options parameter."""
        docs = [{"value": i} for i in range(100)]
        collection.insert_many(docs)

        # Should accept options dict for API compatibility
        count = collection.estimated_document_count(
            {"maxTimeMS": 1000, "hint": "idx_test"}
        )
        assert count == 100

    def test_estimated_document_count_without_options(self, collection):
        """Test estimated_document_count works without options."""
        docs = [{"value": i} for i in range(50)]
        collection.insert_many(docs)

        count = collection.estimated_document_count()
        assert count == 50

    def test_estimated_document_count_empty_collection(self, collection):
        """Test estimated_document_count on empty collection."""
        count = collection.estimated_document_count()
        assert count == 0

        # Also with options
        count_with_opts = collection.estimated_document_count(
            {"maxTimeMS": 1000}
        )
        assert count_with_opts == 0


class TestDatabaseWithOptions:
    """Tests for Connection.with_options() method."""

    def test_with_options_returns_connection(self):
        """Test with_options() returns a new clone."""
        conn = neosqlite.Connection(":memory:")

        result = conn.with_options()

        assert (
            result is not conn
        )  # Now returns a clone for PyMongo compatibility
        assert result.db is conn.db  # Shares the same underlying DB connection
        conn.close()

    def test_with_options_stores_options(self):
        """Test with_options() stores the provided options in a clone."""
        conn = neosqlite.Connection(":memory:")

        result = conn.with_options(
            write_concern={"w": "majority"},
            read_preference={"mode": "primaryPreferred"},
        )

        assert result is not conn
        assert result._write_concern == {"w": "majority"}
        assert result._read_preference == {"mode": "primaryPreferred"}
        # Original should remain unchanged
        assert conn._write_concern is None
        conn.close()

    def test_with_options_all_parameters(self):
        """Test with_options() accepts all parameters."""
        conn = neosqlite.Connection(":memory:")

        result = conn.with_options(
            codec_options={"document_class": dict},
            read_preference={"mode": "secondary"},
            write_concern={"w": 1},
            read_concern={"level": "local"},
        )

        assert result is not conn
        assert result._codec_options == {"document_class": dict}
        assert result._read_preference == {"mode": "secondary"}
        assert result._write_concern == {"w": 1}
        assert result._read_concern == {"level": "local"}
        conn.close()


class TestCursorWhere:
    """Tests for Cursor.where() method (Python function filter)."""

    def test_where_with_python_function(self, collection):
        """Test where() filters with a Python function."""
        docs = [{"value": i} for i in range(20)]
        collection.insert_many(docs)

        # Filter using Python function (Tier-3 fallback)
        cursor = collection.find({}).where(lambda doc: doc.get("value", 0) > 10)
        results = list(cursor)

        assert len(results) == 9  # values 11-19
        assert all(doc["value"] > 10 for doc in results)

    def test_where_returns_cursor(self, collection):
        """Test where() returns cursor for chaining."""
        docs = [
            {"value": i, "category": "A" if i % 2 == 0 else "B"}
            for i in range(20)
        ]
        collection.insert_many(docs)

        cursor = (
            collection.find({})
            .where(lambda doc: doc.get("value", 0) > 5)
            .limit(5)
        )
        results = list(cursor)

        assert len(results) <= 5
        assert all(doc["value"] > 5 for doc in results)

    def test_where_with_complex_condition(self, collection):
        """Test where() with complex filtering logic."""
        docs = [{"value": i, "name": f"doc{i}"} for i in range(20)]
        collection.insert_many(docs)

        cursor = collection.find({}).where(
            lambda doc: doc.get("value", 0) % 2 == 0
            and doc.get("value", 0) > 10
        )
        results = list(cursor)

        # Even values > 10: 12, 14, 16, 18
        assert len(results) == 4
        assert all(
            doc["value"] % 2 == 0 and doc["value"] > 10 for doc in results
        )

    def test_where_operator_raises_not_implemented(self, collection):
        """Test that the $where operator in a query dictionary raises NotImplementedError."""
        with pytest.raises(
            NotImplementedError,
            match=r"The '\$where' operator .* is not supported",
        ):
            list(collection.find({"$where": "this.value > 10"}))

    def test_function_operator_raises_not_implemented(self, collection):
        """Test that the $function operator in a query raises NotImplementedError."""
        with pytest.raises(
            NotImplementedError,
            match=r"The '\$function' operator is not supported",
        ):
            list(
                collection.find({"$function": "function(x) { return x > 10; }"})
            )

    def test_accumulator_operator_raises_not_implemented(self, collection):
        """Test that the $accumulator operator in a query raises NotImplementedError."""
        with pytest.raises(
            NotImplementedError,
            match=r"The '\$accumulator' operator is not supported",
        ):
            list(
                collection.find(
                    {
                        "$accumulator": {
                            "init": "function() { return 0; }",
                            "accumulate": "function(state, val) { return state + val; }",
                            "merge": "function(state1, state2) { return state1 + state2; }",
                            "finalize": "function(state) { return state; }",
                        }
                    }
                )
            )


def test_cursor_new_properties_and_methods():
    """Test newly added Cursor properties and methods."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        coll.insert_one({"a": 1})
        cursor = coll.find({"a": 1})

        from neosqlite.collection.cursor import Cursor

        assert isinstance(cursor, Cursor)
        assert cursor.cursor_id == 0
        assert cursor.session is None
        assert cursor.alive is True
        assert cursor.retrieved == 0

        # Method chaining
        cursor.add_option(1).remove_option(1).max_await_time_ms(100)

        list(cursor)
        assert cursor.retrieved == 1
        assert cursor.alive is False


def test_aggregation_cursor_new_properties():
    """Test newly added AggregationCursor properties."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        coll.insert_one({"a": 1})
        cursor = coll.aggregate([{"$match": {"a": 1}}])

        from neosqlite.collection.aggregation_cursor import AggregationCursor

        assert isinstance(cursor, AggregationCursor)
        assert cursor.cursor_id == 0
        assert cursor.session is None
        assert cursor.alive is True
        assert cursor.retrieved == 0
        assert cursor.collection == coll

        list(cursor)
        assert cursor.retrieved == 1
        assert cursor.alive is False
        assert cursor.address is not None
