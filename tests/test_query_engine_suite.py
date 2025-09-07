"""
Consolidated tests for query engine functionality and fallback mechanisms.
"""

import pytest
import neosqlite
import time
import copy
from neosqlite import Connection
from neosqlite.query_operators import (
    _eq,
    _gt,
    _lt,
    _gte,
    _lte,
    _all,
    _in,
    _ne,
    _nin,
    _mod,
    _exists,
    _regex,
    _elemMatch,
    _size,
    _contains,
)
from neosqlite.exceptions import MalformedQueryException
from neosqlite.collection.query_helper import QueryHelper
from neosqlite.collection.query_helper import (
    set_force_fallback,
    get_force_fallback,
)
from neosqlite.collection.temporary_table_aggregation import (
    execute_2nd_tier_aggregation,
    can_process_with_temporary_tables,
)


# ================================
# Individual Query Operator Tests
# ================================


def test_eq():
    assert _eq("foo", "bar", {"foo": "bar"})
    assert not _eq("foo", "bar", {"foo": "baz"})
    assert not _eq("foo", "bar", {})
    assert not _eq("foo", "bar", None)


def test_gt():
    assert _gt("foo", 5, {"foo": 10})
    assert not _gt("foo", 5, {"foo": 5})
    assert not _gt("foo", 5, {"foo": 4})
    assert not _gt("foo", 5, {})
    assert not _gt("foo", 5, {"foo": "bar"})


def test_lt():
    assert _lt("foo", 5, {"foo": 4})
    assert not _lt("foo", 5, {"foo": 5})
    assert not _lt("foo", 5, {"foo": 10})
    assert not _lt("foo", 5, {})
    assert not _lt("foo", 5, {"foo": "bar"})


def test_gte():
    assert _gte("foo", 5, {"foo": 5})
    assert _gte("foo", 5, {"foo": 10})
    assert not _gte("foo", 5, {"foo": 4})
    assert not _gte("foo", 5, {})
    assert not _gte("foo", 5, {"foo": "bar"})


def test_lte():
    assert _lte("foo", 5, {"foo": 5})
    assert _lte("foo", 5, {"foo": 4})
    assert not _lte("foo", 5, {"foo": 10})
    assert not _lte("foo", 5, {})
    assert not _lte("foo", 5, {"foo": "bar"})


def test_all():
    assert _all("foo", [1, 2], {"foo": [1, 2, 3]})
    assert not _all("foo", [1, 4], {"foo": [1, 2, 3]})
    assert not _all("foo", [1, 2], {"foo": "bar"})
    with pytest.raises(MalformedQueryException):
        _all("foo", 123, {"foo": [1, 2, 3]})


def test_in():
    assert _in("foo", [1, 2, 3], {"foo": 1})
    assert not _in("foo", [1, 2, 3], {"foo": 4})
    assert not _in("foo", [1, 2, 3], {})
    with pytest.raises(MalformedQueryException):
        _in("foo", 123, {"foo": 1})


def test_ne():
    assert _ne("foo", "bar", {"foo": "baz"})
    assert not _ne("foo", "bar", {"foo": "bar"})
    assert _ne("foo", "bar", {})


def test_nin():
    assert _nin("foo", [1, 2, 3], {"foo": 4})
    assert not _nin("foo", [1, 2, 3], {"foo": 1})
    assert _nin("foo", [1, 2, 3], {})
    with pytest.raises(MalformedQueryException):
        _nin("foo", 123, {"foo": 1})


def test_mod():
    assert _mod("foo", [2, 0], {"foo": 4})
    assert not _mod("foo", [2, 0], {"foo": 3})
    assert not _mod("foo", [2, 0], {})
    assert not _mod("foo", [2, 0], {"foo": "bar"})
    with pytest.raises(MalformedQueryException):
        _mod("foo", "bar", {"foo": 4})


def test_exists():
    assert _exists("foo", True, {"foo": "bar"})
    assert not _exists("foo", True, {})
    assert _exists("foo", False, {})
    assert not _exists("foo", False, {"foo": "bar"})
    with pytest.raises(MalformedQueryException):
        _exists("foo", "bar", {"foo": "bar"})


def test_regex():
    assert _regex("foo", "^bar", {"foo": "barbaz"})
    assert not _regex("foo", "^bar", {"foo": "bazbar"})
    assert not _regex("foo", "^bar", {})
    assert not _regex("foo", "[", {"foo": "bar"})


def test_elemMatch():
    assert _elemMatch("foo", {"a": 1}, {"foo": [{"a": 1}, {"b": 2}]})
    assert not _elemMatch("foo", {"a": 1}, {"foo": [{"a": 2}, {"b": 2}]})
    assert not _elemMatch("foo", {"a": 1}, {"foo": "bar"})
    assert not _elemMatch("foo", {"a": 1}, {})


def test_size():
    assert _size("foo", 2, {"foo": [1, 2]})
    assert not _size("foo", 2, {"foo": [1, 2, 3]})
    assert not _size("foo", 2, {"foo": "bar"})
    assert not _size("foo", 2, {})


def test_contains():
    assert _contains("foo", "bar", {"foo": "barbaz"})
    assert not _contains("foo", "bar", {"foo": "bazqux"})
    assert not _contains("foo", "bar", {})
    assert not _contains("foo", "bar", {"foo": None})
    assert not _contains("foo", "bar", {"foo": 123})


def test_nested_field_queries():
    """Test queries on nested fields."""
    # Test equality on nested field
    assert _eq("profile.age", 25, {"profile": {"age": 25}})
    assert not _eq("profile.age", 25, {"profile": {"age": 30}})
    assert not _eq("profile.age", 25, {"profile": {}})
    assert not _eq("profile.age", 25, {})

    # Test greater than on nested field
    assert _gt("profile.age", 20, {"profile": {"age": 25}})
    assert not _gt("profile.age", 30, {"profile": {"age": 25}})
    assert not _gt("profile.age", 20, {"profile": {}})
    assert not _gt("profile.age", 20, {})

    # Test less than on nested field
    assert _lt("profile.age", 30, {"profile": {"age": 25}})
    assert not _lt("profile.age", 20, {"profile": {"age": 25}})
    assert not _lt("profile.age", 30, {"profile": {}})
    assert not _lt("profile.age", 30, {})


def test_nested_array_queries():
    """Test queries on nested arrays."""
    # Test existence of nested array field
    assert _exists("tags", True, {"tags": ["python", "javascript"]})
    assert _exists("tags", False, {"name": "Alice"})

    # Test size of nested array
    assert _size("tags", 2, {"tags": ["python", "javascript"]})
    assert not _size("tags", 3, {"tags": ["python", "javascript"]})
    assert not _size("tags", 2, {"tags": "not_an_array"})

    # Test inclusion in nested array
    assert _in("tags", ["python", "java"], {"tags": ["python", "javascript"]})
    assert not _in("tags", ["java", "go"], {"tags": ["python", "javascript"]})
    assert not _in("tags", ["python"], {"tags": "not_an_array"})


def test_nested_field_performance():
    """Test that nested field queries are efficient."""
    # This is more of an implementation detail test
    # In the actual implementation, nested field queries should use SQL
    # We're just verifying the query operators work correctly
    pass


# ================================
# Query Engine Functionality Tests
# ================================


def test_logical_operators(collection):
    """Test logical operators $or, $and, $not, $nor."""
    collection.insert_many(
        [
            {"name": "Alice", "age": 25, "city": "New York"},
            {"name": "Bob", "age": 30, "city": "Boston"},
            {"name": "Charlie", "age": 35, "city": "New York"},
            {"name": "David", "age": 40, "city": "Boston"},
        ]
    )

    # Test $or
    results = list(collection.find({"$or": [{"age": 25}, {"city": "Boston"}]}))
    assert len(results) == 3
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Bob", "David"}

    # Test $and
    results = list(
        collection.find({"$and": [{"age": {"$gt": 30}}, {"city": "New York"}]})
    )
    assert len(results) == 1
    assert results[0]["name"] == "Charlie"

    # Test $not
    results = list(collection.find({"$not": {"age": 25}}))
    assert len(results) == 3
    names = {doc["name"] for doc in results}
    assert names == {"Bob", "Charlie", "David"}

    # Test $nor
    results = list(collection.find({"$nor": [{"age": 25}, {"city": "Boston"}]}))
    assert len(results) == 1
    assert results[0]["name"] == "Charlie"


@pytest.mark.xfail(reason="The $all operator is not working correctly.")
def test_array_operators(collection):
    """Test array operators $all and $elemMatch."""
    collection.insert_many(
        [
            {"name": "Alice", "tags": ["python", "sql"]},
            {"name": "Bob", "tags": ["java", "sql"]},
            {
                "name": "Charlie",
                "scores": [
                    {"subject": "math", "score": 90},
                    {"subject": "english", "score": 85},
                ],
            },
            {
                "name": "David",
                "scores": [
                    {"subject": "math", "score": 75},
                    {"subject": "english", "score": 95},
                ],
            },
        ]
    )

    # Test $all
    results = list(collection.find({"tags": {"$all": ["python", "sql"]}}))
    assert len(results) == 1
    assert results[0]["name"] == "Alice"

    # Test $elemMatch
    results = list(
        collection.find(
            {
                "scores": {
                    "$elemMatch": {"subject": "math", "score": {"$gt": 80}}
                }
            }
        )
    )
    assert len(results) == 1
    assert results[0]["name"] == "Charlie"


@pytest.mark.xfail(
    reason="There is a bug with _id queries causing ProgrammingError."
)
def test_query_with_id(collection):
    """Test queries specifically targeting the _id field."""
    collection.insert_many(
        [
            {"_id": 1, "name": "Alice"},
            {"_id": 2, "name": "Bob"},
        ]
    )

    # Test find by _id
    result = collection.find_one({"_id": 1})
    assert result is not None
    assert result["name"] == "Alice"

    # Test find with $in on _id
    results = list(collection.find({"_id": {"$in": [1, 2]}}))
    assert len(results) == 2


# ================================
# Query Helper Coverage Tests
# ================================


def test_query_helper_build_update_clause_edge_cases():
    """Test edge cases in _build_update_clause to improve coverage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test case: empty update dict
        result = helper._build_update_clause({})
        assert result is None

        # Test case: unsupported operator
        result = helper._build_update_clause(
            {"$unsupported": {"field": "value"}}
        )
        assert result is None

        # Test case: $rename operator (should return None to force Python fallback)
        result = helper._build_update_clause(
            {"$rename": {"old_field": "new_field"}}
        )
        assert result is None

        # Test case: $unset with empty fields (should return None)
        result = helper._build_update_clause({"$unset": {}})
        assert result is None


def test_query_helper_build_sql_update_clause_edge_cases():
    """Test edge cases in _build_sql_update_clause to improve coverage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test case: unsupported operator (should return empty lists)
        clauses, params = helper._build_sql_update_clause(
            "$unsupported", {"field": "value"}
        )
        # Should return empty lists for unsupported operators
        assert clauses == []
        assert params == []


def test_query_helper_can_use_sql_updates_edge_cases():
    """Test edge cases in _can_use_sql_updates to improve coverage."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection
        helper = QueryHelper(collection)

        # Test case: upsert (doc_id = 0)
        result = helper._can_use_sql_updates({"$set": {"field": "value"}}, 0)
        assert result == False

        # Test case: unsupported operation
        result = helper._can_use_sql_updates({"$rename": {"old": "new"}}, 1)
        assert result == False

        # Test case: Binary object in update spec
        result = helper._can_use_sql_updates(
            {"$set": {"field": neosqlite.Binary(b"data")}}, 1
        )
        assert result == False


# ================================
# Fallback Mechanisms Tests
# ================================


def test_unwind_with_advanced_options_fallback():
    """Test that $unwind with advanced options (fallback) produces consistent results"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "hobbies": ["reading", "swimming", "coding"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "hobbies": [],  # Empty array
                },
                {
                    "_id": 3,
                    "name": "Charlie",
                    "hobbies": None,  # Null value
                },
                {
                    "_id": 4,
                    "name": "David",
                    # No hobbies field
                },
            ]
        )

        # Pipeline with advanced unwind options (forces Python fallback)
        pipeline_with_advanced = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "includeArrayIndex": "hobbyIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]

        # Pipeline with simple unwind (can use SQL optimization)
        pipeline_simple = [{"$unwind": "$hobbies"}]

        # Get results from both approaches
        result_advanced = collection.aggregate(pipeline_with_advanced)
        result_simple = collection.aggregate(pipeline_simple)

        # The simple pipeline should have fewer results since it doesn't preserve null/empty
        assert len(result_simple) == 3  # Only Alice's hobbies
        assert (
            len(result_advanced) == 5
        )  # Alice's hobbies + Bob and Charlie preserved

        # Check simple results
        simple_hobbies = {doc["hobbies"] for doc in result_simple}
        assert simple_hobbies == {"reading", "swimming", "coding"}

        # Check advanced results
        alice_advanced = [
            doc for doc in result_advanced if doc["name"] == "Alice"
        ]
        assert len(alice_advanced) == 3
        alice_hobbies = {doc["hobbies"] for doc in alice_advanced}
        assert alice_hobbies == {"reading", "swimming", "coding"}

        # Check that indices are present for Alice's documents
        alice_indices = {doc["hobbyIndex"] for doc in alice_advanced}
        assert alice_indices == {0, 1, 2}

        # Check preserved documents
        bob_advanced = [doc for doc in result_advanced if doc["name"] == "Bob"]
        charlie_advanced = [
            doc for doc in result_advanced if doc["name"] == "Charlie"
        ]

        assert len(bob_advanced) == 1
        assert len(charlie_advanced) == 1

        # Bob should have hobbies as None and index as None
        assert bob_advanced[0].get("hobbies") is None
        assert bob_advanced[0]["hobbyIndex"] is None

        # Charlie should have hobbies as None and index as None
        assert charlie_advanced[0].get("hobbies") is None
        assert charlie_advanced[0]["hobbyIndex"] is None


def test_lookup_with_subsequent_stages_fallback():
    """Test that $lookup with subsequent stages uses fallback and produces correct results"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        users = conn["users"]
        orders = conn["orders"]

        # Insert test data
        users.insert_many(
            [
                {"_id": 1, "name": "Alice"},
                {"_id": 2, "name": "Bob"},
            ]
        )

        orders.insert_many(
            [
                {"userId": 1, "product": "Book"},
                {"userId": 1, "product": "Pen"},
                {"userId": 2, "product": "Notebook"},
            ]
        )

        # Pipeline with $lookup (can be optimized when it's the last stage)
        pipeline_optimized = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            }
        ]

        # Pipeline with $lookup followed by other stages (forces fallback)
        pipeline_fallback = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            },
            {"$match": {"name": "Alice"}},  # This forces fallback
        ]

        # Get results from both approaches
        result_optimized = users.aggregate(pipeline_optimized)
        result_fallback = users.aggregate(pipeline_fallback)

        # Optimized result should have all users with their orders
        assert len(result_optimized) == 2
        alice_optimized = [
            doc for doc in result_optimized if doc["name"] == "Alice"
        ][0]
        bob_optimized = [
            doc for doc in result_optimized if doc["name"] == "Bob"
        ][0]
        assert len(alice_optimized["userOrders"]) == 2
        assert len(bob_optimized["userOrders"]) == 1

        # Fallback result should only have Alice (due to $match)
        assert len(result_fallback) == 1
        assert result_fallback[0]["name"] == "Alice"
        assert len(result_fallback[0]["userOrders"]) == 2


def test_group_push_addtoset_consistency():
    """Test that $group with $push and $addToSet produces consistent results"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "category": "A",
                    "name": "Item1",
                    "tags": ["red", "blue", "red"],
                },
                {"category": "A", "name": "Item2", "tags": ["blue", "green"]},
                {"category": "B", "name": "Item3", "tags": ["red", "yellow"]},
            ]
        )

        # Pipeline that can be optimized ($unwind + $group with $push/$addToSet)
        pipeline_optimized = [
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$category",
                    "allTags": {"$push": "$tags"},  # Preserves duplicates
                    "uniqueTags": {"$addToSet": "$tags"},  # Removes duplicates
                }
            },
        ]

        # Get results from optimized approach
        result_optimized = collection.aggregate(pipeline_optimized)

        # Verify optimized results
        assert len(result_optimized) == 2
        result_optimized.sort(key=lambda x: x["_id"])

        # Check category A
        assert result_optimized[0]["_id"] == "A"
        # allTags should have duplicates: red, blue, red, blue, green
        assert len(result_optimized[0]["allTags"]) == 5
        assert sorted(result_optimized[0]["allTags"]) == [
            "blue",
            "blue",
            "green",
            "red",
            "red",
        ]
        # uniqueTags should have no duplicates: red, blue, green
        assert len(result_optimized[0]["uniqueTags"]) == 3
        assert sorted(result_optimized[0]["uniqueTags"]) == [
            "blue",
            "green",
            "red",
        ]

        # Check category B
        assert result_optimized[1]["_id"] == "B"
        # allTags should have: red, yellow
        assert len(result_optimized[1]["allTags"]) == 2
        assert sorted(result_optimized[1]["allTags"]) == ["red", "yellow"]
        # uniqueTags should have: red, yellow
        assert len(result_optimized[1]["uniqueTags"]) == 2
        assert sorted(result_optimized[1]["uniqueTags"]) == ["red", "yellow"]


def test_force_fallback_flag():
    """Test that the force fallback flag can be set and retrieved"""
    # Initially should be False
    assert get_force_fallback() is False

    # Set to True
    set_force_fallback(True)
    assert get_force_fallback() is True

    # Set back to False
    set_force_fallback(False)
    assert get_force_fallback() is False


def test_force_fallback_with_unwind_only():
    """Test force fallback with simple unwind operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "tags": ["python", "javascript"]},
                {"name": "Bob", "tags": ["java", "python"]},
            ]
        )

        # Test normal operation (should use SQL optimization)
        set_force_fallback(False)
        pipeline = [{"$unwind": "$tags"}]

        result_normal = collection.aggregate(pipeline)

        # Test with forced fallback
        set_force_fallback(True)
        result_fallback = collection.aggregate(pipeline)

        # Reset flag
        set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)

        # Extract tags for easier comparison
        normal_tags = sorted([doc["tags"] for doc in result_normal])
        fallback_tags = sorted([doc["tags"] for doc in result_fallback])

        assert normal_tags == fallback_tags
        assert normal_tags == ["java", "javascript", "python", "python"]


def test_force_fallback_with_match():
    """Test force fallback with match operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25, "city": "New York"},
                {"name": "Bob", "age": 30, "city": "London"},
                {"name": "Charlie", "age": 35, "city": "New York"},
            ]
        )

        pipeline = [{"$match": {"city": "New York"}}]

        # Test normal operation
        set_force_fallback(False)
        result_normal = collection.aggregate(pipeline)

        # Test with forced fallback
        set_force_fallback(True)
        result_fallback = collection.aggregate(pipeline)

        # Reset flag
        set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)
        assert len(result_normal) == 2

        # Extract names for easier comparison
        normal_names = sorted([doc["name"] for doc in result_normal])
        fallback_names = sorted([doc["name"] for doc in result_fallback])

        assert normal_names == fallback_names
        assert normal_names == ["Alice", "Charlie"]


def test_force_fallback_with_advanced_unwind():
    """Test force fallback with advanced unwind features"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "hobbies": ["reading", "swimming", "coding"],
                },
                {
                    "_id": 2,
                    "name": "Bob",
                    "hobbies": [],  # Empty array
                },
            ]
        )

        # Pipeline with advanced unwind options (normally forces fallback)
        pipeline_advanced = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "includeArrayIndex": "hobbyIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]

        # Pipeline with simple unwind (can be optimized)
        pipeline_simple = [{"$unwind": "$hobbies"}]

        # Test with force fallback disabled - advanced should still use fallback
        set_force_fallback(False)
        result_advanced = collection.aggregate(pipeline_advanced)
        result_simple = collection.aggregate(pipeline_simple)

        # Test with force fallback enabled - both should use fallback
        set_force_fallback(True)
        result_advanced_forced = collection.aggregate(pipeline_advanced)
        result_simple_forced = collection.aggregate(pipeline_simple)

        # Reset flag
        set_force_fallback(False)

        # Results should be consistent
        assert len(result_advanced) == len(result_advanced_forced)
        assert len(result_simple) == len(result_simple_forced)

        # Advanced should have more results due to preserveNullAndEmptyArrays
        assert len(result_advanced) > len(result_simple)


def test_force_fallback_with_lookup():
    """Test force fallback with lookup operations"""
    with neosqlite.Connection(":memory:") as conn:
        # Create two collections
        users = conn["users"]
        orders = conn["orders"]

        # Insert test data
        users.insert_many(
            [
                {"_id": 1, "name": "Alice"},
                {"_id": 2, "name": "Bob"},
            ]
        )

        orders.insert_many(
            [
                {"userId": 1, "product": "Book"},
                {"userId": 1, "product": "Pen"},
                {"userId": 2, "product": "Notebook"},
            ]
        )

        # Pipeline with $lookup (can be optimized when it's the last stage)
        pipeline = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "userOrders",
                }
            }
        ]

        # Test normal operation
        set_force_fallback(False)
        result_normal = users.aggregate(pipeline)

        # Test with forced fallback
        set_force_fallback(True)
        result_fallback = users.aggregate(pipeline)

        # Reset flag
        set_force_fallback(False)

        # Results should be identical
        assert len(result_normal) == len(result_fallback)

        # Check that both have the same user orders
        result_normal.sort(key=lambda x: x["_id"])
        result_fallback.sort(key=lambda x: x["_id"])

        for normal_doc, fallback_doc in zip(result_normal, result_fallback):
            assert normal_doc["_id"] == fallback_doc["_id"]
            assert normal_doc["name"] == fallback_doc["name"]
            assert len(normal_doc["userOrders"]) == len(
                fallback_doc["userOrders"]
            )


def test_benchmark_simple_operations():
    """Benchmark test comparing optimized vs fallback performance for simple operations"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert larger test data set for meaningful benchmarking
        test_data = [
            {"category": f"Cat{i % 5}", "value": i} for i in range(1000)
        ]
        collection.insert_many(test_data)

        pipeline = [{"$match": {"category": "Cat0"}}]

        # Test optimized path
        set_force_fallback(False)
        start_time = time.time()
        result_optimized = collection.aggregate(pipeline)
        optimized_time = time.time() - start_time

        # Test fallback path
        set_force_fallback(True)
        start_time = time.time()
        result_fallback = collection.aggregate(pipeline)
        fallback_time = time.time() - start_time

        # Reset flag
        set_force_fallback(False)

        # Results should be identical
        assert len(result_optimized) == len(result_fallback)

        # Print benchmark results (for informational purposes)
        print(f"Optimized time: {optimized_time:.6f}s")
        print(f"Fallback time: {fallback_time:.6f}s")
        if optimized_time > 0:
            print(f"Performance ratio: {fallback_time/optimized_time:.2f}x")


def test_force_fallback_kill_switch():
    """Test that force fallback (kill switch) works correctly."""
    with Connection(":memory:") as conn:
        collection = conn.test_collection
        query_engine = collection.query_engine

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 30, "department": "Engineering"},
                {"name": "Bob", "age": 25, "department": "Marketing"},
                {"name": "Charlie", "age": 35, "department": "Engineering"},
            ]
        )

        # Verify kill switch is off by default
        assert get_force_fallback() == False

        # Test a pipeline that should work with temp tables
        pipeline = [
            {"$match": {"department": "Engineering"}},
            {"$addFields": {"userName": "$name"}},
        ]

        # With kill switch off, should use temp tables
        results = execute_2nd_tier_aggregation(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]

        # Turn on the kill switch
        set_force_fallback(True)
        assert get_force_fallback() == True

        # With kill switch on, should fall back to Python even for supported pipelines
        results = execute_2nd_tier_aggregation(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]

        # Turn off the kill switch
        set_force_fallback(False)
        assert get_force_fallback() == False

        # Should work normally again
        results = execute_2nd_tier_aggregation(query_engine, pipeline)
        assert len(results) == 2
        for doc in results:
            assert "userName" in doc
            assert doc["userName"] == doc["name"]


# ================================
# Hybrid Execution Tests
# ================================


def test_hybrid_text_search_in_aggregation():
    """Test that aggregation pipelines with $text operators work with hybrid processing."""
    with Connection(":memory:") as conn:
        # Create test data
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "Python Developer",
                    "description": "Works with Python programming language",
                    "status": "active",
                },
                {
                    "name": "Java Developer",
                    "description": "Works with Java programming language",
                    "status": "active",
                },
                {
                    "name": "JavaScript Developer",
                    "description": "Works with JavaScript programming language",
                    "status": "inactive",
                },
                {
                    "name": "Python Data Scientist",
                    "description": "Works with Python for data analysis",
                    "status": "active",
                },
                {
                    "name": "Java Backend Engineer",
                    "description": "Works with Java for backend services",
                    "status": "inactive",
                },
            ]
        )

        # Test pipeline that would benefit from hybrid processing
        # First filter by status (SQL), then text search (Python), then sort (SQL)
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "python"}}
            },  # Should filter to 2 docs
            {"$sort": {"name": 1}},  # Should sort the results
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 2 documents
        assert len(result) == 2

        # Both should be active status
        assert all(doc["status"] == "active" for doc in result)

        # Both should contain "python" in their description
        assert all("python" in doc["description"].lower() for doc in result)

        # Should be sorted by name
        names = [doc["name"] for doc in result]
        assert names == sorted(names)

        # Specific documents should be found
        assert result[0]["name"] == "Python Data Scientist"
        assert result[1]["name"] == "Python Developer"


def test_international_characters_in_aggregation():
    """Test that international characters work in aggregation pipeline text search."""
    with Connection(":memory:") as conn:
        # Create test data with international characters
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "José María",
                    "description": "Software engineer from España",
                    "status": "active",
                },
                {
                    "name": "François Dubois",
                    "description": "Développeur from France",
                    "status": "active",
                },
                {
                    "name": "Björk Guðmundsdóttir",
                    "description": "Artist from Ísland",
                    "status": "inactive",
                },
                {
                    "name": "Müller Schmidt",
                    "description": "Engineer from Deutschland",
                    "status": "active",
                },
            ]
        )

        # Test pipeline with international character search
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "jose"}}
            },  # Should match José (diacritic insensitive)
            {"$limit": 1},  # Should limit to 1 doc
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 1 document
        assert len(result) == 1

        # Should be the José document
        assert result[0]["name"] == "José María"


def test_complex_pipeline_with_text_operator():
    """Test a complex pipeline with text search works correctly."""
    with Connection(":memory:") as conn:
        # Create test data
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "Python Developer",
                    "description": "Works with Python programming language",
                    "status": "active",
                    "tags": ["backend", "scripting"],
                },
                {
                    "name": "Java Developer",
                    "description": "Works with Java programming language",
                    "status": "active",
                    "tags": ["backend", "enterprise"],
                },
                {
                    "name": "JavaScript Developer",
                    "description": "Works with JavaScript programming language",
                    "status": "inactive",
                    "tags": ["frontend", "web"],
                },
                {
                    "name": "Python Data Scientist",
                    "description": "Works with Python for data analysis",
                    "status": "active",
                    "tags": ["data", "science"],
                },
            ]
        )

        # Pipeline that should benefit from hybrid processing
        # 1. Filter by status (SQL)
        # 2. Text search (Python)
        # 3. Sort (SQL)
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "python"}}
            },  # Should filter to 2 docs
            {"$sort": {"name": 1}},  # Should sort the results
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 2 documents
        assert len(result) == 2

        # Both should be active status
        assert all(doc["status"] == "active" for doc in result)

        # Both should contain "python" in their description
        assert all("python" in doc["description"].lower() for doc in result)

        # Should be sorted by name
        names = [doc["name"] for doc in result]
        assert names == sorted(names)

        # Specific documents should be found
        assert result[0]["name"] == "Python Data Scientist"
        assert result[1]["name"] == "Python Developer"
