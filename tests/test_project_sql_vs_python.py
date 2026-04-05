# coding: utf-8
"""
Comparison tests for $project stage: SQL path vs Python path.

These tests verify that the SQL implementation of $project in
TemporaryTableAggregationProcessor produces identical results to the Python
fallback implementation. Each test runs the same pipeline twice:
1. SQL path (kill switch off)
2. Python path (kill switch on)

Results must be identical for both paths.
"""

import json

import neosqlite
from neosqlite.collection.query_helper import set_force_fallback


def _normalize_docs(docs):
    """Normalize documents for comparison: sort by JSON string."""
    return sorted([json.dumps(d, sort_keys=True, default=str) for d in docs])


def _compare_sql_vs_python(collection, pipeline):
    """Run pipeline with both SQL and Python paths, assert identical results."""
    # SQL path
    results_sql = list(collection.aggregate(pipeline))

    # Python path (kill switch on)
    set_force_fallback(True)
    try:
        results_python = list(collection.aggregate(pipeline))
    finally:
        set_force_fallback(False)

    sql_normalized = _normalize_docs(results_sql)
    py_normalized = _normalize_docs(results_python)

    assert sql_normalized == py_normalized, (
        f"SQL and Python paths produce different results.\n"
        f"SQL ({len(results_sql)}): {sql_normalized}\n"
        f"Python ({len(results_python)}): {py_normalized}"
    )
    return results_sql


# ================================
# Simple projection tests
# ================================


def test_project_simple_inclusion(connection):
    """Test simple field inclusion: {field: 1}."""
    coll = connection["test_project_simple"]
    coll.insert_many(
        [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
        ]
    )

    pipeline = [{"$project": {"name": 1, "age": 1}}]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    names = {r["name"] for r in results}
    assert names == {"Alice", "Bob"}
    # _id should be included by default
    assert all("_id" in r for r in results)
    # city should be excluded
    assert all("city" not in r for r in results)


def test_project_simple_exclusion(connection):
    """Test field exclusion: {field: 0}."""
    coll = connection["test_project_exclusion"]
    coll.insert_many(
        [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
        ]
    )

    pipeline = [{"$project": {"city": 0}}]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    # city should be excluded
    assert all("city" not in r for r in results)
    # name, age, _id should be included
    assert all("name" in r and "age" in r and "_id" in r for r in results)


def test_project_id_exclusion(connection):
    """Test _id exclusion: {_id: 0, name: 1}."""
    coll = connection["test_project_id_exclusion"]
    coll.insert_many(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
    )

    pipeline = [{"$project": {"_id": 0, "name": 1}}]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    # _id should be excluded
    assert all("_id" not in r for r in results)
    # name should be included
    assert all("name" in r for r in results)


# ================================
# Field reference tests
# ================================


def test_project_field_reference(connection):
    """Test field references: {alias: "$field.path"}."""
    coll = connection["test_project_field_ref"]
    coll.insert_many(
        [
            {
                "person": {"first": "Alice", "last": "Smith"},
                "address": {"city": "NYC", "zip": "10001"},
            },
            {
                "person": {"first": "Bob", "last": "Jones"},
                "address": {"city": "LA", "zip": "90001"},
            },
        ]
    )

    pipeline = [
        {
            "$project": {
                "firstName": "$person.first",
                "lastName": "$person.last",
                "city": "$address.city",
            }
        }
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    first_names = {r["firstName"] for r in results}
    assert first_names == {"Alice", "Bob"}
    cities = {r["city"] for r in results}
    assert cities == {"NYC", "LA"}


# ================================
# Project with $unwind tests
# ================================


def test_project_after_unwind(connection):
    """Test $project after $unwind (without $text)."""
    coll = connection["test_project_unwind"]
    coll.insert_many(
        [
            {
                "author": "Alice",
                "posts": [
                    {"title": "Post 1", "views": 100},
                    {"title": "Post 2", "views": 200},
                ],
            },
            {
                "author": "Bob",
                "posts": [
                    {"title": "Post 3", "views": 300},
                ],
            },
        ]
    )

    pipeline = [
        {"$unwind": "$posts"},
        {
            "$project": {
                "author": 1,
                "title": "$posts.title",
                "views": "$posts.views",
            }
        },
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 3  # 2 + 1 posts
    titles = {r["title"] for r in results}
    assert titles == {"Post 1", "Post 2", "Post 3"}


def test_project_with_unwind_nested_path(connection):
    """Test $project with nested field references after $unwind."""
    coll = connection["test_project_unwind_nested"]
    coll.insert_many(
        [
            {
                "department": "Engineering",
                "employees": [
                    {"name": "Alice", "skills": ["Python", "SQL"]},
                    {"name": "Bob", "skills": ["JavaScript", "React"]},
                ],
            },
        ]
    )

    pipeline = [
        {"$unwind": "$employees"},
        {
            "$project": {
                "department": 1,
                "empName": "$employees.name",
            }
        },
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    names = {r["empName"] for r in results}
    assert names == {"Alice", "Bob"}


# ================================
# Project with $match tests
# ================================


def test_project_after_match(connection):
    """Test $project after $match."""
    coll = connection["test_project_match"]
    coll.insert_many(
        [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
        ]
    )

    pipeline = [
        {"$match": {"active": True}},
        {"$project": {"name": 1, "age": 1, "_id": 0}},
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    names = {r["name"] for r in results}
    assert names == {"Alice", "Charlie"}


# ================================
# Project with $sort/$skip/$limit tests
# ================================


def test_project_after_sort_limit(connection):
    """Test $project after $sort and $limit."""
    coll = connection["test_project_sort_limit"]
    coll.insert_many(
        [
            {"name": "Alice", "score": 85},
            {"name": "Bob", "score": 92},
            {"name": "Charlie", "score": 78},
            {"name": "Diana", "score": 95},
        ]
    )

    pipeline = [
        {"$sort": {"score": neosqlite.DESCENDING}},
        {"$limit": 2},
        {"$project": {"name": 1, "score": 1, "_id": 0}},
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    assert results[0]["name"] == "Diana"
    assert results[1]["name"] == "Bob"


# ================================
# Multiple $project stages
# ================================


def test_multiple_project_stages(connection):
    """Test pipeline with multiple $project stages."""
    coll = connection["test_multi_project"]
    coll.insert_many(
        [
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"a": 5, "b": 6, "c": 7, "d": 8},
        ]
    )

    pipeline = [
        {"$project": {"a": 1, "b": 1, "c": 1}},  # Keep a, b, c
        {"$project": {"a": 1, "b": 1}},  # Keep a, b only
    ]
    results = _compare_sql_vs_python(coll, pipeline)

    assert len(results) == 2
    assert all("a" in r and "b" in r for r in results)
    assert all("c" not in r and "d" not in r for r in results)
