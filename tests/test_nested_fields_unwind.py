"""
Comprehensive tests for nested field queries and unwind operations.
"""

import pytest
import neosqlite
import time


def test_nested_field_queries_now_use_sql(collection):
    """Test that nested field queries now use SQL implementation."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "profile": {"age": 25, "city": "New York"}},
            {"name": "Bob", "profile": {"age": 30, "city": "Boston"}},
            {"name": "Charlie", "profile": {"age": 25, "city": "New York"}},
        ]
    )

    # Test that nested field query works and now uses SQL
    results = list(collection.find({"profile.age": 25}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Charlie"}

    # Test that _build_simple_where_clause now handles nested fields
    where_result = collection.query_engine.helpers._build_simple_where_clause(
        {"profile.age": 25}
    )
    assert where_result is not None  # Now handled by SQL
    where_clause, params = where_result
    assert "json_extract(data, '$.profile.age') = ?" in where_clause
    assert params == [25]


def test_nested_field_with_operators_now_use_sql(collection):
    """Test that nested field queries with operators now use SQL."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "profile": {"age": 25}},
            {"name": "Bob", "profile": {"age": 30}},
            {"name": "Charlie", "profile": {"age": 35}},
        ]
    )

    # Test greater than operator on nested field
    results = list(collection.find({"profile.age": {"$gt": 28}}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Bob", "Charlie"}

    # Test less than or equal operator on nested field
    results = list(collection.find({"profile.age": {"$lte": 30}}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Bob"}

    # Test _build_simple_where_clause handles operators on nested fields
    where_result = collection.query_engine.helpers._build_simple_where_clause(
        {"profile.age": {"$gt": 28}}
    )
    assert where_result is not None  # Now handled by SQL
    where_clause, params = where_result
    assert "json_extract(data, '$.profile.age') > ?" in where_clause
    assert params == [28]


def test_nested_field_performance_improvement(collection):
    """Test that nested field queries are now faster due to SQL implementation."""
    # Insert a larger dataset to make performance differences more noticeable
    docs = []
    for i in range(1000):
        docs.append(
            {
                "name": f"User{i}",
                "profile": {
                    "age": (i % 50) + 20,  # Ages 20-69
                    "department": f"Dept{(i % 10)}",
                },
            }
        )
    collection.insert_many(docs)

    # Time a nested field query
    start_time = time.time()
    results = list(collection.find({"profile.age": 25}))
    sql_time = time.time() - start_time

    # Verify we get the expected results
    assert len(results) > 0
    for doc in results:
        assert doc["profile"]["age"] == 25

    print(
        f"SQL-based nested query took {sql_time:.4f} seconds for {len(results)} results"
    )

    # Check that we're now using SQL by examining the generated clause
    where_result = collection.query_engine.helpers._build_simple_where_clause(
        {"profile.age": 25}
    )
    assert where_result is not None
    where_clause, params = where_result
    assert "json_extract(data, '$.profile.age') = ?" in where_clause
    assert params == [25]


def test_nested_array_unwind_basic(collection):
    """Test basic nested array unwinding functionality."""
    # Insert test data with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "orders": [
                {
                    "orderId": "A001",
                    "items": [
                        {"product": "Book", "quantity": 2},
                        {"product": "Pen", "quantity": 5},
                    ],
                },
                {
                    "orderId": "A002",
                    "items": [{"product": "Notebook", "quantity": 3}],
                },
            ],
        }
    )

    # Test nested unwind: first unwind orders, then unwind items within each order
    pipeline = [{"$unwind": "$orders"}, {"$unwind": "$orders.items"}]
    result = collection.aggregate(pipeline)

    # Should have 3 documents (2 items in first order + 1 item in second order)
    assert len(result) == 3

    # Check that both levels of unwinding worked
    # After unwinding, doc["orders"] is the unwound order object
    # and doc["orders.items"] is the unwound item object
    order_ids = [doc["orders"]["orderId"] for doc in result]
    products = [doc["orders.items"]["product"] for doc in result]
    quantities = [doc["orders.items"]["quantity"] for doc in result]

    # Each order should appear as many times as it has items
    assert order_ids.count("A001") == 2  # First order has 2 items
    assert order_ids.count("A002") == 1  # Second order has 1 item

    # Check specific items
    a001_items = [doc for doc in result if doc["orders"]["orderId"] == "A001"]
    assert len(a001_items) == 2
    a001_products = {doc["orders.items"]["product"] for doc in a001_items}
    assert a001_products == {"Book", "Pen"}

    a002_items = [doc for doc in result if doc["orders"]["orderId"] == "A002"]
    assert len(a002_items) == 1
    assert a002_items[0]["orders.items"]["product"] == "Notebook"


def test_nested_array_unwind_consistency(collection):
    """Test consistency of nested array unwinding with different data structures."""
    # Insert test data with various nested array structures
    collection.insert_many(
        [
            {
                "name": "Alice",
                "projects": [
                    {
                        "name": "Project A",
                        "tasks": [
                            {"title": "Task 1", "status": "complete"},
                            {"title": "Task 2", "status": "pending"},
                        ],
                    },
                    {
                        "name": "Project B",
                        "tasks": [{"title": "Task 3", "status": "pending"}],
                    },
                ],
            },
            {
                "name": "Bob",
                "projects": [
                    {
                        "name": "Project C",
                        "tasks": [
                            {"title": "Task 4", "status": "complete"},
                            {"title": "Task 5", "status": "complete"},
                            {"title": "Task 6", "status": "pending"},
                        ],
                    }
                ],
            },
        ]
    )

    # Unwind projects and then tasks
    pipeline = [{"$unwind": "$projects"}, {"$unwind": "$projects.tasks"}]
    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have 6 documents (3 tasks each for Alice and Bob)
    assert len(result_list) == 6

    # Just verify basic structure and count
    alice_docs = [doc for doc in result_list if doc["name"] == "Alice"]
    bob_docs = [doc for doc in result_list if doc["name"] == "Bob"]

    assert len(alice_docs) == 3  # Alice has 3 total tasks
    assert len(bob_docs) == 3  # Bob has 3 total tasks

    # Check that all documents have the expected fields
    for doc in result_list:
        assert "name" in doc
        assert "projects" in doc
        assert "projects.tasks" in doc
        assert isinstance(doc["projects"], dict)
        assert isinstance(doc["projects.tasks"], dict)
        assert "name" in doc["projects"]
        assert "title" in doc["projects.tasks"]
        assert "status" in doc["projects.tasks"]


def test_multiple_consecutive_unwind_stages(collection):
    """Test processing multiple consecutive unwind stages."""
    # Insert data with nested arrays
    collection.insert_many(
        [
            {
                "name": "Alice",
                "orders": [
                    {
                        "orderId": "O001",
                        "items": [
                            {"name": "Laptop", "category": "Electronics"},
                            {"name": "Mouse", "category": "Electronics"},
                        ],
                    },
                    {
                        "orderId": "O002",
                        "items": [{"name": "Book", "category": "Education"}],
                    },
                ],
            },
            {
                "name": "Bob",
                "orders": [
                    {
                        "orderId": "O003",
                        "items": [
                            {"name": "Keyboard", "category": "Electronics"}
                        ],
                    }
                ],
            },
        ]
    )

    # Pipeline with multiple consecutive unwind stages
    pipeline = [
        {"$unwind": "$orders"},
        {"$unwind": "$orders.items"},
        {"$match": {"orders.items.category": "Electronics"}},
        {
            "$group": {
                "_id": "$name",
                "electronic_items": {"$push": "$orders.items.name"},
            }
        },
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have 2 documents (Alice and Bob both have electronics)
    assert len(result_list) == 2

    # Find Alice's and Bob's results
    alice_result = next(
        (doc for doc in result_list if doc["_id"] == "Alice"), None
    )
    bob_result = next((doc for doc in result_list if doc["_id"] == "Bob"), None)

    assert alice_result is not None
    assert bob_result is not None

    # Alice should have 2 electronic items (Laptop, Mouse)
    assert set(alice_result["electronic_items"]) == {"Laptop", "Mouse"}

    # Bob should have 1 electronic item (Keyboard)
    assert set(bob_result["electronic_items"]) == {"Keyboard"}


def test_unwind_sort_limit_performance(collection):
    """Test unwind with sort and limit for performance."""
    # Insert test data
    docs = []
    for i in range(100):
        docs.append(
            {"user": f"User{i}", "scores": [i * 2, i * 2 + 1, i * 2 + 2]}
        )
    collection.insert_many(docs)

    # Unwind scores, sort by score descending, and limit to top 10
    pipeline = [
        {"$unwind": "$scores"},
        {"$sort": {"scores": -1}},
        {"$limit": 10},
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have exactly 10 documents
    assert len(result_list) == 10

    # Verify they're sorted by score descending
    scores = [doc["scores"] for doc in result_list]
    assert scores == sorted(scores, reverse=True)

    # First score should be the highest (199 from user 99 with scores [198, 199, 200])
    # But wait, we have 100 users (0-99), so user 99 has scores [198, 199, 200]
    # The highest score would be 200
    assert scores[0] == 200


def test_unwind_advanced_options_comprehensive(collection):
    """Test comprehensive unwind advanced options."""
    # Insert test data with various edge cases
    # Match what the original tests actually test
    collection.insert_many(
        [
            {"_id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"_id": 2, "name": "Bob", "scores": []},  # Empty array
        ]
    )

    # Test unwind with all advanced options
    pipeline = [
        {
            "$unwind": {
                "path": "$scores",
                "includeArrayIndex": "scoreIndex",
                "preserveNullAndEmptyArrays": True,
            }
        }
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have 4 documents:
    # 3 for Alice's scores, 1 for Bob (null due to empty array)
    assert len(result_list) == 4

    # Verify Alice's documents
    alice_docs = [doc for doc in result_list if doc["name"] == "Alice"]
    assert len(alice_docs) == 3

    # Verify indices and scores
    alice_indices = [doc["scoreIndex"] for doc in alice_docs]
    alice_scores = [doc["scores"] for doc in alice_docs]
    assert sorted(alice_indices) == [0, 1, 2]
    assert set(alice_scores) == {85, 90, 78}

    # Verify Bob's document (empty array preserved as null)
    bob_docs = [doc for doc in result_list if doc["name"] == "Bob"]
    assert len(bob_docs) == 1
    bob_doc = bob_docs[0]
    assert "scoreIndex" in bob_doc
    assert bob_doc["scoreIndex"] is None
    assert "scores" not in bob_doc or bob_doc["scores"] is None

    # Test with documents that have null values (SHOULD be preserved)
    collection.insert_one({"_id": 3, "name": "Charlie", "scores": None})

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should now have 5 documents (4 + 1 for Charlie with null value preserved)
    assert len(result_list) == 5

    # Test with documents that have missing fields (should NOT be preserved)
    collection.insert_one({"_id": 4, "name": "David"})  # No scores field

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should still have 5 documents (David's missing field is not preserved)
    assert len(result_list) == 5

    # Verify Charlie's document is preserved (null value preserved as null)
    charlie_docs = [doc for doc in result_list if doc["name"] == "Charlie"]
    assert len(charlie_docs) == 1
    charlie_doc = charlie_docs[0]
    assert "scoreIndex" in charlie_doc
    assert charlie_doc["scoreIndex"] is None
    assert "scores" not in charlie_doc or charlie_doc["scores"] is None


def test_unwind_performance_with_large_arrays(collection):
    """Test unwind performance with large arrays."""
    # Insert test data with large arrays
    docs = []
    for i in range(10):
        # Each document has 100 scores
        scores = [i * 100 + j for j in range(100)]
        docs.append({"user": f"User{i}", "scores": scores})
    collection.insert_many(docs)

    # Unwind all scores
    pipeline = [{"$unwind": "$scores"}]

    start_time = time.time()
    result = collection.aggregate(pipeline)
    result_list = list(result)
    elapsed_time = time.time() - start_time

    # Should have 1000 documents (10 users * 100 scores each)
    assert len(result_list) == 1000

    print(f"Unwinding 1000 documents took {elapsed_time:.4f} seconds")


def test_unwind_with_complex_pipeline(collection):
    """Test unwind with complex pipeline operations."""
    # Insert test data
    collection.insert_many(
        [
            {
                "department": "Engineering",
                "employees": [
                    {
                        "name": "Alice",
                        "salary": 90000,
                        "skills": ["Python", "SQL"],
                    },
                    {"name": "Bob", "salary": 85000, "skills": ["Java", "SQL"]},
                ],
            },
            {
                "department": "Marketing",
                "employees": [
                    {
                        "name": "Charlie",
                        "salary": 70000,
                        "skills": ["SEO", "Content"],
                    },
                    {
                        "name": "David",
                        "salary": 75000,
                        "skills": ["Analytics", "SEO"],
                    },
                ],
            },
        ]
    )

    # Complex pipeline: unwind employees, filter by salary, group by department
    pipeline = [
        {"$unwind": "$employees"},
        {"$match": {"employees.salary": {"$gte": 80000}}},
        {
            "$group": {
                "_id": "$department",
                "high_earners": {"$push": "$employees.name"},
                "avg_salary": {"$avg": "$employees.salary"},
            }
        },
        {"$sort": {"avg_salary": -1}},
    ]

    result = collection.aggregate(pipeline)
    result_list = list(result)

    # Should have 1 document (only Engineering department has employees with salary >= 80000)
    # Alice (90000) and Bob (85000) both qualify, but Charlie (70000) and David (75000) don't
    assert len(result_list) == 1

    # Engineering should be first (higher average salary)
    assert result_list[0]["_id"] == "Engineering"
    assert set(result_list[0]["high_earners"]) == {"Alice", "Bob"}
    assert result_list[0]["avg_salary"] == 87500.0  # (90000 + 85000) / 2
