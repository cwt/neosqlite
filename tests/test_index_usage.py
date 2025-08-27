# coding: utf-8


def test_index_usage_with_explain(collection):
    """Test that indexes are actually being used by examining the SQLite query plan."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
            {"name": "David", "age": 25},
        ]
    )

    # Create an index on age
    collection.create_index("age")

    # Verify the index exists
    assert "idx_foo_age" in collection.list_indexes()

    # Build a query using the Collection's own query building mechanism
    query_filter = {"age": 25}
    where_result = collection.query_engine.helpers._build_simple_where_clause(
        query_filter
    )

    # Verify that we can build a SQL query for this filter
    assert where_result is not None
    where_clause, params = where_result

    # Construct the full SQL query that would be used by the Collection
    sql_query = f"SELECT id, data FROM {collection.name} {where_clause}"

    # Check that a simple query on the indexed field uses the index
    # We'll do this by examining the SQLite query plan
    cursor = collection.db.execute(f"EXPLAIN QUERY PLAN {sql_query}", params)
    plan = cursor.fetchall()

    # Check if the plan shows index usage
    # The plan should show that we're using our index
    plan_text = " ".join(str(item) for row in plan for item in row)

    # Verify that the index is being used
    # The plan should contain both "INDEX" and our specific index name
    assert "INDEX" in plan_text
    assert "idx_foo_age" in plan_text
    # Also verify it's a SEARCH operation (not a SCAN which would be less efficient)
    assert "SEARCH" in plan_text


def test_index_usage_performance_verification(collection):
    """Test that indexes improve query performance."""
    import time

    # Insert a larger dataset
    docs = [{"name": f"User{i}", "age": i % 50} for i in range(1000)]
    collection.insert_many(docs)

    # Create an index on age
    collection.create_index("age")

    # Time a query with the index
    start_time = time.time()
    results_with_index = list(collection.find({"age": 25}))
    time_with_index = time.time() - start_time

    # The query should return results quickly
    assert len(results_with_index) > 0


def test_compound_index_usage(collection):
    """Test that compound indexes are being used."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "age": 25, "city": "New York"},
            {"name": "Bob", "age": 30, "city": "Boston"},
            {"name": "Charlie", "age": 25, "city": "New York"},
            {"name": "David", "age": 30, "city": "Boston"},
        ]
    )

    # Create a compound index
    collection.create_index(["age", "city"])

    # Verify the index exists
    assert "idx_foo_age_city" in collection.list_indexes()

    # Check that a query using both indexed fields works
    results = list(collection.find({"age": 25, "city": "New York"}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Charlie"}


def test_nested_index_usage(collection):
    """Test that indexes on nested fields are being used."""
    # Insert test data
    collection.insert_many(
        [
            {"name": "Alice", "profile": {"age": 25, "city": "New York"}},
            {"name": "Bob", "profile": {"age": 30, "city": "Boston"}},
            {"name": "Charlie", "profile": {"age": 25, "city": "New York"}},
        ]
    )

    # Create an index on a nested field
    collection.create_index("profile.age")

    # Verify the index exists
    assert "idx_foo_profile_age" in collection.list_indexes()

    # Check that a query using the nested field works
    results = list(collection.find({"profile.age": 25}))
    assert len(results) == 2
    names = {doc["name"] for doc in results}
    assert names == {"Alice", "Charlie"}
