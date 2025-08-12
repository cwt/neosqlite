# coding: utf-8
import time
import neosqlite


def test_nested_field_performance_improvement():
    """Test that nested field queries are now faster due to SQL implementation."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

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

        # Check that _build_simple_where_clause now handles this query
        where_result = collection._build_simple_where_clause(
            {"profile.age": 25}
        )
        assert where_result is not None
        where_clause, params = where_result
        assert "json_extract(data, '$.profile.age') = ?" in where_clause
        assert params == [25]

        print(
            "Nested field queries are now handled by SQL instead of Python fallback!"
        )


if __name__ == "__main__":
    test_nested_field_performance_improvement()
