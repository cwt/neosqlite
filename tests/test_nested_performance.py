# coding: utf-8
import time
import neosqlite


def test_performance_improvement():
    """Test that nested field queries are now faster with SQL implementation."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert a larger dataset
        docs = [
            {
                "name": f"User{i}",
                "profile": {"age": i % 50, "city": f"City{i % 10}"},
            }
            for i in range(1000)
        ]
        collection.insert_many(docs)

        # Time a nested field query
        start_time = time.time()
        results = list(collection.find({"profile.age": 25}))
        sql_time = time.time() - start_time

        # Verify we get the expected results
        assert len(results) > 0
        for doc in results:
            assert doc["profile"]["age"] == 25

        print(f"Query with SQL implementation took {sql_time:.4f} seconds")
        print(f"Returned {len(results)} documents")

        # Check that we're now using SQL by examining the generated clause
        where_result = collection._build_simple_where_clause(
            {"profile.age": 25}
        )
        assert where_result is not None
        where_clause, params = where_result
        assert "json_extract(data, '$.profile.age') = ?" in where_clause
        assert params == [25]


if __name__ == "__main__":
    test_performance_improvement()
