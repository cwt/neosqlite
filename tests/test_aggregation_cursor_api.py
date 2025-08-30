import neosqlite


def test_aggregation_cursor_api():
    """Test that AggregationCursor implements the PyMongo API correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice", "age": 25, "city": "New York"},
                {"name": "Bob", "age": 30, "city": "San Francisco"},
                {"name": "Charlie", "age": 35, "city": "New York"},
            ]
        )

        # Test aggregation
        pipeline = [{"$match": {"age": {"$gte": 30}}}, {"$sort": {"name": 1}}]

        cursor = collection.aggregate(pipeline)

        # Test that cursor implements the iterator protocol
        assert hasattr(cursor, "__iter__")
        assert hasattr(cursor, "__next__")

        # Test that cursor has len method
        assert hasattr(cursor, "__len__")
        assert len(cursor) == 2

        # Test that cursor has getitem method
        assert hasattr(cursor, "__getitem__")
        first_doc = cursor[0]
        assert first_doc["name"] == "Bob"

        # Test iteration
        docs = list(cursor)
        assert len(docs) == 2
        assert docs[0]["name"] == "Bob"
        assert docs[1]["name"] == "Charlie"

        # Test that we can iterate multiple times
        docs2 = list(cursor)
        assert len(docs2) == 2
        assert docs2[0]["name"] == "Bob"
        assert docs2[1]["name"] == "Charlie"

        # Test sort method
        cursor2 = collection.aggregate(pipeline)
        cursor2.sort(key=lambda x: x["name"], reverse=True)
        docs3 = list(cursor2)
        assert len(docs3) == 2
        assert docs3[0]["name"] == "Charlie"
        assert docs3[1]["name"] == "Bob"

        # Test to_list method
        cursor3 = collection.aggregate(pipeline)
        docs4 = cursor3.to_list()
        assert len(docs4) == 2
        assert docs4[0]["name"] == "Bob"
        assert docs4[1]["name"] == "Charlie"


if __name__ == "__main__":
    test_aggregation_cursor_api()
    print("All tests passed!")
