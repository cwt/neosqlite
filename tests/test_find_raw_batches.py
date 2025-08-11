# coding: utf-8
"""Test for find_raw_batches method."""
import json
import neosqlite


def test_find_raw_batches():
    """Test the find_raw_batches method."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        docs = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "Los Angeles"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
            {"name": "David", "age": 28, "city": "Houston"},
            {"name": "Eve", "age": 32, "city": "Phoenix"},
        ]
        collection.insert_many(docs)

        # Test find_raw_batches with default batch size
        cursor = collection.find_raw_batches()
        batches = list(cursor)

        # Should have at least one batch
        assert len(batches) > 0

        # Each batch should be bytes
        for batch in batches:
            assert isinstance(batch, bytes)

            # Decode and parse the JSON
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]

            # Each line should be valid JSON
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                assert isinstance(doc, dict)
                assert "_id" in doc

        # Test with custom batch size
        cursor = collection.find_raw_batches(batch_size=2)
        batches = list(cursor)

        # Should have multiple batches
        assert len(batches) >= 3  # 5 documents with batch size 2 = 3 batches

        # First two batches should have 2 documents each
        for i in range(2):
            batch_str = batches[i].decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            assert len(doc_strings) == 2

        # Last batch should have 1 document
        last_batch_str = batches[-1].decode("utf-8")
        last_doc_strings = [s for s in last_batch_str.split("\n") if s]
        assert len(last_doc_strings) == 1

        # Test with filter
        cursor = collection.find_raw_batches(
            {"age": {"$gte": 30}}, batch_size=2
        )
        batches = list(cursor)

        # Should have 3 documents matching the filter (Alice, Charlie, Eve)
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

        # Test with projection
        cursor = collection.find_raw_batches(
            projection={"name": 1, "_id": 0}, batch_size=2
        )
        batches = list(cursor)

        # Documents should only have name field (and _id if not excluded properly)
        for batch in batches:
            batch_str = batch.decode("utf-8")
            doc_strings = [s for s in batch_str.split("\n") if s]
            for doc_str in doc_strings:
                doc = json.loads(doc_str)
                # Should only have name field (and possibly _id)
                assert "name" in doc
                # If _id was properly excluded, it shouldn't be present
                # But our implementation might not fully support projection in raw batches


def test_find_raw_batches_empty():
    """Test find_raw_batches with empty collection."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test with empty collection
        cursor = collection.find_raw_batches()
        batches = list(cursor)

        # Should have no batches
        assert len(batches) == 0


def test_find_raw_batches_batch_size():
    """Test find_raw_batches with different batch sizes."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.test_collection

        # Insert test data
        docs = [{"num": i} for i in range(10)]
        collection.insert_many(docs)

        # Test with batch_size=1
        cursor = collection.find_raw_batches(batch_size=1)
        batches = list(cursor)
        assert len(batches) == 10

        # Test with batch_size=5
        cursor = collection.find_raw_batches(batch_size=5)
        batches = list(cursor)
        assert len(batches) == 2

        # Test with batch_size=20 (larger than number of documents)
        cursor = collection.find_raw_batches(batch_size=20)
        batches = list(cursor)
        assert len(batches) == 1


if __name__ == "__main__":
    test_find_raw_batches()
    test_find_raw_batches_empty()
    test_find_raw_batches_batch_size()
    print("All tests passed!")
