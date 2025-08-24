from neosqlite import Connection, Binary


def test_binary_equality():
    """Test Binary equality in queries."""
    print("=== Testing Binary Equality ===")

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create and store binary data
        binary_data = Binary(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")

        result = collection.insert_one(
            {"name": "binary_test", "binary_field": binary_data}
        )

        print(f"Original binary: {binary_data}")
        print(f"Original type: {type(binary_data)}")

        # Retrieve and check equality
        doc = collection.find_one({"name": "binary_test"})
        retrieved_binary = doc["binary_field"]

        print(f"Retrieved binary: {retrieved_binary}")
        print(f"Retrieved type: {type(retrieved_binary)}")

        # Test equality
        print(f"Are they equal? {binary_data == retrieved_binary}")
        print(f"Bytes equal? {bytes(binary_data) == bytes(retrieved_binary)}")
        print(
            f"Subtypes equal? {binary_data.subtype == retrieved_binary.subtype}"
        )

        # Test query
        docs = list(collection.find({"binary_field": binary_data}))
        print(f"Query found {len(docs)} documents")

        if docs:
            queried_binary = docs[0]["binary_field"]
            print(f"Queried binary: {queried_binary}")
            print(
                f"Queried binary == original: {queried_binary == binary_data}"
            )


if __name__ == "__main__":
    test_binary_equality()
