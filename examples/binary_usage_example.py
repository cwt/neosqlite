from neosqlite import Connection, Binary


def example_binary_usage():
    """Example showing how to use the Binary class."""
    print("=== Binary Usage Example ===")
    print()

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Create binary data using the Binary class
        binary_data = Binary(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
        large_binary = Binary(b"x" * 1000)  # 1KB of data

        # Store in document - this works now!
        result = collection.insert_one(
            {
                "name": "binary_example",
                "binary_field": binary_data,
                "large_data": large_binary,
                "metadata": {"inner_binary": Binary(b"nested binary data")},
            }
        )

        print(f"Inserted document with database ID: {result.inserted_id}")

        # Retrieve and use the binary data
        doc = collection.find_one({"name": "binary_example"})

        print(f"Document _id (ObjectId): {doc['_id']}")
        print(f"Retrieved binary type: {type(doc['binary_field'])}")
        print(f"Retrieved binary data: {doc['binary_field']}")
        print(f"Binary subtype: {doc['binary_field'].subtype}")
        print(f"Large data size: {len(doc['large_data'])} bytes")
        print(f"Nested binary: {doc['metadata']['inner_binary']}")

        # Query with binary data
        docs = list(collection.find({"binary_field": binary_data}))
        print(f"\nFound {len(docs)} documents matching binary query")

        # Convert to regular bytes if needed
        raw_bytes = bytes(doc["binary_field"])
        print(f"Converted to bytes: {raw_bytes}")


if __name__ == "__main__":
    example_binary_usage()
